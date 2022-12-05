using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using CoreTweet;
using Twigaten.Lib;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using System.Runtime;
using System.Net.Sockets;

namespace Twigaten.Crawl
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        ///<summary>ここにUserStreamerを格納しておく KeyはUserID</summary>
        readonly ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public static async Task<UserStreamerManager> Create()
        {
            var ret = new UserStreamerManager();
            await ret.AddAll().ConfigureAwait(false);
            return ret;
        }

        ///<summary>DBからtokenを適切に読み込んでUserStreamerを追加する</summary>
        public async Task AddAll()
        {
            var setting = await db.SelectUserStreamerSetting(DBHandler.SelectTokenMode.CurrentProcess).ConfigureAwait(false);
            foreach (var t in setting)
            {
                Add(t);
            }
        }

        ///<summary>UserStreamerを生成して追加する</summary>
        bool Add(UserStreamer.UserStreamerSetting setting)
        {
            if (setting.Token == null) { return false; }
            if (Streamers.ContainsKey(setting.Token.UserId))
            {
                //Console.WriteLine("{0} {1}: Already running.", DateTime.Now, t.UserId);
                return false;
            }
            else
            {
                //Console.WriteLine("{0} {1}: Assigned.", DateTime.Now, t.UserId);
                UserStreamer s = new UserStreamer(setting);
                return Streamers.TryAdd(setting.Token.UserId, s);
            }
        }

        ///<summary>再試行に失敗した回数を入れる KeyはUserID</summary>
        readonly ConcurrentDictionary<long, int> RevokeRetryUserID = new ConcurrentDictionary<long, int>();

        ///<summary>Revokeの回数を数えると同時に次回のツイート取得の延期を行う</summary>
        void MarkRevoked(UserStreamer Streamer)
        {
            var UserId = Streamer.Token.UserId;
            if (!RevokeRetryUserID.TryGetValue(UserId, out var status)) { status = 0; }
            switch (status)
            {
                //再試行毎に待ち時間を適当に延ばす
                case 0:
                    //不規則なUnauthorized対策として再試行まで時間をおく
                    //15分待てば大丈夫っぽいけどダメなときはダメ
                    RevokeRetryUserID[UserId] = 1;
                    Streamer.PostponeConnect(1200);
                    break;
                case 1:
                    RevokeRetryUserID[UserId] = 2;
                    Streamer.PostponeConnect();
                    break;
                case 2:
                    //つまり3(以上)になったらTokenを消す
                    RevokeRetryUserID[UserId] = 3;
                    break;
                default:
                    //そんなバナナ
                    UnmarkRevoked(Streamer);
                    break;
            }
        }

        ///<summary>ツイート取得等に成功したTokenをRevoke再試行対象から外す</summary>
        bool UnmarkRevoked(UserStreamer Streamer) { return RevokeRetryUserID.TryRemove(Streamer.Token.UserId, out _); }
        ///<summary>RevokeされたTokenを全部StreamersとDBから消す</summary>
        async Task RemoveRevokedTokens()
        {
            foreach (var r in RevokeRetryUserID.Where(r => 3 <= r.Value).OrderBy(r => r.Key).ToArray())
            {
                if (await db.DeleteToken(r.Key).ConfigureAwait(false) >= 0
                    && RevokeRetryUserID.TryRemove(r.Key, out _))
                {
                    if (Streamers.TryRemove(r.Key, out var streamer))
                    {
                        streamer.Dispose();
                        Console.WriteLine("{0}: Streamer removed", streamer.Token.UserId);
                    }
                    //Streamersからの削除に失敗したらちゃんと再試行する
                    else { RevokeRetryUserID[r.Key] = 3; }
                }
            }
        }

        ///<summary>CrawlParentに定期的になんでもいいからパケットを投げて生存報告をする</summary>
        static readonly UdpClient WatchDogUdp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, (config.crawl.WatchDogPort ^ (Process.GetCurrentProcess().Id & 0x3FFF))));
        static readonly IPEndPoint WatchDogEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, config.crawl.WatchDogPort);
        static readonly int ThisPid = Process.GetCurrentProcess().Id;

        /// <summary>
        /// ConnectBlockが処理したUserStreamerの一覧
        /// </summary>
        readonly ConcurrentBag<UserStreamer> ConnectBlockProceeeded = new ConcurrentBag<UserStreamer>();

        /// <summary>
        /// ConnectBlockの並列数 所要時間に応じて適応的に変える
        /// </summary>
        int ConnectBlockConcurrency = config.crawl.MaxReconnectThreads;

        /// <summary>
        /// これを定期的に呼んで再接続やFriendの取得をやらせる
        /// 1分経過しても終わらないときは打ち切る
        /// 一度も取得していないアカウント→取得待ちが多い順 に処理する
        /// </summary>
        public async Task ConnectStreamers()
        {
            //アカウントが1個も割り当てられなくなってたら自殺する
            if (!await db.ExistThisPid().ConfigureAwait(false)) { Environment.Exit(1); }

            var LastCrawlTime = new Dictionary<long, long>();
            while (ConnectBlockProceeeded.TryTake(out var proceeded))
            {
                //ツイートの取得時刻を保存する(たぶん)
                //ここで(前回のConnectStreamers()で)REST取得したやつとUser streamに接続済みのやつだけ処理する(もうないけど)
                if (proceeded.LastReceivedTweetId != 0 || proceeded.NeedConnect() == UserStreamer.NeedConnectResult.StreamConnected)
                {
                    LastCrawlTime[proceeded.Token.UserId] = proceeded.LastMessageTime.ToUnixTimeSeconds();
                }
            }
            var StoreLastCrawlTimeTask = db.StoreCrawlInfo_Timeline(LastCrawlTime);
            Counter.PrintReset();

            //TLがある程度進んでいるアカウントと一定時間取得してないアカウントだけ接続する
            var now = DateTimeOffset.UtcNow;
            var StreamersSelected = Streamers.Select(s => s.Value)
                .Where(s => s.EstimatedTweetToReceive >= config.crawl.StreamSpeedTweets
                    || (now - s.LastMessageTime).TotalSeconds > config.crawl.MaxRestInterval)
                .OrderByDescending(s => s.EstimatedTweetToReceive)
                .ThenBy(s => s.LastMessageTime)
                .ToArray();

            // UserStreamerの再接続やRESTによるツイート取得などを行うやつ
            var ConnectBlock = new ActionBlock<UserStreamer>(async (Streamer) =>
            {
                try
                {
                    var NeedConnect = Streamer.NeedConnect();
                    //VerifyCredentialsを実行して成功したらtrue
                    bool RevokeCheckSuccess = false;
                    //初回とRevoke疑いのときだけVerifyCredentials()する
                    //プロフィールを取得したい
                    if (NeedConnect == UserStreamer.NeedConnectResult.Postponed) { return; }
                    else if (NeedConnect == UserStreamer.NeedConnectResult.First || RevokeRetryUserID.ContainsKey(Streamer.Token.UserId))
                    {
                        switch (await Streamer.VerifyCredentials().ConfigureAwait(false))
                        {
                            case UserStreamer.TokenStatus.Locked:
                                Streamer.PostponeConnect(); return;
                            case UserStreamer.TokenStatus.Revoked:
                                MarkRevoked(Streamer); return;
                            case UserStreamer.TokenStatus.Failure:
                                return;
                            case UserStreamer.TokenStatus.Success:
                                RevokeCheckSuccess = true;
                                break;
                        }
                    }

                    //Streamに接続したりRESTだけにしたり
                    if (NeedConnect == UserStreamer.NeedConnectResult.StreamConnected)
                    {
                        if (Streamer.NeedStreamSpeed() == UserStreamer.NeedStreamResult.RestOnly) { Streamer.DisconnectStream(); return; }
                        else { Counter.ActiveStreamers.Increment(); }
                    }
                    else
                    {
                        //TLの速度を測定して必要ならStreamに接続
                        Counter.RestedStreamers.Increment();
                        switch (await Streamer.RestTimeline().ConfigureAwait(false))
                        {
                            case UserStreamer.TokenStatus.Revoked:
                                //長期間ログインされていないアカウントはVerifyCredentialsしか通らなくなるっぽい
                                //またVerifyCredentialsに失敗するまで放っとく
                                if (RevokeCheckSuccess) { Streamer.PostponeConnect(); }
                                //普通のRevoke疑い
                                else { MarkRevoked(Streamer); }
                                break;
                            case UserStreamer.TokenStatus.Locked:
                                if (RevokeCheckSuccess) { UnmarkRevoked(Streamer); }
                                Streamer.PostponeConnect();
                                break;
                            default:
                                if (RevokeCheckSuccess) { UnmarkRevoked(Streamer); }
                                UserStreamer.NeedStreamResult NeedStream = Streamer.NeedStreamSpeed();
                                if (NeedStream == UserStreamer.NeedStreamResult.Stream) { Streamer.RecieveStream(); Counter.ActiveStreamers.Increment(); }
                                //DBが求めていればToken読み込み直後だけ自分のツイートも取得(初回サインイン狙い
                                if (Streamer.NeedRestMyTweet)
                                {
                                    await Streamer.RestMyTweet().ConfigureAwait(false);
                                    //User streamに繋がない場合はこっちでフォローを取得する必要がある
                                    if (NeedStream != UserStreamer.NeedStreamResult.Stream) { await Streamer.RestFriend().ConfigureAwait(false); }
                                    await Streamer.RestBlock().ConfigureAwait(false);
                                    Streamer.NeedRestMyTweet = false;
                                }
                                break;
                        }
                    }
                    ConnectBlockProceeeded.Add(Streamer);
                }
                catch (Exception e) { Console.WriteLine("ConnectBlock Faulted: {0}", e); }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = ConnectBlockConcurrency,
                BoundedCapacity = ConnectBlockConcurrency + 1,    //これでもawaitする
                SingleProducerConstrained = true,
            });

            var connectStopWatch = Stopwatch.StartNew();
            bool connectTimedout = false;
            using (var cancel = new CancellationTokenSource(60000))
            {
                try
                {
                    foreach (var s in StreamersSelected)
                    {
                        if (!await ConnectBlock.SendAsync(s, cancel.Token).ConfigureAwait(false)) { break; }    //そんなバナナ

                        //ツイートが詰まってたら休む 
                        while (UserStreamerStatic.NeedConnectPostpone() && !cancel.Token.IsCancellationRequested)
                        {
                            await Task.Delay(1000, cancel.Token).ConfigureAwait(false);
                        }
                    }
                    ConnectBlock.Complete();
                    await ConnectBlock.Completion.ConfigureAwait(false);
                }
                catch (TaskCanceledException) 
                {
                    ConnectBlock.Complete();
                    connectTimedout = true; 
                }
            }

            //ConnectBlockの同時接続数を調整する
            connectStopWatch.Stop();
            int oldConnectBlockConcurrency = ConnectBlockConcurrency;
            if (connectTimedout) { ConnectBlockConcurrency = config.crawl.MaxReconnectThreads; }
            else if (connectStopWatch.ElapsedMilliseconds < Math.Max(
                60000 - 2 * (60000 - config.crawl.TargetCrawlDuration),
                config.crawl.TargetCrawlDuration / 2
                )) { ConnectBlockConcurrency = Math.Max(1, ConnectBlockConcurrency - 1); }
            else if (config.crawl.TargetCrawlDuration < connectStopWatch.ElapsedMilliseconds)
            {
                ConnectBlockConcurrency = Math.Clamp((int)Math.Ceiling(ConnectBlockConcurrency * (connectStopWatch.ElapsedMilliseconds / (double)config.crawl.TargetCrawlDuration)), 1, config.crawl.MaxReconnectThreads);
            }
            if (oldConnectBlockConcurrency != ConnectBlockConcurrency) { Console.WriteLine("ConnectBlockConcurrency: {0} -> {1}", oldConnectBlockConcurrency, ConnectBlockConcurrency); }

            await StoreLastCrawlTimeTask.ConfigureAwait(false);
            //Revoke後再試行にも失敗したTokenはここで消す
            await RemoveRevokedTokens().ConfigureAwait(false);

            //接続が全然進まないときは殺してもらう
            if (0 < Counter.ActiveStreamers.Get() + Counter.RestedStreamers.Get()) { await WatchDogUdp.SendAsync(BitConverter.GetBytes(ThisPid), sizeof(int), WatchDogEndPoint).ConfigureAwait(false); }

            UserStreamerStatic.ShowCount();
        }

        ///<summary>最後に取得したツイートのIDなどをDBに保存する</summary>
        public async Task StoreCrawlStatus() { await db.StoreUserStreamerSetting(Streamers.Select(s => s.Value.Setting)).ConfigureAwait(false); }
    }
}
