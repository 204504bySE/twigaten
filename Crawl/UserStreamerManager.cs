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

        private UserStreamerManager()
        {
            ConnectBlock = new ActionBlock<UserStreamer>(
                async (Streamer) =>
                {
                    try
                    {
                        var NeedConnect = Streamer.NeedConnect();
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
                                    MarkRevoked(Streamer.Token.UserId); return;
                                case UserStreamer.TokenStatus.Failure:
                                    return;
                                case UserStreamer.TokenStatus.Success:
                                    UnmarkRevoked(Streamer.Token.UserId);
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
                                case UserStreamer.TokenStatus.Locked:
                                    Streamer.PostponeConnect(); break;
                                case UserStreamer.TokenStatus.Revoked:
                                    if (RevokeCheckSuccess) { Streamer.PostponeConnect(); } //VerifyCredentialsしか通らない謎アカウントがたくさんある
                                    else { MarkRevoked(Streamer.Token.UserId); }
                                    break;
                                default:
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
                    MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                    BoundedCapacity = config.crawl.ReconnectThreads + 1,    //これでもawaitする
                    SingleProducerConstrained = true,
                });
        }
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

        ///<summary>再試行にも失敗したらtrue KeyはUserID</summary>
        readonly ConcurrentDictionary<long, bool> RevokeRetryUserID = new ConcurrentDictionary<long, bool>();
        ///<summary>Revoke直後→再試行マーク 2連続Revoked→Token削除</summary>
        void MarkRevoked(long UserId)
        {
            if (RevokeRetryUserID.TryGetValue(UserId, out bool b))
            {
                //再試行にも失敗した→削除対象にする
                if (!b) { RevokeRetryUserID[UserId] = true; }
            }
            //初めてRevokedにされた→再試行する
            else { RevokeRetryUserID[UserId] = false; }
        }
        ///<summary>ツイート取得等に成功したTokenをRevoke再試行対象から外す</summary>
        bool UnmarkRevoked(long UserId) { return RevokeRetryUserID.TryRemove(UserId, out _); }
        ///<summary>RevokeされたTokenを全部StreamersとDBから消す</summary>
        async Task RemoveRevokedTokens()
        {
            foreach (var r in RevokeRetryUserID.Where(r => r.Value).OrderBy(r => r.Key).ToArray())
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
                    else { RevokeRetryUserID[r.Key] = true; }
                }
            }
        }

        ///<summary>CrawlParentに定期的になんでもいいからパケットを投げて生存報告をする</summary>
        static readonly UdpClient WatchDogUdp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, (config.crawl.WatchDogPort ^ (Process.GetCurrentProcess().Id & 0x3FFF))));
        static readonly IPEndPoint WatchDogEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, config.crawl.WatchDogPort);
        static readonly int ThisPid = Process.GetCurrentProcess().Id;

        /// <summary>
        /// UserStreamerの再接続やRESTによるツイート取得などを行うやつ
        /// </summary>
        readonly ActionBlock<UserStreamer> ConnectBlock;

        /// <summary>
        /// ConnectBlockが処理したUserStreamerの一覧
        /// </summary>
        readonly ConcurrentBag<UserStreamer> ConnectBlockProceeeded = new ConcurrentBag<UserStreamer>();

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
                }
                catch (TaskCanceledException) { }
            }

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
