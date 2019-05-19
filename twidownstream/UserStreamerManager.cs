using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Collections.Generic;
using System.Collections.Concurrent;
using CoreTweet;
using twitenlib;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using System.Runtime;
using System.Net.Sockets;

namespace twidownstream
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        ///<summary>ここにUserStreamerを格納しておく KeyはUserID</summary>
        readonly ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        private UserStreamerManager() { }
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

        public int Count { get { return Streamers.Count; } }


        ///<summary>再試行にも失敗したらtrue KeyはUserID</summary>
        readonly ConcurrentDictionary<long, bool> RevokeRetryUserID = new ConcurrentDictionary<long, bool>();
        ///<summary>Revoke直後→再試行マーク 2連続Revoked→Token削除</summary>
        void MarkRevoked(UserStreamer Streamer)
        {
            if (RevokeRetryUserID.TryGetValue(Streamer.Token.UserId, out bool b))
            {
                if (!b) { RevokeRetryUserID[Streamer.Token.UserId] = true; }
            }
            else { RevokeRetryUserID[Streamer.Token.UserId] = false; }    //次回もRevokeされてたら消えてもらう
        }
        ///<summary>ツイート取得等に成功したTokenをRevoke再試行対象から外す</summary>
        bool UnmarkRevoked(UserStreamer streamer) { return RevokeRetryUserID.TryRemove(streamer.Token.UserId, out _); }

        static readonly UdpClient WatchDogUdp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, (config.crawl.WatchDogPort ^ (Process.GetCurrentProcess().Id & 0x3FFF))));
        static readonly IPEndPoint WatchDogEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, config.crawl.WatchDogPort);
        static readonly int ThisPid = Process.GetCurrentProcess().Id;

        ///<summary>これを定期的に呼んで再接続やFriendの取得をやらせる</summary>
        public async Task<int> ConnectStreamers()
        {
            //アカウントが1個も割り当てられなくなってたら自殺する
            async Task ExistThisPid() { if (!await db.ExistThisPid().ConfigureAwait(false)) { Environment.Exit(1); } }

            await ExistThisPid().ConfigureAwait(false);
            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            var ConnectBlock = new ActionBlock<UserStreamer>(
            async (Streamer) =>
            {
                try
                {
                    var NeedConnect = Streamer.NeedConnect();
                    //初回とRevoke疑いのときだけVerifyCredentials()する
                    //プロフィールを取得したい
                    if (NeedConnect == UserStreamer.NeedConnectResult.Postponed) { return; }
                    else if (NeedConnect == UserStreamer.NeedConnectResult.First)
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
                                UnmarkRevoked(Streamer);
                                NeedConnect = UserStreamer.NeedConnectResult.JustNeeded;    //無理矢理接続処理に突っ込む #ウンコード
                                break;
                        }
                    }

                    //Streamに接続したりRESTだけにしたり
                    if (NeedConnect == UserStreamer.NeedConnectResult.StreamConnected)
                    {
                        if (Streamer.NeedStreamSpeed() == UserStreamer.NeedStreamResult.RestOnly) { Streamer.DisconnectStream(); return; }
                        else { Interlocked.Increment(ref ActiveStreamers); }
                    }
                    else
                    {
                        //TLの速度を測定して必要ならStreamに接続
                        switch (await Streamer.RecieveRestTimelineAuto().ConfigureAwait(false))
                        {
                            case UserStreamer.TokenStatus.Locked:
                                Streamer.PostponeConnect(); break;
                            case UserStreamer.TokenStatus.Revoked:
                                MarkRevoked(Streamer); break;
                            default:
                                UserStreamer.NeedStreamResult NeedStream = Streamer.NeedStreamSpeed();
                                if (NeedStream == UserStreamer.NeedStreamResult.Stream) { Streamer.RecieveStream(); Interlocked.Increment(ref ActiveStreamers); }
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
                }
                catch (Exception e) { Console.WriteLine("ConnectBlock Faulted: {0}", e); }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                BoundedCapacity = config.crawl.ReconnectThreads << 4,    //これでもawaitする
                SingleProducerConstrained = true,
            });

            //SetMaxConnections(0);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            Counter.PrintReset();
            foreach (var s in Streamers)
            {
                if (!await ConnectBlock.SendAsync(s.Value).ConfigureAwait(false)) { break; }    //そんなバナナ
                SetMaxConnections(ActiveStreamers);
                while (true)
                {
                    if (sw.ElapsedMilliseconds > 60000)
                    {
                        sw.Restart();
                        Counter.PrintReset();
                        await WatchDogUdp.SendAsync(BitConverter.GetBytes(ThisPid), sizeof(int), WatchDogEndPoint).ConfigureAwait(false);
                        await ExistThisPid().ConfigureAwait(false);
                    }
                    //ツイートが詰まってたら休む                    
                    if (UserStreamerStatic.NeedConnectPostpone())
                    {
                        await ExistThisPid().ConfigureAwait(false);
                        await Task.Delay(1000).ConfigureAwait(false);
                    }
                    else { break; }
                } 
            }
            ConnectBlock.Complete();
            await ConnectBlock.Completion.ConfigureAwait(false);
            await WatchDogUdp.SendAsync(BitConverter.GetBytes(ThisPid), sizeof(int), WatchDogEndPoint).ConfigureAwait(false);
            return ActiveStreamers;
        }

        ///<summary>crawlprocessの値を更新したりRevokeされたTokenを消したりする</summary>
        public async Task UpdateStreamers()
        {
            //先にRevokeされたTokenを削除する
            foreach(var r in RevokeRetryUserID.Where(r => r.Value).OrderBy(r => r.Key).ToArray())
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
            //DBにいろいろ保存する
            await db.StoreUserStreamerSetting(Streamers.Select(s => s.Value.Setting)).ConfigureAwait(false);
        }

        private void SetMaxConnections(int basecount, bool Force = false)
        {
            int max = basecount
                + (config.crawl.MediaDownloadThreads << 1)
                + (config.crawl.ReconnectThreads << 1)
                + config.crawl.MaxDBConnections
                + config.crawl.DefaultConnectionThreads;
            if (Force || ServicePointManager.DefaultConnectionLimit < max)
            {
                ServicePointManager.DefaultConnectionLimit = max;
            }
        }
    }
}
