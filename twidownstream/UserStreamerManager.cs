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

namespace twidownstream
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        readonly ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();   //longはUserID
        readonly ConcurrentDictionary<long, byte> RevokeRetryUserID = new ConcurrentDictionary<long, byte>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        private UserStreamerManager() { }
        public static async ValueTask<UserStreamerManager> Create()
        {
            UserStreamerManager ret = new UserStreamerManager();
            await ret.AddAll().ConfigureAwait(false);
            return ret;
        }

        public async Task AddAll()
        {
            Tokens[] token = await db.Selecttoken(DBHandler.SelectTokenMode.CurrentProcess).ConfigureAwait(false);
            Tokens[] tokenRest = await db.Selecttoken(DBHandler.SelectTokenMode.RestInStreamer).ConfigureAwait(false);
            //Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, token.Length);
            foreach (Tokens t in tokenRest)
            {            
                if (Add(t) && Streamers.TryGetValue(t.UserId, out UserStreamer s)) { s.NeedRestMyTweet = true; }
            }
            foreach (Tokens t in token)
            {
                Add(t);
            }
        }

        bool Add(Tokens t)
        {
            if (t == null) { return false; }
            if (Streamers.ContainsKey(t.UserId))
            {
                //Console.WriteLine("{0} {1}: Already running.", DateTime.Now, t.UserId);
                return false;
            }
            else
            {
                //Console.WriteLine("{0} {1}: Assigned.", DateTime.Now, t.UserId);
                UserStreamer s = new UserStreamer(t);
                return Streamers.TryAdd(t.UserId, s);
            }
        }

        public int Count { get { return Streamers.Count; } }

        async Task RevokeRetry(UserStreamer Streamer)
        {
            if (RevokeRetryUserID.ContainsKey(Streamer.Token.UserId))
            {
                await db.DeleteToken(Streamer.Token.UserId).ConfigureAwait(false);
                RevokeRetryUserID.TryRemove(Streamer.Token.UserId, out byte z);
                RemoveStreamer(Streamer);
            }
            else { RevokeRetryUserID.TryAdd(Streamer.Token.UserId, 0); }    //次回もRevokeされてたら消えてもらう
        }
        
        ///<summary>これを定期的に呼んで再接続やFriendの取得をやらせる</summary>
        public async ValueTask<int> ConnectStreamers()
        {
            if (!await db.ExistThisPid().ConfigureAwait(false)) { Environment.Exit(1); }

            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            ActionBlock<UserStreamer> ConnectBlock = new ActionBlock<UserStreamer>(
            async (Streamer) =>
            {
                try
                {
                    UserStreamer.NeedConnectResult NeedConnect = Streamer.NeedConnect();
                    //初回とRevoke疑いのときだけVerifyCredentials()する
                    //プロフィールを取得したい
                    if (NeedConnect == UserStreamer.NeedConnectResult.Postponed) { }    //何もしない
                    else if (NeedConnect == UserStreamer.NeedConnectResult.First)
                    {
                        switch (await Streamer.VerifyCredentials().ConfigureAwait(false))
                        {
                            case UserStreamer.TokenStatus.Locked:
                                Streamer.PostponeConnect(); return;
                            case UserStreamer.TokenStatus.Revoked:
                                await RevokeRetry(Streamer).ConfigureAwait(false); return;
                            case UserStreamer.TokenStatus.Failure:
                                return;
                            case UserStreamer.TokenStatus.Success:
                                RevokeRetryUserID.TryRemove(Streamer.Token.UserId, out byte gomi);
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
                                await RevokeRetry(Streamer).ConfigureAwait(false); break;
                            default:
                                UserStreamer.NeedStreamResult NeedStream = Streamer.NeedStreamSpeed();
                                if (NeedStream == UserStreamer.NeedStreamResult.Stream) { Streamer.RecieveStream(); Interlocked.Increment(ref ActiveStreamers); }
                                //DBが求めていればToken読み込み直後だけ自分のツイートも取得(初回サインイン狙い
                                if (Streamer.NeedRestMyTweet)
                                {
                                    Streamer.NeedRestMyTweet = false;
                                    await Streamer.RestMyTweet().ConfigureAwait(false);
                                    //User streamに繋がない場合はこっちでフォローを取得する必要がある
                                    if (NeedStream != UserStreamer.NeedStreamResult.Stream) { await Streamer.RestFriend().ConfigureAwait(false); }
                                    await Streamer.RestBlock().ConfigureAwait(false);
                                    await db.StoreRestDonetoken(Streamer.Token.UserId).ConfigureAwait(false);
                                }
                                break;
                        }
                    }
                }
                finally { Streamer.ConnectWaiting = false; }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                BoundedCapacity = config.crawl.ReconnectThreads << 1,    //これでもawaitする
                SingleProducerConstrained = true,
            });

            SetMaxConnections(0);

            Stopwatch sw = new Stopwatch();
            sw.Start();
            ShowCount();
            foreach (KeyValuePair<long, UserStreamer> s in Streamers.ToArray())  //ここでスナップショットを作る
            {
                if (!s.Value.ConnectWaiting)
                {
                    s.Value.ConnectWaiting = true;
                    await ConnectBlock.SendAsync(s.Value).ConfigureAwait(false);
                }
                do
                {
                    SetMaxConnections(ActiveStreamers);
                    if (sw.ElapsedMilliseconds > 60000)
                    {   //ここでGCする #ウンコード
                        sw.Restart();
                        ShowCount();
                        //GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                        //GC.Collect();
                    }
                    //ツイートが詰まってたら休む
                    if (UserStreamerStatic.NeedConnectPostpone()) { await Task.Delay(1000).ConfigureAwait(false); }
                } while (UserStreamerStatic.NeedConnectPostpone());
            }
            ConnectBlock.Complete();
            await ConnectBlock.Completion.ConfigureAwait(false);
            return ActiveStreamers;

            //カウンターを表示したりいろいろ
            void ShowCount()
            {
                Counter.PrintReset();
                UserStreamerStatic.ShowCount();
            }
        }

        ///<summary>Revokeされた後の処理</summary>
        void RemoveStreamer(UserStreamer Streamer)
        {
            Streamer.Dispose();
            Streamers.TryRemove(Streamer.Token.UserId, out UserStreamer z);  //つまり死んだStreamerは除外される
            Console.WriteLine("{0}: Streamer removed", Streamer.Token.UserId);
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
            ThreadPool.GetMinThreads(out int w, out int c);
            if (Force || w < max) { ThreadPool.SetMinThreads(max, c); }
        }
    }
}
