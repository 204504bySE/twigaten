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
            await ret.AddAll();
            return ret;
        }

        public async Task AddAll()
        {
            Tokens[] token = await db.Selecttoken(DBHandler.SelectTokenMode.CurrentProcess);
            Tokens[] tokenRest = await db.Selecttoken(DBHandler.SelectTokenMode.RestInStreamer);
            SetMaxConnections(false, token.Length);
            //Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, token.Length);
            foreach (Tokens t in tokenRest)
            {            
                if (Add(t) && Streamers.TryGetValue(t.UserId, out UserStreamer s)) { s.NeedRestMyTweet = true; }
            }
            foreach (Tokens t in token)
            {
                Add(t);
            }
            SetMaxConnections();
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

        ActionBlock<UserStreamer> ConnectBlock;
        void InitConnectBlock()
        {
            ConnectBlock = new ActionBlock<UserStreamer>(
            async (Streamer) =>
            {
                UserStreamer.NeedConnectResult NeedConnect = Streamer.NeedConnect();
                //初回とRevoke疑いのときだけVerifyCredentials()する
                //プロフィールを取得したい
                switch (NeedConnect == UserStreamer.NeedConnectResult.Verify
                    ? await Streamer.VerifyCredentials() : UserStreamer.TokenStatus.Success)
                {
                    case UserStreamer.TokenStatus.Success:
                        if (NeedConnect == UserStreamer.NeedConnectResult.Postponed) { }    //何もしない
                        else if (NeedConnect == UserStreamer.NeedConnectResult.StreamConnected)
                        {
                            if(Streamer.NeedStreamSpeed() == UserStreamer.NeedStreamResult.RestOnly) { Streamer.DisconnectStream(); }
                        }
                        else
                        {
                            //TLはREST→Streamの順にしてTweetTimeのスレッドセーフ化を不要にする
                            switch (await Streamer.RecieveRestTimelineAuto())
                            {
                                case UserStreamer.TokenStatus.Locked:
                                    Streamer.PostponeConnect(); break;
                                case UserStreamer.TokenStatus.Revoked:
                                    await RevokeRetry(Streamer); break;
                                default:
                                    UserStreamer.NeedStreamResult NeedStream = Streamer.NeedStreamSpeed();
                                    if (NeedStream == UserStreamer.NeedStreamResult.Stream) { Streamer.RecieveStream(); }
                                    //DBが求めていればToken読み込み直後だけ自分のツイートも取得(初回サインイン狙い
                                    if (Streamer.NeedRestMyTweet)
                                    {
                                        Streamer.NeedRestMyTweet = false;
                                        await Streamer.RestMyTweet();
                                        //User streamに繋がない場合はこっちでフォローを取得する必要がある
                                        if (NeedStream != UserStreamer.NeedStreamResult.Stream) { await Streamer.RestFriend(); }
                                        await Streamer.RestBlock();
                                        await db.StoreRestDonetoken(Streamer.Token.UserId);
                                    }
                                    break;
                            }
                        }
                        break;
                    case UserStreamer.TokenStatus.Locked:
                        Streamer.PostponeConnect();
                        break;
                    case UserStreamer.TokenStatus.Revoked:
                        await RevokeRetry(Streamer); break;
                }
                Streamer.ConnectWaiting = false;
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                BoundedCapacity = config.crawl.ReconnectThreads << 1,    //これでもawaitする
                SingleProducerConstrained = true,
            });

            async Task RevokeRetry(UserStreamer Streamer)
            {
                if (RevokeRetryUserID.ContainsKey(Streamer.Token.UserId))
                {
                    await db.DeleteToken(Streamer.Token.UserId);
                    RevokeRetryUserID.TryRemove(Streamer.Token.UserId, out byte z);
                    RemoveStreamer(Streamer);
                }
                else { RevokeRetryUserID.TryAdd(Streamer.Token.UserId, 0); }    //次回もRevokeされてたら消えてもらう
            }
        }

        //これを定期的に呼んで再接続やFriendの取得をやらせる
        //StreamerLockerのロック解除もここでやる
        public async ValueTask<int> ConnectStreamers()
        {
            if (!await db.ExistThisPid()) { Environment.Exit(1); }

            int ActiveStreamers = 0;  //再接続が不要だったやつの数
            StreamerLocker.Unlock();
            InitConnectBlock();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            CountUnlock();
            foreach (KeyValuePair<long, UserStreamer> s in Streamers.ToArray())  //ここでスナップショットを作る
            {
                if (!s.Value.ConnectWaiting)
                {
                    s.Value.ConnectWaiting = true;
                    await ConnectBlock.SendAsync(s.Value);
                }
                if (s.Value.NeedConnect() == UserStreamer.NeedConnectResult.StreamConnected) { ActiveStreamers++; }

                do
                {
                    if (sw.ElapsedMilliseconds > 60000)
                    {   //ここでGCする #ウンコード
                        sw.Restart();
                        CountUnlock();
                        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                        GC.Collect();
                    }
                    //ツイートが詰まってたら休む
                    if (UserStreamerStatic.NeedConnectPostpone()) { await Task.Delay(1000); }
                } while (UserStreamerStatic.NeedConnectPostpone());
            }
            ConnectBlock.Complete();
            await ConnectBlock.Completion;
            return ActiveStreamers;

            //カウンターを表示したりいろいろ
            void CountUnlock()
            {
                Counter.PrintReset();
                UserStreamerStatic.ShowCount();
                StreamerLocker.Unlock();
            }
        }

        void RemoveStreamer(UserStreamer Streamer)
        //Revokeされた後の処理
        {
            Streamer.Dispose();
            Streamers.TryRemove(Streamer.Token.UserId, out UserStreamer z);  //つまり死んだStreamerは除外される
            SetMaxConnections(true);
            Console.WriteLine("{0} {1}: Streamer removed", DateTime.Now, Streamer.Token.UserId);
        }

        private void SetMaxConnections(bool Force = false, int basecount = 0)
        {
            int MaxConnections = Math.Max(basecount, Streamers.Count) + config.crawl.DefaultConnectionThreads;
            if (Force || ServicePointManager.DefaultConnectionLimit < MaxConnections)
            {
                ServicePointManager.DefaultConnectionLimit = MaxConnections;
            }
        }
    }
}
