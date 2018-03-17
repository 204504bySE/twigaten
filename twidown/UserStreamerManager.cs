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

namespace twidown
{
    class UserStreamerManager
    //UserStreamerの追加, 保持, 削除をこれで行う
    {
        readonly ConcurrentDictionary<long, UserStreamer> Streamers = new ConcurrentDictionary<long, UserStreamer>();   //longはUserID
        readonly ConcurrentDictionary<long, byte> RevokeRetryUserID = new ConcurrentDictionary<long, byte>();

        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public UserStreamerManager()
        {
            InitConnectBlock();
            AddAll();
        }

        public void AddAll()
        {
            Tokens[] token = db.Selecttoken(DBHandler.SelectTokenMode.StreamerAll);
            Tokens[] tokenRest = db.Selecttoken(DBHandler.SelectTokenMode.RestinStreamer);
            SetMaxConnections(false, token.Length);
            Console.WriteLine("{0} App: {1} tokens loaded.", DateTime.Now, token.Length);
            foreach(Tokens t in tokenRest)
            {
                if (Add(t) && Streamers.TryGetValue(t.UserId, out UserStreamer s))
                {
                    s.NeedRestMyTweet = true;
                }
            }
            foreach (Tokens t in token)
            {
                Add(t);
            }
            SetMaxConnections(true);
        }

        bool Add(Tokens t)
        {
            if (t == null) { return false; }
            if (Streamers.ContainsKey(t.UserId))
            {
                //Console.WriteLine("{0} {1}: Already running.", DateTime.Now, t.UserId);
                return true;
            }
            else
            {
                //Console.WriteLine("{0} {1}: Assigned.", DateTime.Now, t.UserId);
                UserStreamer s = new UserStreamer(t);
                return Streamers.TryAdd(t.UserId, s);
            }
        }

        public int Count { get { return Streamers.Count; } }

        ActionBlock<(long, UserStreamer)> ConnectBlock;
        void InitConnectBlock()
        {
            ConnectBlock = new ActionBlock<(long Id, UserStreamer Streamer)>(
            (s) =>
            {
                //必要なときだけVerifyCredentials()する
                UserStreamer.TokenStatus Result = s.Streamer.RecieveRestTimeline();
                Counter.RestAccounts.Increment();
                if (Result != UserStreamer.TokenStatus.Canceled) { Counter.RestAttempts.Increment(); }
                switch (Result)
                {
                    case UserStreamer.TokenStatus.Success:
                        if (s.Streamer.NeedRestMyTweet)
                        {
                            s.Streamer.NeedRestMyTweet = false;
                            s.Streamer.RestMyTweet();
                            db.StoreRestDonetoken(s.Id);
                        }
                        else { db.StoreRestNeedtoken(s.Id); }
                        Counter.RestSuccess.Increment();
                        break;
                    case UserStreamer.TokenStatus.Locked:
                        s.Streamer.PostponeRetry();
                        break;
                    case UserStreamer.TokenStatus.Revoked:
                        if (RevokeRetryUserID.ContainsKey(s.Id))
                        {
                            db.DeleteToken(s.Id);
                            RevokeRetryUserID.TryRemove(s.Id, out byte z);
                            RemoveStreamer(s.Streamer);
                        }
                        else { s.Streamer.PostponeRetry(60); RevokeRetryUserID.TryAdd(s.Id, 0); }   //延期しないと一瞬で死ぬ
                        break;
                }
                s.Streamer.ConnectWaiting = false;
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.ReconnectThreads,
                SingleProducerConstrained = true
            });
        }

        //これを定期的に呼んで再接続やFriendの取得をやらせる
        //StreamerLockerのロック解除もここでやる
        public int ConnectStreamers()
        {
            if (!db.ExistThisPid()) { Environment.Exit(1); }

            int EnqueueCount = 0;
            StreamerLocker.Unlock();
            Counter.PrintReset();
            UserStreamerStatic.ShowCount();

            //TickCount Tick = new TickCount(0);
            foreach (KeyValuePair<long, UserStreamer> s in Streamers.ToArray())  //ここでスナップショットを作る
            {
                if (!s.Value.PostPoned() && !s.Value.ConnectWaiting)
                {
                    s.Value.ConnectWaiting = true;
                    ConnectBlock.Post((s.Key, s.Value));
                    EnqueueCount++;
                }
            }
            return EnqueueCount;
        }

        void RemoveStreamer(UserStreamer Streamer)
        //Revokeされた後の処理
        {
            Streamers.TryRemove(Streamer.Token.UserId, out UserStreamer z);  //つまり死んだStreamerは除外される
            SetMaxConnections(true);
            Console.WriteLine("{0} {1}: Streamer removed", DateTime.Now, Streamer.Token.UserId);
        }

        private void SetMaxConnections(bool Force = false, int basecount = 0)
        {
            int MaxConnections = Math.Max(basecount, Streamers.Count) + config.crawl.DefaultConnections;
            if (Force || ServicePointManager.DefaultConnectionLimit < MaxConnections)
            {
                ServicePointManager.DefaultConnectionLimit = MaxConnections;
            }
        }
    }
}
