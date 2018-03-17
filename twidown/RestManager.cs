using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using twitenlib;

namespace twidown
{
    class RestManager
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = config.crawl.RestTweetThreads * 3;
            Task.Factory.StartNew(() => { IntervalProcess(); }, TaskCreationOptions.LongRunning);
        }

        void IntervalProcess()
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            while (true)
            {
                StreamerLocker.Unlock();
                Counter.PrintReset();
                Thread.Sleep(60000);
            }
        }

        public int Proceed()
        {
            Tokens[] tokens = db.Selecttoken(DBHandler.SelectTokenMode.RestProcess);
            if (tokens.Length > 0) { Console.WriteLine("{0} App: {1} Accounts to REST", DateTime.Now, tokens.Length); }
            Parallel.ForEach(tokens,
                new ParallelOptions { MaxDegreeOfParallelism = config.crawl.RestTweetThreads },
                (Tokens t) => 
            {
                UserStreamer s = new UserStreamer(t);
                s.RestBlock();
                s.RestMyTweet();
                db.StoreRestDonetoken(t.UserId);
            });
            return tokens.Length;
        }
    }
}
