using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using twitenlib;
using System.Runtime;
using System.Threading.Tasks.Dataflow;

namespace twidownstream
{
    class RestManager
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = Math.Max(config.crawl.DefaultConnectionThreads, config.crawl.RestTweetThreads * 3);
            Task.Run(async () => { await IntervalProcess(); });
        }

        async Task IntervalProcess()
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            while (true)
            {
                Counter.PrintReset();
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                GC.Collect();
                await Task.Delay(60000);
            }
        }

        public async ValueTask<int> Proceed()
        {
            Tokens[] tokens = await db.Selecttoken(DBHandler.SelectTokenMode.All);
            if (tokens.Length > 0) { Console.WriteLine("App: {0} Accounts to REST", tokens.Length); }
            ActionBlock<Tokens> RestProcess = new ActionBlock<Tokens>(async (t) => 
            {
                UserStreamer s = new UserStreamer(t);
                await s.RestFriend();
                await s.RestBlock();
                await s.RestMyTweet();
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.RestTweetThreads,
                BoundedCapacity = config.crawl.RestTweetThreads << 1
            });
            foreach(Tokens t in tokens) { await RestProcess.SendAsync(t); }
            RestProcess.Complete();
            await RestProcess.Completion;
            return tokens.Length;
        }
    }
}
