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
            ServicePointManager.DefaultConnectionLimit = Math.Max(config.crawl.DefaultConnections, config.crawl.RestTweetThreads * 3);
            Task.Run(async () => { await IntervalProcess(); });
        }

        async Task IntervalProcess()
        {
            Thread.CurrentThread.Priority = ThreadPriority.Highest;
            while (true)
            {
                StreamerLocker.Unlock();
                Counter.PrintReset();
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                GC.Collect();
                await Task.Delay(60000);
            }
        }

        public async Task<int> Proceed()
        {
            Tokens[] tokens = await db.Selecttoken(DBHandler.SelectTokenMode.RestProcess);
            if (tokens.Length > 0) { Console.WriteLine("{0} App: {1} Accounts to REST", DateTime.Now, tokens.Length); }
            ActionBlock<Tokens> RestBlock = new ActionBlock<Tokens>(async (t) => 
            {
                UserStreamer s = new UserStreamer(t);
                await s.RestBlock();
                await s.RestMyTweet();
                await db.StoreRestDonetoken(t.UserId);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.RestTweetThreads,
                BoundedCapacity = config.crawl.RestTweetThreads << 1
            });
            foreach(Tokens t in tokens) { await RestBlock.SendAsync(t); }
            RestBlock.Complete();
            await RestBlock.Completion;
            return tokens.Length;
        }
    }
}
