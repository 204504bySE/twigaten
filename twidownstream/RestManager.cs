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
using System.Diagnostics;

namespace twidownstream
{
    class RestManager
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;
        ///<summary>全Tokenでこれを共有するとHttpClientも共有される</summary>
        static readonly ConnectionOptions TokenOptions = new ConnectionOptions
        {
            DisableKeepAlive = false,
            UseCompression = true,
            UseCompressionOnStreaming = true
        };

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = Math.Max(config.crawl.DefaultConnectionThreads, config.crawl.RestTweetThreads * 3);
        }

        public async Task<int> Proceed()
        {
            var tokens = await db.Selecttoken(DBHandler.SelectTokenMode.All);
            foreach(var t in tokens) { t.ConnectionOptions = TokenOptions; }
            if (tokens.Length > 0) { Console.WriteLine("App: {0} Accounts to REST", tokens.Length); }
            var RestProcess = new ActionBlock<Tokens>(async (t) => 
            {
                var s = new UserStreamer(t);
                await s.RestFriend();
                await s.RestBlock();
                await s.RestMyTweet();
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.RestTweetThreads,
                BoundedCapacity = config.crawl.RestTweetThreads << 1
            });
            var sw = Stopwatch.StartNew();
            foreach(var t in tokens)
            {
                await RestProcess.SendAsync(t);
                if(sw.ElapsedMilliseconds > 60000)
                {
                    Counter.PrintReset();
                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                    GC.Collect();
                    sw.Restart();
                }
            }
            RestProcess.Complete();
            await RestProcess.Completion;
            return tokens.Length;
        }
    }
}
