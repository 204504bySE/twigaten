using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using Twigaten.Lib;
using System.Runtime;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;

namespace Twigaten.Crawl
{
    class RestManager
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public RestManager()
        {
            ServicePointManager.DefaultConnectionLimit = Math.Max(config.crawl.DefaultConnectionThreads, config.crawl.RestTweetThreads * 3);
        }

        public async Task<int> Proceed()
        {
            var tokens = (await db.SelectUserStreamerSetting(DBHandler.SelectTokenMode.All).ConfigureAwait(false)).ToArray();
            if (tokens.Length > 0) { Console.WriteLine("App: {0} Accounts to REST", tokens.Length); }
            var RestProcess = new ActionBlock<Tokens>(async (t) => 
            {
                var s = new UserStreamer(new UserStreamer.UserStreamerSetting() { Token = t });
                await s.RestFriend().ConfigureAwait(false);
                await s.RestBlock().ConfigureAwait(false);
                await s.RestMyTweet().ConfigureAwait(false);
                await s.VerifyCredentials().ConfigureAwait(false);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.RestTweetThreads,
                BoundedCapacity = config.crawl.RestTweetThreads << 1
            });
            var sw = Stopwatch.StartNew();
            foreach(var t in tokens)
            {
                await RestProcess.SendAsync(t.Token).ConfigureAwait(false);

                while (true)
                {
                    if (sw.ElapsedMilliseconds > 60000)
                    {
                        Counter.PrintReset();
                        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                        GC.Collect();
                        sw.Restart();
                    }
                    //ツイートが詰まってたら休む                    
                    if (UserStreamerStatic.NeedConnectPostpone()) { await Task.Delay(1000).ConfigureAwait(false); }
                    else { break; }
                }
            }
            RestProcess.Complete();
            await RestProcess.Completion.ConfigureAwait(false);
            await UserStreamerStatic.Complete().ConfigureAwait(false);
            Counter.PrintReset();
            return tokens.Length;
        }
    }
}
