using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime;
using System.Net;
using System.Diagnostics;

namespace twidownstream
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.EnableDnsRoundRobin = true;

            twitenlib.Config config = twitenlib.Config.Instance;
            //結局Minを超えると死ぬのでMinを大きくしておくしかない
            //User streamを使うときだけ対応する
            if(config.crawl.StreamSpeedSeconds > 0)
            {
                //ThreadPool.GetMinThreads(out int MinThreads, out int CompletionThreads);
                ThreadPool.GetMaxThreads(out int MaxThreads, out int CompletionThreads);
                ThreadPool.SetMinThreads(MaxThreads, CompletionThreads);
                //ThreadPool.SetMaxThreads(MaxThreads, CompletionThreads);
                //Console.WriteLine("App: ThreadPool: {0}, {1}", MinThreads, CompletionThreads);
            }

            if (args.Length >= 1 && args[0] == "/REST")
            {
                Console.WriteLine("App: Running in REST mode.");
                int RestCount = await new RestManager().Proceed().ConfigureAwait(false);
                Console.WriteLine("App: {0} Accounts REST Tweets Completed.", RestCount);
                return;
            }

            await Task.Delay(10000).ConfigureAwait(false);
            var manager = await UserStreamerManager.Create().ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            while (true)
            {
                int Connected = await manager.ConnectStreamers().ConfigureAwait(false);
                Console.WriteLine("App: {0} / {1} Accounts Streaming.", Connected, manager.Count);
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                GC.Collect();
                sw.Stop();
                //早く終わったときだけ休む(home_timelineの15/15min取得制限に準ずる)
                long Elapsed = sw.ElapsedMilliseconds;
                if (Elapsed < 60000)
                {
                    await Task.Delay(60000 - (int)Elapsed).ConfigureAwait(false);
                    sw.Restart();
                    //ついでにその時だけ最後に取得したツイート等をDBに保存する
                    await manager.StoreCrawlStatus().ConfigureAwait(false);
                }
                else { sw.Restart(); }
                //↓再読み込みしても一部しか反映されないけどね
                config.Reload();
                await manager.AddAll().ConfigureAwait(false);
            }
        }
    }
}