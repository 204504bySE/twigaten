using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime;
using System.Net;
using System.Diagnostics;

namespace Twigaten.Crawl
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.EnableDnsRoundRobin = true;

            Lib.Config config = Lib.Config.Instance;
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
            else if (args.Length >= 1 && long.TryParse(args[0], out long user_id))
            {
                Console.WriteLine("App: Running in user_id mode: {0}", user_id);
                await new RestManager().OneAccount(user_id).ConfigureAwait(false);
                return;
            }

            await Task.Delay(10000).ConfigureAwait(false);
            var manager = await UserStreamerManager.Create().ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            while (true)
            {
                await manager.ConnectStreamers().ConfigureAwait(false);
                //早く終わったときだけ休む(home_timelineの15/15min取得制限に準ずる)
                long Elapsed = sw.ElapsedMilliseconds;
                if (Elapsed < 60000)
                {
                    await Task.Delay(Math.Min(10000, 60000 - (int)Elapsed >> 1)).ConfigureAwait(false);
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要                    GC.wait
                    GC.Collect();
                    //まだ時間が残ってたら休む
                    Elapsed = sw.ElapsedMilliseconds;
                    if (Elapsed < 60000) { await Task.Delay(60000 - (int)Elapsed).ConfigureAwait(false); }
                    sw.Restart();
                }
                else 
                {
                    GC.Collect();
                    sw.Restart(); 
                }
                //最後に取得したツイート等をDBに保存する(画像の消化が終わることを期待して待ってからやる)
                await manager.StoreCrawlStatus().ConfigureAwait(false);
                //↓再読み込みしても一部しか反映されないけどね
                config.Reload();
                await manager.AddAll().ConfigureAwait(false);
            }
        }
    }
}