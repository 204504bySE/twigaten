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
            ThreadPool.GetMaxThreads(out int _, out int CompletionThreads);
            ThreadPool.SetMinThreads(32000, CompletionThreads);
            ThreadPool.SetMaxThreads(32000, CompletionThreads);
            await Task.Delay(10000);

            if (args.Length >= 1 && args[0] == "/REST")
            {
                Console.WriteLine("{0} App: Running in REST mode.", DateTime.Now);
                int RestCount = await new RestManager().Proceed();
                Console.WriteLine("{0} App: {1} Accounts REST Tweets Completed.", DateTime.Now, RestCount);
                return;
            }

            UserStreamerManager manager = await UserStreamerManager.Create();
            Stopwatch sw = new Stopwatch();
            while (true)
            {
                sw.Restart();
                int Connected = await manager.ConnectStreamers();
                Console.WriteLine("{0} App: {1} / {2} Accounts Streaming.", DateTime.Now, Connected, manager.Count);
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                GC.Collect();
                sw.Stop();
                if (sw.ElapsedMilliseconds < 60000) { await Task.Delay((int)(60000 - sw.ElapsedMilliseconds)); }
                await manager.AddAll();
            }
        }
    }
}