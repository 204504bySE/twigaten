using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime;
using System.Net;
using System.Diagnostics;
using twidown;

namespace twidown
{
    class Program
    {
        static void Main(string[] args)
        {
            ServicePointManager.ReusePort = true;
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            ServicePointManager.EnableDnsRoundRobin = true;
            Thread.Sleep(10000);

            if (args.Length >= 1 && args[0] == "/REST")
            {
                Console.WriteLine("{0} App: Running in REST mode.", DateTime.Now);
                Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.Idle;
                int RestCount = new RestManager().Proceed();
                Console.WriteLine("{0} App: {1} Accounts REST Tweets Completed.", DateTime.Now, RestCount);
                Thread.Sleep(10000);
                return;
            }

            UserStreamerManager manager = new UserStreamerManager();
            Stopwatch sw = new Stopwatch();
            while (true)
            {
                sw.Restart();
                int EnqueueCount = manager.ConnectStreamers();
                Console.WriteLine("{0} App: {1} Accounts Queued.", DateTime.Now, EnqueueCount); //何かWriteLineしないとConnectBlockが動かないんだ #ウンコード
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                GC.Collect();

                do { Thread.Sleep(100); } while (sw.ElapsedMilliseconds < 60000);    //やけくそ
                manager.AddAll();
            }
        }
    }
}