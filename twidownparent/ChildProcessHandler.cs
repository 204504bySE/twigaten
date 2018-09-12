using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using System.Linq;
using twitenlib;
using System.Net.Sockets;
using System.Net;
using System.Collections.Concurrent;

namespace twidownparent
{
    class ChildProcessHandler
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = new DBHandler();
        ChildWatchDog WatchDog = new ChildWatchDog();
        readonly Dictionary<int, Process> Children = new Dictionary<int, Process>();

        public int Start()
        {
            try
            {
                var info = string.IsNullOrWhiteSpace(config.crawlparent.DotNetChild) ?
                    new ProcessStartInfo(config.crawlparent.ChildPath) :
                    new ProcessStartInfo(config.crawlparent.DotNetChild, config.crawlparent.ChildPath);
                info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
                info.WindowStyle = ProcessWindowStyle.Minimized;
                using (var retProcess = new Process()
                {
                    StartInfo = info,
                    EnableRaisingEvents = true
                })
                {
                    retProcess.Start();
                    Children[retProcess.Id] = retProcess;
                    WatchDog.Add(retProcess.Id);
                    return retProcess.Id;
                }

            }
            catch { return -1; }
        }

        public async ValueTask<int> DeleteDead()
        {
            int count = 0;
            foreach (var p in Children.ToArray())
            {
                try
                {
                    DateTimeOffset? watch = WatchDog.Get(p.Key);
                    //しばらく反応がなかったり死んでたりしたら停止する
                    if (!watch.HasValue 
                        || (DateTimeOffset.Now - watch.Value).TotalSeconds > config.crawlparent.WatchDogTimeout
                        || !p.Value.Responding)
                    {
                        //try { p.Value.Kill(); } catch(Exception e) { Console.WriteLine(e); }
                        count += await db.Deletepid(p.Key);
                        WatchDog.Remove(p.Key);
                    }
                }
                catch { }
            }
            return count;
        }

        public int Count => Children.Count;
    }

    class ChildWatchDog : IDisposable
    {
        readonly UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, Config.Instance.crawlparent.WatchDogPort));
        readonly Task WatchDogReceiver;
        readonly CancellationTokenSource Cancel = new CancellationTokenSource();
        readonly ConcurrentDictionary<int, DateTimeOffset> LastWatchTime = new ConcurrentDictionary<int, DateTimeOffset>();

        public ChildWatchDog()
        {
            WatchDogReceiver = Task.Run(async () =>
            {
                while (!Cancel.Token.IsCancellationRequested)
                {
                    var Received = await Udp.ReceiveAsync();
                    if (Received.Buffer.Length < sizeof(int)) { continue; }
                    int pid = BitConverter.ToInt32(Received.Buffer, 0);
                    LastWatchTime[pid] = DateTimeOffset.UtcNow;
                }
                Udp.Dispose();
            });
        }

        public DateTimeOffset? Get(int pid) { return LastWatchTime.TryGetValue(pid, out DateTimeOffset Time) ? Time : null as DateTimeOffset?;  }
        public bool Add(int pid) { return LastWatchTime.TryAdd(pid, DateTimeOffset.UtcNow); }
        public bool Remove(int pid) { return LastWatchTime.TryRemove(pid, out DateTimeOffset gomi); }

        public void Dispose() { Cancel.Cancel(); }
    }
}
