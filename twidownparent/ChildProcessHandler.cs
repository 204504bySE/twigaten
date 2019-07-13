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

        ///<summary>Key=pid, Value=起動したtwidownのProcess</summary>
        readonly Dictionary<int, Process> ProcessInfo = new Dictionary<int, Process>();
        ///<summary>Key=pid, Value=割り当てたアカウントの数</summary>
        readonly Dictionary<int, int> TokenCount = new Dictionary<int, int>();

        ///<summary>新しいtwidownを起動する</summary>
        ///<returns>新しいtwidownのpid 失敗したら-1</returns>
        public int Start()
        {
            try
            {
                var info = string.IsNullOrWhiteSpace(config.crawlparent.DotNetChild) ?
                    new ProcessStartInfo(config.crawlparent.ChildPath) :
                    new ProcessStartInfo(config.crawlparent.DotNetChild, config.crawlparent.ChildPath);
                info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
                info.WindowStyle = ProcessWindowStyle.Minimized;

                var retProcess = Process.Start(info);
                ProcessInfo[retProcess.Id] = retProcess;
                TokenCount[retProcess.Id] = 0;
                WatchDog.Add(retProcess.Id);
                return retProcess.Id;
            }
            catch { return -1; }
        }

        ///<summary>一番空いてるっぽいプロセスにアカウントを割り当てる
        ///各プロセスのアカウント数の上限は関知しない</summary>
        public async Task AssignToken(IEnumerable<long> user_id, bool GetMyTweet)
        {
            //プロセスは事前に起動してくれ
            if(TokenCount.Count == 0) { return; }

            var assigns = new List<(long user_id, int pid)>();

            //アカウントの割り当てを作っていく
            foreach(long u in user_id)
            {
                var minProcess = TokenCount.OrderBy(t => t.Value).First();
                assigns.Add((u, minProcess.Key));
                //対応するプロセスのTokenCountを当然のように足す
                TokenCount[minProcess.Key]++;
            }

            //DBにまとめて書き込む
            if (!await db.AssignTokens(assigns, GetMyTweet).ConfigureAwait(false))
            {
                //失敗したらTokenCountを元に戻す
                foreach(var t in TokenCount.Keys)
                {
                    TokenCount[t] -= assigns.Where(a => a.pid == t).Count();
                }
            }

        }

        ///<summary>死んでるっぽいプロセスにkillを送っていなかったことにする</summary>
        ///<returns>死んだことにしたプロセスの数</returns>
        public async ValueTask<int> DeleteDead()
        {
            int count = 0;
            foreach (var p in ProcessInfo.ToArray())
            {
                try
                {
                    DateTimeOffset? watch = WatchDog.Get(p.Key);
                    //しばらく反応がなかったり死んでたりしたら停止する
                    if (!watch.HasValue 
                        || (DateTimeOffset.Now - watch.Value).TotalSeconds > config.crawlparent.WatchDogTimeout
                        || !p.Value.Responding)
                    {
                        try { p.Value.Kill(); } catch(Exception e) { Console.WriteLine(e); }
                        count += await db.Deletepid(p.Key).ConfigureAwait(false);
                        WatchDog.Remove(p.Key);

                        ProcessInfo.Remove(p.Key);
                        TokenCount.Remove(p.Key);
                    }
                }
                catch { }
            }
            return count;
        }

        public int Count => ProcessInfo.Count;
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
