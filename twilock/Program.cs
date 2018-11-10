using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using twitenlib;

namespace twilock
{
    class Program
    {
        ///<summary>複数のtwidownstream間でツイートの処理を調停するやつだけど結局使ってない</summary>
        static async Task Main(string[] args)
        {
            try
            {
                //CheckOldProcess.CheckandExit();

                RemoveOldSet<long> LockedTweets = new RemoveOldSet<long>(Config.Instance.locker.TweetLockSize);

                UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, Config.Instance.locker.UdpPort));

                int ReceiveCount = 0;
                int SuccessCount = 0;
                Stopwatch sw = new Stopwatch();
                byte[] TrueByte = BitConverter.GetBytes(true);
                byte[] FalseByte = BitConverter.GetBytes(false);

                //雑なプロセス間通信

                ActionBlock<UdpReceiveResult> TweetLockBlock = new ActionBlock<UdpReceiveResult>(async (Received) => 
                {
                    long tweet_id = BitConverter.ToInt64(Received.Buffer, 0);
                    if (LockedTweets.Add(tweet_id))
                    {
                        await Udp.SendAsync(TrueByte, sizeof(bool), Received.RemoteEndPoint).ConfigureAwait(false); //Lockできたらtrue
                        SuccessCount++;
                    }
                    else { await Udp.SendAsync(FalseByte, sizeof(bool), Received.RemoteEndPoint).ConfigureAwait(false); }//Lockできなかったらfalse
                }, new ExecutionDataflowBlockOptions()
                {
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = 1
                });

                sw.Start();
                while (true)
                {
                    await TweetLockBlock.SendAsync(await Udp.ReceiveAsync().ConfigureAwait(false)).ConfigureAwait(false); 
                    ReceiveCount++;
                    if (sw.ElapsedMilliseconds >= 60000)
                    {
                        sw.Restart();
                        Console.WriteLine("{0}: {1} / {2} Tweets Locked", DateTime.Now, SuccessCount, ReceiveCount);
                        SuccessCount = 0; ReceiveCount = 0;
                        GC.Collect();
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); System.Threading.Thread.Sleep(2000); Environment.Exit(1); }   //何かあったら諦めて死ぬ
        }
    }
}
