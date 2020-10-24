using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Twigaten.Lib;
namespace Twigaten.DctHash
{
    class Program
    {
        const long RetryWait = 1000;

        static async Task Main(string[] args)
        {
            int connectionCount = 0;
            var config = Config.Instance.dcthashserver;
            var listener = new TcpListener(config.ListenIPv6 ? IPAddress.IPv6Loopback : IPAddress.Loopback, config.ListenPort);
            bool restartListener = false;
            var restartListerSw = Stopwatch.StartNew();
            listener.Start();
            while (true)
            {
                try
                {
                    if (restartListener)
                    {
                        if (RetryWait <= restartListerSw.ElapsedMilliseconds)
                        {
                            restartListerSw.Restart();
                            listener.Stop();
                            listener.Start();
                            Console.WriteLine("Listening restarted.", listener.LocalEndpoint);
                        }
                        restartListener = false;

                        Console.WriteLine("Listening on {0}", listener.LocalEndpoint);
                    }
                    var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    client.NoDelay = true;
                    Interlocked.Increment(ref connectionCount);
#pragma warning disable CS4014 // この呼び出しは待機されなかったため、現在のメソッドの実行は呼び出しの完了を待たずに続行されます
                    Task.Run(async () =>
                    {
                        var endpoint = client.Client.RemoteEndPoint;
                        Console.WriteLine("New connection({0}): {1}", connectionCount, endpoint);
                        try
                        {
                            using var tcp = client.GetStream();
                            using var reader = new MessagePackStreamReader(tcp);
                            while (true)
                            {
                                using var cancel = new CancellationTokenSource(60000);
                                var msgpack = await reader.ReadAsync(cancel.Token);
                                if (!msgpack.HasValue) { break; }
                                var req = MessagePackSerializer.Deserialize<PictHashRequest>(msgpack.Value);
                                using var mem = new MemoryStream(req.MediaFile, false);
                                var res = new PictHashResult() { UniqueId = req.UniqueId, DctHash = PictHash.DCTHash(mem, req.Crop) };
                                await MessagePackSerializer.SerializeAsync(tcp, res, null, cancel.Token).ConfigureAwait(false);
                            }
                        }
                        catch (OperationCanceledException) { restartListener = true; }
                        catch (Exception e) { restartListener = true; Console.WriteLine(e); }
                        Interlocked.Decrement(ref connectionCount);
                        Console.WriteLine("Disconnected({0}): {1}", connectionCount, endpoint);
                    });
#pragma warning restore CS4014 // この呼び出しは待機されなかったため、現在のメソッドの実行は呼び出しの完了を待たずに続行されます
                }
                catch (Exception e) 
                {
                    Console.WriteLine(e);
                    restartListener = true;
                    await Task.Delay((int)RetryWait).ConfigureAwait(false);
                }
            }
        }
    }

}
