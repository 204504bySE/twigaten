using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using SixLabors.ImageSharp.Advanced;
using SixLabors.ImageSharp.PixelFormats;
using Twigaten.Lib;

#pragma warning disable CS4014 // この呼び出しは待機されなかったため、現在のメソッドの実行は呼び出しの完了を待たずに続行されます
namespace Twigaten.DctHash
{
    [SupportedOSPlatform("windows")]
    class Program
    {
        static async Task Main(string[] args)
        {
            //int connectionCount = 0;
            var config = Config.Instance.dcthashserver;
            var listener = new TcpListener(config.ListenIPv6 ? IPAddress.IPv6Any : IPAddress.Any, config.ListenPort);

            try { listener.Start(); }
            catch
            {
                Console.WriteLine("Failed to listen {0} Exiting.", listener.LocalEndpoint);
                return;
            }
            Console.WriteLine("Listening {0}", listener.LocalEndpoint);

            using var gcTimer = new Timer((_) =>
            {
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect();
            }, null, 600000, 600000);

            while (true)
            {
                var _client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                //Interlocked.Increment(ref connectionCount);

                Task.Run(async () =>
                {
                    var client = _client;
                    client.NoDelay = true;
                    var endpoint = client.Client.RemoteEndPoint;
                    //Console.WriteLine("New connection({0}): {1}", connectionCount, endpoint);
                    try
                    {
                        using var tcp = client.GetStream();
                        using var reader = new MessagePackStreamReader(tcp);
                        while (true)
                        {
                            using var cancel = new CancellationTokenSource(60000);
                            PictHashRequest req;
                            {
                                var msgpack = await reader.ReadAsync(cancel.Token).ConfigureAwait(false);
                                if (!msgpack.HasValue)
                                {
                                    Console.WriteLine("Failed to read MessagePack stream");
                                    break;
                                }
                                req = MessagePackSerializer.Deserialize<PictHashRequest>(msgpack.Value);
                            }
                            PictHashResult res;
                            long? dctHash;
                            using (var mediaMem = new MemoryStream(req.MediaFile, false))
                            using (var mem = new MemoryStream())
                            {
                                //GdiPlusが腐ってるのでImageSharpで読み込む
                                using (var img = SixLabors.ImageSharp.Image.Load<Rgba32>(mediaMem))
                                {
                                    img.Save(mem, img.GetConfiguration().ImageFormatsManager.FindEncoder(SixLabors.ImageSharp.Formats.Bmp.BmpFormat.Instance));
                                }
                                mem.Seek(0, SeekOrigin.Begin);
                                dctHash = PictHash.DCTHash(mem, req.Crop);
                            }
                            res = new PictHashResult() { UniqueId = req.UniqueId, DctHash = dctHash };

                            await MessagePackSerializer.SerializeAsync(tcp, res, null, cancel.Token).ConfigureAwait(false);
                        }
                    }
                    catch (ExternalException e) { Console.WriteLine(e.Message); }   //gdiplusの例外
                    catch (OperationCanceledException) { }  //CancellationToken
                    catch (Exception e) { Console.WriteLine(e); }
                    finally { client.Dispose(); }
                    //Interlocked.Decrement(ref connectionCount);
                    //Console.WriteLine("Disconnected({0}): {1}", connectionCount, endpoint);
                });
            }
        }
    }

}
