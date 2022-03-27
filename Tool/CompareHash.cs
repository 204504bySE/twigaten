using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MessagePack;
using Twigaten.Lib;

namespace Twigaten.Tool
{
    class CompareHash
    {
        public static async Task Proceed()
        {
            int mismatch = 0;
            int failure = 0;
            long[] mismatchBits = new long[sizeof(long) * 8];
            var config = Config.Instance;
            var db = new DBHandler();

            int mediaCount = 0;

            foreach (string p in (await db.GetMediaPath(DateTimeOffset.UtcNow.ToUnixTimeSeconds()).ConfigureAwait(false)).MediaPath)
            {
                byte[] mediabytes;
                try
                {
                    using (var file = File.OpenRead(Path.Combine(config.crawl.PictPaththumb, p)))
                    using (var mem = new MemoryStream())
                    {
                        await file.CopyToAsync(mem).ConfigureAwait(false);
                        mediabytes = mem.ToArray();
                    }
                }
                catch (Exception e) { Console.WriteLine(e.Message); continue; }

                mediaCount++;
                var a = PictHashClient.DCTHash(mediabytes, 0, "192.168.238.126");
                var b = PictHashClient.DCTHash(mediabytes, 0, "localhost");
                await Task.WhenAll(a, b).ConfigureAwait(false);
                if(!a.Result.HasValue || !b.Result.HasValue)
                {
                    failure++;
                    Console.Write(mediaCount);
                    if (!a.Result.HasValue) { Console.Write(" a "); }
                    if (!b.Result.HasValue) { Console.Write(" b "); }
                    Console.WriteLine("\tfailure"); 
                }
                else if (a.Result.Value != b.Result.Value) 
                {
                    mismatch++;
                    ulong bits = (ulong)(a.Result.Value ^ b.Result.Value);
                    mismatchBits[Popcnt.X64.PopCount(bits)]++;
                    Console.WriteLine("{0:X16}", bits);
                }
            }
            for (int i = 0; i < mismatchBits.Length; i++)
            {
                if (0 < mismatchBits[i]) { Console.WriteLine("{0}: {1}", i, mismatchBits[i]); }
            }
            Console.WriteLine("{0} / {1} mismatches.", mismatch, mediaCount);
        }

        public static async Task Marathon()
        {
            int mismatch = 0;
            int failure = 0;
            long[] mismatchBits = new long[sizeof(long) * 8];
            var config = Config.Instance;
            var db = new DBHandler();

            using var client = new TcpClient("localhost", 12306);
            client.NoDelay = true;
            using var tcp = client.GetStream();
            using var reader = new MessagePackStreamReader(tcp);

            long downloaded_at = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            int mediaCount = 0;
            while (true)
            {
                var result = await db.GetMediaPath(downloaded_at).ConfigureAwait(false);
                downloaded_at = result.MinDownloadedAt - 1;

                var compareHashBlock = new ActionBlock<string>(async (p) => 
                {
                    byte[] mediabytes;
                    try
                    {
                        using (var file = File.OpenRead(Path.Combine(config.crawl.PictPaththumb, p)))
                        using (var mem = new MemoryStream())
                        {
                            await file.CopyToAsync(mem).ConfigureAwait(false);
                            mediabytes = mem.ToArray();
                        }
                    }
                    catch (Exception e) { Console.WriteLine(e.Message); return; }

                    var a = PictHashClient.DCTHash(mediabytes, 0, "192.168.238.126");
                    var b = PictHashClient.DCTHash(mediabytes, 0, "localhost");
                    await Task.WhenAll(a, b).ConfigureAwait(false);
                    if (!a.Result.HasValue || !b.Result.HasValue)
                    {
                        Interlocked.Increment(ref failure); 
                        Console.WriteLine("\t\t\tfailure"); 
                    }
                    else if (a.Result.Value != b.Result.Value)
                    {
                        Interlocked.Increment(ref mismatch);
                        ulong bits = (ulong)(a.Result.Value ^ b.Result.Value);
                        Interlocked.Increment(ref mismatchBits[Popcnt.X64.PopCount(bits)]);
                        //Console.WriteLine("{0:X16}", bits);
                    }
                }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });

                foreach (string p in result.MediaPath)
                {
                    compareHashBlock.Post(p);
                }
                mediaCount += result.MediaPath.Length;
                compareHashBlock.Complete();
                await compareHashBlock.Completion;

                for (int i = 0; i < mismatchBits.Length; i++)
                {
                    if (0 < mismatchBits[i]) { Console.WriteLine("{0}: {1}", i, mismatchBits[i]); }
                }
                Console.WriteLine("{0} / {1} mismatches.", mismatch, mediaCount);
            }
        }
    }

    static class PictHashClient
    {
        readonly struct TcpPoolItem : IDisposable
        {
            readonly TcpClient Client;
            public readonly NetworkStream Stream { get; }
            public MessagePackStreamReader Reader { get; }
            public TcpPoolItem(string hostName, int port)
            {
                Client = new TcpClient(hostName, port) { NoDelay = true };
                Stream = Client.GetStream();
                Reader = new MessagePackStreamReader(Stream);
            }
            public bool Connected => Client?.Connected ?? false;

            public void Dispose()
            {
                try
                {
                    Reader?.Dispose();
                    Stream?.Dispose();
                    Client?.Dispose();
                }
                catch { }
            }
        }

        static readonly ConcurrentDictionary<string, ConcurrentBag<TcpPoolItem>> TcpPool = new();
        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHash(byte[] Source, long media_id, string HostName)
        {
            if (!TcpPool.TryGetValue(HostName, out var tcpHost)) 
            {
                tcpHost = new ConcurrentBag<TcpPoolItem>();
                TcpPool[HostName] = tcpHost;
            }
            if (!tcpHost.TryTake(out var tcp)) { tcp = new TcpPoolItem(HostName, 12306); }
            long? ret = null;
            try
            {
                using var cancel = new CancellationTokenSource(10000);
                await MessagePackSerializer.SerializeAsync(tcp.Stream, new PictHashRequest() { UniqueId = media_id, MediaFile = Source }, null, cancel.Token).ConfigureAwait(false);
                var msgpack = await tcp.Reader.ReadAsync(cancel.Token);

                if (msgpack.HasValue)
                {
                    var result = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value);
                    if (result.UniqueId == media_id) { ret = result.DctHash; }
                }
                tcpHost.Add(tcp);
            }
            catch { tcp.Dispose(); }
            return ret;
        }
    }

}
