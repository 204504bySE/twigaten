using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Twigaten.Lib;

namespace Twigaten.Crawl
{
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

        readonly static ConcurrentBag<TcpPoolItem> TcpPool = new ConcurrentBag<TcpPoolItem>();
        readonly static Stopwatch PoolRelease = Stopwatch.StartNew();
        static readonly Random random = new Random();

        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHash(byte[] Source, long media_id, string HostName, int Port)
        {
            for (int i = 0; i < 5; i++)
            {
                TcpPool.TryTake(out var tcp);
                try
                {
                    //可能な限りプールされた接続を使おうとする
                    while (!tcp.Connected)
                    {
                        tcp.Dispose();
                        if (!TcpPool.TryTake(out tcp)) { tcp = new TcpPoolItem(HostName, Port); }
                    }
                    using var cancel = new CancellationTokenSource(10000);
                    await MessagePackSerializer.SerializeAsync(tcp.Stream, new PictHashRequest() { UniqueId = media_id, MediaFile = Source },null, cancel.Token).ConfigureAwait(false);
                    var msgpack = await tcp.Reader.ReadAsync(cancel.Token);
                    TcpPool.Add(tcp);

                    if (msgpack.HasValue) 
                    {
                        var result = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value);
                        if (result.UniqueId == media_id) { return result.DctHash; }
                    }
                }
                catch 
                {
                    tcp.Dispose();
                    //再試行はdcthashの再起動待ち時間をある程度考慮して選ぶ
                    int retryms = 4000 << i;
                    await Task.Delay(random.Next(retryms, retryms << 1)).ConfigureAwait(false);
                }
            }
            return null;
        }
    }
}
