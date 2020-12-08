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

namespace Twigaten.Web
{
    /// <summary>
    /// Crawlからほぼ丸コピ
    /// </summary>
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

        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHashCrop(byte[] Source, string HostName, int Port)
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
                long unique = Environment.TickCount64;
                await MessagePackSerializer.SerializeAsync(tcp.Stream, new PictHashRequest() { UniqueId = unique, Crop = true, MediaFile = Source }, null, cancel.Token).ConfigureAwait(false);
                var msgpack = await tcp.Reader.ReadAsync(cancel.Token);
                TcpPool.Add(tcp);
                if (msgpack.HasValue)
                {
                    var result = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value);
                    if (result.UniqueId == unique) { return result.DctHash; }
                }
            }
            catch { tcp.Dispose(); }
            return null;
        }
    }
}
