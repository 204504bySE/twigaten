﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;

namespace Twigaten.Lib.DctHash
{
    class DctHashClient
    {
        class TcpPoolItem : IDisposable
        {
            TcpClient Client;
            public NetworkStream Stream { get; init; }
            public MessagePackStreamReader Reader { get; init; }

            public static async Task<TcpPoolItem> Connect(string hostName, int port)
            {
                try
                {
                    var client = new TcpClient() { NoDelay = true };
                    await client.ConnectAsync(hostName, port).ConfigureAwait(false);
                    var stream = client.GetStream();
                    var reader = new MessagePackStreamReader(stream);
                    return new TcpPoolItem()
                    {
                        Client = client,
                        Stream = stream,
                        Reader = reader
                    };
                }
                catch { return null; }
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
        readonly ConcurrentBag<TcpPoolItem> TcpPool = new ConcurrentBag<TcpPoolItem>();

        readonly string HostName;
        readonly int Port;
        public DctHashClient(string HostName, int Port)
        {
            this.HostName = HostName;
            this.Port = Port;
        }

        readonly Random random = new Random();

        ///<summary>
        ///クソサーバーからDCTHashをもらってくる
        ///自動リトライつき
        ///</summary>
        public async Task<long?> DCTHash(byte[] Source, long media_id)
        {
            for (int i = 0; i < 4; i++)
            {
                TcpPool.TryTake(out var tcp);
                try
                {
                    //可能な限りプールされた接続を使おうとする
                    while (tcp?.Connected != true)
                    {
                        tcp?.Dispose();
                        if (!TcpPool.TryTake(out tcp)) { tcp = await TcpPoolItem.Connect(HostName, Port).ConfigureAwait(false); }
                    }
                    using var cancel = new CancellationTokenSource(10000);
                    await MessagePackSerializer.SerializeAsync(tcp.Stream, new PictHashRequest() { UniqueId = media_id, MediaFile = Source }, null, cancel.Token).ConfigureAwait(false);
                    var msgpack = await tcp.Reader.ReadAsync(cancel.Token);
                    TcpPool.Add(tcp);

                    if (msgpack.HasValue)
                    {
                        var result = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value);
                        if (result.UniqueId == media_id) { return result.DctHash; }
                    }
                }
                catch (TaskCanceledException)
                {
                    tcp?.Dispose();
                }
                catch
                {
                    tcp?.Dispose();
                    //再試行はdcthashの再起動待ち時間をある程度考慮して選ぶ
                    int retryms = 5000;
                    await Task.Delay(random.Next(retryms, retryms << 1)).ConfigureAwait(false);
                }
            }
            return null;
        }

        ///<summary>
        ///クソサーバーからDCTHashをもらってくる
        ///web(画像で検索)用 リトライなし
        ///</summary>
        public async Task<long?> DCTHashCrop(byte[] Source)
        {
            TcpPool.TryTake(out var tcp);
            try
            {
                //可能な限りプールされた接続を使おうとする
                while (tcp?.Connected != true)
                {
                    tcp?.Dispose();
                    if (!TcpPool.TryTake(out tcp)) { tcp = await TcpPoolItem.Connect(HostName, Port).ConfigureAwait(false); }
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
            catch { tcp?.Dispose(); }
            return null;
        }
    }
}
