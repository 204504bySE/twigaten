using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Twigaten.Lib;

namespace Twigaten.Web
{
    static class PictHash
    {
        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHash(byte[] Source, string HostName, int Port)
        {
            using var Client = new TcpClient(HostName, Port) { NoDelay = true };
            using var Stream = Client.GetStream();
            using var Reader = new MessagePackStreamReader(Stream);
            long id = Environment.TickCount64;
            await MessagePackSerializer.SerializeAsync(Stream, new PictHashRequest() { UniqueId = id, Crop = true, MediaFile = Source }).ConfigureAwait(false);
            var msgpack = await Reader.ReadAsync(CancellationToken.None);
            if (msgpack.HasValue)
            {
                var result = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value);
                if (result.UniqueId == id) { return result.DctHash; }
            }
            return null;
        }
    }
}
