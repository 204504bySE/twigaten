using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Twigaten.Lib;

namespace Twigaten.Tool
{
    class CompareHash
    {
        readonly static HttpClient Http = new HttpClient(new HttpClientHandler() { UseCookies = false });
        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        static async Task<long?> DCTHash(byte[] Source, string ServerUrl, string FileName)
        {
            try
            {
                using (var Form = new MultipartFormDataContent())
                using (var File = new ByteArrayContent(Source))
                {
                    File.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("form-data")
                    {
                        Name = "File",
                        FileName = FileName,
                    };
                    Form.Add(File);
                    using (var req = new HttpRequestMessage(HttpMethod.Post, ServerUrl) { Content = Form })
                    using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                    {
                        if (!res.IsSuccessStatusCode) { Console.WriteLine(res.StatusCode); return null; }
                        if (long.TryParse(await res.Content.ReadAsStringAsync().ConfigureAwait(false), out long ret)) { return ret; }
                        else { return null; }
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); return null; }
        }

        public static async Task Proceed()
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


            foreach (string p in await db.GetMediaPath().ConfigureAwait(false))
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

                await MessagePackSerializer.SerializeAsync(tcp, new PictHashRequest() { MediaFile = mediabytes });
                var msgpack = await reader.ReadAsync(CancellationToken.None);
                long? hash = null;
                if (msgpack.HasValue) { hash = MessagePackSerializer.Deserialize<PictHashResult>(msgpack.Value).DctHash; }
                var a = Task.FromResult(hash);

                var b = DCTHash(mediabytes, @"http://[::1]:12305/hash/dct", Path.GetFileName(p));
                await Task.WhenAll(a, b).ConfigureAwait(false);
                if(!a.Result.HasValue || !b.Result.HasValue) { failure++; Console.WriteLine("\t\t\tfailure"); }
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
            Console.WriteLine("{0} mismatches.", mismatch);
        }
    }
    [MessagePackObject]
    public class PictHashRequest
    {
        [Key(0)]
        public long UniqueId { get; set; }
        [Key(1)]
        public bool Crop { get; set; }
        [Key(2)]
        public byte[] MediaFile { get; set; }
    }
    [MessagePackObject]
    public class PictHashResult
    {
        [Key(0)]
        public long UniqueId { get; set; }
        [Key(1)]
        public long? DctHash { get; set; }
    }
}
