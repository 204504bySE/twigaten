using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Twigaten.Lib;

namespace Twigaten.Web
{
    static class PictHash
    {
        readonly static HttpClient Http = new HttpClient(new HttpClientHandler() { UseCookies = false });
        static readonly Config config = Config.Instance;
        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHash(byte[] mem, string ServerUrl, string FileName)
        {
            try
            {
                MultipartFormDataContent Form = new MultipartFormDataContent();
                ByteArrayContent File = new ByteArrayContent(mem);
                File.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("form-data")
                {
                    Name = "File",
                    FileName = FileName,
                };
                Form.Add(File);
                using (HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, ServerUrl) { Content = Form })
                using (HttpResponseMessage res = await Http.SendAsync(req).ConfigureAwait(false))
                {
                    if (long.TryParse(await res.Content.ReadAsStringAsync().ConfigureAwait(false), out long ret)) { return ret; }
                    else { return null; }
                }
            }
            catch { return null; }
        }
    }
}
