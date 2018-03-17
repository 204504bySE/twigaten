using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
namespace twidown
{
    static class PictHash
    {
        readonly static HttpClient Http = new HttpClient(new HttpClientHandler() { UseCookies = false });
        ///<summary>クソサーバーからDCTHashをもらってくる</summary>
        public static async Task<long?> DCTHash(Stream Source, string ServerUrl, string FileName)
        {
            try
            {
                //こいつらは勝手にstreamを閉じるのでコピーしないと死ぬ
                using (MemoryStream m = new MemoryStream())
                {
                    Source.CopyTo(m);
                    m.Seek(0, SeekOrigin.Begin);
                    using (MultipartFormDataContent Form = new MultipartFormDataContent())
                    using (StreamContent File = new StreamContent(m))
                    {
                        File.Headers.ContentDisposition = new System.Net.Http.Headers.ContentDispositionHeaderValue("form-data")
                        {
                            Name = "File",
                            FileName = FileName,
                        };
                        Form.Add(File);
                        using (HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Post, ServerUrl) { Content = Form })
                        using (HttpResponseMessage res = await Http.SendAsync(req))
                        {
                            if (!res.IsSuccessStatusCode) { Console.WriteLine(res.StatusCode); return null; }
                            if (long.TryParse(await res.Content.ReadAsStringAsync(), out long ret)) { return ret; }
                            else { return null; }
                        }
                    }
                }
            }
            catch (Exception e) { Console.WriteLine(e); return null; }
        }
    }
}
