using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.StaticFiles;
using twimgproxy;

namespace aspcoretest.Controllers
{
    [Route("twimg")]
    public class twimgController : Controller
    {
        static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12
        });
        static readonly FileExtensionContentTypeProvider ExtMime = new FileExtensionContentTypeProvider();
        static readonly DBHandler DB = new DBHandler();

        [HttpGet("thumb/{FileName}")]
        public async Task<IActionResult> thumb(string FileName)
        {
            if(!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long media_id)) { return StatusCode(400); }
            var Source = await DB.SelectThumbUrl(media_id).ConfigureAwait(false);
            if(Source == null) { return StatusCode(404); }

            if (!ExtMime.TryGetContentType(FileName, out string mime)) { mime = "application/octet-stream"; };
            return await Download(Source.Value.Url + (Source.Value.Url.IndexOf("twimg.com") >= 0 ? ":thumb" : ""), Source.Value.Referer, mime).ConfigureAwait(false);
        }

        [HttpGet("profile_image/{FileName}")]
        public async Task<IActionResult> profile_image(string FileName)
        {
            if (!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long user_id)) { return StatusCode(400); }
            var Source = await DB.SelectProfileImageUrl(user_id).ConfigureAwait(false);
            if (Source == null) { return StatusCode(404); }

            if (!ExtMime.TryGetContentType(FileName, out string mime)) { mime = "application/octet-stream"; };
            return await Download(Source.Value.Url, Source.Value.Referer, mime).ConfigureAwait(false);
        }

        //ファイルをダウンロードしてそのまんま返す
        async Task<IActionResult> Download(string Url, string Referer, string mime)
        {
            using (var req = new HttpRequestMessage(HttpMethod.Get, Url))
            {
                req.Headers.Referrer = new Uri(Referer);
                using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                {
                    if (res.IsSuccessStatusCode)
                    {

                        return File(await res.Content.ReadAsByteArrayAsync().ConfigureAwait(false), mime);
                    }
                    else { return StatusCode((int)res.StatusCode); }
                }
            }
        }

        [HttpGet("index")]
        public IActionResult Index()
        {
            return Content("ぬるぽ", "text/plain");
        }
    }
}