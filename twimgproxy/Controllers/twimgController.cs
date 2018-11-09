using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.StaticFiles;
using static twimgproxy.twimgStatic;

namespace twimgproxy.Controllers
{
    [Route("twimg")]
    public class twimgController : Controller
    {

        [HttpGet("thumb/{FileName}")]
        public async Task<IActionResult> thumb(string FileName)
        {
            if(!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long media_id)) { return StatusCode(400); }
            var MediaInfo = await DB.SelectThumbUrl(media_id).ConfigureAwait(false);
            if(MediaInfo == null) { return StatusCode(404); }

            if (!ExtMime.TryGetContentType(FileName, out string mime)) { mime = "application/octet-stream"; };
            var result = await Download(MediaInfo.Value.media_url + (MediaInfo.Value.media_url.IndexOf("twimg.com") >= 0 ? ":thumb" : ""), MediaInfo.Value.tweet_url, mime).ConfigureAwait(false);
            if (result.Removed) { Removed.RemoveTweetQueue.Post(MediaInfo.Value.source_tweet_id); }
            return result.Result;
        }

        [HttpGet("profile_image/{FileName}")]
        public async Task<IActionResult> profile_image(string FileName)
        {
            if (!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long user_id)) { return StatusCode(400); }
            var Source = await DB.SelectProfileImageUrl(user_id).ConfigureAwait(false);
            if (Source == null) { return StatusCode(404); }

            if (!ExtMime.TryGetContentType(FileName, out string mime)) { mime = "application/octet-stream"; };
            return (await Download(Source.Value.Url, Source.Value.Referer, mime).ConfigureAwait(false)).Result;
        }

        ///<summary>ファイルをダウンロードしてそのまんま返す
        ///Removedは404と410で判定</summary>
        async Task<(bool Removed, IActionResult Result)> Download(string Url, string Referer, string mime)
        {
            Counter.MediaTotal.Increment();
            using (var req = new HttpRequestMessage(HttpMethod.Get, Url))
            {
                req.Headers.Referrer = new Uri(Referer);
                using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                {
                    if (res.IsSuccessStatusCode)
                    {
                        Counter.MediaSuccess.Increment();
                        return (false, File(await res.Content.ReadAsByteArrayAsync().ConfigureAwait(false), mime));
                    }
                    else { return (res.StatusCode == HttpStatusCode.NotFound || res.StatusCode == HttpStatusCode.Gone, StatusCode((int)res.StatusCode)); }
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