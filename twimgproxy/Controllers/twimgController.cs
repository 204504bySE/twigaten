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
using twitenlib;
using static twimgproxy.twimgStatic;

namespace twimgproxy.Controllers
{
    [Route("twimg")]
    public class twimgController : Controller
    {
        /// <summary>ツイ画像(:thumbサイズ)を鯖内→横流し で探して返す</summary>
        [HttpGet("thumb/{FileName}")]
        public async Task<IActionResult> thumb(string FileName)
        {
            if(!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long media_id)) { return StatusCode(400); }

            //まずは鯖内のファイルを探す 拡張子はリクエストURLを信頼して手を抜く
            string localmedia = MediaFolderPath.ThumbPath(media_id, FileName);
            if (System.IO.File.Exists(localmedia)) { return File(System.IO.File.OpenRead(localmedia), GetMime(FileName), true); }

            //鯖内にファイルがなかったのでtwitterから横流しする
            var MediaInfo = await DB.SelectThumbUrl(media_id).ConfigureAwait(false);
            if(MediaInfo == null) { return StatusCode(404); }
            
            var result = await Download(MediaInfo.Value.media_url + (MediaInfo.Value.media_url.IndexOf("twimg.com") >= 0 ? ":thumb" : ""),
                MediaInfo.Value.tweet_url, GetMime(FileName)).ConfigureAwait(false);
            if (result.Removed) { Removed.Enqueue(MediaInfo.Value.source_tweet_id); }
            return result.Result;
        }

        /// <summary>ユーザーのアイコンを鯖内→横流し で探して返す</summary>
        [HttpGet("profile_image/{FileName}")]
        public async Task<IActionResult> profile_image(string FileName)
        {
            if (!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long user_id)) { return StatusCode(400); }
            //まずは鯖内のファイルを探す 拡張子はリクエストURLを信頼して手を抜く
            //初期アイコンではないものとして探す
            string localmedia = MediaFolderPath.ProfileImagePath(user_id, false, FileName);
            if (System.IO.File.Exists(localmedia)) { return File(System.IO.File.OpenRead(localmedia), GetMime(FileName), true); }

            var Source = await DB.SelectProfileImageUrl(user_id).ConfigureAwait(false);
            if (Source == null) { return StatusCode(404); }
            //初期アイコンなら改めて鯖内を探す
            if (Source.Value.is_default_profile_image)
            {
                string defaulticon = MediaFolderPath.ProfileImagePath(user_id, true, Source.Value.Url);
                if (System.IO.File.Exists(defaulticon)) { return File(System.IO.File.OpenRead(defaulticon), GetMime(Source.Value.Url), true); }
            }
            //鯖内にファイルがなかったのでtwitterから横流しする
            return (await Download(Source.Value.Url, Source.Value.Referer, GetMime(FileName)).ConfigureAwait(false)).Result;
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
                    else { return (res.StatusCode == HttpStatusCode.NotFound
                                || res.StatusCode == HttpStatusCode.Gone
                                || res.StatusCode == HttpStatusCode.Forbidden,
                                StatusCode((int)res.StatusCode)); }
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