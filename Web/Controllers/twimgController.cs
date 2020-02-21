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
using Twigaten.Lib;
using static Twigaten.Web.twimgStatic;

namespace Twigaten.Web.Controllers
{
    [Route("twimg")]
    public class twimgController : Controller
    {
        /// <summary>ツイ画像(:thumbサイズ)を鯖内 > 横流し で探して返す</summary>
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
            
            var ret = await Download(MediaInfo.Value.media_url + (MediaInfo.Value.media_url.IndexOf("twimg.com") >= 0 ? ":thumb" : ""),
                MediaInfo.Value.tweet_url).ConfigureAwait(false);
            if (RemovedStatusCode(ret.StatusCode)) { Removed.Enqueue(MediaInfo.Value.source_tweet_id); }
            if (ret.FileBytes != null)
            {
                //画像の取得に成功したわけだし保存しておきたい
                StoreMediaBlock.Post((MediaInfo.Value, ret.FileBytes));

                return File(ret.FileBytes, GetMime(FileName));
            }
            else { return StatusCode((int)ret.StatusCode); }
        }

        /// <summary>ユーザーのアイコンを鯖内 > 横流し で探して返す</summary>
        [HttpGet("profile_image/{FileName}")]
        public async Task<IActionResult> profile_image(string FileName)
        {
            if (!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long user_id)) { return StatusCode(400); }
            //まずは鯖内のファイルを探す 拡張子はリクエストURLを信頼して手を抜く
            //初期アイコンではないものとして探す
            string localmedia = MediaFolderPath.ProfileImagePath(user_id, false, FileName);
            if (System.IO.File.Exists(localmedia)) { return File(System.IO.File.OpenRead(localmedia), GetMime(FileName), true); }

            var ProfileImageInfo = await DB.SelectProfileImageUrl(user_id).ConfigureAwait(false);
            if (ProfileImageInfo == null) { return StatusCode(404); }
            //初期アイコンなら改めて鯖内を探す
            if (ProfileImageInfo.Value.is_default_profile_image)
            {
                string defaulticon = MediaFolderPath.ProfileImagePath(user_id, true, ProfileImageInfo.Value.profile_image_url);
                if (System.IO.File.Exists(defaulticon)) { return File(System.IO.File.OpenRead(defaulticon), GetMime(ProfileImageInfo.Value.profile_image_url), true); }
            }
            //鯖内にファイルがなかったのでtwitterから横流しする
            var ret = await Download(ProfileImageInfo.Value.profile_image_url, ProfileImageInfo.Value.tweet_url).ConfigureAwait(false);
            if (ret.FileBytes != null)
            {
                //画像の取得に成功したわけだし保存しておきたい
                //初期アイコンはいろいろ面倒なのでここではやらない
                if (!ProfileImageInfo.Value.is_default_profile_image)
                {
                    StoreProfileImageBlock.Post((ProfileImageInfo.Value, ret.FileBytes));
                }
                return File(ret.FileBytes, GetMime(FileName));
            }
            else { return StatusCode((int)ret.StatusCode); }
        }

        ///<summary>ファイルをダウンロードしてそのまんま返す
        ///ダウンロードに失敗したらFileBytesはnull</summary>
        async Task<(HttpStatusCode StatusCode, byte[] FileBytes)> Download(string Url, string Referer)
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
                        return (res.StatusCode, await res.Content.ReadAsByteArrayAsync().ConfigureAwait(false));
                    }
                    else { return (res.StatusCode, null); }
                }
            }
        }

        /// <summary>
        /// このステータスコードがTwitterから返ってきたらその画像は削除されたとみなす
        /// </summary>
        /// <param name="StatusCode"></param>
        /// <returns></returns>
        bool RemovedStatusCode(HttpStatusCode StatusCode)
        {
            return StatusCode == HttpStatusCode.NotFound
                || StatusCode == HttpStatusCode.Gone
                || StatusCode == HttpStatusCode.Forbidden;
        }

        [HttpGet("index")]
        public IActionResult Index()
        {
            return Content("ぬるぽ", "text/plain");
        }
    }
}