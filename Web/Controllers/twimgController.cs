using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Twigaten.Lib;
using static Twigaten.Web.twimgStatic;

namespace Twigaten.Web.Controllers
{
    [Route("twimg")]
    public class TwimgController : ControllerBase
    {
        readonly IWebHostEnvironment _WebHostEnvironment;
        /// <summary>
        /// DIでIWebHostEnvironmentを受け取る(wwwrootなどが取れる)
        /// </summary>
        /// <param name="environment"></param>
        public TwimgController(IWebHostEnvironment environment)
        {
            _WebHostEnvironment = environment;

            //profile_image_card用の画像をここで読み込んでおく
            if (FrameImage == null) { FrameImage = Image.FromFile(Path.Combine(_WebHostEnvironment.WebRootPath, "img/tenframe.png")); }
        }


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

        /// <summary>ユーザーのアイコンを探して返す</summary>
        [HttpGet("profile_image/{FileName}")]
        public async Task<IActionResult> profile_image(string FileName)
        {
            //変な文字列を突っ込まれてたときの安易な対策
            FileName = Path.GetFileName(FileName);

            var Icon = await FindProfileImage(FileName).ConfigureAwait(false);
            if (Icon.Data != null) { return File(Icon.Data, GetMime(FileName), true); }
            else { return StatusCode(Icon.StatusCode); }
        }

        /// <summary>
        /// ユーザーのアイコンをはめ込む額縁画像(コンストラクタで読み込む)
        /// </summary>
        static Image FrameImage;
        /// <summary>ユーザーのアイコンを探して返す</summary>
        [HttpGet("profile_image/{FileName}/card.png")]
        public async Task<IActionResult> profile_image_card(string FileName)
        {
            //変な文字列を突っ込まれてたときの安易な対策
            FileName = Path.GetFileName(FileName);

            var Icon = await FindProfileImage(FileName).ConfigureAwait(false);
            if (Icon.Data != null)
            {
                //額縁画像をコピーして使う
                using (var Frame = new Bitmap(FrameImage))  //これでコピーされる
                using (var mem = new MemoryStream(Icon.Data))
                using (var IconImage = Image.FromStream(mem))
                using (var g = Graphics.FromImage(Frame))
                using (var ret = new MemoryStream())
                {
                    //額縁っぽいやつの中心にアイコンを描く
                    g.DrawImage(IconImage, (Frame.Width - IconImage.Width) >> 1, (Frame.Height - IconImage.Height) >> 1);
                    Frame.Save(ret, ImageFormat.Png);
                    return File(ret.ToArray(), GetMime("card.png"));
                }
            }
            //アイコンが見つからないときは額縁画像を返す
            else { return LocalRedirect("/img/tenframe.png"); }
        }

        /// <summary>
        /// ユーザーのアイコンを実際に探す
        /// 鯖内 > 横流し の優先度
        /// </summary>
        /// <param name="FileName">アカウントID.拡張子</param>
        /// <returns></returns>
        async Task<(int StatusCode, byte[] Data)> FindProfileImage(string FileName)
        {

            //ファイル名の先頭が"_"だったら初期アイコンのURLとみなす
            if (FileName.StartsWith('_'))
            {
                string trimmedname = MediaFolderPath.DefaultProfileImagePath(FileName.Substring(1));
                if (System.IO.File.Exists(MediaFolderPath.DefaultProfileImagePath(trimmedname))) 
                {
                    using (var file = System.IO.File.OpenRead(trimmedname))
                    using (var mem = new MemoryStream())
                    {
                        await file.CopyToAsync(mem).ConfigureAwait(false);
                        return (StatusCodes.Status200OK, mem.ToArray());
                    }
                }
                else { return (StatusCodes.Status404NotFound, null); }
            }

            //↑以外はuser_idとみなす
            if (!long.TryParse(Path.GetFileNameWithoutExtension(FileName), out long user_id)) { return (StatusCodes.Status400BadRequest, null); }

            //まずは鯖内のファイルを探す 拡張子はリクエストURLを信頼して手を抜く
            //初期アイコンではないものとして探す
            string localmedia = MediaFolderPath.ProfileImagePath(user_id, false, FileName);
            if (System.IO.File.Exists(localmedia)) 
            {
                using (var file = System.IO.File.OpenRead(localmedia))
                using (var mem = new MemoryStream())
                {
                    await file.CopyToAsync(mem).ConfigureAwait(false);
                    return (StatusCodes.Status200OK, mem.ToArray());
                }
            }

            var ProfileImageInfo = await DB.SelectProfileImageUrl(user_id).ConfigureAwait(false);
            if (ProfileImageInfo == null) { return (StatusCodes.Status404NotFound, null); }
            //初期アイコンなら改めて鯖内を探す(通常はここには来ない)
            if (ProfileImageInfo.Value.is_default_profile_image)
            {
                string defaulticon = MediaFolderPath.ProfileImagePath(user_id, true, ProfileImageInfo.Value.profile_image_url);
                if (System.IO.File.Exists(defaulticon)) 
                {
                    using (var file = System.IO.File.OpenRead(defaulticon))
                    using (var mem = new MemoryStream())
                    {
                        await file.CopyToAsync(mem).ConfigureAwait(false);
                        return (StatusCodes.Status200OK, mem.ToArray());
                    }
                }
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
                return (StatusCodes.Status200OK, ret.FileBytes);
            }
            else { return ((int)ret.StatusCode, null); }
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
    }
}