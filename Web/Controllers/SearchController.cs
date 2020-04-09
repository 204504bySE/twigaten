using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Twigaten.Lib;
using Twigaten.Web.Parameters;
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web.Controllers
{
    /// <summary>
    /// ここには「画像を検索」しかない
    /// 他の検索はPagesにある
    /// </summary>
    [Route("/search")]
    [ApiController]
    public class SearchController : ControllerBase
    {
        static readonly string HashServerCropUrl = Config.Instance.web.HashServerCropUrl;
        [HttpPost("media")]
        public async Task<ActionResult<long>> Media(IFormFile File)
        {
            var Params = new LoginParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);

            byte[] mem = new byte[File.Length];
            using (var memstream = new MemoryStream(mem))
            {
                await File.CopyToAsync(memstream);
            }
            long? hash = await PictHash.DCTHash(mem, HashServerCropUrl, File.FileName).ConfigureAwait(false);

            //見つからなかったりhashを計算できなかったりしたら検索ページに戻す
            if (hash == null) { return LocalRedirect("/search"); }
            var MatchMedia = await DBView.HashtoTweet(hash, Params.ID).ConfigureAwait(false);
            if (MatchMedia == null) { return LocalRedirect("/search"); }
            //その画像を含む最も古いツイートにリダイレクト
            return LocalRedirect("/tweet/" + MatchMedia.Value.tweet_id.ToString()+ "#" + MatchMedia.Value.media_id.ToString());
        }
    }
}