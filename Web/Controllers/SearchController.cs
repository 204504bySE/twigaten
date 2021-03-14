using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Twigaten.Lib;
using Twigaten.Lib.DctHash;
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
        static readonly DctHashClient PictHash = new(Config.Instance.web.HashServerCropHost, Config.Instance.web.HashServerCropPort);

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
            long? hash = await PictHash.DCTHashCrop(mem).ConfigureAwait(false);

            //見つからなかったりhashを計算できなかったりしたら検索ページに戻す
            if (hash == null)
            {
                HttpContext.Response.Headers.Add("Location", "/search");
                return StatusCode(StatusCodes.Status303SeeOther);
            }
            var MatchMedia = await View.HashtoTweet(hash, Params.ID).ConfigureAwait(false);
            if (MatchMedia == null) 
            {
                HttpContext.Response.Headers.Add("Location", "/search");
                return StatusCode(StatusCodes.Status303SeeOther);
            }
            //その画像を含む最も古いツイートにリダイレクト
            HttpContext.Response.Headers.Add("Location", "/tweet/" + MatchMedia.Value.tweet_id.ToString()+ "#" + MatchMedia.Value.media_id.ToString());
            return StatusCode(StatusCodes.Status303SeeOther);
        }
    }
}