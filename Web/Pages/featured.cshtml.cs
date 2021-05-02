using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Twigaten.Lib;
using Twigaten.Web.Parameters;

namespace Twigaten.Web.Pages.Tweet
{
    /// <summary>
    /// ユーザーのツイート一覧
    /// </summary>
    public class FeaturedModel : PageModel
    {
        static readonly DBHandler DB = DBHandler.Instance;

        /// <summary>
        /// これより古いツイを検索する(UnixSeconds)
        /// これに値が入っていたらSnowFlakeにしてBeforeに入れる
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public long? Date { get; set; }

        public bool IsLatest => !Date.HasValue;
        /// <summary>
        /// 「人気のツイート」ではUNIX秒
        /// 1時間前くらいの値を返す
        /// </summary>
        public long? NextOld
        {
            get
            {
                long ThisSeconds = Date ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                if ((ThisSeconds - 3600) * 1000 < SnowFlake.TwEpoch) { return null as long?; }
                else if (ThisSeconds % 3600 == 0) { return ThisSeconds - 3600; }
                else { return ThisSeconds / 3600 * 3600; }
            }
        }
        /// <summary>
        /// 「人気のツイート」ではUNIX秒
        /// 1時間後くらいの値を返す
        /// </summary>
        public long? NextNew => Date.HasValue && 3600 <= DateTimeOffset.UtcNow.ToUnixTimeSeconds() - Date.Value
            ? Date.Value / 3600 * 3600 + 3600
            : null as long?;

        public SimilarMediaTweet[] Tweets { get; private set; }
        public TweetData._user TargetUser { get; private set; }
        public FeaturedParameters Params { get; private set; }

        public long QueryElapsedMilliseconds { get; private set; }
        public async Task OnGetAsync()
        {
            var sw = Stopwatch.StartNew();

            //一瞬でも速くしたいので先にTaskを作って必要なところでawaitする
            Params = new FeaturedParameters();
            var ParamsTask = Params.InitValidate(HttpContext);

            var ThisDate = Date.HasValue ? DateTimeOffset.FromUnixTimeSeconds(Date.Value) : DateTimeOffset.UtcNow;

            await ParamsTask.ConfigureAwait(false);
            Tweets = await DB.SimilarMediaFeatured(3, SnowFlake.SecondinSnowFlake(ThisDate - TimeSpan.FromHours(1), false), SnowFlake.SecondinSnowFlake(ThisDate, true), Params.Featured_Order.Value).ConfigureAwait(false);
            if (Tweets.Length == 0) { HttpContext.Response.StatusCode = StatusCodes.Status404NotFound; }
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}