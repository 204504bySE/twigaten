using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Twigaten.Web.DBHandler;
using Twigaten.Web.Parameters;
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web.Pages.Tweet
{
    public class TweetModel : PageModel
    {
        [BindProperty(SupportsGet = true)]
        public long TweetId{ get; set; }
        [BindProperty(SupportsGet = true)]
        public string MoreStr { get; set; }
        public bool More => MoreStr == "more";
        /// <summary>
        /// 検索エンジン向けのツイートURL(このツイと同じ画像がある一番古いツイ, 別に1個だけあるときのみ)
        /// </summary>
        public long? CanonicalTweetId { get; set; }

        public SimilarMediaTweet[] Tweets;
        public LoginParameters Params;

        public long QueryElapsedMilliseconds { get; private set; }
        public async Task<ActionResult> OnGetAsync()
        {
            var sw = Stopwatch.StartNew();
            var RetweetTask = View.SourceTweetRT(TweetId);
            Params = new LoginParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);

            //RTなら元ツイートにリダイレクトする
            await RetweetTask.ConfigureAwait(false);
            if (RetweetTask.Result.HasValue) { return LocalRedirectPermanent("/tweet/" + RetweetTask.Result.Value.ToString() + (More ? "/more" : "")); }
            
            if (More) { Tweets = await View.SimilarMediaTweet(TweetId, Params.ID, 99).ConfigureAwait(false); }
            else { Tweets = await View.SimilarMediaTweet(TweetId, Params.ID).ConfigureAwait(false); }

            if(Tweets.Length == 0) { HttpContext.Response.StatusCode = StatusCodes.Status404NotFound; }
            else
            {
                //CanonicalTweetIdを探す
                var OldestIds = Tweets.Where(t =>
                {
                    var oldestSimilar = t.Similars.FirstOrDefault();
                    return oldestSimilar != null && oldestSimilar.tweet.created_at < t.tweet.created_at;
                }).Select(t => t.Similars.First().tweet.tweet_id)
                    .ToArray();
                if (OldestIds.Length == Tweets.Length)
                {
                    long OldestId = OldestIds[0];
                    if (OldestIds.All(id => id == OldestId)) { CanonicalTweetId = OldestId; }
                }
            }
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
            return Page();
        }
    }
}