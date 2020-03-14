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
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web.Pages.Tweet
{
    /// <summary>
    /// ユーザーのタイムライン
    /// サインインした本人のやつしか見れない
    /// </summary>
    public class TimelineModel : PageModel
    {
        /// <summary>
        /// これより古いツイを検索する(SnowFlake)
        /// DateがセットされたらSnowFlakeに変換されてこれに突っ込む
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public long? Before { get; set; }
        /// <summary>
        /// これより新しいツイを検索する(SnowFlake)
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public long? After { get; set; }
        /// <summary>
        /// これより古いツイを検索する(UnixSeconds)
        /// これに値が入っていたらSnowFlakeにしてBeforeに入れる
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public long? Date { get; set; }

        public bool IsLatest => !Date.HasValue && !Before.HasValue && !After.HasValue;
        /// <summary>
        /// 「古い」ボタンにセットするSnowFlake(before=)
        /// </summary>
        public long? NextOld
        {
            get
            {
                if (Tweets.Length > 0) { return Tweets.Last().tweet.tweet_id; }
                else if (After.HasValue) { return After; }
                else { return null; }
            }
        }
        /// <summary>
        /// 「新しい」ボタンにセットするSnowFlake(after=)
        /// </summary>
        public long? NextNew
        {
            get
            {
                if (IsLatest) { return null; }
                else if (Tweets.Length > 0) { return Tweets.First().tweet.tweet_id; }
                else if (!Date.HasValue && Before.HasValue) { return Before; }
                else { return null; }
            }
        }

        public SimilarMediaTweet[] Tweets { get; private set; }
        public TweetData._user TargetUser { get; private set; }
        public TLUserParameters Params { get; private set; }


        public long QueryElapsedMilliseconds { get; private set; }
        public async Task<ActionResult> OnGetAsync()
        {
            var sw = Stopwatch.StartNew();

            //一瞬でも速くしたいので先にTaskを作って必要なところでawaitする
            Params = new TLUserParameters();
            var ParamsTask = Params.InitValidate(HttpContext);

            if (Date.HasValue) { Before = SnowFlake.SecondinSnowFlake(DateTimeOffset.FromUnixTimeSeconds(Date.Value), true); }
            long LastTweet = Before ?? After ?? SnowFlake.Now(true);
            bool IsBefore = Before.HasValue || !After.HasValue;

            await ParamsTask.ConfigureAwait(false);
            if (!Params.ID.HasValue) { return LocalRedirect("/"); }
            var TargetUserTask = DBView.SelectUser(Params.ID.Value);
            var TweetsTask = DBView.SimilarMediaTimeline(Params.ID.Value, Params.ID, LastTweet, Params.TLUser_Count, 3, Params.TLUser_RT, Params.TLUser_Show0, IsBefore);

            await Task.WhenAll(TargetUserTask, TweetsTask).ConfigureAwait(false);
            TargetUser = TargetUserTask.Result;
            Tweets = TweetsTask.Result;
            if (Tweets.Length == 0) { HttpContext.Response.StatusCode = StatusCodes.Status404NotFound; }
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
            return Page();
        }
    }
}