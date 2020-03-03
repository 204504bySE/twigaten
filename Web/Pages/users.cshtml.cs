using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Twigaten.Lib;
using Twigaten.Web.Parameters;
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web.Pages.Tweet
{
    public class UsersModel : PageModel
    {
        [BindProperty(SupportsGet = true)]
        public long UserId { get; set; }
        [BindProperty(SupportsGet = true)]
        public long? Before { get; set; }
        [BindProperty(SupportsGet = true)]
        public long? After { get; set; }
        [BindProperty(SupportsGet = true)]
        public long? Date { get; set; }

        public bool IsLatest => !Date.HasValue && !Before.HasValue && !After.HasValue;
        public long? NextOld => (Tweets.Length > 0) ? Tweets.Last().tweet.tweet_id : null as long?;
        public long? NextNew => (!IsLatest && Tweets.Length > 0) ? Tweets.First().tweet.tweet_id : null as long?;

        public SimilarMediaTweet[] Tweets { get; private set; }
        public TweetData._user TargetUser { get; private set; }
        public TLUserParameters Params { get; private set; }


        public long QueryElapsedMilliseconds { get; private set; }
        public async Task OnGetAsync()
        {
            var sw = Stopwatch.StartNew();

            //一瞬でも速くしたいので先にTaskを作って必要なところでawaitする
            var TargetUserTask = DBView.SelectUser(UserId);
            Params = new TLUserParameters();
            var ParamsTask = Params.InitValidate(HttpContext);

            long LastTweet = Date.HasValue 
                ? SnowFlake.SecondinSnowFlake(DateTimeOffset.FromUnixTimeMilliseconds(Date.Value), true)
                : ( Before ?? After ?? SnowFlake.Now(true));
            bool IsBefore = Date.HasValue || Before.HasValue || !After.HasValue;

            await ParamsTask.ConfigureAwait(false);
            var TweetsTask = DBView.SimilarMediaUser(UserId, Params.ID, LastTweet, Params.Count, 3, Params.RT, Params.Show0, IsBefore);

            await Task.WhenAll(TargetUserTask, TweetsTask).ConfigureAwait(false);
            TargetUser = TargetUserTask.Result;
            Tweets = TweetsTask.Result;
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}