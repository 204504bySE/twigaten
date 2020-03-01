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

        public SimilarMediaTweet[] Tweets;
        public TLUserParameters Params;

        public long QueryElapsedMilliseconds { get; private set; }
        public async Task OnGetAsync()
        {
            Params = new TLUserParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();            
            long LastTweet = Date.HasValue 
                ? SnowFlake.SecondinSnowFlake(DateTimeOffset.FromUnixTimeMilliseconds(Date.Value), true)
                : ( Before ?? After ?? SnowFlake.Now(true));
            bool IsBefore = Date.HasValue || Before.HasValue || !After.HasValue;

            Tweets = await DBView.SimilarMediaUser(UserId, Params.ID, LastTweet, Params.Count, 3, Params.RT, Params.Show0, IsBefore).ConfigureAwait(false);
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}