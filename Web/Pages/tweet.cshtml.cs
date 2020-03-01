using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
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

        public SimilarMediaTweet[] Tweets;
        public LoginParameters Params;

        public long QueryElapsedMilliseconds { get; private set; }
        public async Task OnGetAsync()
        {
            Params = new LoginParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();
            if (More) { Tweets = await DBView.SimilarMediaTweet(TweetId, Params.ID, 99).ConfigureAwait(false); }
            else { Tweets = await DBView.SimilarMediaTweet(TweetId, Params.ID).ConfigureAwait(false); }
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;
        }
    }
}