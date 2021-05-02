using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Twigaten.Web.Parameters;

namespace Twigaten.Web
{
    public class SearchUserModel : PageModel
    {
        static readonly Regex ScreenNameRegex = new Regex(@"(?<=twitter\.com\/|@|^)[_\w]+(?=$|\/)", RegexOptions.Compiled);
        static readonly DBHandler DB = DBHandler.Instance;
        /// <summary>
        /// 検索文字列
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public string Q { get; set; }
        /// <summary>
        /// 完全一致するアカウントがいたらそっちに飛ばす nullもtrueとみなす
        /// </summary>
        [BindProperty(SupportsGet = true)]
        public bool? Direct { get; set; }
        public UserSearchParameters Params { get; private set; }

        public TweetData._user[] Users { get; private set; }
        public string SearchScreenName { get; private set; }
        public int Limit { get; private set; }
        public long QueryElapsedMilliseconds { get; private set; }

        public async Task<IActionResult> OnGetAsync()
        {
            if (string.IsNullOrWhiteSpace(Q)) { return LocalRedirect("/search/"); }
            SearchScreenName = ScreenNameRegex.Match(Q.Trim()).Value;
            if (string.IsNullOrWhiteSpace(SearchScreenName)) 
            {
                HttpContext.Response.Headers.Add("Location", "/search");
                return StatusCode(StatusCodes.Status303SeeOther);
            }
            long? TargetUserID = await DBHandler.Instance.SelectID_Unique_screen_name(SearchScreenName).ConfigureAwait(false);
            if (Direct != false && TargetUserID.HasValue) 
            {
                HttpContext.Response.Headers.Add("Location", "/users/" + TargetUserID.Value.ToString());
                return StatusCode(StatusCodes.Status303SeeOther);
            }

            var sw = Stopwatch.StartNew();
            Params = new UserSearchParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);
            Limit = 100;
            //screen_nameを前方一致検索する
            Users = await DB.SelectUserLike(SearchScreenName.Replace(' ', '%').Replace("_", @"\_") + "%", Params.ID, Params.UserSearch_LikeMode.Value, Limit).ConfigureAwait(false);
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;

            return Page(); 
        }
    }
}