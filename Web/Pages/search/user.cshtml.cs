using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Twigaten.Web.Parameters;
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web
{
    public class UserModel : PageModel
    {
        static readonly Regex ScreenNameRegex = new Regex(@"(?<=twitter\.com\/|@|^)[_\w]+(?=[\/_\w]*$)", RegexOptions.Compiled);

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
        public string ScreenName { get; private set; }
        public int Limit { get; private set; }
        public long QueryElapsedMilliseconds { get; private set; }

        public async Task<IActionResult> OnGetAsync()
        {
            if (string.IsNullOrWhiteSpace(Q)) { return LocalRedirect("/search/"); }
            ScreenName = ScreenNameRegex.Match(Q.Trim()).Value;
            if (string.IsNullOrWhiteSpace(ScreenName)) { return LocalRedirect("/search/"); }
            long? TargetUserID = await DBView.SelectID_Unique_screen_name(ScreenName).ConfigureAwait(false);
            if (Direct != false && TargetUserID.HasValue) { return LocalRedirect("/users/" + TargetUserID.Value.ToString()); }

            var sw = Stopwatch.StartNew();
            Params = new UserSearchParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);
            Limit = 100;
            //screen_nameを前方一致検索する
            Users = await DBView.SelectUserLike(ScreenName.Replace(' ', '%').Replace("_", @"\_") + "%", Params.ID, Params.UserSearch_LikeMode.Value, Limit).ConfigureAwait(false);
            QueryElapsedMilliseconds = sw.ElapsedMilliseconds;

            return Page(); 
        }
    }
}