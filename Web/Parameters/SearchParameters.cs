using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using static Twigaten.Web.DBHandler.DBView;

namespace Twigaten.Web.Parameters
{
    public class UserSearchParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public SelectUserLikeMode? UserLikeMode { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(UserLikeMode), out string ModeStr) && Enum.TryParse(typeof(SelectUserLikeMode), ModeStr, out var Parsed))
            { UserLikeMode = (SelectUserLikeMode)Parsed; }
            else { UserLikeMode = SelectUserLikeMode.Show; }
        }
        public UserSearchParameters() : base() { }
    }
}
