using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using static Twigaten.Web.DBHandler;

namespace Twigaten.Web.Parameters
{
    public class UserSearchParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public SelectUserLikeMode? UserSearch_LikeMode { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(UserSearch_LikeMode), out string ModeStr) && Enum.TryParse(typeof(SelectUserLikeMode), ModeStr, out var Parsed))
            { UserSearch_LikeMode = (SelectUserLikeMode)Parsed; }
            else { UserSearch_LikeMode = SelectUserLikeMode.Show; }
        }
        public UserSearchParameters() : base() { }
    }
}
