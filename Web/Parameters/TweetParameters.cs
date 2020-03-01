using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Twigaten.Web.DBHandler;
using static Twigaten.Web.DBHandler.DBView;

namespace Twigaten.Web.Parameters
{
    public class FeaturedParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public TweetOrder? Order { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(Order), out string OrderStr) && Enum.TryParse(typeof(TweetOrder), OrderStr, out var ParsedOrder))
            { Order = (TweetOrder)ParsedOrder; }
            else { Order = TweetOrder.Featured; }
        }
        public FeaturedParameters() : base() { }
    }

    public class TLUserParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public int Count { get; set; }
        ///<summary>Cookie</summary>
        public bool RT { get; set; }
        ///<summary>Cookie</summary>
        public bool Show0 { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(Count), out string CountStr) && int.TryParse(CountStr, out int CountValue)) 
            {
                Count = CountValue;
                if (Count > 50) { Count = 50; }
                else if (Count < 10) { Count = 10; }
            }
            else { Count = 10; }

            if (TryGetCookie(nameof(RT), out string RTStr) && bool.TryParse(RTStr, out bool RTValue)) { RT = RTValue; }
            else { RT = true; }

            if (TryGetCookie(nameof(Show0), out string Show0Str) && bool.TryParse(Show0Str, out bool Show0Value)) { Show0 = Show0Value; }
            else { Show0 = false; }
        }
        public TLUserParameters() : base() { }
    }
}
