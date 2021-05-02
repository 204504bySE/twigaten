using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using static Twigaten.Web.DBHandler;

namespace Twigaten.Web.Parameters
{
    public class FeaturedParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public TweetOrder? Featured_Order { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(Featured_Order), out string OrderStr) && Enum.TryParse(typeof(TweetOrder), OrderStr, out var ParsedOrder))
            { Featured_Order = (TweetOrder)ParsedOrder; }
            else { Featured_Order = TweetOrder.Featured; }
        }
        public FeaturedParameters() : base() { }
    }

    public class TLUserParameters : LoginParameters
    {
        ///<summary>Cookie</summary>
        public int TLUser_Count { get; set; }
        ///<summary>Cookie</summary>
        public bool TLUser_RT { get; set; }
        ///<summary>Cookie</summary>
        public bool TLUser_Show0 { get; set; }

        public override async Task InitValidate(HttpContext _Context)
        {
            await base.InitValidate(_Context).ConfigureAwait(false);
            if (TryGetCookie(nameof(TLUser_Count), out string CountStr) && int.TryParse(CountStr, out int CountValue)) 
            {
                TLUser_Count = CountValue;
                if (TLUser_Count > 50) { TLUser_Count = 50; }
                else if (TLUser_Count < 10) { TLUser_Count = 10; }
            }
            else { TLUser_Count = 10; }

            if (TryGetCookie(nameof(TLUser_RT), out string RTStr) && bool.TryParse(RTStr, out bool RTValue)) { TLUser_RT = RTValue; }
            else { TLUser_RT = true; }

            if (TryGetCookie(nameof(TLUser_Show0), out string TLUser_Show0Str) && bool.TryParse(TLUser_Show0Str, out bool TLUser_Show0Value)) { TLUser_Show0 = TLUser_Show0Value; }
            else { TLUser_Show0 = false; }
        }
        public TLUserParameters() : base() { }
    }
}
