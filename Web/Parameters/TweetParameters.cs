using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Twigaten.Web.DBHandler;
using static Twigaten.Web.DBHandler.DBView;

namespace Twigaten.Web.Parameters
{
    public abstract class SimilarMediaParameters : LoginParameters
    {
        ///<summary>URL</summary>
        public long? UserID { get; set; }
        ///<summary>URL (Format varies)</summary>
        public string Date { get; set; }
        public SimilarMediaParameters() : base() { }
        ///<summary>コピー用(LoginParameters分を除く)</summary>
        public SimilarMediaParameters(SimilarMediaParameters p)
        {
            UserID = p.UserID;
            Date = p.Date;
        }
    }

    public class FeaturedParameters : SimilarMediaParameters
    {
        ///<summary>URL > Cookie</summary>
        public TweetOrder? Order { get; set; }

        public override Task InitValidate(HttpContext _Context)
        {
            if (TryGetCookie(nameof(Order), out string OrderStr) && Enum.TryParse(typeof(TweetOrder), OrderStr, out var ParsedOrder))
            { Order = (TweetOrder)ParsedOrder; }
            else { Order = TweetOrder.Featured; }
            return base.InitValidate(_Context);
        }
        public FeaturedParameters() : base() { }
        ///<summary>コピー用</summary>
        public FeaturedParameters(FeaturedParameters p) : base(p)
        {
            Order = p.Order;
        }
    }

    public class OneTweetParameters : SimilarMediaParameters
    {
        ///<summary>URL</summary>
        public long TweetID { get; set; }
        ///<summary>URL</summary>
        public bool? More { get; set; }
        public override Task InitValidate(HttpContext _Context)
        {
            More = More ?? false;
            return base.InitValidate(_Context);
        }
        public OneTweetParameters() : base() { }
        ///<summary>コピー用(LoginParameters分を除く)</summary>
        public OneTweetParameters(OneTweetParameters p) : base(p)
        {
            TweetID = p.TweetID;
            More = p.More;
        }
    }

    public class TLUserParameters : SimilarMediaParameters
    {
        ///<summary>URL > Cookie</summary>
        public int? Count { get; set; }
        ///<summary>URL > Cookie</summary>
        public bool? RT { get; set; }
        ///<summary>URL > Cookie</summary>
        public bool? Show0 { get; set; }
        ///<summary>URL</summary>
        public long? Before { get; set; }
        ///<summary>URL</summary>
        public long? After { get; set; }

        public override Task InitValidate(HttpContext _Context)
        {
            if (TryGetCookie(nameof(Count), out string CountStr) && int.TryParse(CountStr, out int CountValue)) 
            {
                Count = CountValue;
                if (Count > 50) { Count = 50; }
                if (Count < 10) { Count = 10; }
            }
            else { Count = 10; }

            if (TryGetCookie(nameof(RT), out string RTStr) && bool.TryParse(RTStr, out bool RTValue)) { RT = RTValue; }
            else { RT = true; }

            if (TryGetCookie(nameof(Show0), out string Show0Str) && bool.TryParse(Show0Str, out bool Show0Value)) { Show0 = Show0Value; }
            else { Show0 = false; }

            return base.InitValidate(_Context);
        }
        public TLUserParameters() : base() { }
        ///<summary>コピー用(LoginParameters分を除く)</summary>
        public TLUserParameters(TLUserParameters p) : base(p)
        {
            Count = p.Count;
            RT = p.RT;
            Show0 = p.Show0;
            Before = p.Before;
            After = p.After;
        }
    }
}
