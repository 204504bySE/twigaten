using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// リツイートボタン
    /// </summary>
    [HtmlTargetElement("retweet-intent")]
    public class RetweetIntentTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", $"https://twitter.com/intent/retweet?tweet_id={Tweet.tweet_id}");
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Content.SetHtmlContent(@"<span class=""glyphicon glyphicon-retweet""></span>");
        }
    }

    /// <summary>
    /// ふぁぼボタン
    /// </summary>
    [HtmlTargetElement("favorite-intent")]
    public class FavoriteIntentTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", $"https://twitter.com/intent/favorite?tweet_id={Tweet.tweet_id}");
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Content.SetHtmlContent(@"<span class=""glyphicon glyphicon-star""></span>");
        }
    }
}
