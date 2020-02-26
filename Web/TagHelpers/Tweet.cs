using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Html;
using Microsoft.AspNetCore.Razor.Runtime.TagHelpers;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// ツイートへの直リン
    /// screen-nameおよびtweet-idを指定する
    /// </summary>
    [HtmlTargetElement("tweet-permalink")]
    public class TweetPermalinkTagHelper : TagHelper
    {
        public long TweetId { get; set; }
        public string ScreenName { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;

            string UrlText = "https://twitter.com/" + ScreenName + "/status/" + TweetId.ToString();
            output.Attributes.SetAttribute("href", UrlText);
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Content.SetContent(UrlText);
        }
    }

    /// <summary>
    /// 鳥アイコンをクリックするとTwitterでそのツイを見れるやつ
    /// </summary>
    [HtmlTargetElement("tweet-permalink-bird")]
    public class TweetPermalinkBirdTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "span";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "twigaten:twitterbird");
            output.Content.SetHtmlContent(@"<a href=""https://twitter.com/" + Tweet.user.screen_name + @"/status/" + Tweet.tweet_id.ToString() + @""" rel=""nofollow""><img src=""/Content/images/Twitter_bird_logo_2012.svg"" width=""16"" height=""16"" /></a>");
        }
    }

    [HtmlTargetElement("tweet-date-link")]
    public class TweetDateLinkTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a-unixtime";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.Add("href", @"https://twitter.com/" + Tweet.user.screen_name + @"/status/" + Tweet.tweet_id.ToString());
            output.Attributes.Add("unixtime", Tweet.created_at.ToUnixTimeSeconds());
        }
    }

    [HtmlTargetElement("tweet-date-text")]
    public class TweetDateTextTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "span";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "text_muted");
            output.Attributes.SetAttribute("onload", new HtmlString(@"(function(e) => { e.innerHtml=(new Date("
                + (Tweet.created_at.ToUnixTimeSeconds() * 1000).ToString()
                + @") + new Date().getTimezoneOffset()).toLocaleDateString() })(this);"));
        }
    }
}
