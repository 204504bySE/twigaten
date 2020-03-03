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
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", "https://twitter.com/" + Tweet.user.screen_name + @"/status/" + Tweet.tweet_id.ToString());
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Content.SetHtmlContent(@"<img src=""/img/Twitter_bird_logo_2012.svg"" class=""twigaten-twitterbird"">");
        }
    }

    /// <summary>
    /// 投稿時刻→ツイートへのTwigaten内リンク
    /// 時刻を表示させるにはdata-unixtimeを読んで時刻に変換するJavaScriptが必要
    /// </summary>
    [HtmlTargetElement("tweet-date-link")]
    public class TweetDateLinkTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.Add("href", @"/users/" + Tweet.tweet_id.ToString());
            output.Attributes.SetAttribute("class", "twigaten-unixtime");
            output.Attributes.Add("data-unixtime", Tweet.created_at.ToUnixTimeSeconds());
        }
    }

    /// <summary>
    /// 投稿時刻のテキスト(リンクなし)
    /// 時刻を表示させるにはdata-unixtimeを読んで時刻に変換するJavaScriptが必要
    /// </summary>
    [HtmlTargetElement("tweet-date-text")]
    public class TweetDateTextTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "span";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "text_muted twigaten-unixtime");
            output.Attributes.SetAttribute("data-unixtime", Tweet.created_at.ToUnixTimeSeconds());
        }
    }
}
