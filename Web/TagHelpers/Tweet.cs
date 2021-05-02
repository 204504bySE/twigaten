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
            output.Attributes.SetAttribute("target", "_blank");
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
        public TweetData._tweet MainTweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", @"/tweet/" + Tweet.tweet_id.ToString());
            output.Attributes.SetAttribute("class", "twigaten-unixtime twigaten-cookie-click"
                + (Tweet.created_at < MainTweet?.created_at ? " has-text-weight-bold" : ""));
            output.Attributes.SetAttribute("data-unixtime", Tweet.created_at.ToUnixTimeSeconds());
            if (Tweet.user.isprotected) { output.Attributes.SetAttribute("rel", "nofollow"); }
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

    /// <summary>
    /// 「もっと見る」「別ページで見る」ボタン
    /// TweetにはRTじゃなくて元ツイートを渡す
    /// </summary>
    [HtmlTargetElement("tweet-more-button")]
    public class TweetMoreButtonTagHelper : TagHelper
    {
        public TweetData._tweet Tweet { get; set; }
        public TweetData._media Media { get; set; }
        public bool IsMore { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", "/tweet/" + Tweet.tweet_id.ToString() + (IsMore ? "/more#" : "#") + Media.media_id.ToString());
            output.Attributes.SetAttribute("class", "button button-viewmore " + (IsMore ? "is-primary" : "is-light"));
            if (Tweet.user.isprotected) { output.Attributes.SetAttribute("rel", "nofollow"); }
            output.Content.SetContent(IsMore ? Locale.Locale.SimilarMedia_ViewMore : Locale.Locale.SimilarMedia_SeparatePage);
        }
    }
}
