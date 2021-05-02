using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Html;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
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
            output.Attributes.SetAttribute("href", "https://twitter.com/intent/retweet?tweet_id=" + Tweet.tweet_id.ToString());
            output.Attributes.SetAttribute("rel", "nofollow noopener noreferrer");
            output.Attributes.SetAttribute("target", "_blank");
            output.Content.SetHtmlContent(@"<svg class=""twigaten-glyph fill-retweet""><use xlink:href=""/img/fontawesome.svg#retweet""/></svg>");
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

            output.Attributes.SetAttribute("href", "https://twitter.com/intent/favorite?tweet_id=" + Tweet.tweet_id.ToString());
            output.Attributes.SetAttribute("rel", "nofollow noopener noreferrer");
            output.Attributes.SetAttribute("target", "_blank");
            output.Content.SetHtmlContent(@"<svg class=""twigaten-glyph fill-star""><use xlink:href=""/img/fontawesome.svg#star""/></svg>");
        }
    }

    /// <summary>
    /// ツイートボタン(そのページ自体に対応する)
    /// </summary>
    [HtmlTargetElement("tweet-intent")]
    public class TweetIntentTagHelper : TagHelper
    {
        //https://github.com/aspnet/Mvc/issues/4744
        [ViewContext]
        public ViewContext ViewContext { get; set; }
        public string Path { get; set; }
        public string Fragment { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            var Request = ViewContext.HttpContext.Request;
            //リンク先の絶対URLを生成する
            var Builder = new UriBuilder
            {
                Scheme = Request.IsHttps ? "https" : "http",
                Host = Request.Host.Host,
                Port = Request.Host.Port.HasValue ? Request.Host.Port.Value : -1,
                Path = Path ?? Request.Path
            };
            if (!string.IsNullOrWhiteSpace(Fragment)) { Builder.Fragment = Fragment; }

            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;

            output.Attributes.SetAttribute("href", new HtmlString("https://twitter.com/intent/tweet?text="
                + Uri.EscapeDataString(ViewContext.ViewData["title"] + " - TwiGaTen")
                + "&url=" + Uri.EscapeDataString(Builder.Uri.AbsoluteUri)));
            output.Attributes.SetAttribute("rel", "nofollow noopener noreferrer");
            output.Attributes.SetAttribute("target", "_blank");
            output.Attributes.SetAttribute("class", "button is-outlined is-primary is-small");
            output.Content.SetHtmlContent(@"<img src=""/img/Twitter_bird_logo_2012.svg"" class=""twigaten-twitterbird"">" + Locale.Locale.TweetButton_Tweet);
        }
    }

    /// <summary>
    /// ツイートボタン(特定のツイートおよび画像対応する)
    /// </summary>
    [HtmlTargetElement("tweet-intent-onetweet")]
    public class TweetIntentOneTweetTagHelper : TagHelper
    {
        //https://github.com/aspnet/Mvc/issues/4744
        [ViewContext]
        public ViewContext ViewContext { get; set; }
        public TweetData._tweet Tweet { get; set; }
        public TweetData._media Media { get; set; }
        public bool More { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            var Request = ViewContext.HttpContext.Request;
            //リンク先の絶対URLを生成する
            var Builder = new UriBuilder
            {
                Scheme = Request.IsHttps ? "https" : "http",
                Host = Request.Host.Host,
                Port = Request.Host.Port ?? -1,
                Path = "/tweet/" + Tweet.tweet_id.ToString() + (More ? "/more" : ""),
                Fragment = Media.media_id.ToString()
            };

            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;

            output.Attributes.SetAttribute("href", new HtmlString("https://twitter.com/intent/tweet?text=" 
                + Uri.EscapeDataString(string.Format(Locale.Locale.Title_OneTweet, Tweet.user.screen_name) + " - TwiGaTen")
                + "&url=" + Uri.EscapeDataString(Builder.Uri.AbsoluteUri)));
            output.Attributes.SetAttribute("rel", "nofollow noopener noreferrer");
            output.Attributes.SetAttribute("target", "_blank");
            output.Attributes.SetAttribute("class", "button is-outlined is-primary is-small");
            output.Content.SetHtmlContent(@"<img src=""/img/Twitter_bird_logo_2012.svg"" class=""twigaten-twitterbird"">" + Locale.Locale.TweetButton_Tweet);
        }
    }
}
