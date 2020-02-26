using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Razor.Runtime.TagHelpers;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// Tweet Button 共有URLは path(/tweet/1234567890 など) で指定する
    /// </summary>
    [HtmlTargetElement("tweet-button")]
    public class TweetButtonTagHelper : TagHelper
    {

        //https://github.com/aspnet/Mvc/issues/4744
        [ViewContext]
        public ViewContext ViewContext { get; set; }

        /// <summary>リンク先への絶対パス</summary> 
        public string Path { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;

            output.Attributes.SetAttribute("href", "https://twitter.com/share");
            output.Attributes.SetAttribute("class", "twitter-share-button");
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Attributes.SetAttribute("data-lang", Locale.Locale.TweetButton_Lang);
            output.Attributes.SetAttribute("data-size", "large");

            var Uri = new UriBuilder();
            var Request = ViewContext.HttpContext.Request;

            Uri.Scheme = Request.IsHttps ? "https" : "http";
            Uri.Host = Request.Host.Host;
            Uri.Port = Request.Host.Port.HasValue ? Request.Host.Port.Value : -1;
            Uri.Path = Path ?? Request.Path;
            output.Attributes.SetAttribute("content", Uri.ToString());

            output.Content.SetContent(Locale.Locale.TweetButton_Tweet);
        }
    }
}
