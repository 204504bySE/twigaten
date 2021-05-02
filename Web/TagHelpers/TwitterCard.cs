using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Razor.Runtime.TagHelpers;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// Twitter Card の本文(meta name="twitter:description")
    /// 本文は "content"で指定(なければデフォルトのやつになる)
    /// </summary>
    [HtmlTargetElement("twitter-card-description")]
    public class TwitterCardDescriptionTagHelper : TagHelper
    {
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "meta";
            output.TagMode = TagMode.SelfClosing;

            output.Attributes.SetAttribute("property", "og:description");
            if (!output.Attributes.TryGetAttribute("content", out _))
            {
                output.Attributes.SetAttribute("content", "TwiGaTenは、Twitter上の画像付きツイートから、画像転載と思われるツイートを検索できるWebサイトです。");
            }
        }
    }

    /// <summary>
    /// Twitter Card の画像(meta property="og:image")
    /// 画像は"path"で指定(path="/img/ten120.png" のように書く)(なければデフォルトのやつになる)
    /// </summary>
    [HtmlTargetElement("twitter-card-image")]
    public class TwitterCardImageTagHelper : TagHelper
    {
        //https://github.com/aspnet/Mvc/issues/4744
        [ViewContext]
        public ViewContext ViewContext { get; set; }

        /// <summary>
        /// 額縁に入ったアイコンにする
        /// </summary>
        public TweetData._user User { get; set; }
        /// <summary>
        /// ツイートの画像にする
        /// </summary>
        public TweetData._media Media { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "meta";
            output.TagMode = TagMode.SelfClosing;
            output.Attributes.SetAttribute("property", "og:image");

            var Uri = new UriBuilder();
            var Request = ViewContext.HttpContext.Request;

            Uri.Scheme = Request.IsHttps ? "https" : "http";
            Uri.Host = Request.Host.Host;
            Uri.Port = Request.Host.Port ?? -1;
            if (User != null) { Uri.Path = User.local_profile_image_url + "/card.png"; }
            else if (Media != null) { Uri.Path = Media.local_media_url; }
            else { Uri.Path = "/img/ten120.png"; }
            output.Attributes.SetAttribute("content", Uri.ToString());
        }
    }
}
