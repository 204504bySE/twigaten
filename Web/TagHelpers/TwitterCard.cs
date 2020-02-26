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

        /// <summary>画像の絶対パス</summary> 
        public string Path { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "meta";
            output.TagMode = TagMode.SelfClosing;
            output.Attributes.SetAttribute("property", "og:image");

            var Uri = new UriBuilder();
            var Request = ViewContext.HttpContext.Request;

            Uri.Scheme = Request.IsHttps ? "https" : "http";
            Uri.Host = Request.Host.Host;
            Uri.Port = Request.Host.Port.HasValue ? Request.Host.Port.Value : -1;
            Uri.Path = Path ?? "/img/ten120.png";
            output.Attributes.SetAttribute("content", Uri.ToString());
        }
    }
}
