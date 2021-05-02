using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// つまり画像を表示するやつ
    /// </summary>
    [HtmlTargetElement("media-thumb")]
    public class MediaThumbTagHelper : TagHelper
    {
        public TweetData._media Media { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "div";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "twigaten-thumb");
            output.Content.SetHtmlContent(@"<img data-src=""" + Media.local_media_url + @""" class=""twigaten-thumb"">");
        }
    }

    /// <summary>
    /// 動画やgifアニメを示すアイコンを表示するやつ
    /// </summary>
    [HtmlTargetElement("media-type-icon")]
    public class MediaTypeIconTagHelper : TagHelper
    {
        public TweetData._media Media { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            switch (Media.type)
            {
                case "video":
                    output.Content.SetHtmlContent(@"<svg class=""twigaten-glyph""><use xlink:href=""/img/fontawesome.svg#video""/></svg>");
                    break;
                case "animated_gif":
                    output.Content.SetHtmlContent(@"<svg class=""twigaten-glyph""><use xlink:href=""/img/fontawesome.svg#film""/></svg>");
                    break;
                default:
                    output.SuppressOutput();
                    return;
            }
            output.TagName = "div";
            output.TagMode = TagMode.StartTagAndEndTag;

            output.Attributes.SetAttribute("class", "twigaten-mediatype");
        }
    }

    /// <summary>
    /// 「画像でググる」ボタン
    /// </summary>
    [HtmlTargetElement("media-search-google-button")]
    public class MediaSearchGoogleButton : TagHelper
    {
        public TweetData._media Media { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            string MediaUrl = Media.orig_media_url.IndexOf("twimg.com") >= 0
                ? Media.orig_media_url + ":small"
                : Media.orig_media_url;

            output.Attributes.SetAttribute("href", "https://www.google.com/searchbyimage?image_url=" + WebUtility.UrlEncode(MediaUrl));
            output.Attributes.SetAttribute("rel", "nofollow noopener noreferrer");
            output.Attributes.SetAttribute("target", "_blank");
            output.Attributes.SetAttribute("class", "button is-light is-small button-googlemedia");
            output.Content.SetHtmlContent(@"<svg class=""twigaten-glyph""><use xlink:href=""/img/fontawesome.svg#search""/></svg>" + Locale.Locale.SimilarMedia_GoogleImage);
        }
    }
}
