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
            output.TagName = "img";
            output.TagMode = TagMode.StartTagOnly;
            output.Attributes.SetAttribute("src", Media.local_media_url);
            output.Attributes.SetAttribute("loading", "lazy");
            output.Attributes.SetAttribute("class", "twigaten-thumb");
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
                    output.Content.SetHtmlContent(@"<span class=""glyphicon glyphicon-facetime-video""></span>");
                    break;
                case "animated_gif":
                    output.Content.SetHtmlContent(@"<span class=""glyphicon glyphicon-film""></span>");
                    break;
                default:
                    output.SuppressOutput();
                    return;
            }
            output.TagName = "p";
            output.TagMode = TagMode.StartTagAndEndTag;

            output.Attributes.SetAttribute("class", "text-center");
        }
    }

    [HtmlTargetElement("media-search-google-button")]
    public class MediaSearchGoogleButton : TagHelper
    {
        public TweetData._media Media { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            string MediaUrl = Media.orig_media_url.IndexOf("twimg.com") >= 0
                ? Media.orig_media_url + ":small"
                : Media.orig_media_url;
            
            output.TagName = "a";
            output.Attributes.SetAttribute("href", "https://www.google.com/searchbyimage?image_url=" + WebUtility.UrlEncode(MediaUrl));
            output.Attributes.SetAttribute("class", "btn btn-default btn-xs");
            output.Attributes.SetAttribute("rel", "nofollow");
            output.Content.SetHtmlContent(@"<span class=""glyphicon glyphicon-search""></span>" + Locale.Locale.SimilarMedia_GoogleImage);
        }
    }
}
