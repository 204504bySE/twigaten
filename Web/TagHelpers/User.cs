using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace Twigaten.Web.TagHelpers
{
    /// <summary>
    /// アイコン(そのまんま
    /// </summary>
    [HtmlTargetElement("user-icon")]
    public class UserIconTagHelper : TagHelper
    {
        public TweetData._user User { get; set; }
        /// <summary>
        /// リツイートしたやつ表示用ならtrue
        /// </summary>
        public bool StyleRetweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            if (string.IsNullOrEmpty(User.local_profile_image_url)) { output.SuppressOutput(); return; }
            output.TagName = "div";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "twigaten-icon");
            output.Content.SetHtmlContent(@"<img src=""" + User.local_profile_image_url + @""" loading=""lazy"" class=""twigaten-icon" + (StyleRetweet ? @" twigaten-icon-retweet"">" : @""">"));
        }
    }

    /// <summary>
    /// ユーザーのscreen_nameから /users/*** にリンクするやつ
    /// </summary>
    [HtmlTargetElement("user-screenname-link")]
    public class UserScreennameLinkTagHelper : TagHelper
    {
        public TweetData._user User { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "a";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("href", "/users/" + User.user_id.ToString());
            output.Attributes.SetAttribute("class", "twigaten-screenname twigaten-cookie-href");
            output.Content.SetContent("@" + User.screen_name);
        }
    }
}
