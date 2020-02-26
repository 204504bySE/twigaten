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
        public bool Retweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            if (string.IsNullOrEmpty(User.local_profile_image_url)) { output.SuppressOutput(); return; }
            output.TagName = "img";
            output.TagMode = TagMode.StartTagOnly;
            output.Attributes.SetAttribute("src", User.local_profile_image_url);
            output.Attributes.SetAttribute("loading", "lazy");
            output.Attributes.SetAttribute("class", Retweet ? "twigaten-icon twigaten-icon-retweet" : "twigaten-icon");
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
            output.Attributes.SetAttribute("class", "twigaten-screenname");
            output.Content.SetContent("@" + User.screen_name);
        }
    }
}
