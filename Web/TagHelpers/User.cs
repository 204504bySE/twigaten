using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Razor.TagHelpers;
using Twigaten.Web.DBHandler;
using static Twigaten.Web.Locale.Locale;

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
        /// リツイートしたやつ表示用ならtrue(float:left相当)
        /// </summary>
        public bool StyleRetweet { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            if (string.IsNullOrEmpty(User.local_profile_image_url)) { output.SuppressOutput(); return; }
            output.TagName = "div";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Attributes.SetAttribute("class", "twigaten-icon");
            output.Content.SetHtmlContent(@"<img data-src=""" + User.local_profile_image_url + @""" class=""twigaten-icon" + (StyleRetweet ? @" twigaten-icon-retweet"">" : @""">"));
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
            output.Attributes.SetAttribute("class", "twigaten-screenname twigaten-cookie-click");
            if (User.isprotected) { output.Attributes.SetAttribute("rel", "nofollow"); }
            output.Content.SetContent("@" + User.screen_name);
        }
    }
    /// <summary>
    /// ツイート/タイムラインの取得時刻を表示する(あなたにのみ表示されます)
    /// </summary>
    [HtmlTargetElement("timeline-updated-at")]
    public class TimelineUpdatedAtTagHelper : TagHelper
    {
        public long UpdatedAt { get; set; }
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            output.TagName = "p";
            output.TagMode = TagMode.StartTagAndEndTag;
            output.Content.SetHtmlContent(Headline_TimelineUpdatedAt + @": <span class=""twigaten-unixtime"" data-unixtime=""" + UpdatedAt.ToString() + @"""></span> <span class=""has-text-grey is-size-7"">(" + Headline_ShowOnlyToYou + ")</span>");
        }
    }
}
