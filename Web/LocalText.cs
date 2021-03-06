﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Twigaten.Lib;

namespace Twigaten.Web
{
    /// <summary>
    /// html生成時に文字列をちょっとどうにかするやつ
    /// </summary>
    public static class LocalText
    {
        static readonly Config config = Config.Instance;

        static readonly Regex UrlRegex = new Regex(@"https?://[-_.!~*'()\w;/?:@&=+$,%#]+", RegexOptions.Compiled);
        static readonly Regex HashtagRegex = new Regex(@"(?<=(?:^|[\s　>])[#＃])[a-z0-9_À-ÖØ-öø-ÿĀ-ɏɓ-ɔɖ-ɗəɛɣɨɯɲʉʋʻ̀-ͯḀ-ỿЀ-ӿԀ-ԧⷠ-ⷿꙀ-֑ꚟ-ֿׁ-ׂׄ-ׇׅא-תװ-״﬒-ﬨשׁ-זּטּ-לּמּנּ-סּףּ-פּצּ-ﭏؐ-ؚؠ-ٟٮ-ۓە-ۜ۞-۪ۨ-ۯۺ-ۼۿݐ-ݿࢠࢢ-ࢬࣤ-ࣾﭐ-ﮱﯓ-ﴽﵐ-ﶏﶒ-ﷇﷰ-ﷻﹰ-ﹴﹶ-ﻼ‌ก-ฺเ-๎ᄀ-ᇿ㄰-ㆅꥠ-꥿가-힯ힰ-퟿ﾡ-ￜァ-ヺー-ヾｦ-ﾟｰ０-９Ａ-Ｚａ-ｚぁ-ゖ゙-ゞ㐀-䶿一-鿿꜀-뜿띀-렟-﨟〃々〻]+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        ///<summary>URLとハッシュタグをリンクにする rel="nofollow" 付き</summary>
        public static string TextToLink(string Text)
        {
            if (Text == null) { return null; }
            StringBuilder Builder = new StringBuilder(Text);
            Builder.Replace("<", "&lt;");
            Builder.Replace(">", "&gt;");
            Builder.Replace("\n", "<br />");

            MatchCollection m = UrlRegex.Matches(Builder.ToString());
            for (int i = m.Count - 1; 0 <= i; i--)
            {
                //後ろから順に挿入する
                Builder.Insert(m[i].Index + m[i].Length, "</a>");
                Builder.Insert(m[i].Index, @""" rel=""nofollow"">");
                Builder.Insert(m[i].Index, m[i].Value);
                Builder.Insert(m[i].Index, @"<a href="""); 
            }

            m = HashtagRegex.Matches(Builder.ToString());
            for (int i = m.Count - 1; 0 <= i; i--)
            {
                //後ろから順に挿入する
                Builder.Insert(m[i].Index + m[i].Length, "</a>");
                Builder.Insert(m[i].Index - 1, @""" rel=""nofollow"">");
                Builder.Insert(m[i].Index - 1, HttpUtility.UrlEncode(m[i].Value));
                Builder.Insert(m[i].Index - 1, @"<a href=""https://twitter.com/hashtag/");
            }
            return Builder.ToString();
        }

        ///<summary>Viewに使う画像のURLを返す(ホスト名より後だけ)
        ///orig_media_urlとmedia_idが必要</summary>
        public static string MediaUrl(TweetData._media Media)
        {   //Viewに使う画像のURLを返す
            //orig_media_urlとmedia_idが必要
            if(Media.orig_media_url == null) { return null; }
            else{ return "/twimg/thumb/" + Media.media_id.ToString() + Path.GetExtension(Media.orig_media_url); }
        }

        ///<summary>Viewに使うアイコンのURLを返す(ホスト名より後だけ)
        ///user_idとprofile_image_urlが必要</summary>
        public static string ProfileImageUrl(TweetData._user User, bool IsDefaultProfileImage)
        {
            if (User.local_profile_image_url == null) { return null; }
            if (IsDefaultProfileImage) { return "/twimg/profile_image/" + '_' + Path.GetFileName(User.local_profile_image_url); }
            else { return "/twimg/profile_image/" + User.user_id.ToString() + Path.GetExtension(User.local_profile_image_url); }
        }
    }
}