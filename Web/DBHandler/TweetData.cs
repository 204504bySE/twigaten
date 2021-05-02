using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;

namespace Twigaten.Web
{
    /// <summary>
    /// 画像とその類似画像1セット
    /// </summary>
    public class SimilarMediaTweet : TweetData
    {
        public _tweet tweet = new _tweet();
        public _media media = new _media();
        public SimilarMediaTweet[] Similars;
        /// <summary>
        /// //類似画像の個数(見れないやつを含む
        /// </summary>
        public long SimilarMediaCount { get; set; }
        /// <summary>
        /// 「もっと見る」ボタンが必要ならtrue
        /// </summary>
        public bool ExistsMoreMedia { get; set; }
    }

    /// <summary>
    /// DBのテーブルにほぼそのまま対応するクラス群
    /// </summary>
    public class TweetData
    {
        public class _user
        {
            public long user_id { get; set; }
            public string name { get; set; }
            public string screen_name { get; set; }
            public bool isprotected { get; set; }
            public string local_profile_image_url { get; set; }
            public bool is_default_profile_image { get; set; }
            public string location { get; set; }
            /// <summary>リンクや改行などをhtmlにして突っ込む</summary>
            public string description_html { get; set; }
        }

        public class _tweet
        {
            public _user user { get; } = new _user();
            public long tweet_id { get; set; }
            public DateTimeOffset created_at { get; set; }
            public string text { get; set; }
            /// <summary>リンクや改行などをhtmlにして突っ込む</summary>
            public string text_html { get; set; }
            public _tweet retweet { get; set; }
            public int retweet_count { get; set; }
            public int favorite_count { get; set; }
        }

        public class _media
        {
            public long media_id { get; set; }

            /// <summary>photo/video/animated_gif</summary>
            public string type { get; set; }
            /// <summary>pbs.twimg.comなどのURL</summary>
            public string orig_media_url { get; set; }
            /// <summary>自鯖内のURL(/twimg/~~~ など)</summary>
            public string local_media_url { get; set; }
        }
    }

    public class crawlinfo
    {
        public long? timeline_updated_at { get; set; }
    }
}