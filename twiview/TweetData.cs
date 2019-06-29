using System;
using System.Collections.Generic;
using System.Linq;

namespace twiview
{
    public class SimilarMediaTweet : TweetData
    {
        public _tweet tweet = new _tweet();
        public _media media = new _media();
        public SimilarMediaTweet[] Similars;
        public long SimilarMediaCount { get; set; }  //類似画像の個数(見れないやつを含む
    }

    public class TweetData
    {
        public class _user
        {
            public long user_id { get; set; }
            public string name { get; set; }
            public string screen_name { get; set; }
            public bool isprotected { get; set; }
            public string profile_image_url { get; set; }   //鯖に保存してあればそのURL
            public bool is_default_profile_image { get; set; }
            public string location { get; set; }
            public string description { get; set; }    //リンクや改行などをhtmlにして突っ込む
            //public string origdescription { get; set; }  //今は必要ないね
        }

        public class _tweet
        {
            public _user user { get; } = new _user();
            public long tweet_id { get; set; }
            public DateTimeOffset created_at { get; set; }
            public string text { get; set; }    //リンクや改行などをhtmlにして突っ込む
            //public string origtext { get; set; }  //今は必要ないね
            public _tweet retweet { get; set; }
            public int retweet_count { get; set; }
            public int favorite_count { get; set; }
        }

        public class _media
        {
            public long media_id { get; set; }
            public long source_tweet_id { get; set; }
            public string type { get; set; }
            public string media_url { get; set; }   //鯖に保存してあればそのURL
            public string orig_media_url { get; set; }
            public string local_media_url { get; set; } //鯖に保存してなくても鯖用のURL
            public long dcthash { get; set; }
            //public sbyte dcthash_distance { get; set; }
        }
    }
}