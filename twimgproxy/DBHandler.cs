using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace twimgproxy
{
    public class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("view", "", "localhost", 20, (uint)Math.Min(Environment.ProcessorCount, 40)) { }

        public struct MediaInfo
        {
            public long media_id { get; set; }
            public long source_tweet_id { get; set; }
            public string screen_name { get; set; }
            public string media_url { get; set; }

            public string tweet_url { get { return "https://twitter.com/" + screen_name + "/status/" + source_tweet_id.ToString(); } }
        }

        public async Task<MediaInfo?> SelectThumbUrl(long media_id)
        {
            MediaInfo? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT m.source_tweet_id ,m.media_url, u.screen_name
FROM media m
JOIN tweet t ON m.source_tweet_id = t.tweet_id
JOIN user u USING (user_id)
WHERE media_id = @media_id;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                await ExecuteReader(cmd, (r) => ret = new MediaInfo()
                {
                    media_id = media_id,
                    source_tweet_id = r.GetInt64(0),
                    screen_name = r.GetString(2),
                    media_url = r.GetString(1)
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false);                
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }


        public async Task<int> RemoveDeletedTweet(long tweet_id)
        {
            using(var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async Task<(string Url, string Referer)?> SelectProfileImageUrl(long user_id)
        {
            (string Url, string Referer)? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT profile_image_url, screen_name
FROM user
WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteReader(cmd, (r) => ret = (r.GetString(0), "https://twitter.com/" + r.GetString(1)), IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }
    }
}
