using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace twimgproxy
{
    public class DBHandlerView : twitenlib.DBHandler
    {
        public DBHandlerView() : base("view", "", "localhost", 20, (uint)Math.Min(Environment.ProcessorCount, 40)) { }

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

        ///<summary>そのツイートの全画像に対して
        ///より古い公開ツイートが存在するかどうか(紛らわしい)</summary>
        public async Task<bool> AllHaveOlderMedia(long tweet_id)
        {
            var media_id = new List<long>();
            long? created_at = null;
            using (var cmd = new MySqlCommand(@"SELECT o.created_at, t.media_id
FROM tweet o
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
INNER JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
WHERE o.tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                await ExecuteReader(cmd, (r) =>
                {
                    media_id.Add(r.GetInt64(1));
                    if (!created_at.HasValue) { created_at = r.GetInt64(0); }
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
            //失敗した場合は存在しないことにしてツイートを削除させない
            if (!created_at.HasValue) { return false; }

            var mediacmd = new MySqlCommand(@"SELECT NOT EXISTS (SELECT *
FROM ((
        SELECT t.tweet_id, t.created_at, u.isprotected FROM media m
        JOIN tweet_media USING (media_id)
        JOIN tweet t USING (tweet_id)
        JOIN user u USING (user_id)
        WHERE dcthash = (SELECT dcthash FROM media WHERE media_id = @media_id)
    ) UNION ALL (
        SELECT t.tweet_id, t.created_at, u.isprotected FROM media m
        JOIN tweet_media USING (media_id)
        JOIN tweet t USING (tweet_id)
        JOIN user u USING (user_id)
        INNER JOIN dcthashpair h ON h.hash_sub = m.dcthash
        WHERE h.hash_pri = (SELECT dcthash FROM media WHERE media_id = @media_id)
    )) AS i
WHERE i.tweet_id != @tweet_id
AND i.created_at < (SELECT created_at FROM tweet WHERE tweet_id = @tweet_id)
AND i.isprotected IS FALSE
            );");
            mediacmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
            var mediaparam = mediacmd.Parameters.Add("@media_id", MySqlDbType.Int64);
            foreach (long mid in media_id)
            {
                mediaparam.Value = mid;
                if (await SelectCount(mediacmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false) != 0) { return false; }
            }
            return true;
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
    public class DBHandlerCrawl : twitenlib.DBHandler
    {
        public DBHandlerCrawl() : base("crawl", "", "localhost", 20, 1) { }
        public async Task<int> RemoveDeletedTweet(long tweet_id)
        {
            using (var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
    }
}