using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace Twigaten.Web
{
    public partial class DBHandler : Lib.DBHandler
    {
        public readonly struct MediaInfo
        {
            public long media_id { get; init; }
            public long source_tweet_id { get; init; }
            public string screen_name { get; init; }
            public string media_url { get; init; }
            public string tweet_url { get { return "https://twitter.com/" + screen_name + "/status/" + source_tweet_id.ToString(); } }
        }

        /// <summary>
        /// ツイート画像の元URL
        /// 見つからない場合はnull
        /// </summary>
        public async Task<MediaInfo?> SelectThumbUrl(long media_id)
        {
            MediaInfo? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT m.source_tweet_id ,mt.media_url, u.screen_name
FROM media m
LEFT JOIN media_text mt USING (media_id)
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
                }).ConfigureAwait(false);
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }

        /// <summary>
        /// ツイート画像のblurhash
        /// ない場合はnull(DBには空文字が入っててもnull)
        /// </summary>
        public async Task<string> SelectThumeBlurHash(long media_id)
        {
            string ret = null;
            using (var cmd = new MySqlCommand(@"SELECT blurhash
FROM media_text
WHERE media_id = @media_id;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                await ExecuteReader(cmd, (r) => ret = r.GetString(0)).ConfigureAwait(false);
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return string.IsNullOrWhiteSpace(ret) ? null : ret;
        }

        ///<summary>そのツイートの全画像に対して
        ///より古い公開ツイートが存在するかどうか(紛らわしい)</summary>
        public async Task<bool> AllHaveOlderMedia(long tweet_id)
        {
            var media_id = new List<long>();
            using (var cmd = new MySqlCommand(@"SELECT t.media_id
FROM tweet o
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
INNER JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
WHERE o.tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                await ExecuteReader(cmd, (r) =>
                {
                    media_id.Add(r.GetInt64(0));
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
            //失敗した場合は存在しないことにしてツイートを削除させない
            if (media_id.Count == 0) { return false; }

            //ハッシュ値が同じで古い奴
            using (var mediacmd = new MySqlCommand(@"SELECT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
WHERE dcthash = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
            //ハッシュ値がちょっと違って古い奴
            using (var mediacmd2 = new MySqlCommand(@"SELECT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
JOIN dcthashpairslim h ON h.hash_large = m.dcthash
WHERE h.hash_small = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
            using (var mediacmd3 = new MySqlCommand(@"SELECT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
JOIN dcthashpairslim h ON h.hash_small = m.dcthash
WHERE h.hash_large = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
            {
                mediacmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                mediacmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                mediacmd3.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                var mediaparam = mediacmd.Parameters.Add("@media_id", MySqlDbType.Int64);
                var mediaparam2 = mediacmd2.Parameters.Add("@media_id", MySqlDbType.Int64);
                var mediaparam3 = mediacmd3.Parameters.Add("@media_id", MySqlDbType.Int64);
                foreach (long mid in media_id)
                {
                    mediaparam.Value = mid;
                    mediaparam2.Value = mid;
                    mediaparam3.Value = mid;
                    //どれか一つでも存在する時だけ次の画像に進める
                    if (await SelectCount(mediacmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false) == 0
                        && await SelectCount(mediacmd2, IsolationLevel.ReadUncommitted).ConfigureAwait(false) == 0
                        && await SelectCount(mediacmd3, IsolationLevel.ReadUncommitted).ConfigureAwait(false) == 0)
                    { return false; }
                }
                return true;
            }
        }

        public struct ProfileImageInfo
        {
            public long user_id { get; set; }
            public string profile_image_url { get; set; }
            public string tweet_url { get; set; }

            public bool is_default_profile_image { get; set; }
        }
        public async Task<ProfileImageInfo?> SelectProfileImageUrl(long user_id)
        {
            ProfileImageInfo? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT profile_image_url, screen_name, is_default_profile_image
FROM user
WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteReader(cmd, (r) => ret = new ProfileImageInfo()
                {
                    user_id = user_id,
                    profile_image_url = r.GetString(0),
                    tweet_url = "https://twitter.com/" + r.GetString(1),
                    is_default_profile_image = r.GetBoolean(2)
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }
    }
}
