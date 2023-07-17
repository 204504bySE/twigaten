using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySqlConnector;
using Twigaten.Lib;

namespace Twigaten.Tool
{
    class DBHandlerCommandLine : Lib.DBHandler
    {
        public DBHandlerCommandLine() : base(config.database.Address, config.database.Protocol, 600, (uint)(Environment.ProcessorCount << 2)) { }

        /// <summary>
        /// screen_nameからアカウントIDを調べる
        /// </summary>
        /// <param name="screen_name"></param>
        /// <returns></returns>
        public async Task<long?> LookupAccount(string screen_name)
        {
            try
            {
                using (var cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE screen_name = @screen_name;"))
                {
                    long? ret = null;
                    cmd.Parameters.Add("screen_name", MySqlDbType.VarChar).Value = screen_name.Trim();
                    if (!await ExecuteReader(cmd, (r) => ret = r.GetInt64(0))) { return null; }
                    return ret;
                }
            }
            catch { return null; }
        }

        /// <summary>
        /// アカウントの情報をそれっぽいテキストで出す
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public async Task<string> UserInfo(long user_id)
        {
            try
            {
                using (var cmd = new MySqlCommand(@"SELECT * FROM user WHERE user_id = @user_id;"))
                {
                    var ret = new StringBuilder();
                    cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                    await ExecuteReader(cmd, (r) =>
                    {
                        for (int i = 0; i < r.FieldCount; i++)
                        {
                            ret.Append(r.GetName(i));
                            ret.AppendLine(":");
                            ret.AppendLine(r.GetValue(i).ToString());
                            ret.AppendLine();
                        }
                    }).ConfigureAwait(false);
                    return ret.ToString();
                }
            }
            catch { return null; }
        }

        /// <summary>
        /// そいつのツイートが何個TwiGaTenに保存されているか調べる(RT含む)
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public async Task<long> TweetCount(long user_id)
        {
            try
            {
                using var cmd = new MySqlCommand(@"SELECT COUNT(*) FROM tweet WHERE user_id = @user_id;");
                var ret = new StringBuilder();
                cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                return await SelectCount(cmd).ConfigureAwait(false);
            }
            catch { return -1; }
        }

        /// <summary>
        /// 指定したツイートを消す
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <returns></returns>
        public async Task<bool> DeleteTweet(long tweet_id)
        {
            using var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;");
            using var cmd2 = new MySqlCommand(@"DELETE FROM tweet_text WHERE tweet_id = @tweet_id;");
            cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
            cmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
            return 0 < await ExecuteNonQuery(new[] { cmd, cmd2 }).ConfigureAwait(false);
        }

        /// <summary>
        /// 指定したツイートと類似画像を含むツイートを返す
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <returns></returns>
        public async Task<ICollection<long>> TweetAndSimilar(long tweet_id)
        {
            //そのツイートに含まれる画像を探す
            var dctHashes = new List<long>();
            using (var cmd = new MySqlCommand(@"SELECT
DISTINCT m.dcthash
FROM tweet o
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
WHERE o.tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                await ExecuteReader(cmd, (r) => { dctHashes.Add(r.GetInt64(0)); }).ConfigureAwait(false);
            }
            //各画像の類似画像を含むツイートを探す
            var tweetIds = new HashSet<long>();
            foreach (var dctHash in dctHashes)
            {
                using (var cmd2 = new MySqlCommand(@"SELECT 
DISTINCT o.tweet_id
FROM(
    SELECT t.tweet_id, m.media_id
    FROM ((
            SELECT media_id FROM media 
            WHERE dcthash = @media_hash
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_large = media.dcthash
            WHERE p.hash_small = @media_hash
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_small = media.dcthash
            WHERE p.hash_large = @media_hash
        )
    ) AS i
    JOIN media m ON i.media_id = m.media_id
    JOIN tweet_media t ON m.media_id = t.media_id
ORDER BY t.tweet_id
) AS a
JOIN tweet o USING (tweet_id);"))
                {
                    cmd2.Parameters.Add("@media_hash", MySqlDbType.Int64).Value = dctHash;
                    await ExecuteReader(cmd2, (r) => { tweetIds.Add(r.GetInt64(0)); }).ConfigureAwait(false);
                }
            }

            return tweetIds;
        }

        /// <summary>
        /// 指定したツイートと類似画像を含むツイートをしたアカウントのIDを返す
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <returns></returns>
        public async Task<ICollection<long>> TweetNuke(long tweet_id)
        {
            //そのツイートに含まれる画像を探す
            var dctHashes = new List<long>();
            using (var cmd = new MySqlCommand(@"SELECT
DISTINCT m.dcthash
FROM tweet o
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
WHERE o.tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                await ExecuteReader(cmd, (r) => { dctHashes.Add(r.GetInt64(0)); }).ConfigureAwait(false);
            }
            //各画像の類似画像を含むツイートを探す
            var tweetIds = new HashSet<long>();
            foreach (var dctHash in dctHashes)
            {
                using (var cmd2 = new MySqlCommand(@"SELECT 
DISTINCT o.user_id
FROM(
    SELECT t.tweet_id, m.media_id
    FROM ((
            SELECT media_id FROM media 
            WHERE dcthash = @media_hash
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_large = media.dcthash
            WHERE p.hash_small = @media_hash
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_small = media.dcthash
            WHERE p.hash_large = @media_hash
        )
    ) AS i
    JOIN media m ON i.media_id = m.media_id
    JOIN tweet_media t ON m.media_id = t.media_id
ORDER BY t.tweet_id
) AS a
JOIN tweet o USING (tweet_id);"))
                {
                    cmd2.Parameters.Add("@media_hash", MySqlDbType.Int64).Value = dctHash;
                    await ExecuteReader(cmd2, (r) => { tweetIds.Add(r.GetInt64(0)); }).ConfigureAwait(false);
                }
            }

            return tweetIds;
        }

        /// <summary>
        /// こいつのツイートを全消しする
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public async Task DeleteUser(long user_id)
        {
            const int DeleteUnit = 1000;

            //ツイートを次々消すやつ
            var RemoveTweetBlock = new ActionBlock<long>(async (tweet_id) =>
            {
                Counter.TweetToDelete.Increment();
                if (await DeleteTweet(tweet_id)) { Counter.TweetDeleted.Increment(); }
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = DeleteUnit });


            long LastTweetId = 0;
            var IdList = new Queue<long>();
            //まずはツイートを消す
            while (true)
            {
                IdList.Clear();
                using (var cmd = new MySqlCommand(@"SELECT
tweet_id
FROM tweet
WHERE user_id = @user_id AND tweet_id > @tweet_id 
ORDER BY tweet_id LIMIT @limit;"))
                {
                    cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                    cmd.Parameters.Add("tweet_id", MySqlDbType.Int64).Value = LastTweetId;
                    cmd.Parameters.Add("limit", MySqlDbType.Int64).Value = (long)DeleteUnit;
                    await ExecuteReader(cmd, (r) =>
                    {
                        long id = r.GetInt64(0);
                        IdList.Enqueue(id);
                        LastTweetId = id;
                        Counter.LastTweetID = id;
                    }).ConfigureAwait(false);
                    if (IdList.Count == 0) { break; }
                    while (IdList.TryDequeue(out long id))
                    {
                        await RemoveTweetBlock.SendAsync(id);
                        LastTweetId = id;
                    }
                }
            }
            RemoveTweetBlock.Complete();
            await RemoveTweetBlock.Completion.ConfigureAwait(false);


            //アイコンも忘れずに消す
            using (var cmd = new MySqlCommand(@"SELECT
profile_image_url, is_default_profile_image
FROM user
JOIN user_updated_at USING (user_id)
WHERE user_id = @user_id;"))
            {
                string profile_image_url = null;
                bool is_default_profile_image = true;

                cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteReader(cmd, (r) => { profile_image_url = r.GetString(0); is_default_profile_image = r.GetBoolean(1); }).ConfigureAwait(false);

                if (profile_image_url != null && !is_default_profile_image)
                {
                    File.Delete(MediaFolderPath.ProfileImagePath(user_id, is_default_profile_image, profile_image_url));
                }
            }

            using (var cmd = new MySqlCommand(@"DELETE FROM user WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }

        }

        /// <summary>
        /// タイ語圏っぽいアカウントを全部見つけ出す
        /// </summary>
        /// <returns></returns>
        public async Task ThaiAccounts(Action<long> callback)
        {
            var thaiLatinRegexFull = new Regex(@"^[\sA-Za-z0-9ก-๛]{2,}$");
            var thaiRegex = new Regex(@"[ก-๛]{4,}");
            //名前と自己紹介の両方がタイ語なら多分そうだろう
            using (var cmd = new MySqlCommand(@"SELECT user_id, name, screen_name, description FROM user
WHERE description REGEXP BINARY '^[ก-๛]{4,}$';"))
            {
                await ExecuteReader(cmd, (r) =>
                {
                    var user_id = r.GetInt64(0);
                    var name = r.GetString(1);
                    var screen_name = r.GetString(2);
                    var description = r.GetString(3);
                    //MySQLのREGEXPだとキリル文字やギリシャ文字圏に誤爆するのでここでもやる
                    if (thaiLatinRegexFull.IsMatch(name) && thaiRegex.IsMatch(description))
                    {
                        Console.WriteLine("!! " + user_id.ToString() + "\t" + screen_name + "\t" + name + "\t" + description);
                        callback(user_id);
                    }
                }).ConfigureAwait(false);
            }
        }

    }
}
