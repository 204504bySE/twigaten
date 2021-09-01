using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySqlConnector;
using Twigaten.Lib;

namespace Twigaten.Tool
{
    class DBHandlerCommandLine : Lib.DBHandler
    {
        public DBHandlerCommandLine() : base(config.database.Address, config.database.Protocol, 600, (uint)(Environment.ProcessorCount << 2)) { }

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

        public async Task<long> TweetCount(long user_id)
        {
            try
            {
                using (var cmd = new MySqlCommand(@"SELECT COUNT(*) FROM tweet WHERE user_id = @user_id;"))
                {
                    var ret = new StringBuilder();
                    cmd.Parameters.Add("user_id", MySqlDbType.Int64).Value = user_id;
                    return await SelectCount(cmd).ConfigureAwait(false);
                }
            }
            catch { return -1; }
        }

        public async Task DeleteUser(long user_id)
        {
            const int DeleteUnit = 1000;

            //ツイートを次々消すやつ
            var RemoveTweetBlock = new ActionBlock<long>(async (tweet_id) =>
            {
                Counter.TweetToDelete.Increment();
                using (var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;"))
                using (var cmd2 = new MySqlCommand(@"DELETE FROM tweet_text WHERE tweet_id = @tweet_id;"))
                {
                    cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                    cmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                    if (await ExecuteNonQuery(new[] { cmd, cmd2 }).ConfigureAwait(false) > 0) { Counter.TweetDeleted.Increment(); }
                }
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
    }
}
