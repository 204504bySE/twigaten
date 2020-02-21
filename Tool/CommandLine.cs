using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CommandLine;
using MySql.Data.MySqlClient;
using Twigaten.Lib;

namespace Twigaten.Tool
{
    static class CommandLine
    {
        static readonly DBHandlerCommandLine DB = new DBHandlerCommandLine();
        public static Task Run(string[] args)
        {
            return Parser.Default.ParseArguments<DeleteOption, LookupOption, CleanupOption>(args)
                .MapResult(
                    async (DeleteOption opts) => await DeleteTweetsCommand(opts).ConfigureAwait(false),
                    async (LookupOption opts) => await LookupCommand(opts).ConfigureAwait(false),
                    async (CleanupOption opts) => await CleanupCommand(opts).ConfigureAwait(false),
                    errs => { Console.WriteLine("Invalid argument. See  --help"); return Task.Run(() => { }); }
                );
        }

        [Verb("delete", HelpText = "Delete specified account and its tweets.")]
        class DeleteOption
        {
            [Option('n', "name", HelpText = "screen_name of the account", Required = false)]
            public string screen_name { get; set; }
            [Option('u', "userid", HelpText = "user_id of the account", Required = false)]
            public long? user_id { get; set; }
        }
        static async Task DeleteTweetsCommand(DeleteOption opts)
        {
            long user_id;
            if (opts.user_id.HasValue) { user_id = opts.user_id.Value; }
            else
            {
                var id = await LookupUserId(opts.screen_name).ConfigureAwait(false);
                if (id.HasValue) { user_id = id.Value; }
                else { Console.WriteLine("Account not found."); return; }
            }
            await LookupCommand(new LookupOption() { user_id = user_id }).ConfigureAwait(false);
            Console.Write("Delete this account? [type \"y\" to delete]: ");
            string yn = Console.ReadLine();
            if (yn.Trim() == "y")
            {
                Console.WriteLine("OK. Deleting...");
                Counter.AutoRefresh();
                await DB.DeleteUser(user_id).ConfigureAwait(false);
                Counter.PrintReset();
                Console.WriteLine("＼(^o^)／");
            }
            else { Console.WriteLine("Canceled."); }
        }

        [Verb("lookup", HelpText = "Lookup account information by ID or screen_name.")]
        class LookupOption
        {
            [Option('n', "name", HelpText = "screen_name of the account", Required = false)]
            public string screen_name { get; set; }
            [Option('u', "userid", HelpText = "user_id of the account", Required = false)]
            public long? user_id { get; set; }
        }
        static async Task LookupCommand(LookupOption opts)
        {
            long user_id;
            if (opts.user_id.HasValue) { user_id = opts.user_id.Value; }
            else
            {
                var id = await LookupUserId(opts.screen_name).ConfigureAwait(false);
                if (id.HasValue) { user_id = id.Value; }
                else { Console.WriteLine("Account not found."); return; }
            }
            Console.WriteLine(await DB.UserInfo(user_id).ConfigureAwait(false));
            Console.WriteLine("{0} Tweets of this account is archived.", await DB.TweetCount(user_id).ConfigureAwait(false));
        }
        static async Task<long?> LookupUserId(string screen_name)
        {
            if (!string.IsNullOrWhiteSpace(screen_name))
            {
                var id = await DB.LookupAccount(screen_name).ConfigureAwait(false);
                if (id.HasValue) { return id.Value; }
            }
            return null;
        }

        [Verb("cleanup", HelpText = "Delete tweets deleted from twitter containing reprinted media.")]
        class CleanupOption
        {
            [Option('b', "begin", HelpText = "Begin time in yyyyMMdd (UTC)", Required = true)]
            public string begin { get; set; }
            [Option('e', "exclude", HelpText = "Exclude time in yyyyMMdd (UTC)", Required = true)]
            public string exclude { get; set; }
        }
        static async Task CleanupCommand(CleanupOption opts)
        {
            if (DateTimeOffset.TryParseExact(opts.begin, "yyyyMMdd", DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AssumeUniversal, out var begin)
                && DateTimeOffset.TryParseExact(opts.exclude, "yyyyMMdd", DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AssumeUniversal, out var exclude))
            {
                Console.WriteLine("Cleanup removed tweets...");
                await new RemovedMedia().DeleteRemovedTweet(begin, exclude).ConfigureAwait(false);
                Counter.PrintReset();
                Console.WriteLine("＼(^o^)／");
            }
            else { Console.WriteLine("Invalid argument. See  --help"); }
        }
    }


    class DBHandlerCommandLine : Lib.DBHandler
    {
        public DBHandlerCommandLine() : base("tool", "", config.database.Address, config.database.Protocol, 600, (uint)(Environment.ProcessorCount << 2)) { }

        public async Task<long?> LookupAccount(string screen_name)
        {
            try
            {
                using (var cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE screen_name = @screen_name;"))
                {
                    long? ret = null;
                    cmd.Parameters.Add("screen_name", MySqlDbType.VarChar).Value = screen_name.Trim();
                    if(!await ExecuteReader(cmd, (r) => ret = r.GetInt64(0))) { return null; }
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
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = DeleteUnit }) ;


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
                    if(IdList.Count == 0) { break; }
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
