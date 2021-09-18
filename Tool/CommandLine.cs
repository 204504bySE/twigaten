using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CommandLine;

namespace Twigaten.Tool
{
    static class CommandLine
    {
        static readonly DBHandlerCommandLine DB = new DBHandlerCommandLine();
        public static Task Run(string[] args)
        {
            return Parser.Default.ParseArguments<DeleteOption, LookupOption, CleanupOption, SecretOption>(args)
                .MapResult(
                    async (DeleteOption opts) => await DeleteTweetsCommand(opts).ConfigureAwait(false),
                    async (LookupOption opts) => await LookupCommand(opts).ConfigureAwait(false),
                    async (CleanupOption opts) => await CleanupCommand(opts).ConfigureAwait(false),
                    async (SecretOption opts) => await CompareHashCommand(opts).ConfigureAwait(false),
                    errs => { Console.WriteLine("Invalid argument. See  --help"); return Task.Run(() => { }); }
                ); ;
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

        [Verb("secret", Hidden = true)]
        class SecretOption 
        {
            //2つのサーバー(Windowsとwine等)で同じdct hashが返ってくるか確かめるやつ
            [Option('c', "compare")]
            public bool compare { get; set; }
            [Option('m', "marathon")]
            public bool marathon { get; set; }

            //タイ語圏の収集ツイの治安が悪すぎるので消すやつ(ひどいオプションだ)
            [Option("nothai")]
            public bool nothai { get; set; }
        }
        static async Task CompareHashCommand(SecretOption opts)
        {
            if (opts.compare)
            {
                if (opts.marathon) { await CompareHash.Marathon().ConfigureAwait(false); }
                else { await CompareHash.Proceed().ConfigureAwait(false); }
            }
            else if (opts.nothai)
            {
                var DeleteUserBlock = new ActionBlock<long>(async (user_id) => await DB.DeleteUser(user_id).ConfigureAwait(false));
                Counter.AutoRefresh();
                await DB.ThaiAccounts((user_id) => DeleteUserBlock.Post(user_id)).ConfigureAwait(false);
                DeleteUserBlock.Complete();
                await DeleteUserBlock.Completion.ConfigureAwait(false);
                Counter.PrintReset();
                Console.WriteLine("＼(^o^)／");
            }
            else
            {
                Console.WriteLine("Option required.");
            }
        }

    }

}
