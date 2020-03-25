using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CoreTweet;
using System.Net;
using System.Collections.Generic;
using Twigaten.Lib;
using System.Runtime;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;

namespace Twigaten.Crawl
{
    class RestManager
    {
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        /// <summary>
        /// サインインした全アカウントのツイート, フォロー等を取得する
        /// 常に最大数のツイートを取得する
        /// </summary>
        /// <returns></returns>
        public async Task<int> Proceed()
        {
            var tokens = (await db.SelectUserStreamerSetting(DBHandler.SelectTokenMode.All).ConfigureAwait(false)).ToArray();
            if (tokens.Length > 0) { Console.WriteLine("App: {0} Accounts to REST", tokens.Length); }
            var RestProcess = new ActionBlock<Tokens>(async (t) => 
            {
                //無条件でAPIの最大数のツイートを取得するためにToken以外は捨てる
                var s = new UserStreamer(new UserStreamer.UserStreamerSetting() { Token = t });
                await s.RestFriend().ConfigureAwait(false);
                await s.RestBlock().ConfigureAwait(false);
                //await s.RestMyTweet().ConfigureAwait(false);
                await s.VerifyCredentials().ConfigureAwait(false);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = config.crawl.RestTweetThreads,
                BoundedCapacity = config.crawl.RestTweetThreads << 1
            });
            var sw = Stopwatch.StartNew();
            foreach(var t in tokens)
            {
                await RestProcess.SendAsync(t.Token).ConfigureAwait(false);

                while (true)
                {
                    if (sw.ElapsedMilliseconds > 60000)
                    {
                        Counter.PrintReset();
                        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce; //これは毎回必要らしい
                        GC.Collect();
                        sw.Restart();
                    }
                    //ツイートが詰まってたら休む                    
                    if (UserStreamerStatic.NeedConnectPostpone()) { await Task.Delay(1000).ConfigureAwait(false); }
                    else { break; }
                }
            }
            RestProcess.Complete();
            await RestProcess.Completion.ConfigureAwait(false);
            await UserStreamerStatic.Complete().ConfigureAwait(false);
            Counter.PrintReset();
            return tokens.Length;
        }

        /// <summary>
        /// 指定したアカウントのツイート, フォロー等を取得する
        /// 新規ログイン向け
        /// 常に最大数のツイートを取得する
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public async Task OneAccount(long user_id)
        {
            var t = await db.SelectUserStreamerSetting(user_id).ConfigureAwait(false);
            if (!t.HasValue) { return; }
            //無条件でAPIの最大数のツイートを取得するためにToken以外は捨てる
            var s = new UserStreamer(new UserStreamer.UserStreamerSetting() { Token = t.Value.Token });

            var friend = s.RestFriend();
            var block = s.RestBlock();
            var tweet = s.RestMyTweetMax();
            var cred = s.VerifyCredentials();
            var timeline = s.RestTimeline();
            await Task.WhenAll(friend, block, tweet, cred, timeline).ConfigureAwait(false);

            //ツイート等の取得を行ったことをDBに保存する
            var savesetting = s.Setting;
            savesetting.rest_my_tweet = false;
            await Task.WhenAll(db.StoreUserStreamerSetting(new[] { savesetting }), UserStreamerStatic.Complete()).ConfigureAwait(false);
            Console.WriteLine("{0}: Profile stored\t{1}, {2}", user_id, friend.Result, block.Result);
            Counter.PrintReset();
        }
    }
}
