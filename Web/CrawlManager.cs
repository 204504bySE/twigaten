using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Twigaten.Lib;

namespace Twigaten.Web
{
    static class CrawlManager
    {
        static readonly Config config = Config.Instance;
        static readonly ConcurrentDictionary<long, Process> Crawls = new ConcurrentDictionary<long, Process>();
        /// <summary>
        /// 指定したアカウントだけ取得するCrawlを起動して
        /// 終了待ちをTaskに包んで返す
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public static void Run(long user_id)
        {
            var info = string.IsNullOrWhiteSpace(config.crawlparent.DotNetChild) ?
                new ProcessStartInfo(config.crawlparent.ChildPath, user_id.ToString()) :
                new ProcessStartInfo(config.crawlparent.DotNetChild, config.crawlparent.ChildPath + " " + user_id.ToString());
            info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
            info.WindowStyle = ProcessWindowStyle.Minimized;

            if (Crawls.TryRemove(user_id, out var oldProcess) && !oldProcess.HasExited) { oldProcess.Kill(); }
            Crawls.TryAdd(user_id, Process.Start(info));
        }

        /// <summary>
        /// 事前に起動したCrawlの終了を待機する
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public static Task WhenCrawled(long user_id)
        {
            if (Crawls.TryGetValue(user_id, out var process)) 
            {
                var ret = Task.Run(() => { process.WaitForExit(); });
                ret.ContinueWith((_) =>
                {
                    Crawls.TryRemove(user_id, out var _);

                    //終了後ついでに残骸も片付ける
                    foreach (var c in Crawls.Where(c => c.Value.HasExited))
                    {
                        Crawls.TryRemove(c.Key, out var _);
                    }
                });
                return ret;
            }
            else { return Task.CompletedTask; }
        }
    }
}
