using System;
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
        /// <summary>
        /// 指定したアカウントだけ取得するCrawlを起動して
        /// 終了待ちをTaskに包んで返す
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public static Task Run(long user_id)
        {
            var info = string.IsNullOrWhiteSpace(config.crawlparent.DotNetChild) ?
                new ProcessStartInfo(config.crawlparent.ChildPath, user_id.ToString()) :
                new ProcessStartInfo(config.crawlparent.DotNetChild, config.crawlparent.ChildPath + " " + user_id.ToString());
            info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
            info.WindowStyle = ProcessWindowStyle.Minimized;

            var retProcess = Process.Start(info);
            return Task.Run(() => retProcess.WaitForExit());
        }
    }
}
