using System;
using System.Collections.Generic;
using System.Threading;

using System.Diagnostics;
using System.IO;
using twitenlib;


namespace twidownparent
{
    static class ChildProcessHandler
    {
        static Config config = Config.Instance;

        static public int Start()
        {
            try
            {
                ProcessStartInfo info = string.IsNullOrWhiteSpace(config.crawlparent.DotNetChild) ?
                    new ProcessStartInfo(config.crawlparent.ChildPath) :
                    new ProcessStartInfo(config.crawlparent.DotNetChild, config.crawlparent.ChildPath);
                info.WorkingDirectory = Path.GetDirectoryName(config.crawlparent.ChildPath);
                info.WindowStyle = ProcessWindowStyle.Minimized;
                using (Process retProcess = new Process()
                {
                    StartInfo = info,
                    EnableRaisingEvents = true
                })
                {
                    retProcess.Start();
                    return retProcess.Id;
                }

            }
            catch { return -1; }
        }

        static public bool Alive(int pid)
        {
            //同じIDのプロセスはすぐに出てこないと信じる(めんどい
            try { return Process.GetProcessById(pid)?.Responding == true; }
            catch { return false; }
        }
    }
}
