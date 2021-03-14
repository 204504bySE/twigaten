using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Mono.Unix;
using Twigaten.Lib;

namespace Twigaten.Web
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            //Codepagesを必要とする処理が動くようにする
            //https://stackoverflow.com/questions/49215791/vs-code-c-sharp-system-notsupportedexception-no-data-is-available-for-encodin?noredirect=1&lq=1
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

            var config = Config.Instance.web;
            var host = WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options =>
                {

                    //LinuxではIPv6だけListenすればIPv4もListenされる
                    if (config.ListenIPv6) 
                    {
                        options.Listen(IPAddress.IPv6Loopback, config.ListenPort);
                        Console.WriteLine("Listening [{0}]:{1}", IPAddress.IPv6Loopback, config.ListenPort);
                    }
                    if (config.ListenIPv4) 
                    {
                        options.Listen(IPAddress.Loopback, config.ListenPort);
                        Console.WriteLine("Listening {0}:{1}", IPAddress.Loopback, config.ListenPort);
                    }

                    //UNIXソケットを作る際は事前に消す(再起動時に残ってる)
                    if (!string.IsNullOrWhiteSpace(config.ListenUnixSocketPath))
                    {
                        File.Delete(config.ListenUnixSocketPath);
                        options.ListenUnixSocket(config.ListenUnixSocketPath);
                        Console.WriteLine("Listening unix:{0}", config.ListenUnixSocketPath);
                    }
                })
                .UseStartup<Startup>()
                .ConfigureLogging((hostingContext, logging) =>
                {
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                    logging.AddDebug();
                })
                .Build();

            await host.StartAsync().ConfigureAwait(false);
            //UNIXソケットを作ってから後でパーミッションを変更する
            if (!string.IsNullOrWhiteSpace(config.ListenUnixSocketPath))
            { new UnixFileInfo(config.ListenUnixSocketPath).FileAccessPermissions = FileAccessPermissions.DefaultPermissions; }
            Console.WriteLine("Ready for requests.");

            AutoRefresh();
            await host.WaitForShutdownAsync().ConfigureAwait(false);

        }


        public static void AutoRefresh()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(600000).ConfigureAwait(false);
                    Counter.PrintReset();
                }
            });
        }
    }
}
