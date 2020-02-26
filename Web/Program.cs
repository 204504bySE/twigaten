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
using Twigaten.Lib;

namespace Twigaten.Web
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Counter.AutoRefresh();
            //Codepagesを必要とする処理が動くようにする
            //https://stackoverflow.com/questions/49215791/vs-code-c-sharp-system-notsupportedexception-no-data-is-available-for-encodin?noredirect=1&lq=1
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options =>
                {
                    //LinuxではIPv6だけListenすればIPv4もListenされる
                    options.Listen(IPAddress.IPv6Loopback, Config.Instance.web.ListenPort);
                })
                .UseStartup<Startup>()
                .ConfigureLogging((hostingContext, logging) => {
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                    logging.AddDebug();
                })
                .Build();
    }
}
