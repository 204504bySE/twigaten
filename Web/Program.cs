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

namespace Twigaten.Web
{
    public class Program
    {
        //ポート番号は適宜
        const int ListenPort = 12309;

        public static void Main(string[] args)
        {
            Counter.AutoRefresh();
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options =>
                {
                    //LinuxではIPv6だけListenすればIPv4もListenされる
                    options.Listen(IPAddress.IPv6Loopback, ListenPort);
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
