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

namespace Twigaten.DctHashServer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var config = Config.Instance.dcthashserver;
            WebHost.CreateDefaultBuilder(args)
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
                })
                .UseStartup<Startup>()  
                .ConfigureLogging((hostingContext, logging) =>
                {
                    logging.ClearProviders();
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                    logging.AddDebug();
                })
                .Build()
                .Run();
        }
    }
}
