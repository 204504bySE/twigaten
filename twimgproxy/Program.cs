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

namespace twimgproxy
{
    public class Program
    {
        //ポート番号は適宜
        const int ListenPort = 12309;

        public static void Main(string[] args)
        {
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options =>
                {
                    //LinuxではIPv6AnyだけListenすればIPv4もListenされる
                    options.Listen(IPAddress.IPv6Loopback, ListenPort, listenOptions => { listenOptions.NoDelay = true; });
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseStartup<Startup>()
                .ConfigureLogging((hostingContext, logging) => {
                    logging.ClearProviders();
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.AddFilter("Microsoft.AspNetCore.Mvc", LogLevel.Error);

                })
                .Build();
    }
}
