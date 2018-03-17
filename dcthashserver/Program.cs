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

namespace aspcoretest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options => 
                {
                    options.Listen(IPAddress.Any, 12305, listenOptions => { listenOptions.NoDelay = true; });
                    options.Listen(IPAddress.IPv6Any, 12305, listenOptions => { listenOptions.NoDelay = true; });
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseStartup<Startup>()
                //.UseUrls("http://*:12305/")
                
                .ConfigureLogging((hostingContext, logging) => {
                    logging.ClearProviders();
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.AddFilter("Microsoft.AspNetCore.Mvc", LogLevel.Error);
                })
                .Build();
    }
}
