﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Twigaten.DctHashServer
{
    public class Program
    {
        //ポート番号は適宜
        const int ListenPort = 12305;

        public static void Main(string[] args)
        {
            BuildWebHost(args).Run();
        }

        public static IWebHost BuildWebHost(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseKestrel(options =>
                {
                    //WindowsではIPv4とIPv6は別々にListenする必要がある
                    options.Listen(IPAddress.Any, ListenPort);
                    options.Listen(IPAddress.IPv6Any, ListenPort);
                })
                .UseStartup<Startup>()
                //.UseUrls("http://*:12305/")     
                .ConfigureLogging((hostingContext, logging) =>
                {
                    logging.ClearProviders();
                    logging.AddConfiguration(hostingContext.Configuration.GetSection("Logging"));
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                    logging.AddDebug();
                })
                .Build();
    }
}
