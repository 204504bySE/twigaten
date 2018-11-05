using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.StaticFiles;

namespace twimgproxy
{
    public class twimgStatic
    {
        public static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12
        });
        public static readonly FileExtensionContentTypeProvider ExtMime = new FileExtensionContentTypeProvider();
        public static readonly DBHandler DB = new DBHandler();
        public static readonly RemovedMedia Removed = new RemovedMedia();
    }
}
