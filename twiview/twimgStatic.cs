using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.StaticFiles;

namespace twiview
{
    public class twimgStatic
    {
        public static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12
        });
        static readonly FileExtensionContentTypeProvider ExtMime = new FileExtensionContentTypeProvider();
        public static string GetMime(string FileName)
        {
            if (ExtMime.TryGetContentType(FileName, out string mime)) { return mime; }
            else { return "application/octet-stream"; };
        }
        public static readonly DBHandlerView DB = new DBHandlerView();
        public static readonly DBHandlerCrawl DBCrawl = new DBHandlerCrawl();
        public static readonly RemovedMedia Removed = new RemovedMedia();
    }
}
