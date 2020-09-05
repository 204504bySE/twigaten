using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Twigaten.Web.DBHandler
{
    static class DB
    {
        public static DBCrawl Crawl { get; } = new DBCrawl();
        public static DBToken Token { get; } = new DBToken();
        public static DBTwimg Twimg { get; } = new DBTwimg();
        public static DBView View { get; } = new DBView();
    }
}
