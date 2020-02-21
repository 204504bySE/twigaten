using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Twigaten.Web.DBHandler
{
    static class Instance
    {
        public static DBCrawl DBCrawl { get; } = new DBCrawl();
        public static DBToken DBToken { get; } = new DBToken();
        public static DBTwimg DBTwimg { get; } = new DBTwimg();
        public static DBView DBView { get; } = new DBView();
    }
}
