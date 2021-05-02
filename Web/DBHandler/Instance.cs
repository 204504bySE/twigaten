using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Twigaten.Web
{
    public partial class DBHandler : Lib.DBHandler
    {
        private DBHandler() : base(config.database.Address, config.database.Protocol,20, (uint)config.web.MaxDBConnections, 60) { }
        public static DBHandler Instance { get; } = new DBHandler();
    }
}
