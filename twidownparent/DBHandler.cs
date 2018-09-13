using System;
using System.Collections.Generic;
using System.Threading;

using System.Data;
using MySql.Data.MySqlClient;
using twitenlib;
using System.Threading.Tasks;

namespace twidownparent
{

    class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("crawl", "", Config.Instance.database.Address) { }

        public async Task<long> CountToken()
        {
            using(var cmd = new MySqlCommand("SELECT COUNT(user_id) FROM token;"))
            {
                return await SelectCount(cmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
        }

        ///<summary>Newというより割り当てがないToken</summary>
        public async Task<long[]> SelectNewToken()
        {
            var ret = new List<long>();
            using (var cmd = new MySqlCommand(@"SELECT user_id FROM token
WHERE NOT EXISTS (SELECT * FROM crawlprocess WHERE user_id = token.user_id);"))
            {
                if(await ExecuteReader(cmd, (r) => ret.Add(r.GetInt64(0))).ConfigureAwait(false)) { return ret.ToArray(); }
                else { return new long[0]; }    //例によって全部取得成功しない限り返さない
            }
        }

        public async Task<int> Assigntoken(long user_id, int pid, bool RestMyTweet)
        {
            //Console.WriteLine("{0} Assign: {1} to {2}", DateTime.Now, user_id, pid);
            var cmd = new MySqlCommand(@"INSERT IGNORE INTO crawlprocess (user_id, pid, rest_needed) VALUES(@user_id, @pid, @rest_needed)");
            cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
            cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
            cmd.Parameters.Add("@rest_needed", MySqlDbType.Byte).Value = RestMyTweet ? 2 : 0;
            return await ExecuteNonQuery(cmd).ConfigureAwait(false);
        }


        public async ValueTask<int> SelectBestpid()
        {
            //一番空いてる子プロセスってわけ 全滅なら負数を返すからつまり新プロセスが必要

            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT pid, COUNT(*) FROM crawlprocess GROUP BY pid ORDER BY COUNT(*) LIMIT 1;"))
            {
                Table = await SelectTable(cmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
            if (Table == null || Table.Rows.Count < 1
                || (Table.Rows[0].Field<long?>(1) ?? 0) > config.crawlparent.AccountLimit) { return -1; }
            return Table.Rows[0].Field<int>(0);
        }

        public async ValueTask<int> Insertpid(int pid)
        {
            Console.WriteLine("{0} New PID {1}", DateTime.Now, pid); 
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT IGNORE INTO pid VALUES(@pid)"))
            {
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async ValueTask<int[]> Selectpid()
        {
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT DISTINCT pid FROM crawlprocess;"))
            {
                Table = await SelectTable(cmd).ConfigureAwait(false);
            }
            if (Table == null) { return null; }
            int[] ret = new int[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                ret[i] = Table.Rows[i].Field<int>(0);
            }
            return ret;
        }

        public async ValueTask<int> Deletepid(int pid)
        {
            using (MySqlCommand Cmd = new MySqlCommand(@"DELETE FROM crawlprocess WHERE pid = @pid;"))
            {
                Cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
                int ret = await ExecuteNonQuery(Cmd).ConfigureAwait(false);
                if (ret > 0) { Console.WriteLine("{0} Dead PID: {1}", DateTime.Now, pid); }
                return ret;
            }
        }

        public async ValueTask<int> InitTruncate()
        {
           return await ExecuteNonQuery(new MySqlCommand(@"TRUNCATE TABLE crawlprocess;")).ConfigureAwait(false);
        }
    }
}
