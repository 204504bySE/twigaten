using System;
using System.Collections.Generic;
using System.Threading;

using System.Data;
using MySql.Data.MySqlClient;
using Twigaten.Lib;
using System.Threading.Tasks;

namespace Twigaten.CrawlParent
{

    class DBHandler : Lib.DBHandler
    {
        public DBHandler() : base("crawl", "", config.database.Address, config.database.Protocol) { }

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
            using (var cmd = new MySqlCommand(@"SELECT user_id
FROM token
LEFT JOIN crawlprocess USING (user_id)
WHERE pid IS NULL;"))
            {
                if(await ExecuteReader(cmd, (r) => ret.Add(r.GetInt64(0))).ConfigureAwait(false)) { return ret.ToArray(); }
                else { return new long[0]; }    //例によって全部取得成功しない限り返さない
            }
        }

        const int BulkUnit = 1000;
        const string AssignTokensHead = @"INSERT
INTO crawlprocess (user_id, pid, rest_my_tweet)
VALUES";
        const string AssignTokensTail = @"ON DUPLICATE KEY UPDATE pid = VALUES(pid), rest_my_tweet = VALUES(rest_my_tweet);";
        static readonly string AssignTokensStrFull = BulkCmdStr(BulkUnit, 3, AssignTokensHead, AssignTokensTail);
        
        ///<summary>アカウントをまとめて割り当てる</summary>
        public async Task<bool> AssignTokens(IList<(long user_id, int pid)> tokens, bool RestMyTweet)
        {
            var cmdList = new List<MySqlCommand>();
            int i;
            for(i = 0; i < tokens.Count / BulkUnit; i++)
            {
                var cmd = new MySqlCommand(AssignTokensStrFull);
                for(int j = 0; j < BulkUnit; j++)
                {
                    string numstr = j.ToString();
                    cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64).Value = tokens[BulkUnit * i + j].user_id;
                    cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int32).Value = tokens[BulkUnit * i + j].pid;
                    cmd.Parameters.Add("@c" + numstr, MySqlDbType.Bool).Value = RestMyTweet;
                }
                cmdList.Add(cmd);
            }
            if(tokens.Count % BulkUnit != 0)
            {
                var cmd = new MySqlCommand(BulkCmdStr(tokens.Count % BulkUnit, 3, AssignTokensHead, AssignTokensTail));
                for (int j = 0; j < tokens.Count % BulkUnit; j++)
                {
                    string numstr = j.ToString();
                    cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64).Value = tokens[BulkUnit * i + j].user_id;
                    cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int32).Value = tokens[BulkUnit * i + j].pid;
                    cmd.Parameters.Add("@c" + numstr, MySqlDbType.Bool).Value = RestMyTweet;
                }
                cmdList.Add(cmd);
            }
            return await ExecuteNonQuery(cmdList).ConfigureAwait(false) > 0;
        }

        public async ValueTask<int> Deletepid(int pid)
        {
            using (MySqlCommand Cmd = new MySqlCommand(@"UPDATE crawlprocess SET pid = NULL WHERE pid = @pid;"))
            {
                Cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = pid;
                int ret = await ExecuteNonQuery(Cmd).ConfigureAwait(false);
                if (ret > 0) { Console.WriteLine("{0} Dead PID: {1}", DateTime.Now, pid); }
                return ret;
            }
        }

        ///<summary>twidownparent自身を再起動したときにpidを全消しする</summary>
        public Task<int> NullifyPidAll()
        {
           return ExecuteNonQuery(new MySqlCommand(@"UPDATE crawlprocess SET pid = NULL;"));
        }
    }
}
