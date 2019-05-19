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
        
        ///<summary>アカウントをまとめて割り当てる</summary>
        public async Task<bool> AssignTokens(IEnumerable<(long user_id, int pid)> tokens, bool RestMyTweet)
        {
            var cmdList = new List<MySqlCommand>();

            //めんどくさいのでBulk Insertまではやらない
            foreach(var t in tokens)
            {
                var cmd = new MySqlCommand(@"INSERT
INTO crawlprocess (user_id, pid, rest_my_tweet)
VALUES (@user_id, @pid, @rest_my_tweet)
ON DUPLICATE KEY UPDATE pid=@pid;");
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = t.user_id;
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = t.pid;
                cmd.Parameters.Add("@rest_my_tweet", MySqlDbType.Bool).Value = RestMyTweet;
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
