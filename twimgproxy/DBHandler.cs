using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using twitenlib;

namespace twimgproxy
{
    class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("view", "", "localhost", 20, (uint)Math.Min(Environment.ProcessorCount, 40)) { }

        public async Task<(string Url, string Referer)?> SelectThumbUrl(long media_id)
        { 
            (string Url, string Referer)? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT m.media_url, u.screen_name, t.tweet_id
FROM media m
JOIN tweet t ON m.source_tweet_id = t.tweet_id
JOIN user u USING (user_id)
WHERE media_id = @media_id;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                await ExecuteReader(cmd, (r) => ret = (r.GetString(0), "https://twitter.com/" + r.GetString(1) + "/status/" + r.GetInt64(2).ToString())).ConfigureAwait(false);                
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }

        public async Task<(string Url, string Referer)?> SelectProfileImageUrl(long user_id)
        {
            (string Url, string Referer)? ret = null;
            using (var cmd = new MySqlCommand(@"SELECT profile_image_url, screen_name
FROM user
WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteReader(cmd, (r) => ret = (r.GetString(0), "https://twitter.com/" + r.GetString(1))).ConfigureAwait(false);
            }
            //つまりDBのアクセスに失敗したりしてもnull
            return ret;
        }
    }
}
