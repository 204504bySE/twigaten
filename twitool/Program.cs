using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using System.IO;
using System.Data;
using MySql.Data.MySqlClient;
using System.Threading;
using System.Threading.Tasks.Dataflow;

using CoreTweet;
using twitenlib;

namespace twitool
{
    class Program
    {
        static async Task Main(string[] args)
        {
            /*
            await new RemovedMedia().DeleteRemovedTweet().ConfigureAwait(false);
            Console.WriteLine("＼(^o^)／");
            return;
            */

            //CheckOldProcess.CheckandExit();
            Config config = Config.Instance;
            DBHandler db = new DBHandler();

            await db.RemoveOldMedia();
            await db.RemoveOrphanMedia();
            await db.RemoveOldProfileImage();
        }
        /*
        static void CompareHash()
        {
            foreach (string file in Directory.EnumerateFiles(@"D:\data\ぬるい", "*.jpg", SearchOption.AllDirectories))
            {
                long gdi;
                long mono;
                var req = HttpWebRequest.Create(@"http://172.29.0.233:5000/hash/dct");
                using (var mem = new MemoryStream())
                {
                    File.OpenRead(file).CopyTo(mem);
                    mem.Seek(0, SeekOrigin.Begin);
                    gdi = PictHash.DCTHash(mem).Value;
                    mem.Seek(0, SeekOrigin.Begin);

                    req.Method = "POST";
                    req.ContentType = "application/x-www-form-urlencoded";
                    var data = new ASCIIEncoding().GetBytes("=" + Convert.ToBase64String(mem.ToArray()).TrimEnd('=').Replace('+', '-').Replace('/', '_'));
                    req.ContentLength = data.Length;
                    using (var post = req.GetRequestStream())
                    {
                        post.Write(data.ToArray(), 0, data.Length);
                    }
                    
                        var res = req.GetResponse();
                        using (var monostream = res.GetResponseStream())
                        using (var mem2 = new MemoryStream())
                        {
                            monostream.CopyTo(mem2);
                            monostream.Close();
                            mem2.Seek(0, SeekOrigin.Begin);

                            //Console.WriteLine(new UTF8Encoding().GetString(mem2.ToArray()));
                            mono = long.Parse(new UTF8Encoding().GetString(mem2.ToArray()));
                        }
                        Console.WriteLine("{0}\t{1:X16}\t{2:X16}\t{3:X16}\t{4}", HammingDistance((ulong)gdi, (ulong)mono), gdi ^ mono, gdi, mono, file);
                }
            }
        }
        static int HammingDistance(ulong a, ulong b)
        {
            //xorしてpopcnt
            ulong value = a ^ b;

            //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
            ulong result = value - ((value >> 1) & 0x5555555555555555UL);
            result = (result & 0x3333333333333333UL) + ((result >> 2) & 0x3333333333333333UL);
            return (int)(unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56);
        }
        */
        }


    public class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("tool", "", config.database.Address, config.database.Protocol) { }

        //ツイートが削除されて参照されなくなった画像を消す
        public async Task RemoveOrphanMedia()
        {
            int RemovedCount = 0;
            const int BulkUnit = 1000;
            const string head = @"DELETE FROM media WHERE media_id IN";
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);
            try
            {
                var Table = new List<(long media_id, string media_url)>(BulkUnit);
                do
                {
                    Table.Clear();  //ループ判定が後ろにあるのでここでやるしかない
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media
WHERE source_tweet_id IS NULL
ORDER BY media_id
LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        if (!await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetString(1))))) { return; }
                    }
                    if (Table.Count < 1) { break; }

                    foreach (var row in Table)
                    {
                        File.Delete(Path.Combine(config.crawl.PictPaththumb, row.media_id.ToString() + Path.GetExtension(row.media_url)));
                    }

                    if (Table.Count < BulkUnit)
                    {
                        BulkDeleteCmd = BulkCmdStrIn(Table.Count, head);
                    }
                    using (MySqlCommand delcmd = new MySqlCommand(BulkDeleteCmd))
                    {
                        for (int n = 0; n < Table.Count; n++)
                        {
                            delcmd.Parameters.AddWithValue('@' + n.ToString(), Table[n].media_id);
                        }
                        RemovedCount += await ExecuteNonQuery(delcmd);
                    }
                    //Console.WriteLine("{0} Media removed", RemovedCount);
                } while (Table.Count >= BulkUnit);
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0} Orphan Media removed.", RemovedCount);
        }

        public async Task RemoveOldMedia()
        {
            DriveInfo drive = new DriveInfo(config.crawl.MountPointthumb);
            int RemovedCountFile = 0;
            const int BulkUnit = 1000;
            //Console.WriteLine("{0} / {0} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                var Table = new List<(long media_id, string media_url)>(BulkUnit);
                while (drive.TotalFreeSpace < drive.TotalSize / 16)
                {
                    using (MySqlCommand cmd = new MySqlCommand(@"(SELECT
media_id, media_url
FROM media_downloaded_at
NATURAL JOIN media
ORDER BY downloaded_at
LIMIT @limit)
ORDER BY media_id;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        if(!await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetString(1))))) { return; }
                    }
                    if (Table.Count < BulkUnit) { break; }

                    foreach (var row in Table)
                    {
                        File.Delete(Path.Combine(config.crawl.PictPaththumb, (row.media_id.ToString() + Path.GetExtension(row.media_url))));
                    }

                    MySqlCommand[] Cmd = new MySqlCommand[] {
                        new MySqlCommand(BulkCmdStrIn(Table.Count, @"DELETE FROM media_downloaded_at WHERE media_id IN")),
                        new MySqlCommand(BulkCmdStrIn(Table.Count, @"DELETE FROM media WHERE source_tweet_id IS NULL AND media_id IN")) };
                    for (int n = 0; n < Table.Count; n++)
                    {
                        string atNum = '@' + n.ToString();
                        for (int i = 0; i < Cmd.Length; i++)
                        {
                            Cmd[i].Parameters.Add(atNum, DbType.Int64).Value = Table[n].media_id;
                        }
                    }
                    await ExecuteNonQuery(Cmd);
                    foreach(MySqlCommand c in Cmd) { c.Dispose(); }
                    RemovedCountFile += Table.Count;
                    //Console.WriteLine("{0} Media removed", RemovedCountFile);
                    //Console.WriteLine("{0} / {1} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                    Table.Clear();
                }
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0} Old Media removed.", RemovedCountFile);
        }

        //しばらくツイートがないアカウントのprofile_imageを消す
        public async Task RemoveOldProfileImage()
        {
            DriveInfo drive = new DriveInfo(config.crawl.MountPointProfileImage);
            int RemovedCount = 0;
            const int BulkUnit = 1000;
            const string head = @"UPDATE user SET updated_at = NULL WHERE user_id IN";
            string BulkUpdateCmd = BulkCmdStrIn(BulkUnit, head);
            //Console.WriteLine("{0} / {1} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                var Table = new List<(long user_id, string profile_image_url, bool is_default_profile_image)>(BulkUnit);
                while (drive.TotalFreeSpace < drive.TotalSize / 16)
                {
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, profile_image_url, is_default_profile_image FROM user
WHERE updated_at IS NOT NULL AND profile_image_url IS NOT NULL
ORDER BY updated_at LIMIT @limit;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        if (!await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetString(1), r.GetBoolean(2))))) { return; }
                    }
                    if (Table.Count < BulkUnit) { break; }

                    foreach (var row in Table)
                    {
                        if (!row.is_default_profile_image)
                        {
                            File.Delete(Path.Combine(config.crawl.PictPathProfileImage, row.user_id.ToString() + Path.GetExtension(row.profile_image_url)));
                        }
                    }
                    using (MySqlCommand upcmd = new MySqlCommand(BulkUpdateCmd))
                    {
                        for (int n = 0; n < Table.Count; n++)
                        {
                            upcmd.Parameters.Add('@' + n.ToString(), DbType.Int64).Value = Table[n].user_id;
                        }
                        RemovedCount += await ExecuteNonQuery(upcmd);
                    }
                    //Console.WriteLine("{0} Icons removed", RemovedCount);
                    //Console.WriteLine("{0} / {1} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
                    Table.Clear();
                }
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0} Icons removed.", RemovedCount);
        }




        //画像が削除されて意味がなくなったツイートを消す
        //URL転載したやつの転載元ツイートが消された場合
        public int RemoveOrphanTweet()
        {
            const int BulkUnit = 100;
            const int RangeSeconds = 300;
            const string head = @"DELETE FROM tweet WHERE tweet_id IN";
            string BulkDeleteCmd = BulkCmdStrIn(BulkUnit, head);

            TransformBlock<long, DataTable> GetTweetBlock = new TransformBlock<long, DataTable>(async (long id) =>
            {
                using (MySqlCommand Cmd = new MySqlCommand(@"SELECT tweet_id
FROM tweet
WHERE retweet_id IS NULL
AND NOT EXISTS (SELECT * FROM tweet_media WHERE tweet_media.tweet_id = tweet.tweet_id)
AND tweet_id BETWEEN @begin AND @end
ORDER BY tweet_id DESC;"))
                {
                    Cmd.Parameters.AddWithValue("@begin", id);
                    Cmd.Parameters.AddWithValue("@end", id + SnowFlake.msinSnowFlake * RangeSeconds * 1000 - 1);
                    return await SelectTable(Cmd, IsolationLevel.RepeatableRead);
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });


            DateTimeOffset date = DateTimeOffset.UtcNow.AddHours(-1);
            for (int i = 0; i < 20; i++)
            {
                GetTweetBlock.Post(SnowFlake.SecondinSnowFlake(date, false));
                date = date.AddHours(-1);
            }
            while (true)
            {
                DataTable Table = GetTweetBlock.Receive();
                if (Table.Rows.Count > 0)
                {
                    using (MySqlCommand delcmd = new MySqlCommand(BulkCmdStrIn(Table.Rows.Count, head)))
                    {
                        for (int n = 0; n < Table.Rows.Count; n++)
                        {
                            delcmd.Parameters.AddWithValue("@" + n.ToString(), Table.Rows[n].Field<long>(0));
                        }
                        Console.WriteLine("{0} {1} Tweets removed", date, ExecuteNonQuery(delcmd));
                    }
                }
                GetTweetBlock.Post(SnowFlake.SecondinSnowFlake(date, false));
                date = date.AddSeconds(-RangeSeconds);
            }
        }


        //ツイートが削除されて参照されなくなったユーザーを消す
        public async Task<int> RemoveOrphanUser()
        {
            int RemovedCount = 0;
            DataTable Table;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id, profile_image_url, is_default_profile_image FROM user
WHERE NOT EXISTS (SELECT * FROM tweet WHERE tweet.user_id = user.user_id)
AND NOT EXISTS (SELECT user_id FROM token WHERE token.user_id = user.user_id);"))
            {
                Table = await SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }
            if (Table == null) { return 0; }
            Console.WriteLine("{0} {1} Users to remove", DateTime.Now, Table.Rows.Count);
            Console.ReadKey();
            Parallel.ForEach(Table.AsEnumerable(),
                new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                async (DataRow row) =>  //これ動かないよ
                {
                    bool toRemove;
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT EXISTS(SELECT * FROM tweet WHERE tweet.user_id = @user_id) OR EXISTS(SELECT user_id FROM token WHERE token.user_id = @user_id);"))
                    {
                        cmd.Parameters.AddWithValue("@user_id", row.Field<long>(0));
                        toRemove = (await SelectCount(cmd, IsolationLevel.ReadUncommitted) == 0);
                    }
                    if (toRemove)
                    {
                        using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM user WHERE user_id = @user_id;"))
                        {
                            cmd.Parameters.AddWithValue("@user_id", (long)row[0]);
                            if (await ExecuteNonQuery(cmd) >= 1)
                            {
                                if (!row.Field<bool>(2) && row.Field<string>(1) != null) { File.Delete(Path.Combine(config.crawl.PictPathProfileImage, (row.Field<long>(0)).ToString() + Path.GetExtension(row.Field<string>(1)))); }
                                Interlocked.Increment(ref RemovedCount);
                                if (RemovedCount % 1000 == 0) { Console.WriteLine("{0} {1} Users Removed", DateTime.Now, RemovedCount); }
                            }
                        }
                    }
                });
            return RemovedCount;
        }


        public async Task RemoveOrphanProfileImage()
        {
            int RemoveCount = 0;
            IEnumerable<string> Files = Directory.EnumerateFiles(config.crawl.PictPathProfileImage);
            ActionBlock<string> RemoveBlock = new ActionBlock<string>(async (f) =>
            {
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(*) FROM user WHERE user_id = @user_id;"))
                {
                    cmd.Parameters.AddWithValue("@user_id", Path.GetFileNameWithoutExtension(f));
                    if (await SelectCount(cmd, IsolationLevel.ReadUncommitted) == 0)
                    {
                        File.Delete(f);
                        Interlocked.Increment(ref RemoveCount);
                        Console.WriteLine("{0} {1} Files Removed. Last: {2}", DateTime.Now, RemoveCount, Path.GetFileName(f));
                    }
                }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                BoundedCapacity = Environment.ProcessorCount << 8
            });
            foreach(string f in Directory.EnumerateFiles(config.crawl.PictPathProfileImage))
            {
                await RemoveBlock.SendAsync(f);
            }
            RemoveBlock.Complete();
            await RemoveBlock.Completion;
        }
        /*
        public void Nullify_updated_at()
        {
            const int BulkUnit = 10000;
            MySqlCommand cmd = new MySqlCommand(@"UPDATE user SET updated_at = null WHERE updated_at < @time LIMIT @limit;");
            cmd.Parameters.AddWithValue("@time", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
            cmd.Parameters.AddWithValue("@limit", BulkUnit);
            while (ExecuteNonQuery(cmd) > 0) { Console.WriteLine(DateTime.Now); }
        }
        */
        /*
        public void UpdateisProtected()
        {
            //動かしたらTwitterに怒られたからやっぱダメ
            ServicePointManager.ReusePort = true;
            const int ConnectionLimit = 10;
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit * 2;
            const int BulkUnit = 100;
            DataTable Table;
            int updated = 0;

            using (MySqlCommand cmd = new MySqlCommand(@"SELECT
user_id, token, token_secret
FROM token;"))
            {
                Table = SelectTable(cmd);
            }
            if (Table == null) { return; }
            Tokens[] tokens = new Tokens[Table.Rows.Count];
            for (int i = 0; i < Table.Rows.Count; i++)
            {
                tokens[i] = Tokens.Create(config.token.ConsumerKey,
                    config.token.ConsumerSecret,
                    Table.Rows[i].Field<string>(1),
                    Table.Rows[i].Field<string>(2),
                    Table.Rows[i].Field<long>(0)
                    );
                tokens[i].ConnectionOptions.DisableKeepAlive = false;
                tokens[i].ConnectionOptions.UseCompression = true;
                tokens[i].ConnectionOptions.UseCompressionOnStreaming = true;
            }
            Console.WriteLine(tokens.Length);

            int tokenindex = 0;
            object tokenindexlock = new object();
            var UpdateUserBlock = new TransformBlock<long[], int>((long[] user_id) => {
                int i;
                SelectToken:
                lock (tokenindexlock)
                {
                    if (tokenindex >= tokens.Length) { tokenindex = 0; }
                    i = tokenindex;
                    tokenindex++;
                }
                try
                {
                    tokens[i].Account.VerifyCredentials();
                    CoreTweet.Core.ListedResponse<User> users = tokens[i].Users.Lookup(user_id, false);
                    List<MySqlCommand> cmd = new List<MySqlCommand>();
                    foreach (User user in users)
                    {
                        //Console.WriteLine("{0}\t{1}", user.Id, user.IsProtected);                        
                        MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE user SET isprotected = @isprotected WHERE user_id = @user_id;");
                        cmdtmp.Parameters.AddWithValue("@isprotected", user.IsProtected);
                        cmdtmp.Parameters.AddWithValue("@user_id", user.Id);
                        cmd.Add(cmdtmp);
                    }
                    return ExecuteNonQuery(cmd);
                }
                catch (Exception e) { Console.WriteLine(e); goto SelectToken; }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ConnectionLimit });
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user ORDER BY user_id LIMIT 100 OFFSET 80000;"))
            {
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
            }

            int n = 0;
            while (Table != null && Table.Rows.Count > 0)
            {
                long[] user_id = new long[Table.Rows.Count];
                for (int i = 0; i < Table.Rows.Count; i++)
                {
                    user_id[i] = Table.Rows[i].Field<long>(0);
                }
                UpdateUserBlock.Post(user_id);
                if (n > ConnectionLimit)
                {
                    updated += UpdateUserBlock.Receive();
                    Console.WriteLine(updated);
                }
                else { n++; }

                using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE user_id > @lastid ORDER BY user_id LIMIT 100;"))
                {
                    cmd.Parameters.AddWithValue("@lastid", Table.Rows[Table.Rows.Count - 1].Field<long>(0));
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                }
            }
            UpdateUserBlock.Complete();
            UpdateUserBlock.Completion.Wait();
        }
        
        public void ReHashMedia_Dataflow()
        {
            ServicePointManager.ReusePort = true;
            const int ConnectionLimit = 64;
            ServicePointManager.DefaultConnectionLimit = ConnectionLimit * 4;
            const int BulkUnit = 1000;
            DataTable Table;
            int updated = 0;
            var GetHashBlock = new TransformBlock<KeyValuePair<long, string>, KeyValuePair<long, long?>>(media => {
                return new KeyValuePair<long, long?>(media.Key, downloadforHash(media.Value + ":thumb"));
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ConnectionLimit });
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media ORDER BY media_id DESC LIMIT @limit;"))
            {
                cmd.Parameters.AddWithValue("@limit", BulkUnit);
                Table = SelectTable(cmd,IsolationLevel.ReadUncommitted);
            }
            foreach (DataRow row in Table.Rows)
            {
                GetHashBlock.Post(new KeyValuePair<long, string>((long)row[0], row[1] as string));
            }
            while (Table != null && Table.Rows.Count > 0)
            {
                int LastTableCount = Table.Rows.Count;

                using (MySqlCommand cmd = new MySqlCommand(@"SELECT media_id, media_url FROM media WHERE media_id < @lastid ORDER BY media_id DESC LIMIT @limit;"))
                {
                    cmd.Parameters.AddWithValue("@lastid", (long)Table.Rows[Table.Rows.Count - 1][0]);
                    cmd.Parameters.AddWithValue("@limit", BulkUnit);
                    Table = SelectTable(cmd, IsolationLevel.ReadUncommitted);
                }
                foreach (DataRow row in Table.Rows)
                {
                    GetHashBlock.Post(new KeyValuePair<long, string>((long)row[0], row[1] as string));
                }
                KeyValuePair<long, long?> media = new KeyValuePair<long, long?>(0, null);
                for (int i = 0; i < LastTableCount; i++)
                {
                    media = GetHashBlock.Receive();
                    if (media.Value != null)
                    {
                        using (MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET dcthash=@dcthash WHERE media_id = @media_id"))
                        {
                            cmdtmp.Parameters.AddWithValue("@dcthash", media.Value);
                            cmdtmp.Parameters.AddWithValue("@media_id", media.Key);
                            updated += ExecuteNonQuery(cmdtmp);
                        }
                    }
                }
                Console.WriteLine("{0} {1} hashes updated. last: {2}", DateTime.Now, updated, media.Key);
            }
            GetHashBlock.Complete();
            while (GetHashBlock.OutputCount > 0)
            {
                KeyValuePair<long, long?> media = GetHashBlock.Receive();
                if (media.Value != null)
                {
                    using (MySqlCommand cmdtmp = new MySqlCommand(@"UPDATE media SET dcthash=@dcthash WHERE media_id = @media_id"))
                    {
                        cmdtmp.Parameters.AddWithValue("@dcthash", media.Value);
                        cmdtmp.Parameters.AddWithValue("@media_id", media.Key);
                        updated += ExecuteNonQuery(cmdtmp);
                    }
                }
            }
            Console.WriteLine("{0} {1} hashes updated.", DateTime.Now, updated);
        }

        long? downloadforHash(string uri, string referer = null)
        {
            try
            {
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(uri);
                if (referer != null) { req.Referer = referer; }
                WebResponse res = req.GetResponse();

                using (Stream httpStream = res.GetResponseStream())
                using (MemoryStream mem = new MemoryStream())
                {
                    httpStream.CopyTo(mem); //MemoryStreamはFlush不要(FlushはNOP)
                    mem.Position = 0;
                    return PictHash.DCTHash(mem);
                }
            }
            catch { return null; }
        }
        */
    }
}

