using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using System.IO;
using System.Data;
using MySqlConnector;
using System.Threading;
using System.Threading.Tasks.Dataflow;

using CoreTweet;
using Twigaten.Lib;
using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Twigaten.Tool
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //CheckOldProcess.CheckandExit();
            DBHandler db = new DBHandler();

            if (args.Length == 0)
            {
                await db.RemoveOldMedia();
                await db.RemoveOrphanMedia();
                await db.RemoveOldProfileImage();
            }
            else
            {
                await CommandLine.Run(args).ConfigureAwait(false);
            }
        }
    }


    class DBHandler : Lib.DBHandler
    {
        public DBHandler() : base(config.database.Address, config.database.Protocol) { }

        //ツイートが削除されて参照されなくなった画像を消す
        public async Task RemoveOrphanMedia()
        {
            int RemovedCount = 0;
            const int BulkUnit = 1000;

            try
            {
                var Table = new List<(long media_id, string media_url)>(BulkUnit);
                do
                {
                    Table.Clear();  //ループ判定が後ろにあるのでここでやるしかない
                    using (MySqlCommand cmd = new MySqlCommand(@"SELECT m.media_id, mt.media_url
FROM media m
LEFT JOIN media_downloaded_at md ON m.media_id = md.media_id
JOIN media_text mt ON m.media_id = mt.media_id
WHERE m.source_tweet_id IS NULL
AND (md.downloaded_at IS NULL OR md.downloaded_at < @downloaded_at)
ORDER BY m.media_id
LIMIT @limit;"))
                    {
                        //ダウンロードしたての画像は除く
                        cmd.Parameters.Add("@downloaded_at", MySqlDbType.Int64).Value = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 600;
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        if (!await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetString(1))))) { return; }
                    }
                    if (Table.Count < 1) { break; }

                    var DeleteMediaBlock = new ActionBlock<(long media_id, string media_url)>(async (row) =>
                    {
                        File.Delete(MediaFolderPath.ThumbPath(row.media_id, row.media_url));

                        using var DeleteCmd = new MySqlCommand(@"DELETE FROM media WHERE media_id = @media_id");
                        using var DeleteCmd2 = new MySqlCommand(@"DELETE FROM media_text WHERE media_id = @media_id");
                        DeleteCmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = row.media_id;
                        DeleteCmd2.Parameters.Add("@media_id", MySqlDbType.Int64).Value = row.media_id;

                        int deleted = await ExecuteNonQuery(new[] { DeleteCmd, DeleteCmd2 }).ConfigureAwait(false);
                        if (deleted > 0) { Interlocked.Increment(ref RemovedCount); }
                    }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });

                    foreach (var row in Table) { DeleteMediaBlock.Post(row); }
                    DeleteMediaBlock.Complete();
                    await DeleteMediaBlock.Completion.ConfigureAwait(false);
                    //Console.WriteLine("{0} Orphan Media removed.", RemovedCount);
                } while (Table.Count >= BulkUnit);
            }
            catch (Exception e) { Console.WriteLine(e); return; }
            Console.WriteLine("{0} Orphan Media removed.", RemovedCount);
        }

        public async Task RemoveOldMedia()
        {
            DriveInfo drive = new DriveInfo(config.crawl.PictPaththumb);
            int RemovedCountFile = 0;
            const int BulkUnit = 1000;
            //Console.WriteLine("{0} / {0} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                var Table = new List<(long media_id, string media_url)>(BulkUnit);
                while (drive.TotalFreeSpace < drive.TotalSize / 16)
                {
                    using (MySqlCommand cmd = new MySqlCommand(@"(SELECT
m.media_id, mt.media_url
FROM media_downloaded_at md
JOIN media m on md.media_id = m.media_id
JOIN media_text mt ON m.media_id = mt.media_id
ORDER BY downloaded_at
LIMIT @limit)
ORDER BY media_id;"))
                    {
                        cmd.Parameters.AddWithValue("@limit", BulkUnit);
                        if (!await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetString(1))))) { return; }
                    }
                    if (Table.Count < BulkUnit) { break; }

                    foreach (var row in Table)
                    {
                        File.Delete(MediaFolderPath.ThumbPath(row.media_id, row.media_url));
                    }

                    using (var Cmd = new MySqlCommand(BulkCmdStrIn(Table.Count, @"DELETE FROM media_downloaded_at WHERE media_id IN")))
                    {
                        for (int i = 0; i < Table.Count; i++)
                        {
                            Cmd.Parameters.Add("@" + i.ToString(), DbType.Int64).Value = Table[i].media_id;
                        }
                        await ExecuteNonQuery(Cmd).ConfigureAwait(false);
                    }
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
            DriveInfo drive = new DriveInfo(config.crawl.PictPathProfileImage);
            int RemovedCount = 0;
            const int BulkUnit = 1000;
            const string head = @"DELETE FROM user_updated_at WHERE user_id IN";
            string BulkUpdateCmd = BulkCmdStrIn(BulkUnit, head);
            //Console.WriteLine("{0} / {1} MB Free.", drive.AvailableFreeSpace >> 20, drive.TotalSize >> 20);
            try
            {
                var Table = new List<(long user_id, string profile_image_url, bool is_default_profile_image)>(BulkUnit);
                while (drive.TotalFreeSpace < drive.TotalSize / 16)
                {
                    using (var cmd = new MySqlCommand(@"SELECT
user_id, profile_image_url, is_default_profile_image
FROM user
JOIN user_updated_at USING (user_id)
WHERE profile_image_url IS NOT NULL
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
                            File.Delete(MediaFolderPath.ProfileImagePath(row.user_id, row.is_default_profile_image, row.profile_image_url));
                        }
                    }
                    using (var upcmd = new MySqlCommand(BulkUpdateCmd))
                    {
                        for (int n = 0; n < Table.Count; n++)
                        {
                            upcmd.Parameters.Add("@" + n.ToString(), DbType.Int64).Value = Table[n].user_id;
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

        public async Task<(long MinDownloadedAt, string[] MediaPath)> GetMediaPath(long downloaded_at)
        {
            using (var cmd = new MySqlCommand(@"SELECT
m.media_id, mt.media_url
FROM media m
JOIN media_downloaded_at md ON m.media_id = md.media_id
JOIN media_text mt ON m.media_id = mt.media_id
WHERE md.downloaded_at <= @downloaded_at
ORDER BY md.downloaded_at DESC
LIMIT @limit"))
            {
                cmd.Parameters.AddWithValue("@downloaded_at", downloaded_at);
                cmd.Parameters.AddWithValue("@limit", 1000);

                var ret = new List<string>();
                long minDownloadedAt = long.MaxValue;
                await ExecuteReader(cmd, (r) =>
                {
                    long down = r.GetInt64(0);
                    if (down < minDownloadedAt) { minDownloadedAt = down; }
                    ret.Add(MediaFolderPath.ThumbPath(down, r.GetString(1)));
                }).ConfigureAwait(false);
                return (minDownloadedAt, ret.ToArray());
            }
        }

        /// <summary>
        /// BlurHashが壊れてたから全消しする
        /// </summary>
        /// <returns></returns>
        public async Task DeleteAllBlurHash()
        {
            const string updateCmdStr = @"UPDATE media_text SET blurhash = '' WHERE media_id = @a";

            long MediaCount = 0;
            long InsertCount = 0;

            var brokenHashRegex = new Regex(@"(..)\1{4,}", RegexOptions.Compiled);

            var doblock = new ActionBlock<long>(async (snowflake) =>
            {
                var medialist = new List<(long media_id, string blurhash)>();
                while (true)
                {
                    using (var getcmd = new MySqlCommand(@"SELECT media_id, blurhash FROM media_text USE INDEX(PRIMARY) 
WHERE media_id BETWEEN @begin AND @end
AND blurhash != '';"))
                    {
                        getcmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = snowflake;
                        getcmd.Parameters.Add("@end", MySqlDbType.Int64).Value = snowflake + SnowFlake.msinSnowFlake * 1000 * 60 - 1;
                        if (await ExecuteReader(getcmd, (r) => medialist.Add((r.GetInt64(0), r.GetString(1)))).ConfigureAwait(false)) { break; }
                    }
                }
                Interlocked.Add(ref MediaCount, medialist.Count);
                int localUpdateCount = 0;
                using (var updateCmd = new MySqlCommand(updateCmdStr))
                {
                    var p = updateCmd.Parameters.Add("@a", MySqlDbType.Int64);
                    foreach (var m in medialist)
                    {
                        if (!brokenHashRegex.IsMatch(m.blurhash)) { continue; }
                        p.Value = m.media_id;
                        localUpdateCount += await ExecuteNonQuery(updateCmd).ConfigureAwait(false);
                    }
                }
                Interlocked.Add(ref InsertCount, localUpdateCount);
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount, BoundedCapacity = Environment.ProcessorCount + 1 });

            var sw = Stopwatch.StartNew();

            long endsnowflake = SnowFlake.SecondinSnowFlake(DateTimeOffset.UtcNow, false);
            long snowflakecount;
            for (snowflakecount = 1125015780388372481; snowflakecount < endsnowflake; snowflakecount += SnowFlake.msinSnowFlake * 1000 * 60)
            {
                await doblock.SendAsync(snowflakecount).ConfigureAwait(false);
                if (sw.ElapsedMilliseconds >= 60000)
                {
                    Console.WriteLine("{0}\t{1} / {2}\t{3}", DateTime.Now, InsertCount, MediaCount, snowflakecount);
                    sw.Restart();
                }
            }
            doblock.Complete();
            await doblock.Completion.ConfigureAwait(false);
            Console.WriteLine("{0}\t{1} / {2}\t{3}", DateTime.Now, InsertCount, MediaCount, snowflakecount);
        }
    }
}
