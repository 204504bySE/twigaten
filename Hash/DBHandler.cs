using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using MySqlConnector;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Collections.Concurrent;
using System.IO;

namespace Twigaten.Hash
{
    class DBHandler : Lib.DBHandler
    {
        public DBHandler(HashFile hashfile) : base(config.database.Address, config.database.Protocol, 20, (uint)Math.Min(Environment.ProcessorCount, 40), 86400)
        {
            this.hashfile = hashfile;

            HashUnitBits = Math.Min(63, 64 + 11 - (int)Math.Log(Math.Max(1, hashfile.LastHashCount), 2));
            TableListSize = (int)Math.Max(4096, hashfile.LastHashCount >> (63 - HashUnitBits) << 2);
        }
        const string StoreMediaPairsHead = @"INSERT IGNORE INTO dcthashpairslim VALUES";
        public const int StoreMediaPairsUnit = 1000;
        static readonly string StoreMediaPairsStrFull = BulkCmdStr(StoreMediaPairsUnit, 2, StoreMediaPairsHead);

        readonly HashFile hashfile;
        readonly ConcurrentBag<MySqlCommand> StoreMediaPairsCmdPool = new ConcurrentBag<MySqlCommand>();
        
        public async Task<int> StoreMediaPairs(HashPair[] StorePairs)
        //類似画像のペアをDBに保存
        {
            if (StorePairs.Length > StoreMediaPairsUnit) { throw new ArgumentException(); }
            else if (StorePairs.Length == 0) { return 0; }

            Array.Sort(StorePairs, HashPair.Comparison);   //deadlock防止
            if (StorePairs.Length == StoreMediaPairsUnit)
            {   //MySqlCommandをプールしてMySqlCommandおよびstringの生成を抑制する
                if (!StoreMediaPairsCmdPool.TryTake(out var cmd))
                {
                    cmd = new MySqlCommand(StoreMediaPairsStrFull);
                    for (int i = 0; i < StoreMediaPairsUnit; i++)
                    {
                        string numstr = i.ToString();
                        cmd.Parameters.Insert(i << 1, new MySqlParameter("@a" + numstr, MySqlDbType.Int64));
                        cmd.Parameters.Insert(i << 1 | 1, new MySqlParameter("@b" + numstr, MySqlDbType.Int64));
                    }
                }
                for (int i = 0; i < StoreMediaPairsUnit; i++)
                {
                    cmd.Parameters[i << 1].Value = StorePairs[i].small;
                    cmd.Parameters[i << 1 | 1].Value = StorePairs[i].large;
                }
                int ret = await ExecuteNonQuery(cmd).ConfigureAwait(false);
                cmd.Connection = null;
                StoreMediaPairsCmdPool.Add(cmd);
                return ret;
            }
            else
            {
                using var cmd = new MySqlCommand(BulkCmdStr(StorePairs.Length, 2, StoreMediaPairsHead));
                for (int i = 0; i < StorePairs.Length; i++)
                {
                    string numstr = i.ToString();
                    cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64).Value = StorePairs[i].small;
                    cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64).Value = StorePairs[i].large;
                }
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async Task<long> Min_downloaded_at()
        {
            using (var cmd = new MySqlCommand(@"SELECT MIN(downloaded_at) FROM media_downloaded_at;"))
            {
                return await SelectCount(cmd,IsolationLevel.ReadUncommitted).ConfigureAwait(false);
            }
        }

        //tableをlarge heapに入れたくなかったら64 + "9" - ~~~ が限度
        //ArrayPool使ってるからもう関係ないけど
        internal readonly int HashUnitBits;
        //とりあえず平均より十分大きめに
        internal readonly int TableListSize;

        ///<summary>DBから読み込んだハッシュをそのままファイルに書き出す</summary>
        ///<param name="SaveTime">保存するファイル名に付けるUNIX時刻</param>
        public async Task<long> AllMediaHash(long SaveTime)
        {
            try
            {
                long TotalHashCount = 0;
                string HashFilePath = HashFile.AllHashFilePathBase(SaveTime.ToString());
                using (var writer = new BufferedLongWriter(HashFile.TempFilePath(HashFilePath)))
                {

                    var LoadHashBlock = new TransformBlock<long, AddOnlyList<long>>(async (i) =>
                    {
                        var table = new AddOnlyList<long>(TableListSize);
                        while(true)
                        {
                            using (MySqlCommand cmd = new MySqlCommand(@"SELECT DISTINCT dcthash
FROM media
WHERE dcthash BETWEEN @begin AND @end
GROUP BY dcthash;"))
                            {
                                cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = i << HashUnitBits;
                                cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = ((i + 1) << HashUnitBits) - 1;
                                if( await ExecuteReader(cmd, (r) => table.Add(r.GetInt64(0)), IsolationLevel.ReadUncommitted).ConfigureAwait(false)) { break; }
                                else { table.Clear(); }
                            }
                        }
                        return table;
                    }, new ExecutionDataflowBlockOptions()
                    {
                        MaxDegreeOfParallelism = Environment.ProcessorCount,
                        BoundedCapacity = Environment.ProcessorCount << 1,
                        SingleProducerConstrained = true
                    });
                    var WriterBlock = new ActionBlock<AddOnlyList<long>>(async (table) =>
                    {
                        await writer.Write(table.InnerArray, table.Count).ConfigureAwait(false);
                        TotalHashCount += table.Count;
                        table.Dispose();
                    }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 1 });
                    LoadHashBlock.LinkTo(WriterBlock, new DataflowLinkOptions() { PropagateCompletion = true });


                    for (int i = 0; i < 1 << (64 - HashUnitBits); i++)
                    { 
                        await LoadHashBlock.SendAsync(i).ConfigureAwait(false);
                    }
                    LoadHashBlock.Complete();
                    await WriterBlock.Completion.ConfigureAwait(false);
                }
                File.Move(HashFile.TempFilePath(HashFilePath), HashFilePath);
                return TotalHashCount;
            }
            catch (Exception e) { Console.WriteLine(e); return -1; }
        }

        /// <summary>
        ///dcthashpairに追加する必要があるハッシュを取得するやつ
        ///これが始まった後に追加されたハッシュは無視されるが
        ///次回の実行で拾われるから問題ない
        /// </summary>
        ///<param name="SaveTime">保存するファイル名に付けるUNIX時刻</param>
        ///<param name="BeginTime">downloaded_atがこれ以降のハッシュを取得する</param>
        public async Task<HashSet<long>> NewerMediaHash(long SaveTime, long BeginTime)
        {
            string FilePath = HashFile.NewerHashFilePathBase(SaveTime.ToString());
            try
            {
                var ret = new HashSet<long>();
                const int QueryRangeSeconds = 600;
                var LoadHashBlock = new ActionBlock<long>(async (i) => 
                {
                    var Table = new List<long>();
                    while(true)
                    {
                        using (MySqlCommand cmd = new MySqlCommand(@"SELECT dcthash
FROM media_downloaded_at
NATURAL JOIN media
WHERE downloaded_at BETWEEN @begin AND @end;"))
                        {
                            cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = BeginTime + QueryRangeSeconds * i;
                            cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = BeginTime + QueryRangeSeconds * (i + 1) - 1;
                            if (await ExecuteReader(cmd, (r) => Table.Add(r.GetInt64(0)), IsolationLevel.ReadUncommitted).ConfigureAwait(false)) { break; }
                            else { Table.Clear(); }
                        }
                    }
                    lock (ret) { foreach (long h in Table) { ret.Add(h); } }
                }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
                for(long i = 0; i < Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeSeconds() - BeginTime) / QueryRangeSeconds + 1; i++)
                {
                    LoadHashBlock.Post(i);
                }
                LoadHashBlock.Complete();
                await LoadHashBlock.Completion.ConfigureAwait(false);
                                
                using (var writer = new UnbufferedLongWriter(HashFile.TempFilePath(FilePath)))
                {
                    writer.WriteDestructive(ret.ToArray(), ret.Count);
                }
                File.Move(HashFile.TempFilePath(FilePath), FilePath);
                return ret;
            }catch(Exception e) { Console.WriteLine(e); return null; }
        }
    }
}
