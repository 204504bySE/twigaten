using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Data;
using MySql.Data.MySqlClient;
using System.Threading.Tasks;
using twitenlib;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using System.Collections.Concurrent;

namespace twihash
{
    class DBHandler : twitenlib.DBHandler
    {
        public DBHandler() : base("hash", "", Config.Instance.database.Address,20,(uint)Math.Min(Environment.ProcessorCount, 40)) { }
        
        const string StoreMediaPairsHead = @"INSERT IGNORE INTO dcthashpair VALUES";
        public const int StoreMediaPairsUnit = 1000;
        static readonly string StoreMediaPairsStrFull = BulkCmdStr(StoreMediaPairsUnit, 3, StoreMediaPairsHead);
        static readonly MediaPair.OrderPri OrderPri = new MediaPair.OrderPri();
        static readonly MediaPair.OrderSub OrderSub = new MediaPair.OrderSub();

        public async ValueTask<int> StoreMediaPairs(MediaPair[] StorePairs)
        //類似画像のペアをDBに保存
        {
            if (StorePairs.Length > StoreMediaPairsUnit) { throw new ArgumentOutOfRangeException(); }
            else if (StorePairs.Length == 0) { return 0; }
            else
            {
                int ret;
                //まず昇順
                Array.Sort(StorePairs, OrderPri);   //deadlock防止
                using (MySqlCommand Cmd =  new MySqlCommand(
                    StorePairs.Length == StoreMediaPairsUnit ? StoreMediaPairsStrFull
                        : BulkCmdStr(StorePairs.Length, 3, StoreMediaPairsHead)))
                { 
                    for (int i = 0; i < StorePairs.Length; i++)
                    {
                        string numstr = i.ToString();
                        Cmd.Parameters.Add("@a" + numstr, MySqlDbType.Int64).Value = StorePairs[i].media0;
                        Cmd.Parameters.Add("@b" + numstr, MySqlDbType.Int64).Value = StorePairs[i].media1;
                        Cmd.Parameters.Add("@c" + numstr, MySqlDbType.Int64).Value = StorePairs[i].hammingdistance;
                    }
                    ret = await ExecuteNonQuery(Cmd).ConfigureAwait(false);
                    //次に降順
                    Array.Sort(StorePairs, OrderSub);   //deadlock防止
                    for (int i = 0; i < StorePairs.Length; i++)
                    {
                        string numstr = i.ToString();
                        Cmd.Parameters["@a" + numstr].Value = StorePairs[i].media1;   //↑とは逆
                        Cmd.Parameters["@b" + numstr].Value = StorePairs[i].media0;
                        Cmd.Parameters["@c" + numstr].Value = StorePairs[i].hammingdistance;
                    }
                    return ret + await ExecuteNonQuery(Cmd).ConfigureAwait(false);
                }
            }
        }

        ///<summary>DBから読み込んだハッシュをそのままファイルに書き出す</summary>
        public async ValueTask<long> AllMediaHash()
        {
            try
            {
                using (var writer = new AllHashFileWriter())
                {
                    //large heapに入れたくなかったら64 + "10~11" - ~~~ くらい
                    int HashUnitBits = Math.Min(63, 64 + 16 - (int)Math.Log(config.hash.LastHashCount, 2));
                    //とりあえず平均より十分大きめに
                    int TableListSize = (int)Math.Max(16, config.hash.LastHashCount >> (63 - HashUnitBits) << 1);

                    //これに読み込み用のListを入れてメモリ割り当てを減らしてみる
                    var LongPool = new ConcurrentQueue<List<long>>();
                    var WriterBlock = new ActionBlock<List<long>>(
                        (table) => 
                        {
                            writer.Write(table);
                            if(table.Count <= TableListSize)
                            {
                                table.Clear();
                                LongPool.Enqueue(table);
                            }
                        },
                        new ExecutionDataflowBlockOptions()
                        {
                            MaxDegreeOfParallelism = 1,
                            BoundedCapacity = Environment.ProcessorCount << 1
                        });

                    long TotalHashCount = 0;

                    var LoadHashBlock = new ActionBlock<long>(async (i) =>
                    {
                        if (!LongPool.TryDequeue(out var Table)) { Table = new List<long>(TableListSize); }
                        while(true)
                        {
                            using (MySqlCommand Cmd = new MySqlCommand(@"SELECT dcthash
FROM media
WHERE dcthash BETWEEN @begin AND @end
GROUP BY dcthash;"))
                            {
                                Cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = i << HashUnitBits;
                                Cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = unchecked(((i + 1) << HashUnitBits) - 1);
                                if( await ExecuteReader(Cmd, (r) => Table.Add(r.GetInt64(0))).ConfigureAwait(false)) { break; }
                                else { Table.Clear(); }
                            }
                        }
                        Interlocked.Add(ref TotalHashCount, Table.Count);
                        await WriterBlock.SendAsync(Table);
                    }, new ExecutionDataflowBlockOptions()
                    {
                        MaxDegreeOfParallelism = Environment.ProcessorCount,
                        BoundedCapacity = Environment.ProcessorCount << 1,
                        SingleProducerConstrained = true
                    });

                    for(int i = 0; i < 1 << (64 - HashUnitBits); i++)
                    { 
                        await LoadHashBlock.SendAsync(i).ConfigureAwait(false);
                    }
                    LoadHashBlock.Complete();
                    await LoadHashBlock.Completion.ConfigureAwait(false);
                    WriterBlock.Complete();
                    await WriterBlock.Completion.ConfigureAwait(false);
                    return TotalHashCount;
                }
            }
            catch (Exception e) { Console.WriteLine(e); return -1; }
        }

        //dcthashpairに追加する必要があるハッシュを取得するやつ
        //これが始まった後に追加されたハッシュは無視されるが
        //次回の実行で拾われるから問題ない

        public async ValueTask<HashSet<long>> NewerMediaHash()
        {
            try
            {
                var ret = new HashSet<long>();
                const int QueryRangeSeconds = 600;
                var LoadHashBlock = new ActionBlock<long>(async (i) => 
                {
                    var Table = new List<long>();
                    while(true)
                    {
                        using (MySqlCommand Cmd = new MySqlCommand(@"SELECT dcthash
FROM media_downloaded_at
NATURAL JOIN media
WHERE downloaded_at BETWEEN @begin AND @end;"))
                        {
                            Cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = config.hash.LastUpdate + QueryRangeSeconds * i;
                            Cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = config.hash.LastUpdate + QueryRangeSeconds * (i + 1) - 1;
                            if (await ExecuteReader(Cmd, (r) => Table.Add(r.GetInt64(0))).ConfigureAwait(false)) { break; }
                            else { Table.Clear(); }
                        }
                    }
                    lock (ret) { foreach (long h in Table) { ret.Add(h); } }
                }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
                for(long i = 0; i < Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeSeconds() - config.hash.LastUpdate) / QueryRangeSeconds + 1; i++)
                {
                    LoadHashBlock.Post(i);
                }
                LoadHashBlock.Complete();
                await LoadHashBlock.Completion.ConfigureAwait(false);
                return ret;
            }catch(Exception e) { Console.WriteLine(e); return null; }
        }
    }
}
