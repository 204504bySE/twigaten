using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Threading;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Data;
using twitenlib;
using System.IO;

namespace twihash
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //CheckOldProcess.CheckandExit();
            Config config = Config.Instance;
            DBHandler db = new DBHandler();
            Stopwatch sw = new Stopwatch();

            Console.WriteLine("Loading hash");
            sw.Restart();
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 600;   //とりあえず10分前
            long Count = await db.AllMediaHash();
            HashSet<long> NewHash = null;
            if (config.hash.LastUpdate > 0) //これが0なら全ハッシュを追加処理対象とする
            {
                NewHash = await db.NewerMediaHash();
                if (NewHash == null) { Console.WriteLine("New hash load failed."); Environment.Exit(1); }
                Console.WriteLine("{0} New hash", NewHash.Count);
            }
            GC.Collect();
            sw.Stop();
            if (Count < 0) { Console.WriteLine("Hash load failed."); Environment.Exit(1); }
            else
            {
                Console.WriteLine("{0} Hash loaded in {1} ms", Count, sw.ElapsedMilliseconds);
                config.hash.NewLastHashCount(Count);
            }
            GC.Collect();
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(NewHash, db, config.hash.MaxHammingDistance, config.hash.ExtraBlocks);
            await media.Proceed();
            sw.Stop();
            Console.WriteLine("Multiple Sort, Store: {0}ms", sw.ElapsedMilliseconds);
            File.Delete(SortFile.AllHashFilePath);
            config.hash.NewLastUpdate(NewLastUpdate);
        }
    }

    //ハミング距離が一定以下のハッシュ値のペア
    public struct MediaPair
    {
        public long media0;
        public long media1;
        public sbyte hammingdistance;
        public MediaPair(long _media0, long _media1, sbyte _ham)
        {
            media0 = _media0;
            media1 = _media1;
            hammingdistance = _ham;
        }

        //media0,media1順で比較
        public class OrderPri : IComparer<MediaPair>
        {
            public int Compare(MediaPair a, MediaPair b)
            {
                if (a.media0 < b.media0) { return -1; }
                else if (a.media0 > b.media0) { return 1; }
                else if (a.media1 < b.media1) { return -1; }
                else if (a.media1 > b.media1) { return 1; }
                else { return 0; }
            }
        }
        //media1,media0順で比較
        public class OrderSub : IComparer<MediaPair>
        {
            public int Compare(MediaPair a, MediaPair b)
            {
                if (a.media1 < b.media1) { return -1; }
                else if (a.media1 > b.media1) { return 1; }
                else if (a.media0 < b.media0) { return -1; }
                else if (a.media0 > b.media0) { return 1; }
                else { return 0; }
            }
        }
    }

    //複合ソート法による全ペア類似度検索 とかいうやつ
    //http://d.hatena.ne.jp/tb_yasu/20091107/1257593519
    class MediaHashSorter
    {
        readonly DBHandler db;
        readonly HashSet<long> NewHash;
        readonly int MaxHammingDistance;
        readonly int ExtraBlock;
        readonly Combinations Combi;
        public MediaHashSorter(HashSet<long> NewHash, DBHandler db, int MaxHammingDistance, int ExtraBlock)
        {
            this.NewHash = NewHash; //nullだったら全hashが処理対象
            this.MaxHammingDistance = MaxHammingDistance;
            this.ExtraBlock = ExtraBlock;
            this.db = db;
            Combi = new Combinations(MaxHammingDistance + ExtraBlock, ExtraBlock);
        }

        public async Task Proceed()
        {
            Stopwatch sw = new Stopwatch();
            for (int i = 0; i < Combi.Length; i++)
            {
                sw.Restart();
                GC.Collect();
                (int db, int sort) = await MultipleSortUnit(i);
                sw.Stop();
                Console.WriteLine("{0}\t{1} / {2}\t{3}\t{4}ms ", i, db, sort, Combi.CombiString(i), sw.ElapsedMilliseconds);
            }
        }


        const int bitcount = 64;    //longのbit数
        async ValueTask<(int db, int sort)> MultipleSortUnit(int Index)
        {
            int[] BaseBlocks = Combi[Index];
            int StartBlock = BaseBlocks.Last();
            long FullMask = UnMask(BaseBlocks, Combi.Count);
            string SortedFilePath = await SortFile.MergeSortAll(FullMask);

            int ret = 0;
            int dbcount = 0;

            BatchBlock<MediaPair> PairBatchBlock = new BatchBlock<MediaPair>(DBHandler.StoreMediaPairsUnit);
            ActionBlock<MediaPair[]> PairStoreBlock = new ActionBlock<MediaPair[]>(
                async (MediaPair[] p) =>
                {
                int AddCount;
                    do { AddCount = await db.StoreMediaPairs(p); } while (AddCount < 0);    //失敗したら無限に再試行
                    Interlocked.Add(ref dbcount, AddCount); 
                },
                new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true, MaxDegreeOfParallelism = Environment.ProcessorCount });
            PairBatchBlock.LinkTo(PairStoreBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            ActionBlock<long[]> MultipleSortBlock = new ActionBlock<long[]>((Sorted) =>
            {
                for (int i = 0; i < Sorted.Length; i++)
                {
                    bool NeedInsert_i = NewHash?.Contains(Sorted[i]) ?? true;
                    //long maskedhash_i = Sorted[i] & FullMask;
                    for (int j = i + 1; j < Sorted.Length; j++)
                    {
                        //if (maskedhash_i != (Sorted[j] & FullMask)) { break; }    //これはSortedFileReaderがやってくれる
                        if (NeedInsert_i || NewHash.Contains(Sorted[j]))    //NewHashがnullなら後者は処理されないからセーフ
                        {
                            //ブロックソートで一致した組のハミング距離を測る
                            int ham = HammingDistance(Sorted[i], Sorted[j]);
                            if (ham <= MaxHammingDistance)
                            {
                                //一致したペアが見つかる最初の組合せを調べる
                                int matchblockindex = 0;
                                int x;
                                for (x = 0; x < StartBlock && matchblockindex < BaseBlocks.Length; x++)
                                {
                                    if (BaseBlocks.Contains(x))
                                    {
                                        if (x < BaseBlocks[matchblockindex]) { break; }
                                        matchblockindex++;
                                    }
                                    else
                                    {
                                        long blockmask = UnMask(x, Combi.Count);
                                        if ((Sorted[i] & blockmask) == (Sorted[j] & blockmask))
                                        {
                                            if (x < BaseBlocks[matchblockindex]) { break; }
                                            matchblockindex++;
                                        }
                                    }
                                }
                                //最初の組合せだったときだけ入れる
                                if (x == StartBlock)
                                {
                                    Interlocked.Increment(ref ret);
                                    PairBatchBlock.Post(new MediaPair(Sorted[i], Sorted[j], (sbyte)ham));
                                }
                            }
                        }
                    }
                }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                SingleProducerConstrained = true
            });

            using (SortedFileReader Reader = new SortedFileReader(SortedFilePath, FullMask))
            {
                int MaxInputCount = Environment.ProcessorCount << 10;
                for (long[] Sorted = Reader.ReadBlock(); Sorted != null; Sorted = Reader.ReadBlock())
                {
                    //長さ1の要素はReadBlock()が弾いてくれるのでここでは何も考えない
                    while (MultipleSortBlock.InputCount > MaxInputCount) { await Task.Delay(1); }
                    MultipleSortBlock.Post(Sorted);
                }
            }
            File.Delete(SortedFilePath);
            //余りをDBに入れる
            MultipleSortBlock.Complete(); await MultipleSortBlock.Completion;
            PairBatchBlock.Complete(); await PairStoreBlock.Completion;
            return (dbcount, ret);
        }

        long UnMask(int block, int blockcount)
        {
            return UnMask(new int[] { block }, blockcount);
        }

        long UnMask(int[] blocks, int blockcount)
        {
            long ret = 0;
            foreach (int b in blocks)
            {
                for (int i = bitcount * b / blockcount; i < bitcount * (b + 1) / blockcount && i < bitcount; i++)
                {
                    ret |= 1L << i;
                }
            }
            return ret;
        }

        //ハミング距離を計算する
        int HammingDistance(long a, long b)
        {
            //xorしてpopcnt
            long value = a ^ b;

            //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
            long result = value - ((value >> 1) & 0x5555555555555555L);
            result = (result & 0x3333333333333333L) + ((result >> 2) & 0x3333333333333333L);
            return (int)(unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FL) * 0x101010101010101L) >> 56);
        }
    }
}