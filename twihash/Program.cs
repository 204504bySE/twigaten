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

            Console.WriteLine("{0} Loading hash", DateTime.Now);
            sw.Restart();
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 600;   //とりあえず10分前
            long Count = await db.AllMediaHash();
            HashSet<long> NewHash = null;
            if (config.hash.LastUpdate > 0) //これが0なら全ハッシュを追加処理対象とする
            {
                NewHash = await db.NewerMediaHash();
                if (NewHash == null) { Console.WriteLine("{0} New hash load failed.", DateTime.Now); Thread.Sleep(5000); Environment.Exit(1); }
                Console.WriteLine("{0} {1} New hash", DateTime.Now, NewHash.Count);
            }
            GC.Collect();
            sw.Stop();
            if (Count < 0) { Console.WriteLine("{0} Hash load failed.", DateTime.Now); Thread.Sleep(5000); Environment.Exit(1); }
            else
            {
                Console.WriteLine("{0} {1} Hash loaded in {2} ms", DateTime.Now, Count, sw.ElapsedMilliseconds);
                config.hash.NewLastHashCount(Count);
            }
            GC.Collect();
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(NewHash, db, config.hash.MaxHammingDistance, config.hash.ExtraBlocks);
            await media.Proceed();
            sw.Stop();
            Console.WriteLine("{0} Multiple Sort, Store: {1}ms", DateTime.Now, sw.ElapsedMilliseconds);
            File.Delete(SortFile.AllHashFilePath);
            config.hash.NewLastUpdate(NewLastUpdate);
            Thread.Sleep(5000);
        }
    }
    /*
    //とても変なクラスになってしまっためう
    public class MediaHashArray
    {
        public readonly long[] Hashes;
        public readonly HashSet<long> NewHashes;
        public MediaHashArray(int Length)
        {
            Hashes = new long[Length];
            NewHashes = new HashSet<long>();
            ForceInsert = Config.Instance.hash.LastUpdate <= 0;
            if (Config.Instance.hash.KeepDataRAM) { AutoReadAll(); }
        }
        public int Count = 0;  //実際に使ってる個数
        public readonly bool ForceInsert;

        public bool NeedInsert(int Index)
        {
            return ForceInsert || NewHashes.Contains(Hashes[Index]);
        }

        void AutoReadAll()
        {
            Task.Run(() => {
                Thread.CurrentThread.Priority = ThreadPriority.Lowest;
                while (true)
                {
                    for(int i = 0; i < Hashes.Length; i++) { long a = Hashes[i]; }
                    Thread.Sleep(60000);
                }
            });
        }
    }
    */
    //ハミング距離が一定以下のハッシュ値のペア
    public struct MediaPair
    {
        public readonly long media0;
        public readonly long media1;
        public readonly sbyte hammingdistance;
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
                Console.WriteLine("{0} {1}\t{2} / {3}\t{4}\t{5}ms ", DateTime.Now, i, db, sort, Combi.CombiString(i), sw.ElapsedMilliseconds);
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
                async (MediaPair[] p) => { Interlocked.Add(ref dbcount, await db.StoreMediaPairs(p)); },
                new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true, MaxDegreeOfParallelism = Environment.ProcessorCount });
            PairBatchBlock.LinkTo(PairStoreBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            ActionBlock<long[]> MultipleSortBlock = new ActionBlock<long[]>((Sorted) =>
            {
                for (int i = 0; i < Sorted.Length; i++)
                {
                    bool NeedInsert(long IndexInSorted) => NewHash == null || NewHash.Contains(Sorted[IndexInSorted]);
                    //long maskedhash_i = Sorted[i] & FullMask;
                    bool NeedInsert_i = NeedInsert(i);
                    for (int j = i + 1; j < Sorted.Length; j++)
                    {
                        //if (maskedhash_i != (Sorted[j] & FullMask)) { break; }    //これはSortedFileReaderがやってくれる
                        if (!NeedInsert_i && !NeedInsert(j)) { continue; }
                        //ブロックソートで一致した組のハミング距離を測る
                        int ham = HammingDistance((ulong)Sorted[i], (ulong)Sorted[j]);
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
            }, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = Environment.ProcessorCount << 8,
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                SingleProducerConstrained = true
            });

            using (SortedFileReader Reader = new SortedFileReader(SortedFilePath, FullMask))
            {
                for (long[] Sorted = await Reader.ReadBlock(); Sorted != null; Sorted = await Reader.ReadBlock())
                {
                    if (Sorted.Length >= 2) { await MultipleSortBlock.SendAsync(Sorted); }
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
        /*
        void QuickSortAll(long SortMask, MediaHashArray SortList)
        {
            var QuickSortBlock = new TransformBlock<(int Begin, int End), (int Begin1, int End1, int Begin2, int End2)?>
                (((int Begin, int End) SortRange) => {
                    return QuickSortUnit(SortRange, SortMask, SortList);
                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });

            QuickSortBlock.Post((0, SortList.Count - 1));
            int ProcessingCount = 1;
            do
            {
                (int Begin1, int End1, int Begin2, int End2)? NextSortRange = QuickSortBlock.Receive();
                if (NextSortRange != null)
                {
                    QuickSortBlock.Post((NextSortRange.Value.Begin1, NextSortRange.Value.End1));
                    QuickSortBlock.Post((NextSortRange.Value.Begin2, NextSortRange.Value.End2));
                    ProcessingCount++;  //↑で1個終わって2個始めたから1個増える
                }
                else { ProcessingCount--; } 
            } while (ProcessingCount > 0);
        }

        (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange, long SortMask, MediaHashArray SortList)
        {
            if (SortRange.Begin >= SortRange.End) { return null; }
            
            //要素数が少なかったら挿入ソートしたい
            if (SortRange.End - SortRange.Begin <= 16)
            {
                for (int k = SortRange.Begin + 1; k <= SortRange.End; k++)
                {
                    long TempHash = SortList.Hashes[k];
                    long TempMasked = SortList.Hashes[k] & SortMask;
                    if ((SortList.Hashes[k - 1] & SortMask) > TempMasked)
                    {
                        int m = k;
                        do
                        {
                            SortList.Hashes[m] = SortList.Hashes[m - 1];
                            m--;
                        } while (m > SortRange.Begin
                        && (SortList.Hashes[m - 1] & SortMask) > TempMasked);
                        SortList.Hashes[m] = TempHash;
                    }
                }
                return null;
            }
            
            long PivotMasked = new long[] { SortList.Hashes[SortRange.Begin] & SortMask,
                        SortList.Hashes[(SortRange.Begin >> 1) + (SortRange.End >> 1)] & SortMask,
                        SortList.Hashes[SortRange.End] & SortMask }
                .OrderBy((long a) => a).Skip(1).First();
            int i = SortRange.Begin; int j = SortRange.End;
            while (true)
            {
                while ((SortList.Hashes[i] & SortMask) < PivotMasked) { i++; }
                while ((SortList.Hashes[j] & SortMask) > PivotMasked) { j--; }
                if (i >= j) { break; }
                long SwapHash = SortList.Hashes[i];
                SortList.Hashes[i] = SortList.Hashes[j];
                SortList.Hashes[j] = SwapHash;
                i++; j--;
            }
            return (SortRange.Begin, i - 1, j + 1, SortRange.End);
        }
        */
        //ハミング距離を計算する
        int HammingDistance(ulong a, ulong b)
        {
            //xorしてpopcnt
            ulong value = a ^ b;

            //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
            ulong result = value - ((value >> 1) & 0x5555555555555555UL);
            result = (result & 0x3333333333333333UL) + ((result >> 2) & 0x3333333333333333UL);
            return (int)(unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FUL) * 0x101010101010101UL) >> 56);
        }
    }
}