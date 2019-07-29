using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Runtime.Intrinsics.X86;
using System.Runtime.CompilerServices;
using System.Buffers;
using twitenlib;

namespace twihash
{
    ///<summary>ハミング距離が一定以下のハッシュ値のペアを突っ込むやつ</summary>
    public readonly struct HashPair
    {
        public readonly long small;
        public readonly long large;
        public HashPair(long _media0, long _media1)
        {
            if (_media0 < _media1)
            {
                small = _media0;
                large = _media1;
            }
            else if (_media1 < _media0)
            {
                small = _media1;
                large = _media0;
            }
            else { throw new ArgumentException(nameof(_media0) + " == " + nameof(_media1)); }
        }

        ///<summary>small, large順で比較</summary>
        public static Comparison<HashPair> Comparison { get; } 
            = ((a, b) =>
        {
            if (a.small < b.small) { return -1; }
            else if (a.small > b.small) { return 1; }
            else if (a.large < b.large) { return -1; }
            else if (a.large > b.large) { return 1; }
            else { return 0; }
        });
    }

    /// <summary>
    /// 複合ソート法による全ペア類似度検索 とかいうやつ
    /// http://d.hatena.ne.jp/tb_yasu/20091107/1257593519
    /// </summary>
    class MediaHashSorter
    {
        readonly DBHandler db;
        readonly Config config = Config.Instance;
        readonly HashSet<long> NewHash;
        readonly long MaxHammingDistance;
        readonly long HashCount;
        readonly Combinations Combi;

        readonly BatchBlock<HashPair> PairBatchBlock = new BatchBlock<HashPair>(DBHandler.StoreMediaPairsUnit);
        readonly ActionBlock<HashPair[]> PairStoreBlock;
        int PairCount;
        int DBAddCount;

        public MediaHashSorter(HashSet<long> NewHash, DBHandler db, int MaxHammingDistance, int ExtraBlock, long HashCount)
        {
            this.NewHash = NewHash; //nullだったら全hashが処理対象
            this.MaxHammingDistance = MaxHammingDistance;
            this.db = db;
            this.HashCount = HashCount;
            Combi = new Combinations(MaxHammingDistance + ExtraBlock, ExtraBlock);

            PairStoreBlock = new ActionBlock<HashPair[]>(
            async (p) =>
            {
                int AddCount;
                do { AddCount = await db.StoreMediaPairs(p).ConfigureAwait(false); } while (AddCount < 0);    //失敗したら無限に再試行
                if (0 < AddCount) { Interlocked.Add(ref DBAddCount, AddCount); }
            },
            new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true, MaxDegreeOfParallelism = Environment.ProcessorCount });
            PairBatchBlock.LinkTo(PairStoreBlock, new DataflowLinkOptions() { PropagateCompletion = true });
        }

        public async Task Proceed()
        {
            var SortTask = new List<Task>(Combi.Length);
            for (int i = 0; i < Combi.Length; i++)
            {
                SortTask.Add(await MultipleSortUnit(i).ConfigureAwait(false));
            }
            await Task.WhenAll(SortTask).ConfigureAwait(false);
            PairBatchBlock.Complete();
            await PairStoreBlock.Completion.ConfigureAwait(false);
            Console.WriteLine("{0} / {1} Pairs Inserted",DBAddCount, PairCount);
        }

        const int bitcount = sizeof(long) * 8;    //longのbit数
        async Task<Task> MultipleSortUnit(int Index)
        {
            int[] BaseBlocks = Combi[Index];
            int StartBlock = BaseBlocks.Last();
            long SortMask = UnMask(BaseBlocks, Combi.Count);

            var QuickSortSW = Stopwatch.StartNew();

            //適当なサイズに切ってそれぞれをクイックソート
            int SortedFileCount = await SplitQuickSort.QuickSortAll(SortMask, HashCount).ConfigureAwait(false);
            QuickSortSW.Stop();
            Console.WriteLine("{0}\tFile Sort\t{1}ms ", Index, QuickSortSW.ElapsedMilliseconds);

            long[] UnMasks = Enumerable.Range(0, Combi.Count).Select(i => UnMask(i, Combi.Count)).ToArray();

            var MultipleSortBlock = new ActionBlock<AddOnlyList<long>>((BlockList) =>
            {
                var Block = BlockList.InnerArray;
                var NewHash = this.NewHash;

                int LocalPairCount = 0;  //見つけた組合せの数を数える
                int BlockIndex = 0;
                int CurrentBlockLength;

                //要素数,実際の要素,…,要素数,…,0 という配列を読み込んでいく
                for (; (CurrentBlockLength = (int)Block[BlockIndex]) > 0; BlockIndex += CurrentBlockLength + 1)
                {
                    //「実際の要素」1セット分を取り出す
                    var SortedSpan = Block.AsSpan(BlockIndex + 1, CurrentBlockLength);

                    //新しい値を含まないやつは省略
                    if(NewHash != null)
                    {
                        int i;
                        for(i = 0; i < SortedSpan.Length; i++)
                        {
                            if (NewHash.Contains(SortedSpan[i])) { break; }
                        }
                        if(i == SortedSpan.Length) { continue; }
                    }

                    for (int i = 0; i < SortedSpan.Length; i++)
                    {
                        long Sorted_i = SortedSpan[i];
                        bool NeedInsert_i = NewHash?.Contains(Sorted_i) ?? true;
                        long maskedhash_i = Sorted_i & SortMask;
                        for (int j = i + 1; j < SortedSpan.Length; j++)
                        {
                            long Sorted_j = SortedSpan[j];
                            //if (maskedhash_i != (Sorted[j] & FullMask)) { break; }    //これはSortedFileReaderがやってくれる
                            //すでにDBに入っているペアは処理しない
                            if ((NeedInsert_i || NewHash.Contains(Sorted_j))    //NewHashがnullなら後者は処理されないからセーフ
                                //ブロックソートで一致した組のハミング距離を測る
                                && HammingDistance(Sorted_i, Sorted_j) <= MaxHammingDistance)
                            {
                                //一致したペアが見つかる最初の組合せを調べる
                                int matchblockindex = 0;
                                int x;
                                for (x = 0; x < UnMasks.Length && x < StartBlock && matchblockindex < BaseBlocks.Length; x++)
                                {
                                    if (BaseBlocks.Contains(x))
                                    {
                                        if (x < BaseBlocks[matchblockindex]) { break; }
                                        matchblockindex++;
                                    }
                                    else
                                    {
                                        if ((Sorted_i & UnMasks[x]) == (Sorted_j & UnMasks[x]))
                                        {
                                            if (x < BaseBlocks[matchblockindex]) { break; }
                                            matchblockindex++;
                                        }
                                    }
                                }
                                //最初の組合せだったときだけ入れる
                                if (x == StartBlock)
                                {
                                    LocalPairCount++;
                                    PairBatchBlock.Post(new HashPair(Sorted_i, Sorted_j));
                                }
                            }
                        }
                    }
                }
                BlockList.Dispose();
                Interlocked.Add(ref PairCount, LocalPairCount);
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                BoundedCapacity = config.hash.MultipleSortBufferCount,
                SingleProducerConstrained = true
            });

            var MultipleSortSW = Stopwatch.StartNew();

            //クイックソートしたファイル群をマージソートしながら読み込む
            using (var Reader = new MergeSortReader(SortedFileCount, SortMask, NewHash))
            {
                AddOnlyList<long> Sorted;
                while ((Sorted = Reader.ReadBlocks()) != null)
                {
                    await MultipleSortBlock.SendAsync(Sorted).ConfigureAwait(false);
                }
            }
            //余りをDBに入れるついでにここで制御を戻してしまう
            MultipleSortBlock.Complete();
            return MultipleSortBlock.Completion
                .ContinueWith((_) =>
                {
                    MultipleSortSW.Stop();
                    Console.WriteLine("{0}\tMerge+Comp\t{1}ms", Index, MultipleSortSW.ElapsedMilliseconds);
                });

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
                int maxbit = Math.Min(bitcount * (b + 1) / blockcount, bitcount);
                for (int i = bitcount * b / blockcount; i < maxbit; i++)
                {
                    ret |= 1L << i;
                }
            }
            return ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ///<summary>ハミング距離を計算する</summary>
        long HammingDistance(long a, long b)
        {
            //xorしてpopcnt
            if (Popcnt.IsSupported) { return Popcnt.PopCount((ulong)(a ^ b)); }
            else
            {
                ulong value = (ulong)(a ^ b);
                //http://stackoverflow.com/questions/6097635/checking-cpu-popcount-from-c-sharp
                ulong result = value - ((value >> 1) & 0x5555555555555555L);
                result = (result & 0x3333333333333333L) + ((result >> 2) & 0x3333333333333333L);
                return (long)unchecked(((result + (result >> 4)) & 0xF0F0F0F0F0F0F0FL) * 0x101010101010101L) >> 56;
            }
        }
    }
}
