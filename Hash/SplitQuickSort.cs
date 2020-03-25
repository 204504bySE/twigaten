using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.IO;
using Twigaten.Lib;
using System.Collections.Concurrent;
using System.Threading;
using System.Numerics;

namespace Twigaten.Hash
{
    static class SplitQuickSort
    {
        static readonly Config config = Config.Instance;

        public const string AllHashFileName = "allhash";
        public const string SortFilePrefix = "sort";
        public const string FileExtension = ".zst";
        ///<summary>DBから全ハッシュを読み込んだファイルの命名規則</summary>
        public static string AllHashFilePath { get { return HashFilePath(AllHashFileName); } }
        public static string HashFilePath(string HashFileName) => Path.Combine(config.hash.TempDir, HashFileName) + FileExtension;
        
        ///<summary>ソート過程のファイル名規則</summary>
        public static string SortingFilePath(int index) => Path.Combine(config.hash.TempDir, SortFilePrefix + index.ToString()) + FileExtension;

        ///<summary>ブロックソートをLINQに投げる用</summary>
        class BlockSortComparer : IComparer<long>
        {
            readonly long SortMask;
            public BlockSortComparer(long SortMask) { this.SortMask = SortMask; }
            public int Compare(long x, long y)
            {
                long result = (x & SortMask) - (y & SortMask);
                if(result > 0) { return 1; }
                else if(result < 0) { return -1; }
                else { return 0; }
            }
        }

        ///<summary>MergeSortAllの中でちょっと使うだけ</summary>
        readonly struct FirstSort
        {
            public readonly string WriteFilePath;
            public readonly long[] ToSort;
            public readonly int Length;
            public FirstSort(string WriteFilePath, long[] ToSort, int Length)
            {
                this.WriteFilePath = WriteFilePath;
                this.ToSort = ToSort;
                this.Length = Length;
            }
        }

        ///<summary>全ハッシュを一定サイズに分轄してソートする
        ///分轄されたファイルをマージソートすると完全なソート済み列が得られる</summary>
        public static async Task<int> QuickSortAll(long SortMask, long HashCount)
        {
            //↓ベンチマーク用に古いファイルを使う時用(数字はファイル数に合わせて変える)
            //return 57;
            int FileCount = 0;
            using (var reader = new UnbufferedLongReader(AllHashFilePath))
            {
                int InitialSortUnit = config.hash.InitialSortFileSize / sizeof(long) / Vector<long>.Count * Vector<long>.Count;

                var SortComp = new BlockSortComparer(SortMask);

                //これにソート用の配列を入れてメモリ割り当てを減らしてみる
                var LongPool = new ConcurrentQueue<long[]>();

                //メモリを食い潰さないようにペースを調整させるぞ
                var FirstSortBlock = new ActionBlock<FirstSort>(async (t) =>
                {
                    await QuickSortAll(SortMask, t.ToSort, t.Length, SortComp).ConfigureAwait(false);
                    //Array.Sort(t.ToSort, SortComp);

                    //書き込みは並列で行う
                    using (var writer = new UnbufferedLongWriter(t.WriteFilePath))
                    {
                        writer.WriteDestructive(t.ToSort, t.Length);
                    }
                    LongPool.Enqueue(t.ToSort);
                }, new ExecutionDataflowBlockOptions()
                {   //ここでメモリを節約する、かもしれない
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = config.hash.InitialSortConcurrency,
                    BoundedCapacity = config.hash.InitialSortConcurrency + 1
                });
                //読み込み過ぎてメモリを食い潰さないように進める
                for(; reader.Readable; FileCount++)
                {
                    if (!LongPool.TryDequeue(out long[] ToSort)) { ToSort = new long[InitialSortUnit]; }
                    int ToSortLength = reader.Read(ToSort);
                    await FirstSortBlock.SendAsync(new FirstSort(SortingFilePath(FileCount), ToSort, ToSortLength)).ConfigureAwait(false);
                }
                FirstSortBlock.Complete();
                await FirstSortBlock.Completion.ConfigureAwait(false);
            }
            return FileCount;
        }

        static readonly int QuickSortAllConcurrency = Environment.ProcessorCount / config.hash.InitialSortConcurrency + 1;
        ///<summary>配列の0~SortListLengthまでを並列ソート</summary>
        static async Task QuickSortAll(long SortMask, long[] SortList, int SortListLength, IComparer<long> Comparer)
        {
            //クイックソート1再帰分
            (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange)
            {
                if (SortRange.Begin >= SortRange.End) { return null; }
                //十分に並列化されるか要素数が少なくなったらLINQに投げる
                if (SortRange.End - SortRange.Begin < Math.Max(1048576, SortList.Length / QuickSortAllConcurrency))
                {
                    Array.Sort(SortList, SortRange.Begin, SortRange.End - SortRange.Begin + 1, Comparer);
                    return null;
                }

                var SortSpan = SortList.AsSpan(SortRange.Begin, SortRange.End - SortRange.Begin + 1);

                //ピボットを選ぶ 最初/中間/最後 の3要素の中央値を取る
                long PivotA = SortSpan[0] & SortMask;
                long PivotB = SortSpan[SortSpan.Length >> 1] & SortMask;
                long PivotC = SortSpan[SortSpan.Length - 1] & SortMask;
                long PivotMasked;
                if (PivotA <= PivotB && PivotB <= PivotC || PivotA >= PivotB && PivotB >= PivotC) { PivotMasked = PivotB; }
                else if (PivotA <= PivotC && PivotC <= PivotB || PivotA >= PivotC && PivotC >= PivotB) { PivotMasked = PivotC; }
                else { PivotMasked = PivotA; }

                int i = -1; int j = SortSpan.Length;
                while (true)
                {
                    //do { i++; } while ((SortList[i] & SortMask) < PivotMasked);
                    for (i++; i < SortSpan.Length; i++) { if ((SortSpan[i] & SortMask) >= PivotMasked) { break; } }
                    do { j--; } while ((SortSpan[j] & SortMask) > PivotMasked);
                    if (i >= j) { break; }
                    long SwapHash = SortSpan[i];
                    SortSpan[i] = SortSpan[j];
                    SortSpan[j] = SwapHash;
                }
                return (SortRange.Begin, SortRange.Begin + j, SortRange.Begin + j + 1, SortRange.End);
            }

            //処理中の要素があるかどうかをこれで数える
            int ProcessingCount = 1;
            //順不同で処理させるための小細工 自分自身にはPost()できないからね
            var BufBlock = new BufferBlock<(int Begin, int End)>();

            var QuickSortBlock = new TransformBlock<(int Begin, int End), int>
                (((int Begin, int End) SortRange) => {
                    var next = QuickSortUnit(SortRange);
                    if (next.HasValue)
                    {
                        BufBlock.Post((next.Value.Begin1, next.Value.End1));
                        BufBlock.Post((next.Value.Begin2, next.Value.End2));
                        //処理中の物は1個増える
                        return 1;
                    }
                    else
                    {
                        //処理中の物は1個減る
                        return -1;
                    }
                }, new ExecutionDataflowBlockOptions 
                {
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                    SingleProducerConstrained = true,
                    EnsureOrdered = false
                });

            BufBlock.LinkTo(QuickSortBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            
            QuickSortBlock.Post((0, SortListLength - 1));
            //処理が終わるまで待つだけ
            do { ProcessingCount += await QuickSortBlock.ReceiveAsync().ConfigureAwait(false); }
            while (ProcessingCount > 0);
            BufBlock.Complete();
            await QuickSortBlock.Completion.ConfigureAwait(false);
        }
    }
}
