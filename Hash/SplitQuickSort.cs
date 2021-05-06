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
using System.Data;
using System.Runtime.CompilerServices;
using static Twigaten.Hash.HashFile;

namespace Twigaten.Hash
{
    static class SplitQuickSort
    {
        static readonly Config config = Config.Instance;

        internal const string SortFilePrefix = "sort";

        ///<summary>ソート過程のファイル名規則</summary>
        public static string SortingFilePath(int SortIndex, int FileIndex) => SortingFilePath(SortIndex.ToString() + "_" +  FileIndex.ToString());
        public static string SortingFilePath(string Name) => Path.Combine(config.hash.TempDir, SortFilePrefix + Name) + FileExtension;

        ///<summary>ブロックソートをLINQに投げる用</summary>
        class BlockSortComparer : IComparer<long>
        {
            readonly long SortMask;
            public BlockSortComparer(long SortMask) { this.SortMask = SortMask; }
            public int Compare(long x, long y) => Math.Sign((x & SortMask) - (y & SortMask));
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

        static readonly int InitialSortUnit = config.hash.InitialSortFileSize / sizeof(long) / Vector<long>.Count * Vector<long>.Count;

        ///<summary>全ハッシュを一定サイズに分割してソートする
        ///分割されたファイルをマージソートすると完全なソート済み列が得られる</summary>
        public static async Task<int> QuickSortAll(int Index, long SortMask)
        {
            int FileCount = 0;
            var SortComp = new BlockSortComparer(SortMask);

            //これにソート用の配列を入れてメモリ割り当てを減らしてみる
            var LongPool = new ConcurrentBag<long[]>();
            bool LongPoolReturn = true;
            //ソート用の配列の個数に制限を設ける
            var FirstSortSemaphore = new SemaphoreSlim(config.hash.InitialSortConcurrency);

            //ソートは並列 書き込みは並列させない
            var FirstSortBlock = new TransformBlock<FirstSort, FirstSort>(async (t) =>
            {
                await QuickSortParllel(SortMask, t.ToSort, t.Length, SortComp).ConfigureAwait(false);
                return t;
            }, new ExecutionDataflowBlockOptions()
            {   SingleProducerConstrained = true,
                MaxDegreeOfParallelism = config.hash.InitialSortConcurrency,
            });
            var WriterBlock = new ActionBlock<FirstSort>((t) =>
            {
                using (var writer = new UnbufferedLongWriter(t.WriteFilePath))
                {
                    writer.WriteDestructive(t.ToSort, t.Length);
                }
                //ここでLongPoolに配列を返却する
                if (LongPoolReturn) { LongPool.Add(t.ToSort); }
                FirstSortSemaphore.Release();
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = 1 });
            FirstSortBlock.LinkTo(WriterBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            //まずはAllHashを読む
            using (var reader = new UnbufferedLongReader(AllHashFilePath))
            {
                for(; reader.Readable; FileCount++)
                {
                    await FirstSortSemaphore.WaitAsync().ConfigureAwait(false);
                    if (!LongPool.TryTake(out var ToSort)) { ToSort = new long[InitialSortUnit]; }
                    int ToSortLength = reader.Read(ToSort);
                    FirstSortBlock.Post(new FirstSort(SortingFilePath(Index, FileCount), ToSort, ToSortLength));
                }
            }

            //NewerHashを読む
            int ToSortNewerCursor = 0;
            await FirstSortSemaphore.WaitAsync().ConfigureAwait(false);
            if (!LongPool.TryTake(out var ToSortNewer)) { ToSortNewer = new long[InitialSortUnit]; }
            foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(NewerHashFilePathBase("*"))))
            {
                using (var reader = new BufferedLongReader(filePath))
                {
                    while (reader.Readable)
                    {
                        for (; ToSortNewerCursor < ToSortNewer.Length; ToSortNewerCursor++)
                        {
                            if (!reader.MoveNext(out var next)) { break; }
                            ToSortNewer[ToSortNewerCursor] = next;
                        }
                        if (InitialSortUnit <= ToSortNewerCursor)
                        {
                            FirstSortBlock.Post(new FirstSort(SortingFilePath(Index, FileCount), ToSortNewer, ToSortNewer.Length));
                            FileCount++;
                            ToSortNewerCursor = 0;
                            await FirstSortSemaphore.WaitAsync().ConfigureAwait(false);
                            if (!LongPool.TryTake(out ToSortNewer)) { ToSortNewer = new long[InitialSortUnit]; }
                        }
                    }
                }
            }
            //余った要素もソートさせる FirstSortingCountはもう使わないので放置
            if (0 < ToSortNewerCursor)
            {
                FirstSortBlock.Post(new FirstSort(SortingFilePath(Index, FileCount), ToSortNewer, ToSortNewerCursor));
                FileCount++;
            }
            FirstSortBlock.Complete();
            //ソート用配列は作り終わったので用が済んだ配列は解放させる
            LongPoolReturn = false;
            LongPool.Clear();

            await WriterBlock.Completion.ConfigureAwait(false);
            return FileCount;
        }

        static readonly int QuickSortAllConcurrency = Environment.ProcessorCount / config.hash.InitialSortConcurrency + 1;
        ///<summary>配列の0~SortListLengthまでを並列ソート</summary>
        static async Task QuickSortParllel(long SortMask, long[] SortList, int SortListLength, IComparer<long> Comparer)
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
