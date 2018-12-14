using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.IO;
using twitenlib;
using System.Collections.Concurrent;
using System.Threading;

namespace twihash
{
    static class SortFile
    {
        static readonly Config config = Config.Instance;

        public const string AllHashFileName = "allhash";
        public const string SortFilePrefix = "sort";
        public static string AllHashFilePath { get { return HashFilePath(AllHashFileName); } }
        public static string HashFilePath(string HashFileName) => Path.Combine(config.hash.TempDir, HashFileName);
        
        ///<summary>ソート過程のファイル名規則</summary>
        public static string SortingFilePath(int index) => Path.Combine(config.hash.TempDir, SortFilePrefix + index.ToString());

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
            int FileCount = 0;
            using (var reader = new BufferedLongReader(AllHashFilePath))
            {
                int InitialSortUnit = (int)(config.hash.InitialSortFileSize / sizeof(long));

                var SortComp = new BlockSortComparer(SortMask);

                //これにソート用の配列を入れてメモリ割り当てを減らしてみる
                var LongPool = new ConcurrentQueue<long[]>();

                //メモリを食い潰さないようにペースを調整させるぞ
                var FirstSortBlock = new ActionBlock<FirstSort>(async (t) =>
                {
                    await QuickSortAll(SortMask, t.ToSort, t.Length, SortComp).ConfigureAwait(false);
                    //Array.Sort(t.ToSort, SortComp);

                    //書き込みは並列で行う
                    using (var writer = new BufferedLongWriter(t.WriteFilePath))
                    {
                        for (int j = 0; j < t.Length; j++) { writer.Write(t.ToSort[j]); }
                    }
                    LongPool.Enqueue(t.ToSort);
                }, new ExecutionDataflowBlockOptions()
                {   //ここでメモリを節約する、かもしれない
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = config.hash.InitialSortConcurrency,
                    BoundedCapacity = config.hash.InitialSortConcurrency
                });
                //読み込み過ぎてメモリを食い潰さないように進める
                while (reader.MoveNext())
                {
                    if (!LongPool.TryDequeue(out long[] ToSort)) { ToSort = new long[InitialSortUnit]; }

                    ToSort[0] = reader.Current;//While()で読んでしまったので保存
                    int i = 1;  //というわけで1から始める
                    for (; i < ToSort.Length && reader.MoveNext(); i++) { ToSort[i] = reader.Current; }

                    await FirstSortBlock.SendAsync(new FirstSort(SortingFilePath(FileCount), ToSort, i)).ConfigureAwait(false);
                    FileCount++;
                }
                FirstSortBlock.Complete();
                await FirstSortBlock.Completion.ConfigureAwait(false);
            }
            //なんとなくここで片付けてしまう
            GC.Collect();
            GC.WaitForPendingFinalizers();

            return FileCount;
        }

        ///<summary>配列の0~SortListLengthまでを並列ソート</summary>
        static async Task QuickSortAll(long SortMask, long[] SortList, int SortListLength, IComparer<long> Comparer)
        {
            //順不同で処理させるための小細工
            //自分自身にはPost()できないからね
            var BufBlock = new BufferBlock<(int Begin, int End)>();
            var QuickSortBlock = new TransformBlock<(int Begin, int End), int>
                (((int Begin, int End) SortRange) => {
                    var next = QuickSortUnit(SortRange, SortMask, SortList, Comparer);
                    if (next.HasValue)
                    {
                        BufBlock.Post((next.Value.Begin1, next.Value.End1));
                        BufBlock.Post((next.Value.Begin2, next.Value.End2));
                        return 1;   //処理中の物は1個増える
                    }
                    else { return -1; } //処理中の物は1個減る
                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount, SingleProducerConstrained = true });

            BufBlock.LinkTo(QuickSortBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            
            QuickSortBlock.Post((0, SortListLength - 1));
            int ProcessingCount = 1;
            //処理が終わるまで待つだけ
            do { ProcessingCount += await QuickSortBlock.ReceiveAsync().ConfigureAwait(false); }
            while (ProcessingCount > 0);
            BufBlock.Complete();
            await QuickSortBlock.Completion.ConfigureAwait(false);
        }

        static readonly int ConcurrencyLog = (int)Math.Ceiling(Math.Log(Environment.ProcessorCount, 2) + 1);
        static (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange, long SortMask, long[] SortList, IComparer<long> Comparer)
        {
            if (SortRange.Begin >= SortRange.End) { return null; }
            //十分に並列化されるか要素数が少なくなったらLINQに投げる
            if (SortRange.End - SortRange.Begin < Math.Max(1048576, SortList.Length >> ConcurrencyLog))
            {
                Array.Sort(SortList, SortRange.Begin, SortRange.End - SortRange.Begin + 1, Comparer);
                return null;
            }

            //ふつーにピボットを選ぶ
            long PivotA = SortList[SortRange.Begin] & SortMask;
            long PivotB = SortList[(SortRange.Begin >> 1) + (SortRange.End >> 1)] & SortMask;
            long PivotC = SortList[SortRange.End] & SortMask;
            long PivotMasked;
            if (PivotA <= PivotB && PivotB <= PivotC || PivotA >= PivotB && PivotB >= PivotC) { PivotMasked = PivotB; }
            else if (PivotA <= PivotC && PivotC <= PivotB || PivotA >= PivotC && PivotC >= PivotB) { PivotMasked = PivotC; }
            else { PivotMasked = PivotA; }

            int i = SortRange.Begin; int j = SortRange.End;
            while (true)    //i > jになったら内側のwhileがすぐ終了するのでここで確認する必要はない
            {
                while ((SortList[i] & SortMask) <= PivotMasked) { i++; }
                while ((SortList[j] & SortMask) > PivotMasked) { j--; }
                if (i > j) { break; }
                long SwapHash = SortList[i];
                SortList[i] = SortList[j];
                SortList[j] = SwapHash;
                i++; j--;
            }
            //↑で i > j になるまで進めているので範囲が被らないように戻す
            return (SortRange.Begin, i - 1, j + 1, SortRange.End);
        }
    }
}
