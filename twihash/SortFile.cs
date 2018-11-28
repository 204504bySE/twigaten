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
        static Config config = Config.Instance;

        const string AllHashFileName = "AllHash";
        public static string AllHashFilePath { get { return HashFilePath(AllHashFileName); } }
        public static string HashFilePath(string HashFileName) => Path.Combine(config.hash.TempDir, HashFileName);
        
        ///<summary>ソート過程のファイル名規則</summary>
        static string SortingFilePath(int step, long index) => Path.Combine(config.hash.TempDir, step.ToString() + "-" + index.ToString());

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

        ///<summary>全ハッシュをファイルに書き出しながらソートしていくやつ</summary>
        public static async ValueTask<string> MergeSortAll(long SortMask, long HashCount)
        {
            int FileCount = 0;
            //最初はAllHashから読み出しながら個別のファイルを作る
            using (var reader = new BufferedLongReader(AllHashFilePath))
            {
                //任意の数以上の最小の2のべき乗を求める
                //https://qiita.com/NotFounds/items/4648a3793d1b2a4f11d5
                long nearPow2(long n)
                {
                    // nが0以下の時は0とする。
                    if (n <= 0) return 0;

                    // (n & (n - 1)) == 0 の時は、nが2の冪乗であるため、そのままnを返す。
                    if ((n & (n - 1)) == 0) return n;

                    // bitシフトを用いて、2の冪乗を求める。
                    ulong ret = 1;
                    while (n > 0) { ret <<= 1; n >>= 1; }
                    return (long)ret;
                }

                //ファイル数が2^nになるように処理サイズを調整する
                //int InitialSortUnit = (int)(HashCount / nearPow2(HashCount / (config.hash.InitialSortFileSize / sizeof(long))) + 1);
                //処理サイズを調整しないやつ
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
                while (reader.Readable)
                {
                    if (!LongPool.TryDequeue(out long[] ToSort)) { ToSort = new long[InitialSortUnit]; }
                    int i;
                    for (i = 0; i < ToSort.Length && reader.Readable; i++) { ToSort[i] = reader.Read(); }

                    await FirstSortBlock.SendAsync(new FirstSort(SortingFilePath(0, FileCount), ToSort, i)).ConfigureAwait(false);
                    FileCount++;
                }
                FirstSortBlock.Complete();
                await FirstSortBlock.Completion.ConfigureAwait(false);
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();

            //マージソートするんだけど多数ファイルをまとめてやりたい
            int step = 0;
            int FilePerStep = config.hash.FileSortFilesPerStep;
            if (FilePerStep > FileCount)
            {
                //1回でマージしきれないときはファイル数を均等に振って並列に処理したい
                //(割られる数/+割る数-1)/割る数 → 切り上げの除算
                int temp = (FileCount + FilePerStep - 1) / FilePerStep;
                FilePerStep = (FileCount + temp - 1) / temp;
            }

            //ファイル単位でマージソートしていく
            for (; FileCount > 1; step++)
            {
                var MergeSortBlock = new ActionBlock<int>((begin) =>
                {
                    MergeSortManyUnit(SortMask, step, begin, (begin + FilePerStep > FileCount) ? FileCount % FilePerStep : FilePerStep);

                }, new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = config.hash.FileSortThreads,
                    SingleProducerConstrained = true
                });
                //ソート後のファイル数を計算しておく
                int NextFileCount = FileCount / FilePerStep + (FileCount % FilePerStep == 0 ? 0 : 1);

                for (int i = 0; i < NextFileCount; i++) { MergeSortBlock.Post(i * FilePerStep); }
                MergeSortBlock.Complete();
                await MergeSortBlock.Completion.ConfigureAwait(false);
                FileCount = NextFileCount;
            }
            return SortingFilePath(step, 0);
        }

        ///<summary>多数ファイルのマージソートをちょっと楽にしたい</summary>
        class MergeSortReader : IDisposable
        {
            readonly BufferedLongReader Reader;
            readonly long SortMask;
            readonly string FilePath;

            public MergeSortReader(string FilePath, long SortMask)
            {
                Reader = new BufferedLongReader(FilePath);
                this.SortMask = SortMask;
                this.FilePath = FilePath;
            }

            public bool ReadOK { get; private set; }
            public long Last { get; private set; }
            public long LastMasked { get; private set; }
            public void Read()
            {
                ReadOK = Reader.Readable;
                if (ReadOK)
                {
                    Last = Reader.Read();
                    LastMasked = Last & SortMask;
                }
            }
            public void Dispose()
            {
                Reader.Dispose();
                ReadOK = false;
                File.Delete(FilePath);
            }
        }
        ///<summary>多数ファイルを同時にマージソートする
        ///計算量はよく考えておｋ</summary>
        static void MergeSortManyUnit(long SortMask, int Step, int Begin, int Count)
        {
            string OutFilePath = HashFilePath((Step + 1).ToString() + "-" + ((Begin + Count) / Count - 1).ToString());
            //入力ファイルが1個だったら移動させて済ませる
            if (Count == 1)
            {
                File.Move(HashFilePath(Step.ToString() + "-" + Begin.ToString()), OutFilePath);
                return;
            }
            else if (Count < 1) { throw new ArgumentOutOfRangeException(); }

            var readers = new MergeSortReader[Count];
            for (int i = 0; i < Count; i++)
            {
                readers[i] = new MergeSortReader(HashFilePath(Step.ToString() + "-" + (i + Begin).ToString()), SortMask);
                //最初は全readerに読み込みをやらせる
                readers[i].Read();
            }
            using (var writer = new BufferedLongWriter(OutFilePath))
            {
                //あとは全readerが読めなくなるまで続ける
                while (true)
                {
                    long Min = long.MaxValue;
                    int MinIndex = -1;
                    for (int i = 0; i < Count; i++)
                    {
                        if (readers[i].ReadOK && readers[i].LastMasked <= Min)
                        {
                            Min = readers[i].LastMasked;
                            MinIndex = i;
                        }
                    }
                    if (MinIndex == -1) { break; }
                    writer.Write(readers[MinIndex].Last);
                    readers[MinIndex].Read();
                }
            }
            foreach (var r in readers) { r.Dispose(); }
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

        static (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange, long SortMask, long[] SortList, IComparer<long> Comparer)
        {
            if (SortRange.Begin >= SortRange.End) { return null; }
            /*十分に並列化されるか要素数が少なくなったらLINQに投げる*/
            if (SortRange.End - SortRange.Begin <= Math.Max(1048576, SortList.Length / ((Environment.ProcessorCount << 1) - 1)))
            {
                Array.Sort(SortList, SortRange.Begin, SortRange.End - SortRange.Begin + 1, Comparer);
                return null;
            }
            /*
            //要素数が少なかったら挿入ソートしたい
            if (SortRange.End - SortRange.Begin <= 16)
            {
                for (int k = SortRange.Begin + 1; k <= SortRange.End; k++)
                {
                    long TempHash = SortList[k];
                    long TempMasked = SortList[k] & SortMask;
                    if ((SortList[k - 1] & SortMask) > TempMasked)
                    {
                        int m = k;
                        do
                        {
                            SortList[m] = SortList[m - 1];
                            m--;
                        } while (m > SortRange.Begin
                        && (SortList[m - 1] & SortMask) > TempMasked);
                        SortList[m] = TempHash;
                    }
                }
                return null;
            }
            */
            //ふつーにピボットを選ぶ
            long PivotA = SortList[SortRange.Begin] & SortMask;
            long PivotB = SortList[(SortRange.Begin >> 1) + (SortRange.End >> 1)] & SortMask;
            long PivotC = SortList[SortRange.End] & SortMask;
            long PivotMasked;
            if (PivotA <= PivotB && PivotB <= PivotC || PivotA >= PivotB && PivotB >= PivotC) { PivotMasked = PivotB; }
            else if (PivotA <= PivotC && PivotC <= PivotB || PivotA >= PivotC && PivotC >= PivotB) { PivotMasked = PivotC; }
            else { PivotMasked = PivotA; }

            int i = SortRange.Begin; int j = SortRange.End;
            while (true)
            {
                while ((SortList[i] & SortMask) < PivotMasked) { i++; }
                while ((SortList[j] & SortMask) > PivotMasked) { j--; }
                if (i >= j) { break; }
                long SwapHash = SortList[i];
                SortList[i] = SortList[j];
                SortList[j] = SwapHash;
                i++; j--;
            }
            return (SortRange.Begin, i - 1, j + 1, SortRange.End);
        }
    }
}
