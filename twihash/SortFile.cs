using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.IO;
using twitenlib;
using System.Collections.Concurrent;

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
        ///<summary>全ハッシュをファイルに書き出しながらソートしていくやつ</summary>
        public static async ValueTask<string> MergeSortAll(long SortMask, long HashCount)
        {
            long FileCount = 0;
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
                int InitialSortUnit = (int)(HashCount / nearPow2(HashCount / (config.hash.InitialSortFileSize / sizeof(long))) + 1);
                //int InitialSortUnit = config.hash.InitialSortFileSize / sizeof(long);

                //これにソート用の配列を入れてメモリ割り当てを減らしてみる
                var LongPool = new ConcurrentQueue<long[]>();

                var SortComp = new BlockSortComparer(SortMask);
                var FirstSortBlock = new ActionBlock<(string FilePath, long[] ToSort, int Length)>(async (t) =>
                {
                    await QuickSortAll(SortMask, t.ToSort, t.Length, SortComp).ConfigureAwait(false);
                    //Array.Sort(t.ToSort, SortComp);
                    using (BufferedLongWriter w = new BufferedLongWriter(t.FilePath))
                    {
                        for(int i = 0; i < t.Length; i++) { w.Write(t.ToSort[i]); }
                    }
                    LongPool.Enqueue(t.ToSort);
                }, new ExecutionDataflowBlockOptions()
                {   //ここでメモリを節約する、かもしれない
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = 1,
                    BoundedCapacity = 1
                });

                while (reader.Readable)
                {
                    if (!LongPool.TryDequeue(out long[] ToSort)) { ToSort = new long[InitialSortUnit]; }
                    int i;
                    for (i = 0; i < ToSort.Length && reader.Readable; i++) { ToSort[i] = reader.Read(); }
                    await FirstSortBlock.SendAsync((SortingFilePath(0, FileCount), ToSort, i)).ConfigureAwait(false);
                    FileCount++;
                }
                FirstSortBlock.Complete(); await FirstSortBlock.Completion.ConfigureAwait(false);
            }
            GC.Collect();
            GC.WaitForPendingFinalizers();
            int step = 0;
            //ファイル単位でマージソートしていく
            for (; FileCount > 1; step++)
            {
                var MergeSortBlock = new ActionBlock<int>((i) =>
                {
                    MargeSortUnit(SortMask,
                        SortingFilePath(step, i << 1),
                        SortingFilePath(step, (i << 1) | 1),
                        SortingFilePath(step + 1, i));
                    //余りファイルは最初のファイルにくっつけてしまう(出ないようにしたけどね
                    if (i == 0 && (FileCount & 1) != 0)
                    {
                        string NextFirstPath = SortingFilePath(step + 1, 0);
                        File.Delete(NextFirstPath + "_");
                        File.Move(NextFirstPath, NextFirstPath + "_");
                        MargeSortUnit(SortMask, SortingFilePath(step, FileCount - 1),
                            NextFirstPath + "_", NextFirstPath);
                    }
                }, new ExecutionDataflowBlockOptions()
                {
                    MaxDegreeOfParallelism = config.hash.FileSortThreads
                });

                for(int i = 0; i < FileCount >> 1; i++) { MergeSortBlock.Post(i); }
                MergeSortBlock.Complete();
                await MergeSortBlock.Completion.ConfigureAwait(false);
                FileCount >>= 1;    //ソート後のファイル数はこうなる
            }
            return SortingFilePath(step, 0);
        }

        ///<summary>ファイル2個をマージするやつ</summary>
        static void MargeSortUnit(long SortMask, string InputPathA, string InputPathB, string OutputPath)
        {
            using (var writer = new BufferedLongWriter(OutputPath))
            using (var readerA = new BufferedLongReader(InputPathA))
            using (var readerB = new BufferedLongReader(InputPathB))
            {
                //2つのファイルを並行して読んでいく

                long lastA = 0, lastB = 0, maskedA = 0, maskedB = 0;
                bool hasA = true, hasB = true;  //最後まで読んでしまったフラグ
                void ReadA()
                {
                    if (readerA.Readable)
                    {
                        lastA = readerA.Read();
                        maskedA = lastA & SortMask;
                    }
                    else { hasA = false; }
                }
                void ReadB()
                {
                    if (readerB.Readable)
                    {
                        lastB = readerB.Read();
                        maskedB = lastB & SortMask;
                    }
                    else { hasB = false; }
                }

                ReadA(); ReadB();
                while (hasA || hasB)
                {
                    if (!hasA) { writer.Write(lastB); ReadB(); }
                    else if (!hasB || maskedA < maskedB) { writer.Write(lastA); ReadA(); }
                    else { writer.Write(lastB); ReadB(); }
                }
            }
            //終わったら古いやつは消す
            File.Delete(InputPathA);
            File.Delete(InputPathB);
        }

        static async Task QuickSortAll(long SortMask, long[] SortList, int SortListLength, IComparer<long> Comparer)
        {
            var QuickSortBlock = new TransformBlock<(int Begin, int End), (int Begin1, int End1, int Begin2, int End2)?>
                (((int Begin, int End) SortRange) => {
                    return QuickSortUnit(SortRange, SortMask, SortList, Comparer);
                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });

            QuickSortBlock.Post((0, SortListLength - 1));
            int ProcessingCount = 1;
            do
            {
                (int Begin1, int End1, int Begin2, int End2)? NextSortRange = await QuickSortBlock.ReceiveAsync().ConfigureAwait(false);
                if (NextSortRange != null)
                {
                    QuickSortBlock.Post((NextSortRange.Value.Begin1, NextSortRange.Value.End1));
                    QuickSortBlock.Post((NextSortRange.Value.Begin2, NextSortRange.Value.End2));
                    ProcessingCount++;  //↑で1個終わって2個始めたから1個増える
                }
                else { ProcessingCount--; }
            } while (ProcessingCount > 0);
        }

        static (int Begin1, int End1, int Begin2, int End2)? QuickSortUnit((int Begin, int End) SortRange, long SortMask, long[] SortList, IComparer<long> Comparer)
        {
            if (SortRange.Begin >= SortRange.End) { return null; }
            /*十分に並列化されるか要素数が少なくなったらLINQに投げる*/
            if (SortRange.End - SortRange.Begin <= Math.Max(1048576, SortList.Length / (Math.Max(1, Environment.ProcessorCount - 1) << 1)))
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


    ///<summary>ブロックソート済みのファイルを必要な単位で読み出すやつ</summary>
    class SortedFileReader : IDisposable
    {
        readonly BufferedLongReader reader;
        readonly long FullMask;

        public SortedFileReader(string FilePath, long FullMask)
        {
            reader = new BufferedLongReader(FilePath);
            this.FullMask = FullMask;
        }

        long ExtraReadHash;
        bool HasExtraHash;
        readonly List<long> TempList = new List<long>();

        ///<summary>ブロックソートで一致する範囲だけ読み出す
        ///長さ2以上になるやつだけ返す</summary>
        public long[] ReadBlock()
        {
            long TempHash;
            do
            {
                TempList.Clear();
                if (HasExtraHash) { TempHash = ExtraReadHash; HasExtraHash = false; }
                else if (reader.Readable) { TempHash = reader.Read(); }
                else { return null; }
                TempList.Add(TempHash);
                //MaskしたやつがMaskedKeyと一致しなかったら終了
                long MaskedKey = TempHash & FullMask;
                while (reader.Readable)
                {
                    TempHash = reader.Read();
                    if ((TempHash & FullMask) == MaskedKey) { TempList.Add(TempHash); }
                    //1個余計に読んだので記録しておく
                    else { ExtraReadHash = TempHash; HasExtraHash = true; break; }
                }
            } while (TempList.Count < 2);
            return TempList.ToArray();
        }

        public void Dispose()
        {
            reader.Dispose();
        }
    }
}
