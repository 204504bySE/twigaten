using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.IO;
using twitenlib;
using System.Runtime.InteropServices;
using System.IO.Compression;
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
        public static async ValueTask<string> MergeSortAll(long SortMask)
        {
            long FileCount = 0;
            //最初はAllHashから読み出しながら個別のファイルを作る
            using (var reader = new BufferedLongReader(AllHashFilePath))
            {
                int InitialSortUnit = config.hash.InitialSortFileSize / sizeof(long);
                //これにソート用の配列を入れてメモリ割り当てを減らしてみる
                var LongPool = new ConcurrentQueue<long[]>();
                var SortComp = new BlockSortComparer(SortMask);
                var FirstSortBlock = new ActionBlock<(string FilePath, long[] ToSort)>((t) =>
                {
                    Array.Sort(t.ToSort, SortComp);
                    using (BufferedLongWriter w = new BufferedLongWriter(t.FilePath))
                    {
                        foreach (long h in t.ToSort) { w.Write(h); }
                    }
                    if (t.ToSort.Length == InitialSortUnit) { LongPool.Enqueue(t.ToSort); }
                }, new ExecutionDataflowBlockOptions()
                {
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = config.hash.InitialFileSortThreads,
                    BoundedCapacity = config.hash.InitialFileSortThreads + 1
                });

                while (reader.Readable)
                {
                    if (!LongPool.TryDequeue(out long[] ToSort)) { ToSort = new long[InitialSortUnit]; }
                    int i = 0;
                    for (; i < InitialSortUnit && reader.Readable; i++)
                    {
                        ToSort[i] = reader.Read();
                    }
                    //最後は配列の長さが半端になるので必要な長さの配列を作る
                    if(i < InitialSortUnit)
                    {
                        long[] ToSortLast = new long[i];
                        Array.Copy(ToSort, ToSortLast, i);
                        await FirstSortBlock.SendAsync((SortingFilePath(0, FileCount), ToSortLast));
                    }
                    else { await FirstSortBlock.SendAsync((SortingFilePath(0, FileCount), ToSort)); }
                    FileCount++;
                }
                FirstSortBlock.Complete(); await FirstSortBlock.Completion.ConfigureAwait(false);
            }
            GC.Collect();
            int step = 0;
            //ファイル単位でマージソートしていく
            for (; FileCount > 1; step++)
            {
                ActionBlock<int> MergeSortBlock = new ActionBlock<int>((i) =>
                {
                    MargeSortUnit(SortMask,
                        SortingFilePath(step, i << 1),
                        SortingFilePath(step, (i << 1) | 1),
                        SortingFilePath(step + 1, i));
                    //余りファイルは最初のファイルにくっつけてしまう
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
                bool hasA = true, hasB = true;
                void ReadA()
                {
                    if (readerA.Readable)
                    {
                        lastA = readerA.Read();
                        maskedA = lastA & SortMask;
                    }
                    else { hasA = false; }    //もう読めない→値がない
                }
                void ReadB()
                {
                    if (readerB.Readable)
                    {
                        lastB = readerB.Read();
                        maskedB = lastB & SortMask;
                    }
                    else { hasB = false; }   //もう読めない→値がない
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
    }

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongReader : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferSize;
        readonly FileStream file;
        readonly BrotliStream zip;
        ///<summary>Read()したバイト数</summary>
        public long Position { get; private set; }

        byte[] Buf = new byte[BufSize];
        int ActualBufSize;
        int BufCursor;  //bufの読み込み位置
        byte[] NextBuf = new byte[BufSize];
        Task<int> FillNextBufTask;

        public BufferedLongReader(string FilePath)
        {
            file = File.OpenRead(FilePath);
            zip = new BrotliStream(file, CompressionMode.Decompress);
            //最初はここで強制的に読ませる
            FillNextBuf();
            ActualReadAuto();
        }

        void FillNextBuf()
        {
            FillNextBufTask = zip.ReadAsync(NextBuf, 0, NextBuf.Length);
        }

        void ActualReadAuto()
        {
            if (BufCursor >= ActualBufSize)
            {
                //読み込みが終わってなかったらここで待機される
                ActualBufSize = FillNextBufTask.Result;
                if(ActualBufSize == 0) { Readable = false; }
                byte[] swap = Buf;
                Buf = NextBuf;
                NextBuf = swap;
                BufCursor = 0;
                FillNextBuf();
            }
        }

        ///<summary>続きのデータがあるかどうか</summary>
        public bool Readable { get; private set; } = true;
        public long Read()
        {
            if(!Readable) { throw new InvalidOperationException("EOF"); }
            long ret = BitConverter.ToInt64(Buf, BufCursor);
            Position += sizeof(long);
            BufCursor += sizeof(long);
            ActualReadAuto();
            return ret;
        }

        public void Dispose()
        {
            zip.Dispose();
            file.Dispose();
            Position = long.MaxValue;
            BufCursor = int.MaxValue;
        }
    }

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongWriter : IDisposable
    {
        static readonly Config config = Config.Instance;
        static readonly int BufSize = Config.Instance.hash.ZipBufferSize;
        readonly FileStream file;
        readonly BrotliStream zip;
        byte[] Buf = new byte[BufSize];
        int BufCursor;
        byte[] WriteBuf = new byte[BufSize];
        Task ActualWriteTask;

        public BufferedLongWriter(string FilePath, CompressionLevel Level = CompressionLevel.Fastest)
        {
            file = File.OpenWrite(FilePath);
            //圧縮レベル2を指定する方法は存在しないorz
            zip = new BrotliStream(file, Level);
        }

        public void Write(long Value)
        {
            var Bytes = new LongToBytes() { Long = Value };
            Buf[BufCursor] = Bytes.Byte0;
            Buf[BufCursor + 1] = Bytes.Byte1;
            Buf[BufCursor + 2] = Bytes.Byte2;
            Buf[BufCursor + 3] = Bytes.Byte3;
            Buf[BufCursor + 4] = Bytes.Byte4;
            Buf[BufCursor + 5] = Bytes.Byte5;
            Buf[BufCursor + 6] = Bytes.Byte6;
            Buf[BufCursor + 7] = Bytes.Byte7;
            BufCursor += sizeof(long);
            if (BufCursor >= BufSize) { ActualWrite(); }
        }

        void ActualWrite()
        {
            ActualWriteTask?.Wait();
            byte[] swap = Buf;
            Buf = WriteBuf;
            WriteBuf = swap;
            ActualWriteTask = zip.WriteAsync(WriteBuf, 0, BufCursor);
            BufCursor = 0;
        }
       
        public void Dispose()
        {
            ActualWrite();
            ActualWriteTask.Wait();
            zip.Flush();
            zip.Dispose();
            file.Dispose();
        }

        //https://stackoverflow.com/questions/8827649/fastest-way-to-convert-int-to-4-bytes-in-c-sharp
        [StructLayout(LayoutKind.Explicit)]
        struct LongToBytes
        {
            [FieldOffset(0)]
            public byte Byte0;
            [FieldOffset(1)]
            public byte Byte1;
            [FieldOffset(2)]
            public byte Byte2;
            [FieldOffset(3)]
            public byte Byte3;
            [FieldOffset(4)]
            public byte Byte4;
            [FieldOffset(5)]
            public byte Byte5;
            [FieldOffset(6)]
            public byte Byte6;
            [FieldOffset(7)]
            public byte Byte7;

            [FieldOffset(0)]
            public long Long;
        }
    }

    ///<summary>DBから読んだ全Hashをファイルに書くやつ</summary>
    class AllHashFileWriter : IDisposable
    {
        readonly BufferedLongWriter writer;
        public AllHashFileWriter()
        {
            writer = new BufferedLongWriter(SortFile.AllHashFilePath);
        }

        public void Write(IEnumerable<long> Values)
        {
            foreach (long v in Values) { writer.Write(v); }
        }

        public void Dispose()
        {
            writer.Dispose();
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
