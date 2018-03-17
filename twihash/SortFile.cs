using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.IO;
using twitenlib;
using System.Runtime.InteropServices;

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
        public static string MergeSortAll(long SortMask)
        {
            long FileCount = 0;
            //最初はAllHashから読み出しながら個別のファイルを作る
            using (BufferedLongReader reader = new BufferedLongReader(AllHashFilePath))
            {
                BlockSortComparer SortComp = new BlockSortComparer(SortMask);
                var FirstSortBlock = new ActionBlock<(string FilePath, long[] ToSort)>((t) =>
                {
                    Array.Sort(t.ToSort, SortComp);
                    using (BufferedLongWriter w = new BufferedLongWriter(t.FilePath))
                    {
                        foreach (long h in t.ToSort) { w.Write(h); }
                    }
                }, new ExecutionDataflowBlockOptions()
                {
                    SingleProducerConstrained = true,
                    MaxDegreeOfParallelism = Environment.ProcessorCount,
                    BoundedCapacity = Environment.ProcessorCount << 1
                });

                int InitialSortUnit = config.hash.InitialSortFileSize / sizeof(long);
                for (; reader.Length - reader.Position >= config.hash.InitialSortFileSize; FileCount++)
                {
                    long[] ToSort = new long[InitialSortUnit];
                    for (int i = 0; i < InitialSortUnit; i++) { ToSort[i] = reader.Read(); }
                    FirstSortBlock.SendAsync((SortingFilePath(0, FileCount), ToSort)).Wait();
                }
                int SortLastCount = (int)(reader.Length - reader.Position) / sizeof(long);
                if (SortLastCount > 0)
                {
                    long[] ToSortLast = new long[SortLastCount];
                    for (int i = 0; i < SortLastCount; i++) { ToSortLast[i] = reader.Read(); }
                    FirstSortBlock.SendAsync((SortingFilePath(0, FileCount), ToSortLast)).Wait();
                    FileCount++;    //最後に作ったから足す
                }
                FirstSortBlock.Complete(); FirstSortBlock.Completion.Wait();
            }
            GC.Collect();
            Random rand = new Random();
            int step = 0;
            //ファイル単位でマージソートしていく
            for (; FileCount > 1; step++)
            {
                Parallel.For(0, FileCount >> 1,
                    new ParallelOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount },
                    (i) => {
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
                    });
                FileCount >>= 1;    //ソート後のファイル数はこうなる
            }
            return SortingFilePath(step, 0);
        }

        ///<summary>ファイル2個をマージするやつ</summary>
        static void MargeSortUnit(long SortMask, string InputPathA, string InputPathB, string OutputPath)
        {
            using (BufferedLongWriter writer = new BufferedLongWriter(OutputPath))
            using (BufferedLongReader readerA = new BufferedLongReader(InputPathA))
            using (BufferedLongReader readerB = new BufferedLongReader(InputPathB))
            {
                //2つのファイルを並行して読んでいく


                //残りの書込回数をこれで数える(境界条件対策
                long restA = readerA.Length / sizeof(long), restB = readerB.Length / sizeof(long);

                long lastA = 0, lastB = 0, maskedA, maskedB;
                void ReadA()
                {
                    if (readerA.Readable())
                    {
                        lastA = readerA.Read();
                        maskedA = lastA & SortMask;
                    }
                    else { maskedA = long.MaxValue; }   //maskされてないMax→もう読めない
                }
                void ReadB()
                {
                    if (readerB.Readable())
                    {
                        lastB = readerB.Read();
                        maskedB = lastB & SortMask;
                    }
                    else { maskedB = long.MaxValue; }   //maskされてないMax→もう読めない
                }

                ReadA(); ReadB();
                do
                {
                    if (maskedA < maskedB)
                    {
                        writer.Write(lastA);
                        ReadA();
                        restA--;
                    }
                    else
                    {
                        writer.Write(lastB);
                        ReadB();
                        restB--;
                    }
                } while (restA > 0 || restB > 0);
            }
            //終わったら古いやつは消す
            File.Delete(InputPathA);
            File.Delete(InputPathB);
        }
    }

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongReader : IDisposable
    {
        const int BufSize = 0x100000;

        FileStream file;
        public long Length { get; }
        ///<summary>Read()したバイト数</summary>
        public long Position { get; private set; }

        byte[] buf = new byte[BufSize];
        int bufActualSize;    //bufに実際に入っているバイト数
        int bufCursor;  //bufの読み込み位置

        public BufferedLongReader(string FilePath)
        {
            file = File.OpenRead(FilePath);
            Length = file.Length;
            FillBuffer();
        }

        ///<summary>必要ならbufを更新する</summary>
        void FillBuffer()
        {
            if (bufActualSize <= bufCursor)
            {
                bufActualSize = file.Read(buf, 0, BufSize);
                bufCursor = 0;
            }
        }

        public bool Readable() => Position < Length;
        public long Read()
        {
            if(!Readable()) { throw new InvalidOperationException("EOF"); }
            long ret = BitConverter.ToInt64(buf, bufCursor);
            Position += sizeof(long);
            bufCursor += sizeof(long);
            FillBuffer();
            return ret;
        }

        public void Dispose()
        {
            file.Dispose();
            Position = long.MaxValue;
            bufCursor = int.MaxValue;
        }
    }

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongWriter : IDisposable
    {
        const int BufSize = 0x100000;
        FileStream file;
        byte[] buf = new byte[BufSize];
        int Cursor;

        public BufferedLongWriter(string FilePath)
        {
            file = File.OpenWrite(FilePath);
        }

        public void Write(long Value)
        {
            LongToBytes Bytes = new LongToBytes() { Long = Value };
            buf[Cursor] = Bytes.Byte0;
            buf[Cursor + 1] = Bytes.Byte1;
            buf[Cursor + 2] = Bytes.Byte2;
            buf[Cursor + 3] = Bytes.Byte3;
            buf[Cursor + 4] = Bytes.Byte4;
            buf[Cursor + 5] = Bytes.Byte5;
            buf[Cursor + 6] = Bytes.Byte6;
            buf[Cursor + 7] = Bytes.Byte7;
            Cursor += sizeof(long);
            if (Cursor == BufSize) { ActualWrite(); }
        }

        void ActualWrite()
        {
            file.Write(buf, 0, Cursor);
            Cursor = 0;          
        }

        public void Dispose()
        {
            ActualWrite();
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
        BufferedLongWriter writer;

        public AllHashFileWriter()
        {
            writer = new BufferedLongWriter(SortFile.AllHashFilePath);
        }
        
        public void Write(IEnumerable<long> Values)
        {
            foreach(long v in Values) { writer.Write(v); }
        }

        public void Dispose()
        {
            writer.Dispose();
        }
    }

    ///<summary>ブロックソート済みのファイルを必要な単位で読み出すやつ</summary>
    class SortedFileReader : IDisposable
    {
        BufferedLongReader reader;
        long FullMask;

        public SortedFileReader(string FilePath, long FullMask)
        {
            reader = new BufferedLongReader(FilePath);
            this.FullMask = FullMask;
        }

        long ExtraReadHash;
        bool HasExtraHash;

        ///<summary>ブロックソートで一致する範囲だけ読み出す</summary>
        public long[] ReadBlock()
        {
            if(!reader.Readable()) { return null; }
            List<long> ret = new List<long>();
            long TempHash;
            if (HasExtraHash) { TempHash = ExtraReadHash; HasExtraHash = false; } else { TempHash = reader.Read(); }
            ret.Add(TempHash);
            //MaskしたやつがMaskedKeyと一致しなかったら終了
            long MaskedKey = TempHash & FullMask;
            
            while((TempHash & FullMask) == MaskedKey && reader.Readable())
            {
                TempHash = reader.Read();
                ret.Add(TempHash);
            }
            if((TempHash & FullMask) != MaskedKey)
            {
                ret.RemoveAt(ret.Count - 1);
                //1個余計に読んだので記録しておく
                ExtraReadHash = TempHash; HasExtraHash = true;
            }
            return ret.ToArray();
        }

        public void Dispose()
        {
            reader.Dispose();
        }
    }
}
