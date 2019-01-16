using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using twitenlib;
using Zstandard.Net;

namespace twihash
{
    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongReader : IEnumerable<long>, IEnumerator<long>
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements;
        readonly FileStream file;
        readonly ZstandardStream zip;

        byte[] Buf = new byte[BufSize * sizeof(long)];
        long[] BufAsLong;   //自称Lengthがbyte[]と同じままなのでLengthでアクセスすると死ぬ
        int ActualBufElements;
        int BufCursor;  //BufAsLongの読み込み位置(long単位で動かす)
        byte[] NextBuf = new byte[BufSize * sizeof(long)];
        Task<int> FillNextBufTask;

        public BufferedLongReader(string FilePath)
        {
            file = File.OpenRead(FilePath);
            zip = new ZstandardStream(file, CompressionMode.Decompress);

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
            if (BufCursor >= ActualBufElements)
            {
                //読み込みが終わってなかったらここで待機される
                ActualBufElements = FillNextBufTask.Result / sizeof(long);
                if (ActualBufElements == 0) { Readable = false; }
                byte[] swap = Buf;
                Buf = NextBuf;
                NextBuf = swap;
                BufAsLong = Unsafe.As<byte[], long[]>(ref Buf);
                BufCursor = 0;
                FillNextBuf();
            }
        }

        ///<summary>続きのデータがあるかどうか 内部処理用</summary>
        bool Readable = true;

        public long Current { get; private set; }
        object IEnumerator.Current => Current;

        ///<summary>次のデータを読んでCurrentに入れる Readableが別にあるのキモいけどしょうがない</summary>
        public bool MoveNext()
        {
            if (!Readable) { return false; }
            //差分をとるようにしてちょっと圧縮しやすくしてみよう
            Current += BufAsLong[BufCursor];
            BufCursor++;
            ActualReadAuto();
            return true;
        }

        public void Dispose()
        {
            zip.Dispose();
            file.Dispose();
            BufCursor = int.MaxValue;
        }

        public IEnumerator<long> GetEnumerator() => this;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Reset()
        {
            zip.Seek(0, SeekOrigin.Begin);
            BufCursor = 0;
            Readable = true;
            FillNextBuf();
            ActualReadAuto();            
        }
    }

    ///<summary>WriteInt64()を普通に呼ぶと遅いのでまとめて書く</summary>
    class BufferedLongWriter : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements;
        readonly FileStream file;
        readonly ZstandardStream zip;
        byte[] Buf = new byte[BufSize * sizeof(long)];
        long[] BufAsLong;        
        int BufCursor;  //こいつはlong単位で動かすからな
        byte[] WriteBuf = new byte[BufSize * sizeof(long)];
        Task ActualWriteTask;

        public BufferedLongWriter(string FilePath)
        {
            file = File.OpenWrite(FilePath);
            zip = new ZstandardStream(file, 1);
            //↓自称Lengthがbyte[]と同じままなのでLengthでアクセスすると死ぬ
            BufAsLong = Unsafe.As<byte[], long[]>(ref Buf);
        }

        long LastValue;
        public void Write(long Value)
        {
            //差分をとるようにしてちょっと圧縮しやすくしてみよう
            BufAsLong[BufCursor] = Value - LastValue;
            LastValue = Value;
            BufCursor++;
            if (BufCursor >= BufSize) { ActualWrite(); }
        }

        void ActualWrite()
        {
            ActualWriteTask?.Wait();
            byte[] swap = Buf;
            Buf = WriteBuf;
            WriteBuf = swap;
            BufAsLong = Unsafe.As<byte[], long[]>(ref Buf);
            ActualWriteTask = zip.WriteAsync(WriteBuf, 0, BufCursor * sizeof(long));
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
    }

    ///<summary>DBから読んだ全Hashをファイルに書くやつ</summary>
    class AllHashFileWriter : IDisposable
    {
        readonly BufferedLongWriter writer;
        public AllHashFileWriter()
        {
            writer = new BufferedLongWriter(SplitQuickSort.AllHashFilePath);
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

}
