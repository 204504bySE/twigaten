using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using twitenlib;

namespace twihash
{
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

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongReader : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferSize;
        readonly FileStream file;
        readonly BrotliStream zip;

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
                if (ActualBufSize == 0) { Readable = false; }
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
            if (!Readable) { throw new InvalidOperationException("EOF"); }
            //こっちは素直にBitConverterを使った方が速い
            var ret = BitConverter.ToInt64(Buf, BufCursor);
            BufCursor += sizeof(long);
            ActualReadAuto();
            return ret;
        }

        public void Dispose()
        {
            zip.Dispose();
            file.Dispose();
            BufCursor = int.MaxValue;
        }
    }

    ///<summary>WriteInt64()を普通に呼ぶと遅いのでまとめて書く</summary>
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

}
