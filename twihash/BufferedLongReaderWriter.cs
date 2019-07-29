using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using twitenlib;
using Zstandard.Net;

namespace twihash
{
    /// <summary>
    /// ファイル全体を1発で速く読み込みたい時用
    /// </summary>
    class UnbufferedLongReader : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly ZstandardStream zip;

        public UnbufferedLongReader(string FilePath)
        {
            file = File.OpenRead(FilePath);
            zip = new ZstandardStream(file, CompressionMode.Decompress);
        }

        ///<summary>続きのデータがあるかどうか</summary>
        public bool Readable { get; private set; } = true;

        Vector<long> LastRead = Vector<long>.Zero;
        /// <summary>まとめて読む用</summary>
        /// <param name="Values">読み込み結果を格納する配列
        /// 要素数がVector(long).Countの倍数になってないとデータが壊れる</param>
        /// <returns>読み込んだ要素数</returns>
        public int Read(long[] Values)
        {
            if (!Readable) { return 0; }
            int ValuesCursor = 0;

            while (ValuesCursor < Values.Length)
            {
                int ReadBytes = zip.Read(MemoryMarshal.Cast<long, byte>(Values.AsSpan(ValuesCursor, Math.Min(BufSize, Values.Length - ValuesCursor))));
                if(ReadBytes < sizeof(long)) { Readable = false; break; }
                int ReadElements = ReadBytes / sizeof(long);
                //XORを取って圧縮しやすくしたのを元に戻す
                var TakeDiffSpan = MemoryMarshal.Cast<long, Vector<long>>(Values.AsSpan(ValuesCursor, ReadElements));
                var Before = LastRead;
                for (int i = 0; i < TakeDiffSpan.Length; i++)
                {
                    Before = (TakeDiffSpan[i] ^= Before);
                }
                LastRead = Before;
                ValuesCursor += ReadElements;
            }
            return ValuesCursor;
        }

        bool Disposed;
        public void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;
                zip.Dispose();
                file.Dispose();
            }
        }
    }

    /// <summary>
    /// ファイル全体を一発で速く書き込みたい時用
    /// </summary>
    class UnbufferedLongWriter : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly ZstandardStream zip;

        public UnbufferedLongWriter(string FilePath)
        {
            file = File.OpenWrite(FilePath);
            zip = new ZstandardStream(file, 1, true);
        }

        Vector<long> LastWritten = Vector<long>.Zero;
        /// <summary>ファイル全体をまとめて書き込む用 Valuesは圧縮用に改変される</summary>
        /// <param name="Values">書き込みたい要素を含む配列</param>
        /// <param name="Length">書き込みたい要素数([0]から[Length - 1]まで書き込まれる)</param>
        public void WriteDestructive(long[] Values, int Length)
        {
            int ValuesCursor = 0;
            while (ValuesCursor < Length)
            {
                var ValuesSpan = Values.AsSpan(ValuesCursor, Math.Min(BufSize, Length - ValuesCursor));
                //XORを取って圧縮しやすくする Vector<long>で余った分は圧縮されない
                var TakeDiffSpan = MemoryMarshal.Cast<long, Vector<long>>(ValuesSpan);
                var Before = LastWritten;
                for (int i = 0; i < TakeDiffSpan.Length; i++)
                {
                    var NextLastBefore = TakeDiffSpan[i];
                    TakeDiffSpan[i] ^= Before;
                    Before = NextLastBefore;
                }
                LastWritten = Before;

                zip.Write(MemoryMarshal.Cast<long, byte>(ValuesSpan));
                ValuesCursor += ValuesSpan.Length;
            }
        }

        public void Dispose()
        {
            //ZstdStreamは Flush()せずにDispose()すると読み込み時にUnknown Frame descriptorされる
            zip.Flush();
            zip.Dispose();
            //Flush()してもUnknown Frame descriptorされるときはされるのでおまじない #ウンコード
            file.Flush(true);
            file.Dispose();
        }
    }

    ///<summary>ReadInt64()を普通に呼ぶと遅いのでまとめて読む</summary>
    class BufferedLongReader : IEnumerable<long>, IEnumerator<long>, IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
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

        Vector<long> LastRead = Vector<long>.Zero;
        void FillNextBuf()
        {
            FillNextBufTask = Task.Run(async () =>
            {
                int ReadBytes = await zip.ReadAsync(NextBuf, 0, NextBuf.Length).ConfigureAwait(false);
                if (ReadBytes < sizeof(long)) { return 0; }

                //差分をとるようにしてちょっと圧縮しやすくしてみよう
                //Spanを使いたかったのでローカル関数に隔離
                void TakeDiff()
                {
                    var NextBufAsVector = MemoryMarshal.Cast<byte, Vector<long>>(NextBuf.AsSpan(0, ReadBytes));
                    var Before = LastRead;
                    for (int i = 0; i < NextBufAsVector.Length; i++)
                    {
                        Before = (NextBufAsVector[i] ^= Before);
                    }
                    LastRead = Before;
                }
                TakeDiff();
                return ReadBytes / sizeof(long);
            });
        }

        void ActualReadAuto()
        {
            if (BufCursor >= ActualBufElements)
            {
                //読み込みが終わってなかったらここで待機される
                ActualBufElements = FillNextBufTask.Result;

                //ファイルを最後まで読んだらファイルを閉じてしまう
                if (ActualBufElements == 0)
                {
                    Dispose();
                    Readable = false;
                }
                byte[] swap = Buf;
                Buf = NextBuf;
                NextBuf = swap;
                BufAsLong = Unsafe.As<byte[], long[]>(ref Buf);
                BufCursor = 0;
                FillNextBuf();
            }
        }

        ///<summary>続きのデータがあるかどうか</summary>
        public bool Readable { get; private set; } = true;
        public long Current { get; private set; }
        object IEnumerator.Current => Current;

        ///<summary>次のデータを読んでCurrentに入れる Readableが別にあるのキモいけどしょうがない</summary>
        public bool MoveNext()
        {
            if (!Readable) { return false; }
            
            Current = BufAsLong[BufCursor];
            BufCursor++;
            ActualReadAuto();
            return true;
        }

        bool Disposed;
        public void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;
                zip.Dispose();
                file.Dispose();
                BufCursor = -1;
            }
        }

        public IEnumerator<long> GetEnumerator() => this;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public void Reset() { throw new NotSupportedException(); }
    }

    ///<summary>WriteInt64()を普通に呼ぶと遅いのでまとめて書く</summary>
    class BufferedLongWriter : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly ZstandardStream zip;
        byte[] Buf = new byte[BufSize * sizeof(long)];
        int BufCursor;  //こいつはlong単位で動かすからな
        byte[] WriteBuf = new byte[BufSize * sizeof(long)];
        Task ActualWriteTask;

        public BufferedLongWriter(string FilePath)
        {
            file = File.OpenWrite(FilePath);
            zip = new ZstandardStream(file, 1, true);
        }

        public async Task Write(long[] Values, int Length)
        {
            int ValuesCursor = 0;
            while (ValuesCursor < Length)
            {
                int CopySize = Math.Min(Length - ValuesCursor, BufSize - BufCursor);
                Values.AsSpan(ValuesCursor, CopySize).CopyTo(MemoryMarshal.Cast<byte, long>(Buf).Slice(BufCursor, CopySize));
                BufCursor += CopySize;

                if (BufCursor >= BufSize) { await ActualWrite().ConfigureAwait(false); }
                ValuesCursor += CopySize;
            }
        }

        Vector<long> LastWritten = Vector<long>.Zero;
        async Task ActualWrite()
        {
            if(ActualWriteTask != null) { await ActualWriteTask.ConfigureAwait(false); }
            byte[] swap = Buf;
            Buf = WriteBuf;
            WriteBuf = swap;

            //差分をとるようにしてちょっと圧縮しやすくしてみよう
            //Spanを使いたかったのでローカル関数に隔離
            void TakeDiff()
            {
                var WriteBufAsVector = MemoryMarshal.Cast<byte, Vector<long>>(WriteBuf.AsSpan(0, BufCursor * sizeof(long)));
                var Before = LastWritten;
                for (int i = 0; i < WriteBufAsVector.Length; i++)
                {
                    var NextLastBefore = WriteBufAsVector[i];
                    WriteBufAsVector[i] ^= Before;
                    Before = NextLastBefore;
                }
                LastWritten = Before;
            }
            TakeDiff();
            ActualWriteTask = zip.WriteAsync(WriteBuf, 0, BufCursor * sizeof(long));
            BufCursor = 0;
        }

        public void Dispose()
        {
            if (BufCursor > 0) { ActualWrite().Wait(); }
            ActualWriteTask.Wait();
            //ZstdStreamは Flush()せずにDispose()すると読み込み時にUnknown Frame descriptorされる
            zip.Flush();
            zip.Dispose();
            //Flush()してもUnknown Frame descriptorされるときはされるのでおまじない #ウンコード
            file.Flush(true);
            file.Dispose();
        }
    }
}
