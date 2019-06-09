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
using twitenlib;
using Zstandard.Net;

namespace twihash
{
    /// <summary>
    /// まとめて、速く読み込みたい時用
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

        Vector<long> LastRead; //差分取る用
        /// <summary>まとめて読む用</summary>
        /// <param name="Values">読み込み結果を格納する配列
        /// 要素数がVector(long).Countの倍数になってないとデータが壊れる</param>
        /// <returns>読み込んだ要素数</returns>
        public int Read(long[] Values)
        {
            int ValuesCursor = 0;
            var ValuesSpan = Values.AsSpan();
            var LastBefore = LastRead; //ローカル変数にコピーしてちょっと速くする
            while (ValuesCursor < Values.Length)
            {
                int ReadBytes = zip.Read(MemoryMarshal.Cast<long, byte>(ValuesSpan.Slice(ValuesCursor, Math.Min(BufSize, Values.Length - ValuesCursor))));
                if(ReadBytes < sizeof(long)) { Readable = false; break; }

                int ReadElements = ReadBytes / sizeof(long);
                //XORを取って圧縮しやすくしたのを元に戻す
                var TakeDiffSpan = MemoryMarshal.Cast<long, Vector<long>>(ValuesSpan.Slice(ValuesCursor, ReadElements));
                for(int i = 0; i < TakeDiffSpan.Length; i++)
                {
                    LastBefore = (TakeDiffSpan[i] ^= LastBefore);
                }
                ValuesCursor += ReadElements;
            }
            LastRead = LastBefore;
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
    /// まとめて、速く書き込みたい時用
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

        Vector<long> LastWritten; //差分取る用
        /// <summary>まとめて書き込む用 Valuesは圧縮用に改変される</summary>
        /// <param name="Values">書き込みたい要素を含む配列</param>
        /// <param name="Length">書き込みたい要素数([0]から[Length - 1]まで書き込まれる)
        /// 要素数がVector(long).Countの倍数になってないとデータが壊れる</param>
        public void WriteDestructive(long[] Values, int Length)
        {
            int ValuesCursor = 0;
            var ValuesSpan = Values.AsSpan(0, Length);
            var LastBefore = LastWritten; //ローカル変数にコピーしてちょっと速くする

            while (ValuesCursor < Length)
            {
                var CompressSpan = ValuesSpan.Slice(ValuesCursor, Math.Min(BufSize, Length - ValuesCursor));
                var TakeDiffSpan = MemoryMarshal.Cast<long, Vector<long>>(CompressSpan);
                //XORを取って圧縮しやすくする Vector<long>で余った分は圧縮されない
                for (int i = 0; i < TakeDiffSpan.Length; i++)
                {
                    var NextLastBefore = TakeDiffSpan[i];
                    TakeDiffSpan[i] ^= LastBefore;
                    LastBefore = NextLastBefore;
                }
                zip.Write(MemoryMarshal.Cast<long, byte>(CompressSpan));
                ValuesCursor += CompressSpan.Length;
            }
            LastWritten = LastBefore;
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

        Vector<long> LastRead; //差分取る用
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
                    var LastBefore = LastRead;
                    for (int i = 0; i < NextBufAsVector.Length; i++)
                    {
                        LastBefore = (NextBufAsVector[i] ^= LastBefore);
                    }
                    LastRead = LastBefore;
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
        
        Vector<long> LastWritten; //差分取る用
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
                var LastBefore = LastWritten;
                for (int i = 0; i < WriteBufAsVector.Length; i++)
                {
                    var NextLastBefore = WriteBufAsVector[i];
                    WriteBufAsVector[i] ^= LastBefore;
                    LastBefore = NextLastBefore;
                }
                LastWritten = LastBefore;
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
