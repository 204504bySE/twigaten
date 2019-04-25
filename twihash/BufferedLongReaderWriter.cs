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

        long LastRead; //差分取る用
        void FillNextBuf()
        {
            FillNextBufTask = Task.Run(async () =>
            {
                int ReadBytes = await zip.ReadAsync(NextBuf, 0, NextBuf.Length).ConfigureAwait(false);
                if (ReadBytes < sizeof(long)) { return 0; }
                int ReadElements = ReadBytes / sizeof(long);

                //差分をとるようにしてちょっと圧縮しやすくしてみよう
                //Spanを使いたかったのでローカル関数に隔離
                void TakeDiff()
                {
                    var NextBufAsLong = Unsafe.As<byte[], long[]>(ref NextBuf).AsSpan(0, ReadElements);
                    long LastBefore = LastRead;
                    for (int i = 0; i < NextBufAsLong.Length; i++)
                    {
                        LastBefore = (NextBufAsLong[i] ^= LastBefore);
                    }
                    LastRead = LastBefore;
                }
                TakeDiff();
                return ReadElements;
            });
        }

        void ActualReadAuto()
        {
            if (BufCursor >= ActualBufElements)
            {
                //読み込みが終わってなかったらここで待機される
                ActualBufElements = FillNextBufTask.Result;
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

        /// <summary>
        /// まとめて読む用 こっちを使うとCurrentは更新されない
        /// </summary>
        /// <param name="Values">読み込み結果を格納する配列</param>
        /// <returns>読み込んだ要素数</returns>
        public async Task<int> Read(long[] Values)
        {
            int ValuesCursor = 0;
            while(Readable && ValuesCursor < Values.Length)
            {
                int CopySize = Math.Min(Values.Length - ValuesCursor, ActualBufElements - BufCursor);
                BufAsLong.AsSpan(BufCursor, CopySize).CopyTo(Values.AsSpan(ValuesCursor, CopySize));
                BufCursor += CopySize;
                ValuesCursor += CopySize;
                //ActualReadAuto内ではResultで待機しちゃうのでここでawait
                if (FillNextBufTask != null) { await FillNextBufTask.ConfigureAwait(false); }
                ActualReadAuto();
            }
            return ValuesCursor;
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
                Values.AsSpan(ValuesCursor, CopySize).CopyTo(Unsafe.As<byte[], long[]>(ref Buf).AsSpan(BufCursor, CopySize));
                BufCursor += CopySize;

                if (BufCursor >= BufSize) { await ActualWrite().ConfigureAwait(false); }
                ValuesCursor += CopySize;
            }
        }
        
        long LastWritten; //差分取る用
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
                var WriteBufAsLong = Unsafe.As<byte[], long[]>(ref WriteBuf).AsSpan(0, BufCursor);
                long LastBefore = LastWritten;
                for (int i = 0; i < WriteBufAsLong.Length; i++)
                {
                    long NextLastBefore = WriteBufAsLong[i];
                    WriteBufAsLong[i] ^= LastBefore;
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
