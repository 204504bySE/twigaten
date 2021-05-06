using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Mono.Unix.Native;
using Twigaten.Lib;
using ZstdNet;

namespace Twigaten.Hash
{ 
    /// <summary>
    /// zstdで圧縮されたlongの配列を読み込む
    /// ファイル全体を1発で速く読み込みたい時用
    /// </summary>
    class UnbufferedLongReader : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly DecompressionStream zip;

        public UnbufferedLongReader(string FilePath)
        {
            int ioBufSize = Math.Min(BufSize * sizeof(long), 131072);
            file = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, ioBufSize, FileOptions.SequentialScan);
            zip = new DecompressionStream(file, ioBufSize);
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
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) { Syscall.posix_fadvise((int)file.SafeFileHandle.DangerousGetHandle(), 0, 0, PosixFadviseAdvice.POSIX_FADV_DONTNEED); }
                zip.Dispose();
                file.Dispose();
            }
        }
    }

    /// <summary> 
    /// longの配列をzstdで圧縮して書き込む
    /// ファイル全体を一発で速く書き込みたい時用
    /// </summary>
    class UnbufferedLongWriter : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly CompressionStream zip;

        public UnbufferedLongWriter(string FilePath)
        {
            int ioBufSize = Math.Min(BufSize * sizeof(long), 131072);
            file = new FileStream(FilePath, FileMode.Create, FileAccess.Write, FileShare.None, ioBufSize);
            zip = new CompressionStream(file, new CompressionOptions(1), ioBufSize);
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

        bool Disposed = false;
        public void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;
                zip.Dispose();
                file.Flush(true);
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) { Syscall.posix_fadvise((int)file.SafeFileHandle.DangerousGetHandle(), 0, 0, PosixFadviseAdvice.POSIX_FADV_DONTNEED); }
                file.Dispose();
            }
        }
    }

    ///<summary>
    /// zstdで圧縮されたlongの配列を読み込む
    ///</summary>
    class BufferedLongReader : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly DecompressionStream zip;

        byte[] Buf = new byte[BufSize * sizeof(long)];
        long[] BufAsLong;   //自称Lengthがbyte[]と同じままなのでLengthでアクセスすると死ぬ
        int ActualBufElements;
        int BufCursor;  //BufAsLongの読み込み位置(long単位で動かす)
        byte[] NextBuf = new byte[BufSize * sizeof(long)];
        Task<int> FillNextBufTask;

        public BufferedLongReader(string FilePath)
        {
            int ioBufSize = Math.Min(BufSize * sizeof(long), 131072);
            file = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, ioBufSize, FileOptions.SequentialScan);
            zip = new DecompressionStream(file, ioBufSize);

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
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) { Syscall.posix_fadvise((int)file.SafeFileHandle.DangerousGetHandle(), 0, file.Position, PosixFadviseAdvice.POSIX_FADV_DONTNEED); }
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

        ///<summary>次のデータを読んでCurrentに入れる Readableが別にあるのキモいけどしょうがない</summary>
        public bool MoveNext(out long next)
        {
            if (!Readable) { next = 0; return false; }            
            next = BufAsLong[BufCursor];
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
    }

    ///<summary>
    ///longの配列をzstdで圧縮して書き込む
    ///</summary>
    class BufferedLongWriter : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Vector<long>.Count * Vector<long>.Count;
        readonly FileStream file;
        readonly CompressionStream zip;
        byte[] Buf = new byte[BufSize * sizeof(long)];
        int BufCursor;  //こいつはlong単位で動かすからな
        byte[] WriteBuf = new byte[BufSize * sizeof(long)];
        Task ActualWriteTask;

        public BufferedLongWriter(string FilePath)
        {
            int ioBufSize = Math.Min(BufSize * sizeof(long), 131072);
            file = new FileStream(FilePath, FileMode.Create, FileAccess.Write, FileShare.None, ioBufSize);
            zip = new CompressionStream(file, new CompressionOptions(1), ioBufSize);
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
            ActualWriteTask = zip.WriteAsync(WriteBuf, 0, BufCursor * sizeof(long)).ContinueWith((_) => {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) { Syscall.posix_fadvise((int)file.SafeFileHandle.DangerousGetHandle(), 0, file.Position, PosixFadviseAdvice.POSIX_FADV_DONTNEED); }
            });
            BufCursor = 0;
        }

        bool Disposed = false;
        public void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;
                GC.SuppressFinalize(this);
                if (BufCursor > 0) { ActualWrite().Wait(); }
                ActualWriteTask.Wait();
                zip.Flush();
                zip.Dispose();
                file.Flush(true);
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) { Syscall.posix_fadvise((int)file.SafeFileHandle.DangerousGetHandle(), 0, 0, PosixFadviseAdvice.POSIX_FADV_DONTNEED); }
                file.Dispose();
            }
        }
        ~BufferedLongWriter() { Dispose(); }
    }
}
