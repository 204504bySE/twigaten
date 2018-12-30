using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using twitenlib;

namespace twihash
{
    ///<summary>分轄してブロックソートされたファイルを必要な単位で読み出すやつ
    ///Dispose()でファイルを削除する</summary>
    class SortedFileReader : IDisposable
    {
        static readonly Config config = Config.Instance;

        readonly long SortMask;
        readonly HashSet<long> NewHash;
        readonly MergedEnumerator Reader;
        readonly ReadBuffer<long> Buffer;

        public SortedFileReader(int FileCount, long SortMask, HashSet<long> NewHash)
        {
            this.SortMask = SortMask;
            this.NewHash = NewHash;
            Reader = new MergedEnumerator(FileCount, SortMask);
            Buffer = new ReadBuffer<long>(Reader);
        }

        long ExtraReadHash;
        bool HasExtraHash;
        readonly List<long> TempList = new List<long>();

        ///<summary>ブロックソートで一致する範囲ごとに読み出す
        ///長さ2以上でNewHashが含まれてるやつだけ返す
        ///最後まで読み終わったらnull</summary>
        public long[] ReadBlock()
        {
            long TempHash;
            do
            {
                TempList.Clear();
                if (HasExtraHash) { TempHash = ExtraReadHash; HasExtraHash = false; }
                else if (Buffer.MoveNext()) { TempHash = Buffer.Current; }
                else { return null; }
                TempList.Add(TempHash);
                //MaskしたやつがMaskedKeyと一致しなかったら終了
                long MaskedKey = TempHash & SortMask;
                while (Buffer.MoveNext())
                {
                    TempHash = Buffer.Current;
                    if ((TempHash & SortMask) == MaskedKey)
                    {
                        TempList.Add(TempHash);
                    }
                    //1個余計に読んだので記録しておく
                    else { ExtraReadHash = TempHash; HasExtraHash = true; break; }
                }
            } while (TempList.Count < 2 || (NewHash != null && !TempList.Any(h => NewHash.Contains(h))));
            return TempList.ToArray();
        }

        public void Dispose() { Reader.Dispose(); }
    }

    ///<summary>マージソート対象のファイルを読むクラス ファイル1個ごとに作る</summary>
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
        ///<summary>ファイルを読み終わったらfalseになる</summary>
        public bool ReadOK { get; private set; }
        ///<summary>最後に読み込んだ値(そのまんま)</summary>
        public long Last { get; private set; }
        ///<summary>Last & SortMask</summary>
        public long LastMasked { get; private set; }
        public void Read()
        {
            ReadOK = Reader.MoveNext();
            if (ReadOK)
            {
                Last = Reader.Current;
                LastMasked = Last & SortMask;
            }
        }
        public void Dispose()
        {
            Reader.Dispose();
            ReadOK = false;
        }
        ///<summary>ファイルを消すのはDispose()後 #ウンコード</summary>
        public void DeleteFile() { File.Delete(FilePath); }
    }

    ///<summary>全ファイルをマージソートしながら読み込む</summary>
    class MergedEnumerator : IEnumerator<long>, IEnumerable<long>, IDisposable
    {
        readonly long SortMask;
        readonly MergeSortReader[] Readers;

        public MergedEnumerator(int FileCount, long SortMask)
        {
            this.SortMask = SortMask;   //これをnew MergeSortReader()より先にやらないとﾁｰﾝ
            Readers = new MergeSortReader[FileCount];
            Reset();
        }

        ///<summary>初期化をここに入れておく</summary>
        public void Reset()
        {
            for (int i = 0; i < Readers.Length; i++)
            {
                Readers[i]?.Dispose();
                Readers[i] = new MergeSortReader(SortFile.SortingFilePath(i), SortMask);
                //最初に読み込ませておく必要がある #ウンコード
                Readers[i].Read();
            }
        }

        public long Current { get; private set; }
        object IEnumerator.Current => Current;

        ///<summary>ここでマージソートを進める</summary>
        public bool MoveNext()
        {
            long MinMasked = long.MaxValue;
            int MinIndex = -1;
            //今は何も考えずに全部一気にマージしてるけど
            //ファイルが増えてたら多段階でマージすることも考える必要がある
            for (int i = 0; i < Readers.Length; i++)
            {
                if (Readers[i].ReadOK && Readers[i].LastMasked <= MinMasked)
                {
                    MinMasked = Readers[i].LastMasked;
                    MinIndex = i;
                }
            }
            if (MinIndex == -1) { return false; }   //全Readerが読み込み終了
            else
            {
                Current = Readers[MinIndex].Last;
                Readers[MinIndex].Read();
                return true;
            }
        }

        public IEnumerator<long> GetEnumerator() { return this; }
        IEnumerator IEnumerable.GetEnumerator() { return this; }

        public void Dispose() { foreach (var r in Readers) { r.Dispose(); r.DeleteFile(); } }
    }

    ///<summary>バッファーを用意して別スレッドで読み込みたいだけ</summary>
    class ReadBuffer<T> : IEnumerable<T>, IEnumerator<T> where T: struct
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements / Marshal.SizeOf<T>();

        T[] Buf = new T[BufSize];
        int ActualBufSize;//Bufの有効なサイズ(要素単位であってバイト単位ではない)
        int BufCursor;  //bufの読み込み位置(要素単位であってバイト単位ではない)
        T[] NextBuf = new T[BufSize];
        IEnumerator<T> Source;
        Task<int> FillNextBufTask;

        public ReadBuffer(IEnumerable<T> Source)
        {
            this.Source = Source.GetEnumerator();
            //最初はここで強制的に読ませる
            FillNextBuf();
            ActualReadAuto();
        }

        void FillNextBuf()
        {
            //ここでSourceから読み込めばおｋ
            FillNextBufTask = Task.Run(() =>
            {
                int i = 0;
                for(; i < NextBuf.Length; i++)
                {
                    if (Source.MoveNext()) { NextBuf[i] = Source.Current; }
                    else { break; }
                }
                return i;
            });
        }

        void ActualReadAuto()
        {
            if (BufCursor >= ActualBufSize)
            {
                //読み込みが終わってなかったらここで待機される
                ActualBufSize = FillNextBufTask.Result;
                if (ActualBufSize == 0) { Readable = false; }
                T[] swap = Buf;
                Buf = NextBuf;
                NextBuf = swap;
                BufCursor = 0;
                FillNextBuf();
            }
        }

        ///<summary>続きのデータがあるかどうか</summary>
        bool Readable  = true;

        public T Current { get; private set; }
        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (!Readable) { return false; }
            Current = Buf[BufCursor];
            BufCursor++;
            ActualReadAuto();
            return true;
        }

        public void Dispose()
        {
            BufCursor = int.MaxValue;
            Readable = false;
        }

        public void Reset()
        {
            Source.Reset();
            BufCursor = 0;
            Readable = true;
            FillNextBuf();
            ActualReadAuto();
        }

        public IEnumerator<T> GetEnumerator() => this;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

}
