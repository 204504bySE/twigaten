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
    class MergeSortReader : IDisposable
    {
        static readonly Config config = Config.Instance;

        readonly long SortMask;
        readonly HashSet<long> NewHash;
        readonly MergedEnumerator Reader;
        readonly ReadBuffer<long> Buffer;

        public MergeSortReader(int FileCount, long SortMask, HashSet<long> NewHash)
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

    ///<summary>IEnumeratorをそのまま使うのやめた
    ///Interfaceで済むはずだけどこっちの方がちょっとだけ速いと聞いた #ウンコード</summary>
    abstract class MergeSorterBase : IDisposable
    {
        ///<summary>Read()に失敗したら未定義(´・ω・`)</summary>
        public long Current { get; protected set; }
        ///<summary>Read()に失敗したらlong.MaxValueにする</summary>
        public long CurrentMasked { get; protected set; }
        public abstract void Read();
        public abstract void Dispose();
    }

    ///<summary>Masked順でソート済みのn個のIEnumeratorをマージするやつ</summary>
    class MergeSorter : MergeSorterBase
    {
        readonly MergeSorterBase[] Enumerators;

        public MergeSorter(IEnumerable<MergeSorterBase> Enumerators)
        {
            this.Enumerators = Enumerators.ToArray();
            InitRead();
        }
        public MergeSorter(params MergeSorterBase[] Enumerators)
        {
            this.Enumerators = Enumerators;
            InitRead();
        }
        void InitRead()
        {
            foreach(var e in Enumerators) { e.Read(); }
        }

        ///<summary>ここでマージソートする</summary>
        public override void Read()
        {
            var MinMasked = long.MaxValue;
            int MinIndex = -1;

            for (int i = 0; i < Enumerators.Length; i++)
            {
                if (Enumerators[i].CurrentMasked < MinMasked)
                {                    
                    MinMasked = Enumerators[i].CurrentMasked;
                    MinIndex = i;
                }
            }
            CurrentMasked = MinMasked;
            if (0 <= MinIndex)
            {
                Current = Enumerators[MinIndex].Current;
                Enumerators[MinIndex].Read();
            }
        }
        
        public override void Dispose()
        {
            foreach (var e in Enumerators) { e.Dispose(); }
        }
    }

    ///<summary>マージソート対象のファイルを読むクラス ファイル1個ごとに作る</summary>
    class MergeSortFileReader : MergeSorterBase
    {
        readonly BufferedLongReader Reader;
        readonly long SortMask;
        readonly string FilePath;

        public MergeSortFileReader(string FilePath, long SortMask)
        {
            Reader = new BufferedLongReader(FilePath);
            this.SortMask = SortMask;
            this.FilePath = FilePath;
        }
        
        ///<summary>ファイルを消すのはDispose()後 #ウンコード</summary>
        public void DeleteFile() { File.Delete(FilePath); }
        
        public override void Read()
        {
            if (Reader.MoveNext())
            {
                Current = Reader.Current;
                CurrentMasked = Reader.Current & SortMask;
            }
            else { CurrentMasked = long.MaxValue; }
        }
        public override void Dispose() => Reader.Dispose();
    }


    ///<summary>全ファイルをマージソートしながら読み込む</summary>
    class MergedEnumerator : IEnumerator<long>, IEnumerable<long>
    {
        static readonly int MergeSortCompareUnit = Config.Instance.hash.MergeSortCompareUnit;
        readonly long SortMask;
        ///<summary>Dispose()時にDeleteFile()呼びたいから保持してるだけ</summary>
        readonly MergeSortFileReader[] Readers;
        ///<summary>マージソートの最終結果を出すやつ</summary>
        readonly MergeSorterBase RootEnumerator;

        public MergedEnumerator(int FileCount, long SortMask)
        {
            this.SortMask = SortMask;   //これをnew MergeSortReader()より先にやらないとﾁｰﾝ
            Readers = new MergeSortFileReader[FileCount];
            for (int i = 0; i < Readers.Length; i++)
            {
                Readers[i] = new MergeSortFileReader(SplitQuickSort.SortingFilePath(i), SortMask);
            }

            //マージソート階層を作る
            var SorterQueue = new Queue<MergeSorterBase>(Readers);
            var SorterList = new List<MergeSorterBase>();

            while (1 < SorterQueue.Count)
            {
                for (int i = 0; 0 < SorterQueue.Count && i < MergeSortCompareUnit; i++)
                {
                    SorterList.Add(SorterQueue.Dequeue());
                }
                SorterQueue.Enqueue(new MergeSorter(SorterList));
                SorterList.Clear();
            }
            RootEnumerator = SorterQueue.Dequeue();
        }

        public long Current { get => RootEnumerator.Current; }
        object IEnumerator.Current => Current;
        public bool MoveNext() { RootEnumerator.Read(); return RootEnumerator.CurrentMasked != long.MaxValue; }
        public void Reset() => throw new NotSupportedException();

        public IEnumerator<long> GetEnumerator() => this;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        ///<summary>ここでファイルを削除させる</summary>
        public void Dispose()
        {
            //これでReadersまで連鎖的にDispose()される
            RootEnumerator.Dispose();
            //Dispose()後にファイル削除だけ別に行う #ウンコード
            foreach (var r in Readers) { r.DeleteFile(); }
        }
    }

    ///<summary>バッファーを用意して別スレッドで読み込みたいだけ</summary>
    class ReadBuffer<T> : IEnumerable<T>, IEnumerator<T> where T: struct
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements;

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
                int i;
                for(i = 0; i < NextBuf.Length; i++)
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
