using System;
using System.Buffers;
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
        readonly MergedEnumerator Creator;
        readonly MergeSortBuffer Reader;
        readonly ArrayPool<long> Pool = ArrayPool<long>.Create();

        public MergeSortReader(int FileCount, long SortMask, HashSet<long> NewHash)
        {
            this.SortMask = SortMask;
            this.NewHash = NewHash;
            Creator = new MergedEnumerator(FileCount, SortMask);
            Reader = Creator.Enumerator;
            OneBlockList = new AddOnlyList<long>(BlockElementsMin);
            //最初に1個読んでおく
            Reader.Read();
            LastHash = Reader.Current;
        }

        readonly int BlockElementsMin = config.hash.MultipleSortBufferElements;
        readonly int ReadBlockListSize = (int)Math.Pow(2, Math.Ceiling(Math.Log(config.hash.MultipleSortBufferElements, 2)));
        readonly AddOnlyList<long> OneBlockList;
        ///<summary>ReadBlock()をやり直すときにBuffer.Currentを読み直したくない</summary>
        long LastHash;

        ///<summary>ブロックソートで一致する範囲ごとに読み出す
        ///長さ2以上のやつは全部返す(NewHashが含まれてなくても返す)
        ///最後まで読み終わったらnull</summary>
        ///<returns>要素数,実際の要素,…,要素数,…,0 と繰り返すオレオレ配列</returns>
        public AddOnlyList<long> ReadBlocks()
        {
            //最後に読んだやつがCurrentに残ってるので読む
            long TempHash = LastHash;
            var ReadBlockList = new AddOnlyList<long>(ReadBlockListSize);
            do
            {
                //すでに全要素を読み込んだ後なら抜けてしまおう
                //長さ1なら返さなくていいのでここで問題ない
                if (!Reader.Read()) { break; }
                do
                {
                    //MoveNext()したけどTempHashはまだ変わってないのでここで保存
                    OneBlockList.Clear();
                    OneBlockList.Add(TempHash);
                    //MaskしたやつがMaskedKeyと一致しなかったら終了
                    long MaskedKey = TempHash & SortMask;
                    do
                    {   //↑でMoveNext()したのでここの先頭ではやらない
                        TempHash = Reader.Current;
                        if ((TempHash & SortMask) == MaskedKey) { OneBlockList.Add(TempHash); }
                        else { break; }
                    } while (Reader.Read());
                }
                while (OneBlockList.Count < 2);
                //オレオレ配列1サイクル分を作る
                ReadBlockList.Add(OneBlockList.Count);
                ReadBlockList.AddRange(OneBlockList.AsSpan());
            } while (ReadBlockList.Count < BlockElementsMin);

            //読み込み終了後の呼び出しならnullを返す必要がある
            if(ReadBlockList.Count == 0)
            {
                ReadBlockList.Dispose();
                return null;
            }
            //オレオレ配列を完成させて返す
            ReadBlockList.Add(0);

            //次回のためにTempHashを保存する
            LastHash = TempHash;
            return ReadBlockList; 
        }

        //ReaderはCreator.Dispose()の中でDispose()されるので呼ぶ必要はない
        public void Dispose() { Creator.Dispose(); }
    }

    ///<summary>IEnumeratorをそのまま使うのやめた
    ///Interfaceで済むはずだけどこっちの方がちょっとだけ速いと聞いた #ウンコード</summary>
    abstract class MergeSorterBase : IDisposable
    {
        ///<summary>Read()に失敗したら未定義(´・ω・`)</summary>
        public long Current { get; protected set; }
        ///<summary>成功したら Current & SortMask
        ///失敗したらlong.MaxValue</summary>
        public abstract long Read();
        public abstract void Dispose();
    }

    ///<summary>Masked順でソート済みのn個のIEnumeratorをマージするやつ</summary>
    class MergeSorter : MergeSorterBase
    {
        readonly MergeSorterBase[] Enumerators;
        readonly long[] LastMasked;
        public MergeSorter(IEnumerable<MergeSorterBase> Enumerators) : this(Enumerators.ToArray()) { }
        public MergeSorter(params MergeSorterBase[] Enumerators)
        {
            this.Enumerators = Enumerators;
            LastMasked = new long[Enumerators.Length];
            InitRead();
        }
        void InitRead()
        {
            for(int i = 0; i < LastMasked.Length; i++) { LastMasked[i] = Enumerators[i].Read(); }
        }

        ///<summary>1要素進める 比較用の値が戻る 全部読んだらlong.MaxValue</summary>
        public override long Read()
        {
            long MinMasked = long.MaxValue;
            int MinIndex = -1;

            for (int i = 0; i < LastMasked.Length; i++)
            {
                if (LastMasked[i] < MinMasked)
                {
                    MinMasked = LastMasked[i];
                    MinIndex = i;
                }
            }
            if (0 <= MinIndex)
            {
                var MinEnumerator = Enumerators[MinIndex];
                Current = MinEnumerator.Current;
                LastMasked[MinIndex] = MinEnumerator.Read();
                return MinMasked;
            }
            else { return long.MaxValue; }
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
        public void DeleteFile() { return; File.Delete(FilePath); }
        
        public override long Read()
        {
            if (Reader.MoveNext())
            {
                Current = Reader.Current;
                return Current & SortMask;
            }
            else { return long.MaxValue; }
        }
        public override void Dispose() => Reader.Dispose();
    }


    ///<summary>全ファイルをマージソートしながら読み込む</summary>
    class MergedEnumerator
    {
        static readonly int MergeSortCompareUnit = Config.Instance.hash.MergeSortCompareUnit;
        readonly long SortMask;
        ///<summary>Dispose()時にDeleteFile()呼びたいから保持してるだけ</summary>
        readonly MergeSortFileReader[] Readers;
        ///<summary>マージソートの最終結果を出すやつ</summary>
        public MergeSortBuffer Enumerator { get; }

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
            var TempSorter = new List<MergeSorterBase>();
            var NextSorter = new List<MergeSorterBase>();

            //きれいなマージソート階層を作るようにした(一応)
            while (1 < SorterQueue.Count)
            {
                //1回でマージしきれないときはファイル数を均等に振って並列に処理したい
                int temp = (SorterQueue.Count + MergeSortCompareUnit - 1) / MergeSortCompareUnit;
                int CompareUnit = (FileCount + temp - 1) / temp;
                while (1 < SorterQueue.Count)
                {
                    for (int i = 0; 0 < SorterQueue.Count && i < CompareUnit; i++)
                    {
                        TempSorter.Add(SorterQueue.Dequeue());
                    }
                    if (2 <= TempSorter.Count){ NextSorter.Add(new MergeSorter(TempSorter)); }
                    else { NextSorter.Add(TempSorter.First()); }
                    TempSorter.Clear();
                }
                SorterQueue = new Queue<MergeSorterBase>(NextSorter); 
                NextSorter.Clear();
            }
            //マージ階層の最後はバッファーを噛ませて並列で処理するつもり
            Enumerator = new MergeSortBuffer(SorterQueue.Dequeue());
        }

        ///<summary>ここでファイルを削除させる</summary>
        public void Dispose()
        {
            //これでReadersまで連鎖的にDispose()される
            Enumerator.Dispose();
            //Dispose()後にファイル削除だけ別に行う #ウンコード
            foreach (var r in Readers) { r.DeleteFile(); }
        }
    }

    ///<summary>バッファーを用意して別スレッドで読み込ませる</summary>
    class MergeSortBuffer : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements;

        long[] Buf = new long[BufSize];
        int ActualBufSize;//Bufの有効なサイズ(要素単位であってバイト単位ではない)
        int BufCursor;  //bufの読み込み位置(要素単位であってバイト単位ではない)
        long[] NextBuf = new long[BufSize];
        readonly MergeSorterBase Source;
        Task<int> FillNextBufTask;

        public MergeSortBuffer(MergeSorterBase Source)
        {
            this.Source = Source;
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
                    if (Source.Read() != long.MaxValue) { NextBuf[i] = Source.Current; }
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
                long[] swap = Buf;
                Buf = NextBuf;
                NextBuf = swap;
                BufCursor = 0;
                FillNextBuf();
            }
        }

        public long Current { get; private set; }

        ///続きのデータがあるかどうか
        public bool Readable { get; private set; } = true;

        public bool Read()
        {
            if (!Readable) { return false; }
            Current = Buf[BufCursor];
            BufCursor++;
            ActualReadAuto();
            return true;
        }  

        public void Dispose()
        {
            Source.Dispose();
            BufCursor = int.MaxValue;
            Readable = false;
            Buf = null;
            NextBuf = null;
        }
    }
}
