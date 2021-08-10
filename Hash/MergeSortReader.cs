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
using Twigaten.Lib;

namespace Twigaten.Hash
{
    ///<summary>分割してブロックソートされたファイルを必要な単位で読み出すやつ
    ///重複排除を行うが全て排除はされない(ブロックソート後に同じ値が連続するもののみ)
    ///Dispose()で読み込んだファイルを削除する</summary>
    class MergeSortReader : IDisposable
    {
        static readonly Config config = Config.Instance;

        readonly long SortMask;
        readonly MergedEnumerator Creator;
        readonly MergeSortBuffer Reader;

        public MergeSortReader(int SortIndex, int FileCount, long SortMask)
        {
            this.SortMask = SortMask;
            Creator = new MergedEnumerator(SortIndex, FileCount, SortMask);
            Reader = Creator.Enumerator;
            //最初はここで強制的に読ませる
            ActualRead();
            var Sorted = SortedMemory.Span;
            if (0 < Sorted.Length) { LastValue = ~Sorted[0]; }
        }

        readonly int BlockElementsMin = config.hash.MultipleSortBufferElements;
        readonly int ReadBlockListSize = (int)Math.Pow(2, Math.Ceiling(Math.Log(config.hash.MultipleSortBufferElements, 2)));

        ///<summary>これにソート後の結果を読み込む</summary>
        Memory<long> SortedMemory;
        ///<summary>ソートするやつからまとめて読み込む</summary>
        void ActualRead() { SortedMemory = Reader.Read(); }
        //ReadBlocks()を抜けたときのSortedMemoryの読み込み位置を覚えておく
        int LastIndex;
        //重複排除用 最初は絶対に一致させない
        long LastValue;

        ///<summary>ブロックソートで一致する範囲ごとに読み出す
        ///長さ2以上のやつは全部返す(NewHashが含まれてなくても返す)
        ///最後まで読み終わったらnull</summary>
        ///<returns>(要素数,実際の要素…)を繰り返し、要素数0で終わりを示すオレオレ配列</returns>
        public AddOnlyList<long> ReadBlocks()
        {
            var ReadBlockList = new AddOnlyList<long>(ReadBlockListSize);

            //ブロックソートで一致してる要素数
            int BlockCount = 0;
            //↑の値を入れるReadBlocks内の添字
            int BlockCountIndex = 0;
            //最初は絶対に一致させないように-1
            long MaskedKey = -1;
            //重複排除用
            //long PreviousValue = LastValue;

            int ValueIndex = LastIndex;
            while (0 < SortedMemory.Length)
            {
                //SortedMemoryはActualRead()毎に変わるのでここでSpanにする
                var SortedValues = SortedMemory.Span;

                for (; ValueIndex < SortedValues.Length; ValueIndex++)
                {
                    long Value = SortedValues[ValueIndex];

                    long MaskedValue = Value & SortMask;
                    if (MaskedKey == MaskedValue)
                    {
                        //Maskしたやつ同士が一致するなら普通に続きに加える
                        ReadBlockList.Add(Value);
                        BlockCount++;
                    }
                    else
                    {   //Maskしたやつが一致しない場合はオレオレ配列1サイクル分終了
                        MaskedKey = MaskedValue;
                        //2要素以上あれば確定して次のサイクルに進む
                        if (1 < BlockCount)
                        {
                            ReadBlockList.InnerArray[BlockCountIndex] = BlockCount;
                            BlockCountIndex = ReadBlockList.Count;
                            //とりあえず次のサイクルの要素数のダミーを入れる
                            ReadBlockList.Add(0);
                            //十分な要素数が入ってたらここで終了(↑のAdd(0)は終端を示す0になる)
                            if (BlockElementsMin < ReadBlockList.Count)
                            {
                                //SortedValues[ValueIndex]の値を使わなかったので次回に回す
                                LastIndex = ValueIndex;
                                return ReadBlockList;
                            }
                            ReadBlockList.Add(Value);
                            BlockCount = 1;
                        }
                        //1要素しかないものは無意味なので除外→次の要素で上書き
                        else if (BlockCount == 1)
                        {
                            ReadBlockList.ReplaceTail(Value);
                        }
                        //最初だけ必ずここに入る
                        else
                        {
                            //先頭の要素数のダミーを入れる
                            ReadBlockList.Add(0);
                            ReadBlockList.Add(Value);
                            BlockCount = 1;
                        }
                    }
                    //PreviousValue = Value;
                }
                ActualRead();
                ValueIndex = 0;
            }
            //ここに来るのはファイルの読み込みが終わったときだけ
            //読み込み終了後の呼び出しならnullを返す必要がある
            if (ReadBlockList.Count == 0)
            {
                ReadBlockList.Dispose();
                return null;
            }
            else
            {
                //終端に0をつけて完成
                ReadBlockList.Add(0);
                return ReadBlockList;
            }
        }

        //ReaderはCreator.Dispose()の中でDispose()されるので呼ぶ必要はない
        public void Dispose() { Creator.Dispose(); }
    }

    ///<summary>IEnumeratorをそのまま使うのやめた
    ///Interfaceで済むはずだけどこっちの方がちょっとだけ速いと聞いた #ウンコード</summary>
    abstract class MergeSorterBase : IDisposable
    {
        ///<summary>成功したら Value & SortMask
        ///失敗したらlong.MaxValue</summary>
        public abstract long Read(out long Value);
        public abstract void Dispose();
    }

    ///<summary>Masked順でソート済みのn個のIEnumeratorをマージするやつ</summary>
    class MergeSorter : MergeSorterBase
    {
        readonly MergeSorterBase[] Enumerators;
        readonly (long Masked, long Value)[] LastValues;
        public MergeSorter(IEnumerable<MergeSorterBase> Enumerators) : this(Enumerators.ToArray()) { }
        public MergeSorter(params MergeSorterBase[] Enumerators)
        {
            this.Enumerators = Enumerators;
            LastValues = new (long, long)[Enumerators.Length];
            LastValues = new (long, long)[Enumerators.Length];
            InitRead();
        }
        void InitRead()
        {
            for(int i = 0; i < LastValues.Length; i++)
            {
                ref var v = ref LastValues[i];
                v.Masked = Enumerators[i].Read(out v.Value);
            }
        }

        ///<summary>1要素進める 比較用の値が戻る valueには実際の値が入る 全部読んだらlong.MaxValue</summary>
        public override long Read(out long Value)
        {
            long MinMasked = long.MaxValue;
            int MinIndex = -1;

            for (int i = 0; i < LastValues.Length; i++)
            {
                var LastMasked_i = LastValues[i].Masked;
                if (LastMasked_i < MinMasked)
                {
                    MinMasked = LastMasked_i;
                    MinIndex = i;
                }
            }
            if (0 <= MinIndex)
            {
                ref var Min = ref LastValues[MinIndex];
                Value = Min.Value;
                Min.Masked = Enumerators[MinIndex].Read(out Min.Value);
                return MinMasked;
            }
            else 
            {
                Value = 0;
                return long.MaxValue; 
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
        
        public override long Read(out long Value)
        {
            if (Reader.MoveNext(out var next))
            {
                Value = next;
                return next & SortMask;
            }
            else 
            {
                Value = 0;
                return long.MaxValue; 
            }
        }
        bool Disposed;
        public override void Dispose()
        {
            if (!Disposed)
            {
                Disposed = true;
                Reader.Dispose();
                File.Delete(FilePath);
            }
        }
    }


    ///<summary>全ファイルをマージソートしながら読み込む</summary>
    class MergedEnumerator : IDisposable
    {
        static readonly int MergeSortCompareUnit = Config.Instance.hash.MergeSortCompareUnit;
        ///<summary>マージソートの最終結果を出すやつ</summary>
        public MergeSortBuffer Enumerator { get; }

        public MergedEnumerator(int SortIndex, int FileCount, long SortMask)
        {
            //マージソート階層を作る
            var SorterQueue = new Queue<MergeSorterBase>();
            for (int i = 0; i < FileCount; i++)
            {
                SorterQueue.Enqueue(new MergeSortFileReader(SplitQuickSort.SortingFilePath(SortIndex, i), SortMask));
            }
            var TempSorter = new List<MergeSorterBase>();
            var NextSorter = new List<MergeSorterBase>();

            //きれいなマージソート階層を作るようにした(一応)
            while (1 < SorterQueue.Count)
            {
                //1回でマージしきれないときはファイル数を均等に振って並列に処理したい
                int temp = (SorterQueue.Count + MergeSortCompareUnit - 1) / MergeSortCompareUnit;
                int CompareUnit = (FileCount + temp - 1) / temp;
                while (0 < SorterQueue.Count)
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
        }
    }

    ///<summary>バッファーを用意して別スレッドで読み込ませる
    ///マージソートの最後段で使う
    ///1要素ずつじゃなくてまとめて読み出すようにしたけど速くなったようには見えない</summary>
    class MergeSortBuffer : IDisposable
    {
        static readonly int BufSize = Config.Instance.hash.ZipBufferElements;

        long[] Buf = new long[BufSize];
        long[] NextBuf = new long[BufSize];
        readonly MergeSorterBase Source;
        Task<int> FillNextBufTask;

        public MergeSortBuffer(MergeSorterBase Source)
        {
            this.Source = Source;
            //最初はここで強制的に読ませる
            FillNextBuf();
        }

        void FillNextBuf()
        {
            //ここでSourceから読み込めばおｋ
            FillNextBufTask = Task.Run(() =>
            {
                var Source = this.Source;
                var NextBuf = this.NextBuf;
                int i;
                for(i = 0; i < NextBuf.Length; i++)
                {
                    if (Source.Read(out NextBuf[i]) == long.MaxValue) { break; }
                }
                return i;
            });
        }

        ///<summary>まとめて読み込む
        ///ファイル末尾まで読んだら長さ0のMemoryが返る</summary>
        public Memory<long> Read()
        {
            //読み込みが終わってなかったらここで待機される
            int ReadSize = FillNextBufTask.Result;
            long[] swap = Buf;
            Buf = NextBuf;
            NextBuf = swap;
            FillNextBuf();
            return Buf.AsMemory(0, ReadSize);
        }

        public void Dispose()
        {
            Source.Dispose();
            Buf = null;
            NextBuf = null;
        }
    }
}
