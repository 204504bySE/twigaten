using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace twihash
{
    /// <summary>
    /// ListだとCopyToとかめんどいのでオレオレオブジェクトを作る
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class AddOnlyList<T> : IDisposable where T : struct
    {
        ///<summary>1個でもインスタンスを作った後に変更すると死ぬ</summary>
        public static ArrayPool<T> Pool { get; set; } = ArrayPool<T>.Shared;
        public AddOnlyList(int InitialLength) { InnerArray = Pool.Rent(InitialLength); }
        ///<summary>中の配列を直接覗く
        ///スレッドセーフでもないし全ては自己責任で</summary>
        public T[] InnerArray { get; private set; }
        public int Count { get; private set; }

        /// <summary>
        /// InnerArrayをMinSize+1以上に拡大する
        /// Add()でCount+1を2回計算するのが嫌だったのでこの紛らわしい仕様に
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ExpendIfNeccesary(int MinSize)
        {
            if (InnerArray.Length <= MinSize)
            {
                var NextArray = Pool.Rent(Math.Max(MinSize, InnerArray.Length << 1));
                InnerArray.CopyTo(NextArray, 0);
                Pool.Return(InnerArray);
                InnerArray = NextArray;
            }
        }
        ///<summary>末尾に要素を1個追加</summary>
        public void Add(T value)
        {
            ExpendIfNeccesary(Count);
            InnerArray[Count] = value;
            Count++;
        }
        ///<summary>末尾に要素をまとめて追加</summary>
        public void AddRange(Span<T> values)
        {
            ExpendIfNeccesary(Count + values.Length);
            values.CopyTo(InnerArray.AsSpan(Count, values.Length));
            Count += values.Length;
        }
        ///<summary>末尾の要素を上書き</summary>
        public void ReplaceTail(T value) { InnerArray[Count - 1] = value; }
        ///<summary>末尾の要素を1個削除</summary>
        public void Remove() { Count--; }
        ///<summary>末尾の要素をcount個削除</summary>
        public void Remove(int count)
        {
            if(Count < count) { throw new ArgumentOutOfRangeException(nameof(count)); }
            Count -= count;
        }
        public void Clear() { Count = 0; }
        ///<summary>現時点のスナップショットとして使う</summary>
        public Span<T> AsSpan() { return InnerArray.AsSpan(0, Count); }
        public IEnumerable<T> AsEnumerable() { return InnerArray.Take(Count); }
        public long[] ToArray()
        {
            var ret = new long[Count];
            Array.Copy(InnerArray, ret, Count);
            return ret;
        }

        ///<summary>Dispose済みのこいつを操作してどうなっても知らないし
        ///これをDisposeし忘れてどうなっても知らない(ひどい)</summary>
        public void Dispose()
        {
            if (InnerArray != null)
            {
                Pool.Return(InnerArray);
                InnerArray = null;
            }
            Count = -1;
        }
    }
}
