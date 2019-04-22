using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace twihash
{
    /// <summary>
    /// ListだとCopyToとかめんどいのでオレオレオブジェクトを作る
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class AddOnlyList<T> where T : struct
    {
        public AddOnlyList(int InitialLength) { InnerArray = new T[InitialLength]; }
        public AddOnlyList() : this(1024) { }
        ///<summary>中の配列を直接覗く
        ///スレッドセーフでもないし全ては自己責任で</summary>
        public T[] InnerArray { get; private set; }
        public int Count { get; private set; }
        public void Add(T value)
        {
            if (Count >= InnerArray.Length)
            {
                var NextArray = new T[InnerArray.Length << 1];
                InnerArray.CopyTo(NextArray, 0);
                InnerArray = NextArray;
            }
            InnerArray[Count] = value;
            Count++;
        }
        public void AddRange(Span<T> values)
        {
            values.CopyTo(InnerArray.AsSpan(Count, values.Length));
            Count += values.Length;
        }

        public void Clear() { Count = 0; }
        ///<summary>現時点のスナップショットとして使う</summary>
        public Span<T> Span { get { return InnerArray.AsSpan(0, Count); } }
        public IEnumerable<T> AsEnumerable() { return InnerArray.Take(Count); }
        public long[] ToArray()
        {
            var ret = new long[Count];
            Array.Copy(InnerArray, ret, Count);
            return ret;
        }
    }
}
