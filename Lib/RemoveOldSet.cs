using System;
using System.Collections.Generic;
using System.Text;

namespace Twigaten.Lib
{
    public class RemoveOldSet<T>
    {
        HashSet<T> OldSet;
        HashSet<T> NewSet;

        int MaxSize { get; }
        public RemoveOldSet(int MaxSize)
        {
            //各Setあたりのサイズに変換する
            this.MaxSize = Math.Max(MaxSize >> 1, 1);

            OldSet = new HashSet<T>();
            NewSet = new HashSet<T>();
        }

        public bool Add(T Value)
        {
            RemoveOld();
            return !OldSet.Contains(Value) && NewSet.Add(Value);
        }

        public bool Contains(T Value)
        {
            return OldSet.Contains(Value) || NewSet.Contains(Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void RemoveOld()
        {
            if (NewSet.Count >= MaxSize)
            {
                OldSet.Clear();
                var TempSet = OldSet;
                OldSet = NewSet;
                NewSet = TempSet;
            }
        }
    }
}
