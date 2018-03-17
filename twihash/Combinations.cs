using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace twihash
{
    class Combinations : IEnumerable<int[]>
    {
        public int Count { get; }
        public int Select { get; }
        public int Length { get; }

        public Combinations(int _Count, int _Select)
        {
            if (_Count < 1 || _Select < 1) { throw new ArgumentOutOfRangeException(); }
            if (_Count < _Select) { throw new ArgumentException(); }
            Count = _Count;
            Select = _Select;

            Length = CombiCount(Count, Select);
        }

        //a個からb個選ぶ組合せの個数
        int CombiCount(int a, int b)
        {
            long lengthtmp = 1;
            int d = 1;
            for (int i = a; i > a - b; i--)
            {
                lengthtmp *= i;

                if (lengthtmp % (i - a + b) == 0)
                {
                    lengthtmp /= i - a + b;
                }
                else
                {
                    d *= i - a + b;
                }
                for (int j = 2; j < Math.Sqrt(d); j++)
                {
                    if (d % j == 0 && lengthtmp % j == 0)
                    {
                        lengthtmp /= j;
                        d /= j;
                    }
                }
            }
            lengthtmp /= d;
            return (int)lengthtmp;
        }

        //n個要素が入っていてn番目がaの組合せの要素数
        int RestCombiCount(int n, int a)
        {
            if (n >= Count) { return 0; }    //全部埋まってる場合は0
            return CombiCount(Count - a - 1, Select - n);
        }

        public int IndexOf(int[] Combi)
        {
            int ret = 0;
            for (int i = 0; i < Combi.Length - 1; i++)
            {
                for (int j = (i == 0 ? 0 : Combi[i - 1] + 1); j < Combi[i]; j++)
                {
                    ret += RestCombiCount(i + 1, j);
                }
            }
            ret += Combi.Last() - (Combi.Length <= 1 ? 0 : Combi[Combi.Length - 2] + 1);
            return ret;
        }

        public int[] this[int index]
        {
            get
            {
                if (index < 0 || Length <= index) { throw new IndexOutOfRangeException(); }
                int indextmp = 0;
                int[] ret = new int[Select];
                for (int i = 0; i < Select - 1; i++)
                {
                    int j;
                    for (j = (i == 0 ? 0 : ret[i - 1] + 1); j < Count - (Select - i); j++)
                    {
                        int x = RestCombiCount(i + 1, j);
                        if (indextmp + x > index) { break; }
                        else { indextmp += x; }
                    }
                    ret[i] = j;
                }
                ret[Select - 1] = (Select == 1 ? 0 : ret[Select - 2] + 1) + (index - indextmp);
                return ret;
            }
        }

        public string CombiString(int index)
        {
            StringBuilder builder = new StringBuilder();
            int[] combiarray = this[index];
            int i;
            for (i = 0; i < Select - 1; i++)
            {
                builder.Append(combiarray[i]);
                builder.Append(", ");
            }
            builder.Append(combiarray[i]);
            return builder.ToString();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<int[]> GetEnumerator()
        {
            for (int i = 0; i < Length; i++)
            {
                yield return this[i];
            }
        }
    }

}
