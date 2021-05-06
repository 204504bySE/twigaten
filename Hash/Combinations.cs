using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;

namespace Twigaten.Hash
{
    /// <summary>
    /// nCx(n個の要素からx個選ぶ組合せ)を扱う
    /// </summary>
    class Combinations : IEnumerable<int[]>
    {
        ///<summary>nCxのn</summary>
        public int Choice { get; }
        ///<summary>nCxのx</summary>
        public int Select { get; }
        ///<summary>組合せの個数</summary>
        public int Length { get; }

        public Combinations(int n, int x)
        {
            if (n < 1 || x < 1) { throw new ArgumentOutOfRangeException(); }
            if (n < x) { throw new ArgumentException(); }
            Choice = n;
            Select = x;

            Length = CombiCount(Choice, Select);
        }

        ///<summary>n個からx個選ぶ組合せの個数</summary>
        int CombiCount(int n, int x)
        {
            long lengthtmp = 1;
            int d = 1;
            for (int i = n; i > n - x; i--)
            {
                lengthtmp *= i;

                if (lengthtmp % (i - n + x) == 0)
                {
                    lengthtmp /= i - n + x;
                }
                else
                {
                    d *= i - n + x;
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

        ///<summary>n個要素が入っていてn番目がaである組合せの要素数</summary>
        int RestCombiCount(int n, int a)
        {
            if (n >= Choice) { return 0; }    //全部埋まってる場合は0
            return CombiCount(Choice - a - 1, Select - n);
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
                    for (j = (i == 0 ? 0 : ret[i - 1] + 1); j < Choice - (Select - i); j++)
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
            var builder = new StringBuilder();
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
