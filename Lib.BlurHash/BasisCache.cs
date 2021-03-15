using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
using System.Text;
using Blurhash.Core;

namespace Twigaten.Lib.BlurHash
{
    /// <summary>
    /// blurhashの計算に使うcos(θ)の値を保存する
    /// どうせ150(x)+150(y)通りなのでLRUなどはしない
    /// </summary>
    public class BasisCache : IBasisProviderEncode
    {
        readonly BasisProviderEncode provider = new();
        readonly ConcurrentDictionary<int, Vector<float>[][]> BasisDicX = new();
        readonly ConcurrentDictionary<int, float[][]> BasisDicY = new();

        public Vector<float>[] BasisX(int width, int componentX)
        {
            if(!BasisDicX.TryGetValue(width, out var ret))
            {
                ret = new Vector<float>[9][];
                for(int i = 0; i < ret.Length; i++)
                {
                    ret[i] = provider.BasisX(width, componentX);
                }
                BasisDicX[width] = ret;
            }
            return ret[componentX];
        }

        public float[] BasisY(int height, int componentY)
        {
            if(!BasisDicY.TryGetValue(height, out var ret))
            {
                ret = new float[9][];
                for(int i = 0; i < ret.Length; i++)
                {
                    ret[i] = provider.BasisY(height, componentY);
                }
                BasisDicY[height] = ret;
            }
            return ret[componentY];
        }
    }
}
