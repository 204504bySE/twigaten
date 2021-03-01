using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Twigaten.Lib
{

    /// <summary>
    /// 画像サイズに応じて適切なBlurhash Encoderを返す
    /// どうせ150x150しか降ってこないはずだけどね
    /// そしてTEncoderにはBluehash Encoderしか入れないはず
    /// </summary>
    public class BlurhashPool<TEncoder> where TEncoder:class
    {
        public readonly struct Size : IEquatable<Size>
        {
            public int Width { get; }
            public int Height { get; }
            public Size(int width, int height)
            {
                Width = width;
                Height = height;
            }
            public override int GetHashCode() => Width.GetHashCode() ^ Height.GetHashCode();
            public override bool Equals(object obj)
            {
                if (!(obj is Size)) { return false; }
                var toCompare = (Size)obj;
                return toCompare.Width == Width && toCompare.Height == Height;
            }
            public bool Equals(Size other) => other.Width == Width && other.Height == Height;
            public static bool operator == (Size left, Size right) => left.Equals(right);
            public static bool operator !=(Size left, Size right) => !(left == right);
        }

        class PoolValue
        {
            public TEncoder Encoder { get; }
            public bool Used { get; set; }
            public PoolValue(TEncoder encoder)
            {
                Encoder = encoder;
                Used = false;
            }
        }

        public BlurhashPool(Func<Size, TEncoder> newEncoder)
        {
            NewEncoder = newEncoder;
        }

        readonly ConcurrentDictionary<Size, PoolValue> Pool = new ConcurrentDictionary<Size, PoolValue>();

        readonly Func<Size, TEncoder> NewEncoder;

        public TEncoder GetEncoder(int width, int height)
        {
            var size = new Size(width, height);
            if (!Pool.TryGetValue(size, out var value))
            {
                value = new PoolValue(NewEncoder(size));
                Pool[size] = value;
            }
            value.Used = true;
            return value.Encoder;
        }

        public int RemoveUnused()
        {
            int ret = 0;
            var snapshot = Pool.ToArray();
            foreach (var p in snapshot)
            {
                if (!p.Value.Used) { Pool.TryRemove(p.Key, out var _); ret++; }
                else { p.Value.Used = false; }
            }
            return ret;
        }
        public int Count => Pool.Count;
    }

}
