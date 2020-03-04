using System;
using System.Collections.Generic;
using System.Text;

namespace Twigaten.Lib
{
    ///<summary>Twittter Snowflakeを計算する</summary>
    public static class SnowFlake
    {
        public const long msinSnowFlake = 0x400000L;   //1msはこれだ
        const long TwEpoch = 1288834974657L;
        public static long SecondinSnowFlake(long TimeSeconds, bool Larger)
        {
            if (Larger) { return (TimeSeconds * 1000 + 999 - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (TimeSeconds * 1000 - TwEpoch) << 22; }
        }
        public static long SecondinSnowFlake(DateTimeOffset TimeSeconds, bool Larger)
        {
            return SecondinSnowFlake(TimeSeconds.ToUnixTimeSeconds(), Larger);
        }

        public static long Now(bool Larger)
        {
            if (Larger) { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22; }
        }
        public static DateTimeOffset DatefromSnowFlake(long SnowFlake)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((SnowFlake >> 22) + TwEpoch);
        }
    }
}
