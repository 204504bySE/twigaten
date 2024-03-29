﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Twigaten.Web
{

    /// <summary>
    /// パフォーマンスカウンター的な何か
    /// </summary>
    public static class Counter
    {
        public struct CounterValue
        {
            int Value;
            public void Increment() { Interlocked.Increment(ref Value); }
            public void Add(int v) { Interlocked.Add(ref Value, v); }
            public int Get() { return Value; }
            public int GetReset() { return Interlocked.Exchange(ref Value, 0); }
        }

        //structだからreadonlyにすると更新されなくなるよ
        public static CounterValue MediaSuccess = new CounterValue();
        public static CounterValue MediaTotal = new CounterValue();
        public static CounterValue MediaStored = new CounterValue();
        public static CounterValue MediaBlurhashed = new CounterValue();
        public static CounterValue TweetToCheckDelete = new CounterValue();
        public static CounterValue TweetToDelete = new CounterValue();
        public static CounterValue TweetDeleted = new CounterValue();
        public static void PrintReset()
        {
            if (MediaTotal.Get() > 0) { Console.WriteLine("{0} / {1} / {2} / {3} Media Downloaded", MediaBlurhashed.GetReset(), MediaStored.GetReset(), MediaSuccess.GetReset(), MediaTotal.GetReset()); }
            if (TweetToCheckDelete.Get() > 0) { Console.WriteLine("{0} / {1} / {2} Tweet Deleted", TweetDeleted.GetReset(), TweetToDelete.GetReset(), TweetToCheckDelete.GetReset()); }
        }
    }
}
