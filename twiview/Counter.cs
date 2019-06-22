using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace twiview
{
    public static class Counter
    {
        //パフォーマンスカウンター的な何か
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
        public static CounterValue TweetDeleted = new CounterValue();
        public static CounterValue TweetToDelete = new CounterValue();
        public static void PrintReset()
        {
            if (MediaTotal.Get() > 0) { Console.WriteLine("{0} / {1} Media Downloaded", MediaSuccess.GetReset(), MediaTotal.GetReset()); }
            if (TweetToDelete.Get() > 0) { Console.WriteLine("{0} / {1} Tweet Deleted", TweetDeleted.GetReset(), TweetToDelete.GetReset()); }
        }

        public static void AutoRefresh()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(600000).ConfigureAwait(false);
                    PrintReset();
                }
            });
        }
    }
}
