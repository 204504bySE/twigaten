using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using twitenlib;
using System.IO;
using System.Buffers;

namespace twihash
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //CheckOldProcess.CheckandExit();

            Config config = Config.Instance;
            AddOnlyList<long>.Pool = ArrayPool<long>.Create(
                Math.Max(DBHandler.TableListSize, config.hash.MultipleSortBufferElements),
                Environment.ProcessorCount << 4 + Environment.ProcessorCount);

            DBHandler db = new DBHandler();
            Stopwatch sw = new Stopwatch();
            
            Console.WriteLine("Loading hash");
            sw.Restart();

            //ベンチマーク用に古いAllHashを使う奴
            //long NewLastUpdate = config.hash.LastUpdate;
           // long Count = config.hash.LastHashCount;

            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 600;   //とりあえず10分前
            long Count = await db.AllMediaHash().ConfigureAwait(false);

            HashSet<long> NewHash = null;
            if (config.hash.LastUpdate > 0) //これが0なら全ハッシュを追加処理対象とする
            {
                NewHash = await db.NewerMediaHash().ConfigureAwait(false);
                if (NewHash == null) { Console.WriteLine("New hash load failed."); Environment.Exit(1); }
                Console.WriteLine("{0} New hash", NewHash.Count);
            }
            sw.Stop();
            if (Count < 0) { Console.WriteLine("Hash load failed."); Environment.Exit(1); }
            else
            {
                Console.WriteLine("{0} Hash loaded in {1} ms", Count, sw.ElapsedMilliseconds);
                config.hash.NewLastHashCount(Count);
            }
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(NewHash, db,
                config.hash.MaxHammingDistance,
                //MergeSorterBaseの仕様上SortMaskで最上位bitだけ0にされるとまずいので制限
                Math.Min(config.hash.ExtraBlocks, 32 - config.hash.MaxHammingDistance),
                Count);
            await media.Proceed().ConfigureAwait(false);
            sw.Stop();
            Console.WriteLine("Multiple Sort, Store: {0}ms", sw.ElapsedMilliseconds);

            File.Delete(SplitQuickSort.AllHashFilePath);
            config.hash.NewLastUpdate(NewLastUpdate);
        }
    }
}