using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using Twigaten.Lib;
using System.IO;
using System.Buffers;

namespace Twigaten.Hash
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //CheckOldProcess.CheckandExit();

            Config config = Config.Instance;
            AddOnlyList<long>.Pool = ArrayPool<long>.Create(
                Math.Max(DBHandler.TableListSize, config.hash.MultipleSortBufferElements),
                config.hash.MultipleSortBufferCount + Environment.ProcessorCount);

            DBHandler db = new DBHandler();
            Stopwatch sw = new Stopwatch();
            
            sw.Restart();

            HashSet<long> NewHash = null;
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 60; //とりあえず1分前
            long MinDownloadedAt = await db.Min_downloaded_at().ConfigureAwait(false);

            if(MinDownloadedAt < config.hash.LastUpdate - 600)
            {
                //前回の更新以降のハッシュを読む(つもり)
                Console.WriteLine("Loading New hash.");
                NewHash = await db.NewerMediaHash().ConfigureAwait(false);
                if (NewHash == null) { Console.WriteLine("New hash load failed."); Environment.Exit(1); }
                Console.WriteLine("{0} New hash", NewHash.Count);
            }
            else
            {
                //前回のハッシュ追加から時間が経ち過ぎたりしたらハッシュ取得を全部やり直す
                File.Delete(SplitQuickSort.AllHashFilePath);
                foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(SplitQuickSort.NewerHashFilePath("*"))).ToArray())
                {
                    File.Delete(filePath);
                }
            }
            //全ハッシュの取得はファイルが残っていなければやる
            if(!File.Exists(SplitQuickSort.AllHashFilePath))
            {                
                Console.WriteLine("Loading All hash.");
                long Count = await db.AllMediaHash().ConfigureAwait(false);
                if (Count < 0) { Console.WriteLine("Hash load failed."); Environment.Exit(1); }
                Console.WriteLine("{0} Hash loaded in {1} ms", Count, sw.ElapsedMilliseconds);
                config.hash.NewLastHashCount(Count);
            }
            sw.Stop();
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(NewHash, db,
                config.hash.MaxHammingDistance,
                //MergeSorterBaseの仕様上SortMaskで最上位bitだけ0にされるとまずいので制限
                Math.Min(config.hash.ExtraBlocks, 32 - config.hash.MaxHammingDistance));
            await media.Proceed().ConfigureAwait(false);
            sw.Stop();
            Console.WriteLine("Multiple Sort, Store: {0}ms", sw.ElapsedMilliseconds);

            //マージソート用のファイルが残ってたらここで消す
            foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(SplitQuickSort.SortingFilePath("*"))).ToArray())
            {
                File.Delete(filePath);
            }
            config.hash.NewLastUpdate(NewLastUpdate);
        }
    }
}