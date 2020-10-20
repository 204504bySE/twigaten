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

            var config = Config.Instance;
            var db = new DBHandler();
            var sw = Stopwatch.StartNew();

            HashSet<long> NewHash = null;
            long NewLastUpdate = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            long MinDownloadedAt = await db.Min_downloaded_at().ConfigureAwait(false);
            
            Directory.CreateDirectory(config.hash.TempDir);
            //前回正常に終了せずマージソート用のファイルが残ってたらここで消す
            foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(SplitQuickSort.SortingFilePath("*"))).ToArray())
            {
                File.Delete(filePath);
            }

            if (MinDownloadedAt < config.hash.LastUpdate - 600)
            {
                //前回の更新以降のハッシュを読む(つもり)
                Console.WriteLine("Loading New hash.");

                //前回正常に終了せず残った半端なハッシュを消す
                foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(SplitQuickSort.NewerHashFilePath("*"))).ToArray())
                {
                    if (!long.TryParse(Path.GetFileNameWithoutExtension(filePath).Substring(SplitQuickSort.NewerHashPrefix.Length), out long time)
                        || config.hash.LastUpdate < time) 
                    { File.Delete(filePath); }
                }
                //とりあえず60秒前のハッシュから取得する
                NewHash = await db.NewerMediaHash(config.hash.LastUpdate - 60).ConfigureAwait(false);
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

                //NewHashの中身はAllHashにも含まれることになるので消してしまう
                foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(SplitQuickSort.NewerHashFilePath("*"))).ToArray())
                {
                    File.Delete(filePath);
                }

                long Count = await db.AllMediaHash().ConfigureAwait(false);
                if (Count < 0) { Console.WriteLine("Hash load failed."); Environment.Exit(1); }
                Console.WriteLine("{0} Hash loaded.", Count);
                config.hash.NewLastHashCount(Count);
            }
            
            sw.Stop();
            Console.WriteLine("Hash Load: {0}ms", sw.ElapsedMilliseconds);
            sw.Restart();
            MediaHashSorter media = new MediaHashSorter(NewHash, db,
                config.hash.MaxHammingDistance,
                //MergeSorterBaseの仕様上SortMaskで最上位bitだけ0にされるとまずいので制限
                Math.Min(config.hash.ExtraBlocks, 32 - config.hash.MaxHammingDistance));
            await media.Proceed().ConfigureAwait(false);
            sw.Stop();
            Console.WriteLine("Multiple Sort, Store: {0}ms", sw.ElapsedMilliseconds);

            config.hash.NewLastUpdate(NewLastUpdate);
        }
    }
}