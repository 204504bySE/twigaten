using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Twigaten.Web
{
    /// <summary>
    /// 起動時にwwwroot内のファイルを圧縮する
    /// </summary>
    static class PreCompress
    {
        /// <summary>
        /// 実際に圧縮するやつ
        /// * 圧縮ファイルより非圧縮ファイルの方が新しかったら作り直す
        /// * 非圧縮ファイルがなくなってたら圧縮ファイルも消す
        /// </summary>
        /// <param name="wwwroot">wwwrootの絶対パス</param>
        /// <returns></returns>
        public static Task Proceed(string wwwroot)
        {
            var GzipBlock = new ActionBlock<FileInfo>(async (Info) => 
            {
                using (var ReadFile = File.OpenRead(Info.FullName))
                using (var WriteFile = File.OpenWrite(Info.FullName + ".gz"))
                using (var Zip = new GZipStream(WriteFile, CompressionLevel.Optimal))
                {
                    await ReadFile.CopyToAsync(Zip);
                }
            }, new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true, MaxDegreeOfParallelism = Environment.ProcessorCount });
            var BrotliBlock = new ActionBlock<FileInfo>(async (Info) => 
            {
                using (var ReadFile = File.OpenRead(Info.FullName))
                using (var WriteFile = File.OpenWrite(Info.FullName + ".br"))
                using (var Zip = new BrotliStream(WriteFile, CompressionLevel.Optimal))
                {
                    await ReadFile.CopyToAsync(Zip);
                }
            }, new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true, MaxDegreeOfParallelism = Environment.ProcessorCount });

            //ディレクトリを再帰的に辿るやつ
            void TraverseDirectory(DirectoryInfo Dir)
            {                
                foreach (var Info in Dir.EnumerateFiles())
                {
                    switch (Info.Extension)
                    {
                        //非圧縮ファイルが消えてたら圧縮ファイルを消す
                        case ".br":
                        case ".gz":
                            if (!File.Exists(Path.Combine(Info.DirectoryName, Path.GetFileNameWithoutExtension(Info.Name))))
                            { Info.Delete(); }
                            break;
                        //非圧縮ファイルがなかったり古かったりしたら圧縮する
                        default:
                            var GzipInfo = new FileInfo(Info.FullName + ".gz");
                            if(!GzipInfo.Exists || GzipInfo.LastWriteTimeUtc < Info.LastWriteTimeUtc) { GzipBlock.Post(Info); }
                            var BrotliInfo = new FileInfo(Info.FullName + ".br");
                            if (!BrotliInfo.Exists || BrotliInfo.LastWriteTimeUtc < Info.LastWriteTimeUtc) { BrotliBlock.Post(Info); }
                            break;
                    }
                }
                foreach (var ChildDir in Dir.EnumerateDirectories())
                {
                    TraverseDirectory(ChildDir);
                }
            }

            TraverseDirectory(new DirectoryInfo(wwwroot));
            GzipBlock.Complete();
            BrotliBlock.Complete();
            return Task.WhenAll(GzipBlock.Completion, BrotliBlock.Completion)
                .ContinueWith((_) => { Console.WriteLine("Compression completed."); });
        }
    }
}

