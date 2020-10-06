using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.AspNetCore.StaticFiles;

namespace Twigaten.Web {
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
            Console.WriteLine("Precompressing static files...");

            var CompressBlock = new ActionBlock<FileInfo>(async (Info) =>
            {
                using var ReadFile = File.OpenRead(Info.FullName);
                using var WriteZip = File.Create(Info.FullName + ".gz");
                using var Zip = new GZipStream(WriteZip, CompressionLevel.Optimal);
                using var WriteBrotli = File.Create(Info.FullName + ".br");
                using var Brotli = new BrotliStream(WriteBrotli, CompressionLevel.Optimal);
                await ReadFile.CopyToAsync(Zip);
                ReadFile.Seek(0, SeekOrigin.Begin);
                await ReadFile.CopyToAsync(Brotli);
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });

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
                        //圧縮ファイルがなかったり古かったりしたら圧縮する
                        default:
                            var GzipInfo = new FileInfo(Info.FullName + ".gz");
                            var BrotliInfo = new FileInfo(Info.FullName + ".br");
                            if (!GzipInfo.Exists || GzipInfo.LastWriteTimeUtc < Info.LastWriteTimeUtc
                                || !BrotliInfo.Exists || BrotliInfo.LastWriteTimeUtc < Info.LastWriteTimeUtc) { CompressBlock.Post(Info); }
                            break;
                    }
                }
                foreach (var ChildDir in Dir.EnumerateDirectories())
                {
                    TraverseDirectory(ChildDir);
                }
            }

            TraverseDirectory(new DirectoryInfo(wwwroot));
            CompressBlock.Complete();
            return CompressBlock.Completion.ContinueWith((_) => { Console.WriteLine("Precompression completed."); });
        }
    }
}
