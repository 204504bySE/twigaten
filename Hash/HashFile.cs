using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IniParser;
using IniParser.Model;
using Twigaten.Lib;

namespace Twigaten.Hash
{
    class HashFile
    {
        static readonly Config config = Config.Instance;
        const string AllHashFileName = "allhash";
        const string NewerHashPrefix = "newhash";
        internal const string FileExtension = ".zst";

        ///<summary>DBから全ハッシュを読み込んだファイルの命名規則(フルパス)</summary>
        public static string AllHashFilePathBase(string UnixTime) => HashFilePathBase(AllHashFileName + UnixTime);
        ///<summary>DBから新しいハッシュを読み込んだファイルの命名規則(フルパス)</summary>
        public static string NewerHashFilePathBase(string UnixTime) => HashFilePathBase(NewerHashPrefix + UnixTime);
        static string HashFilePathBase(string HashFileName) => Path.Combine(config.hash.TempDir, HashFileName) + FileExtension;
        /// <summary>全ハッシュのファイルのパス なければnull</summary>
        public static string AllHashFilePath => Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(AllHashFilePathBase("*"))).FirstOrDefault();
        ///<summary>書き込み途中専用のパス(書き込みが終わったら本来の名前にリネームする)</summary>
        public static string TempFilePath(string basePath) => basePath + ".tmp";
        /// <summary>AllHashの更新時刻 なければ0</summary>
        public static long AllHashUpdate => long.TryParse(Path.GetFileNameWithoutExtension(AllHashFilePath).Substring(AllHashFileName.Length), out long ret) ? ret : 0;
        /// <summary>NewerHashの更新時刻 なければ0</summary>
        public static long NewerHashUpdate => Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(NewerHashFilePathBase("*")))
            .Select((f) => long.TryParse(Path.GetFileNameWithoutExtension(f).Substring(NewerHashPrefix.Length), out long t) ? t : 0)
            .Max();

        readonly FileIniDataParser parser;
        readonly IniData ini;
        
        public long LastUpdate
        {
            get { return long.Parse(ini[nameof(HashFile)][nameof(LastUpdate)] ?? "0"); }
            set { ini[nameof(HashFile)][nameof(LastUpdate)] = value.ToString(); Save(); }
        }
        public long LastHashCount
        {
            get { return long.Parse(ini[nameof(HashFile)][nameof(LastHashCount)] ?? "0"); }
            set { ini[nameof(HashFile)][nameof(LastHashCount)] = value.ToString(); Save(); }
        }

        static string iniPath => Path.Combine(config.hash.TempDir, "hash.ini");
        public HashFile()
        {
            parser = new FileIniDataParser();
            ini = parser.ReadFile(iniPath);
        }
        void Save() =>  parser.WriteFile(iniPath, ini);
        


        /// <summary>NewerHashを消す</summary>
        /// <param name="TempOnly">書き込みが終わらなかったやつだけ消す</param>
        public void DeleteNewerHash(bool TempOnly = false)
        {
            foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(TempFilePath(NewerHashFilePathBase("*")))).ToArray())
            {
                File.Delete(filePath);
            }
            if (!TempOnly)
            {
                foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(NewerHashFilePathBase("*"))).ToArray())
                {
                    File.Delete(filePath);
                }
            }
        }

        /// <summary>AllHashを消す</summary>
        /// /// <param name="TempOnly">書き込みが終わらなかったやつだけ消す</param>
        public void DeleteAllHash(bool TempOnly = false)
        {
            foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(TempFilePath(AllHashFilePathBase("*")))).ToArray())
            {
                File.Delete(filePath);
            }
            if (!TempOnly)
            {
                foreach (var filePath in Directory.EnumerateFiles(config.hash.TempDir, Path.GetFileName(AllHashFilePathBase("*"))).ToArray())
                {
                    File.Delete(filePath);
                }
            }
        }
    }
}
