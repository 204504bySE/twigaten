using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using IniParser;
using IniParser.Model;
using MySql.Data.MySqlClient;

namespace Twigaten.Lib
{
    ///<summary>iniファイル読むやつ</summary>
    public class Config
    {
        private static readonly Config _config = new Config();
        private Config() { Reload(); }
        public void Reload()
        {
            try
            {
                string iniPath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location), "twiten.ini");
                var ini = new FileIniDataParser();
                var data = ini.ReadFile(iniPath);
                token = new _token(data);
                crawl = new _crawl(data);
                crawlparent = new _crawlparent(data);
                locker = new _locker(data);
                hash = new _hash(iniPath, ini, data);
                database = new _database(data);
            }
            catch { Console.WriteLine("FAILED TO LOAD twiten.ini"); }   //twiviewではこのconfigクラスは使用しない
        }

        //singletonはこれでインスタンスを取得して使う
        public static Config Instance
        {
            get { return _config; }
        }

        public class _token
        {
            public string ConsumerKey { get; }
            public string ConsumerSecret { get; }
            public _token(IniData data)
            {
                ConsumerKey = data["token"]["ConsumerKey"];
                ConsumerSecret = data["token"]["ConsumerSecret"];
            }
        }
        public _token token;

        public class _crawl
        {
            public string PictPathProfileImage { get; }
            public string PictPaththumb { get; }
            public int StreamSpeedSeconds { get; }
            public int StreamSpeedTweets { get; }
            public int StreamSpeedHysteresis { get; }
            public int MaxRestInterval { get; }
            public int DefaultConnectionThreads { get; }
            public int MaxDBConnections { get; }
            public int RestTweetThreads { get; }
            public int ReconnectThreads { get; }
            public int MediaDownloadThreads { get; }
            public int DeleteTweetBufferSize { get; }
            public int LockedTokenPostpone { get; }
            public int LockerUdpPort { get; }
            public int WatchDogPort { get; }
            public int TweetLockSize { get; }
            public int ConnectPostponeSize { get; }
            public string HashServerUrl { get; }
            public _crawl(IniData data)
            {
                PictPathProfileImage = data["crawl"][nameof(PictPathProfileImage)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\profile_image\");
                PictPaththumb = data["crawl"][nameof(PictPaththumb)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\thumb\");
                StreamSpeedSeconds = int.Parse(data["crawl"][nameof(StreamSpeedSeconds)] ?? "180");
                StreamSpeedTweets = int.Parse(data["crawl"][nameof(StreamSpeedTweets)] ?? "50");
                StreamSpeedHysteresis = int.Parse(data["crawl"][nameof(StreamSpeedHysteresis)] ?? "16");
                MaxRestInterval = int.Parse(data["crawl"][nameof(MaxRestInterval)] ?? "900");
                DefaultConnectionThreads = int.Parse(data["crawl"][nameof(DefaultConnectionThreads)] ?? "1000");
                MaxDBConnections = int.Parse(data["crawl"][nameof(MaxDBConnections)] ?? "10");
                RestTweetThreads = int.Parse(data["crawl"][nameof(RestTweetThreads)] ?? Environment.ProcessorCount.ToString());
                ReconnectThreads = int.Parse(data["crawl"][nameof(ReconnectThreads)] ?? "1");
                MediaDownloadThreads = int.Parse(data["crawl"][nameof(MediaDownloadThreads)] ?? Environment.ProcessorCount.ToString());
                DeleteTweetBufferSize = int.Parse(data["crawl"][nameof(DeleteTweetBufferSize)] ?? "1000");
                LockedTokenPostpone = int.Parse(data["crawl"][nameof(LockedTokenPostpone)] ?? "86400");
                LockerUdpPort = int.Parse(data["crawl"][nameof(LockerUdpPort)] ?? "48250");
                WatchDogPort = int.Parse(data["crawlparent"][nameof(WatchDogPort)] ?? "58250");
                TweetLockSize = int.Parse(data["crawl"][nameof(TweetLockSize)] ?? "10000");
                ConnectPostponeSize = int.Parse(data["crawl"][nameof(ConnectPostponeSize)] ?? "10000");
                HashServerUrl = data["crawl"][nameof(HashServerUrl)];
                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawl crawl;

        public class _crawlparent
        {
            public int AccountLimit { get; }
            public string ChildPath { get; }
            public string LockerPath { get; }
            public string DotNetChild { get; }
            public string DotNetLock { get; }
            public int WatchDogPort { get; }
            public int WatchDogTimeout { get; }

            public _crawlparent(IniData data)
            {
                AccountLimit = int.Parse(data["crawlparent"][nameof(AccountLimit)] ?? "250");
                ChildPath = data["crawlparent"][nameof(ChildPath)] ?? "";
                LockerPath = data["crawlparent"][nameof(LockerPath)] ?? "";
                DotNetChild = data["crawlparent"][nameof(DotNetChild)] ?? "";
                DotNetLock = data["crawlparent"][nameof(DotNetLock)] ?? "";
                WatchDogPort = int.Parse(data["crawlparent"][nameof(WatchDogPort)] ?? "58250");
                WatchDogTimeout = int.Parse(data["crawlparent"][nameof(WatchDogTimeout)] ?? "300");

                //http://absg.hatenablog.com/entry/2014/07/03/195043
                //フォロー6000程度でピークは60ツイート/分程度らしい
            }
        }
        public _crawlparent crawlparent;

        public class _locker
        {
            public int UdpPort { get; }
            public int TweetLockSize { get; }

            public _locker(IniData data)
            {
                UdpPort = int.Parse(data["locker"][nameof(UdpPort)] ?? "48250");
                TweetLockSize = int.Parse(data["locker"][nameof(TweetLockSize)] ?? "65536");
            }
        }
        public _locker locker;

        public class _hash
        {
            readonly string iniPath;
            readonly FileIniDataParser ini;
            readonly IniData data;
            public int MaxHammingDistance { get; }
            public int ExtraBlocks { get; }
            public long LastUpdate { get; }
            public long LastHashCount { get; }
            public string TempDir { get; }
            public int InitialSortFileSize { get; }
            public int InitialSortConcurrency { get; }
            public int MergeSortCompareUnit { get; }
            public int ZipBufferElements { get; }
            public int MultipleSortBufferElements { get; }
            public int MultipleSortBufferCount { get; }
            public _hash(string iniPath, FileIniDataParser ini, IniData data)
            {
                this.iniPath = iniPath; this.ini = ini; this.data = data;
                MaxHammingDistance = int.Parse(data["hash"][nameof(MaxHammingDistance)] ?? "3");
                ExtraBlocks = int.Parse(data["hash"][nameof(ExtraBlocks)] ?? "1");
                LastUpdate = long.Parse(data["hash"][nameof(LastUpdate)] ?? "0");
                LastHashCount = long.Parse(data["hash"][nameof(LastHashCount)] ?? "0");
                TempDir = data["hash"][nameof(TempDir)] ?? "";
                InitialSortFileSize = int.Parse(data["hash"][nameof(InitialSortFileSize)] ?? "1073741824");
                InitialSortConcurrency = int.Parse(data["hash"][nameof(InitialSortConcurrency)] ?? "1");
                MergeSortCompareUnit = int.Parse(data["hash"][nameof(MergeSortCompareUnit)] ?? "2");
                ZipBufferElements = int.Parse(data["hash"][nameof(ZipBufferElements)] ?? "32768");
                MultipleSortBufferElements = int.Parse(data["hash"][nameof(MultipleSortBufferElements)] ?? "25000");
                MultipleSortBufferCount = int.Parse(data["hash"][nameof(MultipleSortBufferCount)] ?? (Environment.ProcessorCount << 4).ToString());
            }
            public void NewLastUpdate(long time)
            {
                data["hash"][nameof(LastUpdate)] = time.ToString();
                ini.WriteFile(iniPath, data);
            }
            public void NewLastHashCount(long Count)
            {
                data["hash"][nameof(LastHashCount)] = Count.ToString();
                ini.WriteFile(iniPath, data);
            }
        }
        public _hash hash;

        public class _database
        {
            public string Address { get; }
            public MySqlConnectionProtocol Protocol { get; }
            public _database(IniData data)
            {
                Address = data["database"][nameof(Address)] ?? "localhost"; //::1だとNotSupportedExceptionになるのだｗ
                Protocol = (MySqlConnectionProtocol)Enum.Parse(typeof(MySqlConnectionProtocol), data["database"][nameof(Protocol)] ?? "Tcp");
            }
        }
        public _database database;
    }
}
