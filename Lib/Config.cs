using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using IniParser;
using IniParser.Model;
using MySqlConnector;

namespace Twigaten.Lib
{
    ///<summary>iniファイル読むやつ</summary>
    public class Config
    {
        private static readonly Config _config = new Config();
        private Config() { Reload(); }
        public void Reload()
        {
            IniData data;
            try
            {
                string iniPath = Environment.GetEnvironmentVariable("TWIGATEN_CONFIG_PATH");
                if (string.IsNullOrWhiteSpace(iniPath))
                {
                    iniPath = Path.Combine(Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location), "twigaten.ini");
                }
                var ini = new FileIniDataParser();
                data = ini.ReadFile(iniPath);
            }
            catch
            {
                data = new IniData();
                Console.WriteLine("FAILED TO LOAD config file.");
            }
            token = new _token(data);
            crawl = new _crawl(data);
            crawlparent = new _crawlparent(data);
            locker = new _locker(data);
            //hashはiniに書き込む必要がある
            hash = new _hash(data);
            dcthashserver = new _dcthashserver(data);
            web = new _web(data);
            database = new _database(data);

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
            public int MaxReconnectThreads { get; }
            public int TargetCrawlDuration { get; }
            public int MediaDownloadThreads { get; }
            public int DeleteTweetBufferSize { get; }
            public int LockedTokenPostpone { get; }
            public int LockerUdpPort { get; }
            public int WatchDogPort { get; }
            public int TweetLockSize { get; }
            public int ConnectPostponeSize { get; }
            public string HashServerHost { get; }
            public int HashServerPort { get; }
            public _crawl(IniData data)
            {
                PictPathProfileImage = data["crawl"][nameof(PictPathProfileImage)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\profile_image\");
                PictPaththumb = data["crawl"][nameof(PictPaththumb)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\thumb\");
                StreamSpeedSeconds = int.Parse(data["crawl"][nameof(StreamSpeedSeconds)] ?? "180");
                StreamSpeedTweets = int.Parse(data["crawl"][nameof(StreamSpeedTweets)] ?? "50");
                StreamSpeedHysteresis = int.Parse(data["crawl"][nameof(StreamSpeedHysteresis)] ?? "16");
                MaxRestInterval = int.Parse(data["crawl"][nameof(MaxRestInterval)] ?? "900");
                DefaultConnectionThreads = int.Parse(data["crawl"][nameof(DefaultConnectionThreads)] ?? "1000");
                MaxDBConnections = int.Parse(data["crawl"][nameof(MaxDBConnections)] ?? Environment.ProcessorCount.ToString());
                RestTweetThreads = int.Parse(data["crawl"][nameof(RestTweetThreads)] ?? Environment.ProcessorCount.ToString());
                MaxReconnectThreads = int.Parse(data["crawl"][nameof(MaxReconnectThreads)] ?? Environment.ProcessorCount.ToString());
                TargetCrawlDuration = int.Parse(data["crawl"][nameof(TargetCrawlDuration)] ?? "50000");
                MediaDownloadThreads = int.Parse(data["crawl"][nameof(MediaDownloadThreads)] ?? Environment.ProcessorCount.ToString());
                DeleteTweetBufferSize = int.Parse(data["crawl"][nameof(DeleteTweetBufferSize)] ?? "1000");
                LockedTokenPostpone = int.Parse(data["crawl"][nameof(LockedTokenPostpone)] ?? "86400");
                LockerUdpPort = int.Parse(data["crawl"][nameof(LockerUdpPort)] ?? "48250");
                WatchDogPort = int.Parse(data["crawlparent"][nameof(WatchDogPort)] ?? "58250");
                TweetLockSize = int.Parse(data["crawl"][nameof(TweetLockSize)] ?? "10000");
                ConnectPostponeSize = int.Parse(data["crawl"][nameof(ConnectPostponeSize)] ?? "10000");
                HashServerHost = data["crawl"][nameof(HashServerHost)] ?? "localhost";
                HashServerPort = int.Parse(data["crawl"][nameof(HashServerPort)] ?? "12306");
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
            public int MaxHammingDistance { get; }
            public int ExtraBlocks { get; }
            public string TempDir { get; }
            public int InitialSortFileSize { get; }
            public int InitialSortConcurrency { get; }
            public int MergeSortCompareUnit { get; }
            public int MergeSortPostponePairCount { get; }
            public int ZipBufferElements { get; }
            public int MultipleSortBufferElements { get; }

            public _hash(IniData data)
            {
                MaxHammingDistance = int.Parse(data["hash"][nameof(MaxHammingDistance)] ?? "3");
                ExtraBlocks = int.Parse(data["hash"][nameof(ExtraBlocks)] ?? "1");
                TempDir = data["hash"][nameof(TempDir)] ?? "";
                InitialSortFileSize = int.Parse(data["hash"][nameof(InitialSortFileSize)] ?? "1073741824");
                InitialSortConcurrency = int.Parse(data["hash"][nameof(InitialSortConcurrency)] ?? "1");
                MergeSortCompareUnit = int.Parse(data["hash"][nameof(MergeSortCompareUnit)] ?? "2");
                MergeSortPostponePairCount = int.Parse(data["hash"][nameof(MergeSortPostponePairCount)] ?? "10000000");
                ZipBufferElements = int.Parse(data["hash"][nameof(ZipBufferElements)] ?? "32768");
                MultipleSortBufferElements = int.Parse(data["hash"][nameof(MultipleSortBufferElements)] ?? "25000");
            }
        }
        public _hash hash;

        public class _dcthashserver
        {
            public bool ListenIPv6 { get; }
            public int ListenPort { get; }
            public _dcthashserver(IniData data)
            {
                ListenIPv6 = bool.Parse(data["dcthashserver"][nameof(ListenIPv6)] ?? "true");
                ListenPort = int.Parse(data["dcthashserver"][nameof(ListenPort)] ?? "12306");
            }
        }
        public _dcthashserver dcthashserver;

        public class _web
        {
            public string CallBackUrl { get; }
            public bool ListenIPv6 { get; }
            public bool ListenIPv4 { get; }
            public int ListenPort { get; }
            public string ListenUnixSocketPath { get; }
            public int MaxDBConnections { get; }
            public string HashServerCropHost { get; }
            public int HashServerCropPort { get; }
            public _web(IniData data)
            {
                CallBackUrl = data["web"][nameof(CallBackUrl)] ?? "";
                ListenIPv6 = bool.Parse(data["web"][nameof(ListenIPv6)] ?? "true");
                ListenIPv4 = bool.Parse(data["web"][nameof(ListenIPv4)] ?? "false");
                ListenPort = int.Parse(data["web"][nameof(ListenPort)] ?? "12309");
                ListenUnixSocketPath = data["web"][nameof(ListenUnixSocketPath)];
                MaxDBConnections = int.Parse(data["crawl"][nameof(MaxDBConnections)] ?? Environment.ProcessorCount.ToString());
                HashServerCropHost = data["web"][nameof(HashServerCropHost)] ?? "localhost";
                HashServerCropPort = int.Parse(data["web"][nameof(HashServerCropPort)] ?? "12306");
            }
        }
        public _web web;

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
