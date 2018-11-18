using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MySql.Data.MySqlClient;
using System.Text;
using System.Data;
using System.Threading;
using System.IO;
using System.Diagnostics;
using IniParser;
using IniParser.Model;
using System.Threading.Tasks;
using System.Data.Common;

namespace twitenlib
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
            catch { }   //twiviewではこのconfigクラスは使用しない
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
            public string MountPointProfileImage { get; }
            public string PictPaththumb { get; }
            public string MountPointthumb { get; }
            public int StreamSpeedSeconds { get; }
            public int StreamSpeedTweets { get; }
            public int StreamSpeedHysteresis { get; }
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
                MountPointProfileImage = data["crawl"][nameof(MountPointProfileImage)] ?? PictPathProfileImage?.Substring(0,1) ?? "";
                PictPaththumb = data["crawl"][nameof(PictPaththumb)] ?? Path.Combine(Directory.GetCurrentDirectory(), @"pict\thumb\");
                MountPointthumb = data["crawl"][nameof(MountPointthumb)] ?? PictPaththumb?.Substring(0,1) ?? "";
                StreamSpeedSeconds = int.Parse(data["crawl"][nameof(StreamSpeedSeconds)] ?? "180");
                StreamSpeedTweets = int.Parse(data["crawl"][nameof(StreamSpeedTweets)] ?? "50");
                StreamSpeedHysteresis = int.Parse(data["crawl"][nameof(StreamSpeedHysteresis)] ?? "16");
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
            public int HashCountOffset { get; }
            public string TempDir { get; }
            public long InitialSortFileSize { get; }
            public int InitialSortConcurrency { get; }
            public int FileSortThreads { get; }
            public int FileSortFilesPerStep { get; }
            public int ZipBufferSize { get; }
            public _hash(string iniPath, FileIniDataParser ini, IniData data)
            {
                this.iniPath = iniPath; this.ini = ini; this.data = data;
                MaxHammingDistance = int.Parse(data["hash"][nameof(MaxHammingDistance)] ?? "3");
                ExtraBlocks = int.Parse(data["hash"][nameof(ExtraBlocks)] ?? "1");
                LastUpdate = long.Parse(data["hash"][nameof(LastUpdate)] ?? "0");
                LastHashCount = long.Parse(data["hash"][nameof(LastHashCount)] ?? "0");
                HashCountOffset = int.Parse(data["hash"][nameof(HashCountOffset)] ?? "5000000");
                TempDir = data["hash"][nameof(TempDir)] ?? "";
                InitialSortFileSize = long.Parse(data["hash"][nameof(InitialSortFileSize)] ?? "1073741824");
                InitialSortConcurrency = int.Parse(data["hash"][nameof(InitialSortConcurrency)] ?? "1");
                FileSortThreads = int.Parse(data["hash"][nameof(FileSortThreads)] ?? Environment.ProcessorCount.ToString());
                FileSortFilesPerStep = int.Parse(data["hash"][nameof(FileSortFilesPerStep)] ?? "2");
                ZipBufferSize = int.Parse(data["hash"][nameof(ZipBufferSize)] ?? "65536");
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
            public _database(IniData data)
            {
                Address = data["database"][nameof(Address)] ?? "127.0.0.1"; //::1だとNotSupportedExceptionになるのだｗ
            }
        }
        public _database database;
    }

    public class DBHandler
    {
        protected static readonly Config config = Config.Instance;
        readonly string ConnectionStr;
        public DBHandler(string user, string pass, string server ="localhost", uint timeout = 20, uint poolsize = 40, uint lifetime = 3600)
        {
            if(lifetime < timeout) { throw new ArgumentException("lifetime < timeout"); }
            var builder = new MySqlConnectionStringBuilder()
            {
                Server = server,
                Database = "twiten",
                SslMode = MySqlSslMode.None,    //にゃーん
                UserID = user,
                Password = pass,
                MinimumPoolSize = 1,
                MaximumPoolSize = poolsize,    //デフォルトは100
                ConnectionLifeTime = lifetime,
                CharacterSet = "utf8mb4",
                DefaultCommandTimeout = timeout    //デフォルトは20(秒
            };
            ConnectionStr = builder.ToString();            
        }
        MySqlConnection NewConnection()
        {
            return new MySqlConnection(ConnectionStr);
        }

        //"(@a0,@b0),(@a1,@b1),(@a2,@b2)…;" という文字列を出すだけ
        //bulk insertとかこれ使おうな
        protected static string BulkCmdStr(int count, int unit, string head)
        {
            if(26 < unit) { throw new ArgumentOutOfRangeException("26 < unit"); }
            var BulkCmd = new StringBuilder(head);
            for (int i = 0; i < count; i++)
            {
                BulkCmd.Append("(@");
                for (int j = 0; j < unit - 1; j++)
                {
                    BulkCmd.Append(Convert.ToChar(0x61 + j));
                    BulkCmd.Append(i);
                    BulkCmd.Append(",@");
                }
                BulkCmd.Append(Convert.ToChar(0x61 + unit - 1));
                BulkCmd.Append(i);
                BulkCmd.Append("),");
            }
            BulkCmd.Remove(BulkCmd.Length - 1, 1)
                .Append(';');
            return BulkCmd.ToString();
        }

        //(@0,@1,@2,@3…);  という文字列
        protected static string BulkCmdStrIn(int count, string head)
        {
            var BulkCmd = new StringBuilder(head);
            BulkCmd.Append("(@");
            for (int i = 0; i < count; i++)
            {
                BulkCmd.Append(i);
                BulkCmd.Append(",@");
            }
            BulkCmd.Remove(BulkCmd.Length - 2, 2);
            BulkCmd.Append(");");
            return BulkCmd.ToString();
        }

        protected async Task<DataTable> SelectTable(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            try
            {
                DataTable ret;
                using (var conn = NewConnection())
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using (var tran = await conn.BeginTransactionAsync(IsolationLevel).ConfigureAwait(false))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        try
                        {
                            using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                            {
                                ret = new DataTable();
                                adapter.Fill(ret);
                            }
                            await tran.CommitAsync().ConfigureAwait(false);
                            return ret;
                        }
                        catch { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch { }
            return null;
        }

        ///<summary>ReadActionには1行読む毎にやる処理を書く 最後まで成功したらTrue</summary>
        protected async Task<bool> ExecuteReader(MySqlCommand cmd, Action<DbDataReader> ReadAction, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            //try
            {
                using (var conn = NewConnection())
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using (var tran = await conn.BeginTransactionAsync(IsolationLevel).ConfigureAwait(false))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        DbDataReader reader = null;
                        try
                        {
                            reader = await cmd.ExecuteReaderAsync().ConfigureAwait(false);
                            while (await reader.ReadAsync().ConfigureAwait(false)) { ReadAction.Invoke(reader); }
                            reader.Close();
                            await tran.CommitAsync().ConfigureAwait(false);
                            return true;
                        }
                        catch { reader?.Close(); await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            //catch { }
            return false;
        }

        ///<summary>SELECT COUNT() 用</summary>
        protected async Task<long> SelectCount(MySqlCommand cmd, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            try
            {
                long ret;
                using (var conn = NewConnection())
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using (var tran = await conn.BeginTransactionAsync(IsolationLevel).ConfigureAwait(false))
                    {
                        try
                        {
                            cmd.Connection = conn;
                            cmd.Transaction = tran;
                            ret = Convert.ToInt64(await cmd.ExecuteScalarAsync().ConfigureAwait(false));
                            await tran.CommitAsync().ConfigureAwait(false);
                            return ret;
                        }
                        catch { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch { }
            return -1;
        }

        protected Task<int> ExecuteNonQuery(MySqlCommand cmd)
        {
            return ExecuteNonQuery(new MySqlCommand[] { cmd });
        }

        ///<summary>
        ///MysqlConnectionとMySQLTransactionを張ってcmdを実行する
        ///戻り値はDBの変更された行数
        ///失敗したら-1
        ///</summary>
        protected async Task<int> ExecuteNonQuery(IEnumerable<MySqlCommand> cmd)
        {
            try
            {
                int ret = 0;
                using (var conn = NewConnection())
                {
                    await conn.OpenAsync().ConfigureAwait(false);
                    using (var tran = await conn.BeginTransactionAsync(IsolationLevel.ReadUncommitted).ConfigureAwait(false))
                    {
                        try
                        {
                            foreach (MySqlCommand c in cmd)
                            {
                                c.Connection = conn;
                                c.Transaction = tran;
                                ret += await c.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }
                            await tran.CommitAsync().ConfigureAwait(false);
                            return ret;
                        }
                        catch { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch { }
            return -1;
        }
    }

    ///<summary>.NET Coreで廃止されてるやつをやっつけで追加</summary>
    static class DbExtension
    {
        public static T Field<T>(this DataRow row, int columnIndex)
        {
            return UnboxT<T>.Unbox(row[columnIndex]);
        }
        public static IEnumerable<DataRow> AsEnumerable(this DataTable table)
        {
            for(int i = 0; i < table.Rows.Count; i++) { yield return table.Rows[i]; }
        }


        //DataRowExtenstions.cs
        private static class UnboxT<T>
        {
            internal static readonly Converter<object, T> Unbox = Create(typeof(T));

            private static Converter<object, T> Create(Type type)
            {
                if (type.IsValueType)
                {
                    if (type.IsGenericType && !type.IsGenericTypeDefinition && (typeof(Nullable<>) == type.GetGenericTypeDefinition()))
                    {
                        return (Converter<object, T>)Delegate.CreateDelegate(
                            typeof(Converter<object, T>),
                                typeof(UnboxT<T>)
                                    .GetMethod("NullableField", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)
                                    .MakeGenericMethod(type.GetGenericArguments()[0]));
                    }
                    return ValueField;
                }
                return ReferenceField;
            }

            private static T ReferenceField(object value)
            {
                return ((DBNull.Value == value) ? default(T) : (T)value);
            }

            private static T ValueField(object value)
            {
                if (DBNull.Value == value)
                {
                    throw new InvalidCastException();
                }
                return (T)value;
            }

            private static Nullable<TElem> NullableField<TElem>(object value) where TElem : struct
            {
                if (DBNull.Value == value)
                {
                    return default(Nullable<TElem>);
                }
                return new Nullable<TElem>((TElem)value);
            }
        }
    }

    static class SnowFlake
    {
        public const long msinSnowFlake = 0x400000L;   //1msはこれだ
        const long TwEpoch = 1288834974657L;
        public static long SecondinSnowFlake(long TimeSeconds, bool Larger)
        {
            if (Larger) { return (TimeSeconds * 1000 + 999 - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (TimeSeconds * 1000 - TwEpoch) << 22; }
        }
        public static long SecondinSnowFlake(DateTimeOffset TimeSeconds, bool Larger)
        {
            return SecondinSnowFlake(TimeSeconds.ToUnixTimeSeconds(), Larger);
        }
        　
        public static long Now(bool Larger)
        {
            if (Larger) { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22 | 0x3FFFFFL; }
            else { return (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - TwEpoch) << 22; }
        }
        public static DateTimeOffset DatefromSnowFlake(long SnowFlake)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds((SnowFlake >> 22) + TwEpoch);
        }
    }

    public class RemoveOldSet<T>
    {
        HashSet<T> OldSet;
        HashSet<T> NewSet;

        int MaxSize { get; }
        public RemoveOldSet(int MaxSize)
        {
            //各Setあたりのサイズに変換する
            this.MaxSize = Math.Max(MaxSize >> 1, 1);

            OldSet = new HashSet<T>();
            NewSet = new HashSet<T>();
        }

        public bool Add(T Value)
        {
            RemoveOld();
            return !OldSet.Contains(Value) && NewSet.Add(Value);
        }

        public bool Contains(T Value)
        {
            return OldSet.Contains(Value) || NewSet.Contains(Value);
        }

        void RemoveOld()
        {
            if (NewSet.Count >= MaxSize)
            {
                OldSet.Clear();
                var TempSet = OldSet;
                OldSet = NewSet;
                NewSet = TempSet;
            }
        }
    }
    
    static class CharCodes
    {
        static Encoding ascii = Encoding.GetEncoding(1252, new EncoderReplacementFallback(""), DecoderFallback.ReplacementFallback);
        public static string KillNonASCII(string Str)
        {
            if (Str == null) { return null; }
            //ASCII範囲外は消しちゃう(めんどくさい
            byte[] bytes = ascii.GetBytes(Str);
            return ascii.GetString(bytes);
        }
    }

    static class CheckOldProcess
    {
        public static void CheckandExit()
        {   //同名のプロセスがすでに動いていたら終了する
            Process CurrentProc = Process.GetCurrentProcess();
            Process[] proc = Process.GetProcessesByName(CurrentProc.ProcessName);
            foreach (Process p in proc)
            {
                if (p.Id != CurrentProc.Id)
                {
                    Console.WriteLine("Another Instance of {0} is Running.", CurrentProc.ProcessName);
                    Thread.Sleep(5000);
                    Environment.Exit(1);
                }
            }
        }
    }

}