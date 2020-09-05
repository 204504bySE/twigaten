using System;
using System.Collections.Generic;
using System.Linq;
using MySqlConnector;
using System.Text;
using System.Data;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Data.Common;

namespace Twigaten.Lib
{    public class DBHandler
    {
        protected static readonly Config config = Config.Instance;
        readonly string ConnectionStr;
        public DBHandler(string server, MySqlConnectionProtocol protocol, uint timeout = 20, uint poolsize = 40, uint lifetime = 3600)
        {
            if(lifetime != 0 && lifetime < timeout) { throw new ArgumentException("lifetime < timeout"); }
            var builder = new MySqlConnectionStringBuilder()
            {
                Server = server, 
                ConnectionProtocol = protocol,
                Database = "twiten",
                SslMode = MySqlSslMode.None,    //にゃーん
                UserID = "twigaten",
                Password = "",
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

        ///<summary>head + "(@a0,@b0),(@a1,@b1),…,(@a{count},@b{count}) + tail;" という文字列を生成する
        ///"INSERT INTO ... VALUES" など</summary>
        protected static string BulkCmdStr(int count, int unit, string head, string tail = "")
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
                .Append(tail)
                .Append(';');
            return BulkCmd.ToString();
        }

        ///<summary>head + "(@0,@1,@2,@3…);" という文字列を生成する
        ///"SELECT ... WHERE ... IN"など</summary>
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
        ///<summary>head + "fields[0] = ELT(FIELD(keyname,@0,@1…)@a0,@a1…), fields[1] = ELT(FIELD(keyname,@0,@1…)@b0,@b1…) WHERE keyname IN (@0,@1…) という文字列を生成する
        ///"UPDATE ... SET" など</summary>>
        public static string BulkCmdStrUpdate(int count, string head, string keyname, params string[] fields)
        {
            var BulkCmd = new StringBuilder(head);
            for (int i = 0; i < fields.Length; i++)
            {
                if (0 < i) { BulkCmd.Append(", "); }
                else { BulkCmd.Append(' '); }

                BulkCmd.Append(fields[i]);
                BulkCmd.Append(" = ELT(FIELD(");
                BulkCmd.Append(keyname);
                for (int j = 0; j < count; j++)
                {
                    BulkCmd.Append(",@");
                    BulkCmd.Append(j.ToString());
                }
                BulkCmd.Append(')');
                for (int j = 0; j < count; j++)
                {
                    BulkCmd.Append(",@");
                    BulkCmd.Append(Convert.ToChar(0x61 + i));
                    BulkCmd.Append(j.ToString());
                }
                BulkCmd.Append(')');
            }
            BulkCmd.Append(" WHERE ");
            BulkCmd.Append(keyname);
            BulkCmd.Append(" IN ");
            return BulkCmdStrIn(count, BulkCmd.ToString());
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
                        catch (MySqlException) { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch (Exception e) { }
            return null;
        }

        ///<summary>ReadActionには1行読む毎にやる処理を書く 最後まで成功したらTrue</summary>
        protected async Task<bool> ExecuteReader(MySqlCommand cmd, Action<DbDataReader> ReadAction, IsolationLevel IsolationLevel = IsolationLevel.ReadCommitted)
        {
            try
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
                        catch (MySqlException e) { reader?.Close(); await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch (Exception e) { }
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
                        catch (MySqlException) { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch (Exception e) { }
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
                    using (var tran = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted).ConfigureAwait(false))
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
                        catch (MySqlException) { await tran.RollbackAsync().ConfigureAwait(false); }
                    }
                }
            }
            catch (Exception e) { }
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

            private static TElem? NullableField<TElem>(object value) where TElem : struct
            {
                if (DBNull.Value == value)
                {
                    return default(TElem?);
                }
                return new TElem?((TElem)value);
            }
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