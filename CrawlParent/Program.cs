using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Twigaten.Lib;

namespace Twigaten.CrawlParent
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //多重起動防止
            //CheckOldProcess.CheckandExit();

            var config = Config.Instance;
            var db = new DBHandler();

            await db.NullifyPidAll().ConfigureAwait(false);
            var child = new ChildProcessHandler();

            //子プロセスが複数あったらいるかもしれないけど今は無用(´・ω・`)
            //LockerHandler.CheckAndStart();
            //画像保存用フォルダはここで作る
            MediaFolderPath.MkdirAll();

            bool GetMyTweet = false;    //後から追加されたアカウントはstreamer側で自分のツイートを取得させる
            Stopwatch LoopWatch = new Stopwatch();
            while (true)
            {
                LoopWatch.Restart();
                
                long[] users = await db.SelectNewToken().ConfigureAwait(false);
                int NeedProcessCount = (int)(await db.CountToken().ConfigureAwait(false) / config.crawlparent.AccountLimit + 1);
                if (users.Length > 0)
                {
                    Console.WriteLine("Assigning {0} tokens", users.Length);
                    //アカウント数からして必要な個数のtwidownを起動する
                    while (NeedProcessCount > child.Count)
                    {
                        int newpid = child.Start();
                        if (newpid < 0) { await Task.Delay(60000).ConfigureAwait(false); continue; }    //雑すぎるエラー処理
                        Console.WriteLine("New PID: {0}", newpid);
                    }
                    //あとはボコボコ突っ込む
                    await child.AssignToken(users, GetMyTweet).ConfigureAwait(false);
                }
                GetMyTweet = true;


                //ここでプロセス間通信を監視して返事がなかったら再起動する
                do
                {
                    await child.DeleteDead().ConfigureAwait(false);
                    //LockerHandler.CheckAndStart();
                    await Task.Delay(10000).ConfigureAwait(false);
                } while (LoopWatch.ElapsedMilliseconds < 60000);
                LoopWatch.Stop();
            }
        }
    }
}
