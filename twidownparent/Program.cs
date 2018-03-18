using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using twitenlib;

namespace twidownparent
{
    class Program
    {
        static async Task Main(string[] args)
        {
            //多重起動防止
            //CheckOldProcess.CheckandExit();

            Config config = Config.Instance;
            DBHandler db = new DBHandler();

            //LockerHandler.CheckAndStart();

            bool GetMyTweet = false;    //後から追加されたアカウントはstreamer側で自分のツイートを取得させる
            Stopwatch LoopWatch = new Stopwatch();
            while (true)
            {
                LoopWatch.Restart();

                await db.DeleteDeadpid();
                long[] users = await db.SelectNewToken();
                int NeedProcessCount = (int)(await db.CountToken() / config.crawlparent.AccountLimit + 1);
                int CurrentProcessCount = (int)await db.CountPid();

                if (users.Length > 0)
                {
                    Console.WriteLine("{0} Assigning {1} tokens", DateTime.Now, users.Length);
                    if (NeedProcessCount > 0 && CurrentProcessCount >= 0)
                    {
                        //アカウント数からして必要な個数のtwidownを起動する
                        for (int i = 0; i < NeedProcessCount - CurrentProcessCount; i++)
                        {
                            int newpid = ChildProcessHandler.Start();
                            if (newpid < 0) { continue; }    //雑すぎるエラー処理
                            await db.Insertpid(newpid);
                        }
                    }

                    int usersIndex = 0;
                    for (; usersIndex < users.Length; usersIndex++)
                    {
                        int pid = await db.SelectBestpid();
                        if (pid < 0)
                        {
                            int newpid = ChildProcessHandler.Start();
                            if (newpid < 0) { await Task.Delay(1000); }    //雑すぎるエラー処理
                            pid = newpid;
                            await db.Insertpid(pid);
                        }
                        await db.Assigntoken(users[usersIndex], pid, GetMyTweet);
                    }
                }
                GetMyTweet = true;


                //ここでプロセス間通信を監視して返事がなかったら再起動する
                do
                {
                    //LockerHandler.CheckAndStart();
                    await Task.Delay(1000);
                } while (LoopWatch.ElapsedMilliseconds < 60000);
                LoopWatch.Stop();
            }
        }
    }
}
