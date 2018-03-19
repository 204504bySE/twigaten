using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CoreTweet;
using CoreTweet.Streaming;
using twitenlib;
using twidown;
using System.Net.Http;

namespace twidownstream
{
    class UserStreamerStatic
    {
        //Singleton
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        static UserStreamerStatic()
        {
            DeleteTweetBatch.LinkTo(DeleteTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
            TweetDistinctBlock.LinkTo(HandleTweetBlock, new DataflowLinkOptions { PropagateCompletion = true });
            Udp.Client.SendTimeout = 1000;
            Udp.Client.ReceiveTimeout = 1000;
        }

        public static void ShowCount()
        {
            int a, b;
            if ((a = TweetDistinctBlock.InputCount) > 0 | (b = HandleTweetBlock.InputCount) > 0)
            { Console.WriteLine("{0} App: {1} -> {2} Tweets in buffer", DateTime.Now, a, b); }
            if ((a = DownloadStoreMediaBlock.InputCount) > 0) { Console.WriteLine("{0} App: {1} Media in buffer", DateTime.Now, a); }
        }

        public static bool NeedConnectPostpone()
        {
            return TweetDistinctBlock.InputCount > config.crawl.ConnectPostponeSize
                || HandleTweetBlock.InputCount > config.crawl.ConnectPostponeSize
                || DownloadStoreMediaBlock.InputCount > config.crawl.ConnectPostponeSize;
        }

        //ツイートをDBに保存したりRTを先に保存したりする
        //アイコンを適宜保存する
        public static async Task HandleTweetRest(Status x, Tokens t)   //REST用
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; }   //画像なしツイートを捨てる
            await TweetDistinctBlock.SendAsync((x, t, false));
        }

        public static async Task HandleStatusMessage(Status x, Tokens t)
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media != null)  //画像なしツイートを捨てる
            { await TweetDistinctBlock.SendAsync((x, t, true)); }
        }

        public static async Task HandleDeleteMessage(DeleteMessage x)
        {
            //DeleteTweetBufferSizeが小さいとツイートよりツイ消しが先に処理されるかも
            await DeleteTweetBatch.SendAsync(x.Id);
        }



        static readonly RemoveOldSet<long> TweetLock = new RemoveOldSet<long>(config.crawl.TweetLockSize);
        static readonly UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, (config.crawl.LockerUdpPort ^ (Process.GetCurrentProcess().Id & 0x3FFF))));
        static readonly IPEndPoint LockerEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, config.crawl.LockerUdpPort);
        static readonly Stopwatch LastTweetSw = new Stopwatch();
        static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12
        });

        //static IPEndPoint GomiEndPoint;
        static bool LockTweet(long tweet_id)
        {
            return TweetLock.Add(tweet_id);
            /*
            //雑にプロセス内でもLockしておく
            if (!TweetLock.Add(tweet_id)) { return false; }
            if (TweetLock.Count >= config.crawl.TweetLockSize) { TweetLock.Clear(); }

            //twidownparentでもLockを確認する リトライあり
            LastTweetSw.Restart();
            do
            {
                try
                {
                    await Udp.SendAsync(BitConverter.GetBytes(tweet_id), sizeof(long), LockerEndPoint);                    
                    return BitConverter.ToBoolean(Udp.Receive(ref GomiEndPoint), 0);
                }
                catch (Exception e)
                {
                    Console.WriteLine("{0}\t{1}", DateTime.Now, e);
                    await Task.Delay(500);
                }
            } while (LastTweetSw.ElapsedMilliseconds < 10000);
            return false;   //返事が来なかったらこれ
            */
        }

        static TransformBlock<(Status, Tokens, bool), (Status, Tokens, bool)?> TweetDistinctBlock
            = new TransformBlock<(Status x, Tokens t, bool stream),(Status, Tokens, bool)?>((m) =>
            {   //ここでLockする(1スレッドなのでHashSetでおｋ
                if (LockTweet(m.x.Id)) { return m; }
                else { return null; }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1
            });
        static ActionBlock<(Status, Tokens, bool)?> HandleTweetBlock = new ActionBlock<(Status x, Tokens t, bool stream)?>(async x =>
        {   //Tokenを渡すためだけにKeyValuePairにしている #ウンコード
            //画像なしツイートは先に捨ててるのでここでは確認しない
            if (x != null) { await HandleTweet(x.Value.x, x.Value.t, x.Value.stream); }
        }, new ExecutionDataflowBlockOptions()
        {
            MaxDegreeOfParallelism = Math.Max(Math.Max(Environment.ProcessorCount, config.crawl.MaxDBConnections), config.crawl.MediaDownloadThreads), //一応これで
            SingleProducerConstrained = true
        });

        static async Task HandleTweet(Status x, Tokens t, bool stream)    //stream(=true)からのツイートならふぁぼRT数を上書きする
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; } //画像なしツイートを捨てる
            //RTを先にやる(キー制約)
            if (x.RetweetedStatus != null) { await HandleTweet(x.RetweetedStatus, t, stream); }
            if (StreamerLocker.LockUser(x.User.Id)) { await db.StoreUser(x, await DownloadStoreProfileImage(x), stream); }
            if (stream) { Counter.TweetToStoreStream.Increment(); }
            else { Counter.TweetToStoreRest.Increment(); }
            int r;
            if ((r = await db.StoreTweet(x, stream)) >= 0)
            {
                if (r > 0)
                {
                    if (stream) { Counter.TweetStoredStream.Increment(); }
                    else { Counter.TweetStoredRest.Increment(); }
                }
                if (x.RetweetedStatus == null) { await DownloadStoreMedia(x, t); }
            }
            //StreamerLocker.UnlockTweet(x.Id);  //Lockは事前にやっておくこと
        }

        static async ValueTask<bool> DownloadStoreProfileImage(Status x)
        {
            //アイコンが更新または未保存ならダウンロードする
            //RTは自動でやらない
            //ダウンロード成功したかどうかを返すのでStoreUserに投げる
            //(古い奴のURLがDBにあれば古いままになる)
            if (x.User.Id == null) { return false; }
            string ProfileImageUrl = x.User.ProfileImageUrlHttps ?? x.User.ProfileImageUrl;
            DBHandler.ProfileImageInfo d = await db.NeedtoDownloadProfileImage(x.User.Id.Value, ProfileImageUrl);
            if (!d.NeedDownload || !StreamerLocker.LockProfileImage((long)x.User.Id)) { return false; }

            //新しいアイコンの保存先 卵アイコンは'_'をつけただけの名前で保存するお
            string LocalPath = x.User.IsDefaultProfileImage ?
                config.crawl.PictPathProfileImage + '_' + Path.GetFileName(ProfileImageUrl) :
                config.crawl.PictPathProfileImage + x.User.Id.ToString() + Path.GetExtension(ProfileImageUrl);

            bool DownloadOK = false; 
            if (!x.User.IsDefaultProfileImage || !File.Exists(LocalPath))
            {
                for (int RetryCount = 0; RetryCount < 2; RetryCount++)
                {
                    try
                    {
                        using (HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, ProfileImageUrl))
                        {
                            req.Headers.Referrer = new Uri(StatusUrl(x));
                            using (HttpResponseMessage res = await Http.SendAsync(req))
                            {
                                if (res.IsSuccessStatusCode)
                                {
                                    using (FileStream file = File.Create(LocalPath))
                                    {
                                        await res.Content.CopyToAsync(file);
                                        await file.FlushAsync();
                                    }
                                    DownloadOK = true; break;
                                }
                                else if (res.StatusCode == HttpStatusCode.Forbidden
                                    || res.StatusCode == HttpStatusCode.NotFound)
                                { DownloadOK = false; break; }
                            }
                        }
                    }
                    catch { continue; }
                }
            }
            else { DownloadOK = true; } //卵アイコンのダウンロード不要でもtrue
            if (DownloadOK)
            {
                string oldext = Path.GetExtension(d.OldProfileImageUrl);
                string newext = Path.GetExtension(ProfileImageUrl);
                if (!d.isDefaultProfileImage && oldext != null && oldext != newext)  //卵アイコンはこのパスじゃないしそもそも消さない
                { File.Delete(config.crawl.PictPathProfileImage + x.User.Id.ToString() + oldext); }
            }
            return DownloadOK;
        }

        static ActionBlock<KeyValuePair<Status, Tokens>> DownloadStoreMediaBlock = new ActionBlock<KeyValuePair<Status, Tokens>>(async a =>
        {
            Status x = a.Key;
            Tokens t = a.Value;
            Lazy<HashSet<long>> RestId = new Lazy<HashSet<long>>();   //同じツイートを何度も処理したくない
            foreach (MediaEntity m in x.ExtendedEntities.Media ?? x.Entities.Media)
            {
                Counter.MediaTotal.Increment();

                //URLぶち抜き転載の場合はここでツイートをダウンロード(すでにあればキャンセルされる
                //x.Idのツイートのダウンロード失敗については何もしない(成功したツイートのみPostするべき
                bool OtherSourceTweet = m.SourceStatusId.HasValue && m.SourceStatusId.Value != x.Id;    //URLぶち抜きならtrue
                switch (await db.ExistMedia_source_tweet_id(m.Id))
                {
                    case true:
                        if (OtherSourceTweet) { await db.Storetweet_media(x.Id, m.Id); }
                        continue;
                    case null:
                        if (OtherSourceTweet && RestId.Value.Add(x.Id)) { await DownloadOneTweet(m.SourceStatusId.Value, t); }
                        await db.Storetweet_media(x.Id, m.Id);
                        await db.UpdateMedia_source_tweet_id(m, x);
                        continue;
                    case false:
                        if (OtherSourceTweet && RestId.Value.Add(x.Id)) { await DownloadOneTweet(m.SourceStatusId.Value, t); }    //コピペつらい
                        break;   //画像の情報がないときだけダウンロードする
                }
                string MediaUrl = m.MediaUrlHttps ?? m.MediaUrl;
                string LocalPaththumb = config.crawl.PictPaththumb + m.Id.ToString() + Path.GetExtension(MediaUrl);  //m.Urlとm.MediaUrlは違う
                string uri = MediaUrl + (MediaUrl.IndexOf("twimg.com") >= 0 ? ":thumb" : "");

                for (int RetryCount = 0; RetryCount < 2; RetryCount++)
                {
                    try
                    {
                        using (HttpRequestMessage req = new HttpRequestMessage(HttpMethod.Get, uri))
                        {
                            req.Headers.Referrer = new Uri(StatusUrl(x));
                            using (HttpResponseMessage res = await Http.SendAsync(req))
                            {
                                if (res.IsSuccessStatusCode)
                                {
                                    byte[] mem = await res.Content.ReadAsByteArrayAsync();
                                    long? dcthash = await PictHash.DCTHash(mem, config.crawl.HashServerUrl, Path.GetFileName(MediaUrl));
                                    if (dcthash != null && (await db.StoreMedia(m, x, (long)dcthash)) > 0)
                                    {
                                        using (FileStream file = File.Create(LocalPaththumb))
                                        {
                                            await file.WriteAsync(mem, 0, mem.Length);
                                            await file.FlushAsync();
                                        }
                                        Counter.MediaSuccess.Increment();
                                    }
                                    break;
                                }
                                else if (res.StatusCode == HttpStatusCode.NotFound
                                    || res.StatusCode == HttpStatusCode.Forbidden)
                                { break; }
                            }
                        }
                    }
                    catch { continue; }
                }
                Counter.MediaToStore.Increment();
                //URL転載元もペアを記録する
                if (OtherSourceTweet) { await db.Storetweet_media(m.SourceStatusId.Value, m.Id); }
            }
        }, new ExecutionDataflowBlockOptions()
        {
            MaxDegreeOfParallelism = config.crawl.MediaDownloadThreads
        });

        public static  Task DownloadStoreMedia(Status x, Tokens t)
        {
            if (x.RetweetedStatus != null)
            {   //そもそもRTに対してこれを呼ぶべきではない
                return DownloadStoreMedia(x.RetweetedStatus, t);
            }
            return DownloadStoreMediaBlock.SendAsync(new KeyValuePair<Status, Tokens>(x, t));
        }

        //API制限対策用
        static ConcurrentDictionary<Tokens, DateTimeOffset> OneTweetReset = new ConcurrentDictionary<Tokens, DateTimeOffset>();
        //source_tweet_idが一致しないやつは元ツイートを取得したい
        static async Task DownloadOneTweet(long StatusId, Tokens Token)
        {
            if (OneTweetReset.ContainsKey(Token) && OneTweetReset[Token] > DateTimeOffset.Now) { return; }
            OneTweetReset.TryRemove(Token, out DateTimeOffset gomi);
            //if (!StreamerLocker.LockTweet(StatusId)) { return; }  //もうチェックしなくていいや(雑
            try
            {
                if (await db.ExistTweet(StatusId)) { return; }
                var res = await Token.Statuses.LookupAsync(id => StatusId, include_entities => true, tweet_mode => TweetMode.Extended);
                if (res.RateLimit.Remaining < 1) { OneTweetReset[Token] = res.RateLimit.Reset.AddMinutes(1); }  //とりあえず1分延長奴
                await HandleTweet(res.First(), Token, true);
            }
            catch { Console.WriteLine("{0} {1} REST Tweet failed: {2}", DateTime.Now, Token.UserId, StatusId); return; }
            //finally { StreamerLocker.UnlockTweet(StatusId); }
        }

        static BatchBlock<long> DeleteTweetBatch = new BatchBlock<long>(config.crawl.DeleteTweetBufferSize);
        //ツイ消しはここでDBに投げることにした
        static ActionBlock<long[]> DeleteTweetBlock = new ActionBlock<long[]>
            (async (long[] ToDelete) => {
                foreach (long d in (await db.StoreDelete(ToDelete.Distinct().ToArray()))) { await DeleteTweetBatch.SendAsync(d); }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1
            });

        //ツイートのURLを作る
        public static string StatusUrl(Status x)
        {
            return "https://twitter.com/" + x.User.ScreenName + "/status/" + x.Id;
        }


    }
    //ツイートの処理を調停する感じの奴
    public static class StreamerLocker
    {
        //storeuser用
        //UnlockUser()はない Unlock()で処理する
        static ConcurrentDictionary<long, byte> LockedUsers = new ConcurrentDictionary<long, byte>();
        public static bool LockUser(long? Id) { return Id != null && LockedUsers.TryAdd((long)Id, 0); }

        //DownloadProfileImage用
        static ConcurrentDictionary<long, byte> LockedProfileImages = new ConcurrentDictionary<long, byte>();
        public static bool LockProfileImage(long Id) { return LockedProfileImages.TryAdd(Id, 0); }

        //これを外から呼び出してロックを解除する
        public static void Unlock()
        {
            LockedUsers.Clear();
            LockedProfileImages.Clear();
            //LockedTweetsClearFlag = true;
        }
    }

    public static class Counter
    {
        //パフォーマンスカウンター的な何か
        public struct CounterValue
        {
            int Value;
            public void Increment() { Interlocked.Increment(ref Value); }
            public void Add(int v) { Interlocked.Add(ref Value, v); }
            public int Get() { return Value; }
            public int GetReset() { return Interlocked.Exchange(ref Value, 0); }
        }

        public static CounterValue MediaSuccess = new CounterValue();
        public static CounterValue MediaToStore = new CounterValue();
        public static CounterValue MediaTotal = new CounterValue();
        public static CounterValue TweetStoredStream = new CounterValue();
        public static CounterValue TweetStoredRest = new CounterValue();
        public static CounterValue TweetToStoreStream = new CounterValue();
        public static CounterValue TweetToStoreRest = new CounterValue();
        public static CounterValue TweetToDelete = new CounterValue();
        public static CounterValue TweetDeleted = new CounterValue();
        //ひとまずアイコンは除外しようか
        public static void PrintReset()
        {
            if (TweetToStoreStream.Get() > 0 || TweetToStoreRest.Get() > 0)
            {
                Console.WriteLine("{0} App: {1} + {2} / {3} + {4} Tweet Stored", DateTime.Now,
                TweetStoredStream.GetReset(), TweetStoredRest.GetReset(),
                TweetToStoreStream.GetReset(), TweetToStoreRest.GetReset());
            }
            if (TweetToDelete.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Deleted", DateTime.Now, TweetDeleted.GetReset(), TweetToDelete.GetReset()); }
            if (MediaToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Media Stored", DateTime.Now, MediaSuccess.GetReset(), MediaToStore.GetReset(), MediaTotal.GetReset()); }
        }
    }

}


