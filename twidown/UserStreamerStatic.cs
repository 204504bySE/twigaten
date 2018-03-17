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

namespace twidown
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

        //ツイートをDBに保存したりRTを先に保存したりする
        //アイコンを適宜保存する
        public static void HandleTweetRest(Status x, Tokens t, bool update)   //REST用
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; }   //画像なしツイートを捨てる
            TweetDistinctBlock.Post(new Tuple<Status, Tokens, bool>(x, t, false));
        }

        public static void HandleStatusMessage(Status x, Tokens t)
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media != null)  //画像なしツイートを捨てる
            { TweetDistinctBlock.Post(new Tuple<Status, Tokens, bool>(x, t, true)); }
        }

        public static void HandleDeleteMessage(DeleteMessage x)
        {
            //DeleteTweetBufferSizeが小さいとツイートよりツイ消しが先に処理されるかも
            DeleteTweetBatch.Post(x.Id);
        }

        static readonly HashSet<long> TweetLock = new HashSet<long>();
        static readonly UdpClient Udp = new UdpClient(new IPEndPoint(IPAddress.IPv6Loopback, (config.crawl.LockerUdpPort ^ (Process.GetCurrentProcess().Id & 0x3FFF))));
        static readonly IPEndPoint LockerEndPoint = new IPEndPoint(IPAddress.IPv6Loopback, config.crawl.LockerUdpPort);
        static readonly Stopwatch LockTweetSw;
        static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12
        });

        static bool LockTweet(long tweet_id)
        {
            //雑にプロセス内でもLockしておく
            if (!TweetLock.Add(tweet_id)) { return false; }
            if (TweetLock.Count >= config.crawl.TweetLockSize) { TweetLock.Clear(); }
            //twidownparentでもLockを確認する リトライあり
            LockTweetSw.Restart();
            while(LockTweetSw.ElapsedMilliseconds < 10000)
            {
                try
                {
                    Udp.Send(BitConverter.GetBytes(tweet_id), sizeof(long), LockerEndPoint);
                    IPEndPoint RemoteUdp = null;
                    return BitConverter.ToBoolean(Udp.Receive(ref RemoteUdp), 0);
                }
                catch (Exception e)
                {
                    Console.WriteLine("{0}\t{1}", DateTime.Now, e);
                    Thread.Sleep(500);
                }
            }
            return false;   //返事が来なかったらこれ
        }

        static TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>> TweetDistinctBlock
            = new TransformBlock<Tuple<Status, Tokens, bool>, Tuple<Status, Tokens, bool>>(x =>
            {   //ここでLockする(1スレッドなのでHashSetでおｋ

                if (LockTweet(x.Item1.Id)) { return x; }
                else { return null; }
            }, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1
            });
        static ActionBlock<Tuple<Status, Tokens, bool>> HandleTweetBlock = new ActionBlock<Tuple<Status, Tokens, bool>>(async x =>
        {   //Tokenを渡すためだけにKeyValuePairにしている #ウンコード
            //画像なしツイートは先に捨ててるのでここでは確認しない
            if (x != null) { await HandleTweet(x.Item1, x.Item2, x.Item3); }
        }, new ExecutionDataflowBlockOptions()
        {
            MaxDegreeOfParallelism = config.crawl.MaxDBConnections, //一応これで
            SingleProducerConstrained = true
        });

        static async Task HandleTweet(Status x, Tokens t, bool update)    //stream(=true)からのツイートならふぁぼRT数を上書きする
        {
            if ((x.ExtendedEntities ?? x.Entities)?.Media == null) { return; } //画像なしツイートを捨てる
            //RTを先にやる(キー制約)
            if (x.RetweetedStatus != null) { await HandleTweet(x.RetweetedStatus, t, update); }
            if (StreamerLocker.LockUser(x.User.Id))
            {
                if (update) { db.StoreUser(x, false); await DownloadStoreProfileImage(x); }
                else { db.StoreUser(x, false, false); }
            }
            Counter.TweetToStoreRest.Increment(); 
            int r;
            if ((r = db.StoreTweet(x, update)) >= 0)
            {
                if (r > 0) { Counter.TweetStoredRest.Increment(); }
                if (x.RetweetedStatus == null) { DownloadStoreMedia(x, t); }
            }
            //StreamerLocker.UnlockTweet(x.Id);  //Lockは事前にやっておくこと
        }

        static async Task DownloadStoreProfileImage(Status x)
        {
            //アイコンが更新または未保存ならダウンロードする
            //RTは自動でやらない
            //ダウンロード成功してもしなくてもそれなりにDBに反映する
            //(古い奴のURLがDBにあれば古いままになる)
            if (x.User.Id == null) { return; }
            string ProfileImageUrl = x.User.ProfileImageUrlHttps ?? x.User.ProfileImageUrl;
            DBHandler.ProfileImageInfo d = db.NeedtoDownloadProfileImage(x.User.Id.Value, ProfileImageUrl);
            if (!d.NeedDownload || !StreamerLocker.LockProfileImage((long)x.User.Id)) { return; }

            //新しいアイコンの保存先 卵アイコンは'_'をつけただけの名前で保存するお
            string LocalPath = x.User.IsDefaultProfileImage ?
                config.crawl.PictPathProfileImage + '_' + Path.GetFileName(ProfileImageUrl) :
                config.crawl.PictPathProfileImage + x.User.Id.ToString() + Path.GetExtension(ProfileImageUrl);

            bool DownloadOK = true; //卵アイコンのダウンロード不要でもtrue
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
                                    using (FileStream file = File.Create(LocalPath)) { await req.Content.CopyToAsync(file); await file.FlushAsync(); }
                                    break;
                                }
                                else if(res.StatusCode == HttpStatusCode.NotFound
                                    || res.StatusCode == HttpStatusCode.Forbidden)
                                { DownloadOK = false; break; }
                            }
                        }
                    }
                    catch { continue; }
                }
            }
            if (DownloadOK)
            {
                string oldext = Path.GetExtension(d.OldProfileImageUrl);
                string newext = Path.GetExtension(ProfileImageUrl);
                if (!d.isDefaultProfileImage && oldext != null && oldext != newext)  //卵アイコンはこのパスじゃないしそもそも消さない
                { File.Delete(config.crawl.PictPathProfileImage + x.User.Id.ToString() + oldext); }
                db.StoreUser(x, true);
            }
            else { db.StoreUser(x, false); }
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
                switch (db.ExistMedia_source_tweet_id(m.Id))
                {
                    case true:
                        if (OtherSourceTweet) { db.Storetweet_media(x.Id, m.Id); }
                        continue;
                    case null:
                        if (OtherSourceTweet && RestId.Value.Add(x.Id)) { await DownloadOneTweet(m.SourceStatusId.Value, t); }
                        db.Storetweet_media(x.Id, m.Id);
                        db.UpdateMedia_source_tweet_id(m, x);
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
                                    using (MemoryStream mem = new MemoryStream())
                                    {
                                        await res.Content.CopyToAsync(mem);
                                        mem.Seek(0, SeekOrigin.Begin);
                                        long? dcthash = await PictHash.DCTHash(mem, config.crawl.HashServerUrl, Path.GetFileName(MediaUrl));
                                        if (dcthash != null && (db.StoreMedia(m, x, (long)dcthash)) > 0)
                                        {
                                            mem.Seek(0, SeekOrigin.Begin);
                                            using (FileStream file = File.Create(LocalPaththumb)) { await mem.CopyToAsync(file); await file.FlushAsync(); }
                                            Counter.MediaSuccess.Increment();
                                        }
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
                if (OtherSourceTweet) { db.Storetweet_media(m.SourceStatusId.Value, m.Id); }
            }
        }, new ExecutionDataflowBlockOptions()
        {
            MaxDegreeOfParallelism = config.crawl.MediaDownloadThreads
        });

        public static void DownloadStoreMedia(Status x, Tokens t)
        {
            if (x.RetweetedStatus != null)
            {   //そもそもRTに対してこれを呼ぶべきではない
                DownloadStoreMedia(x.RetweetedStatus, t);
                return;
            }
            DownloadStoreMediaBlock.Post(new KeyValuePair<Status, Tokens>(x, t));
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
                if (db.ExistTweet(StatusId)) { return; }
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
            ((long[] ToDelete) => {
                foreach (long d in db.StoreDelete(ToDelete.Distinct().ToArray())) { DeleteTweetBatch.Post(d); }
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
        public static CounterValue RestSuccess = new CounterValue();
        public static CounterValue RestAttempts = new CounterValue();
        public static CounterValue RestAccounts = new CounterValue();
        public static CounterValue TweetStoredRest = new CounterValue();
        public static CounterValue TweetToStoreRest = new CounterValue();
        public static CounterValue TweetToDelete = new CounterValue();
        public static CounterValue TweetDeleted = new CounterValue();
        //ひとまずアイコンは除外しようか
        public static void PrintReset()
        {
            if (RestAccounts.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Accounts RESTed", DateTime.Now, RestSuccess.GetReset(), RestAttempts.GetReset(), RestAccounts.GetReset()); }
            if (TweetToStoreRest.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Stored", DateTime.Now, TweetStoredRest.GetReset(), TweetToStoreRest.GetReset()); }
            if (TweetToDelete.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} Tweet Deleted", DateTime.Now, TweetDeleted.GetReset(), TweetToDelete.GetReset()); }
            if (MediaToStore.Get() > 0) { Console.WriteLine("{0} App: {1} / {2} / {3} Media Stored", DateTime.Now, MediaSuccess.GetReset(), MediaToStore.GetReset(), MediaTotal.GetReset()); }
        }
    }

}


