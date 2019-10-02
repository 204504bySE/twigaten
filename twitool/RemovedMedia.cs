using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySql.Data.MySqlClient;
using twitenlib;

namespace twitool
{

    class RemovedMedia : twitenlib.DBHandler
    {
        const int DownloadConcurrency = 256;
        public RemovedMedia() : base("tool", "", config.database.Address, config.database.Protocol, 600, (uint)(Environment.ProcessorCount << 2))
        {
            Counter.AutoRefresh();
        }

        static readonly HttpClient Http = new HttpClient(new HttpClientHandler()
        {
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.All,
            SslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13
        })
        {
            DefaultRequestVersion = HttpVersion.Version20
        };

        //twimgで画像が消えてたら条件付きでツイを消したい
        public async Task DeleteRemovedTweet(DateTimeOffset Begin, DateTimeOffset Exclude)
        {
            //本当にツイートを消すやつ
            var RemoveTweetBlock = new ActionBlock<long>(async (tweet_id) =>
            {
                //もっと古い公開ツイートがあるのは先に確認したぞ
                Counter.TweetToDelete.Increment();
                using (var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;"))
                using (var cmd2 = new MySqlCommand(@"DELETE FROM tweet_text WHERE tweet_id = @tweet_id;"))
                {
                    cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                    cmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                    if (await ExecuteNonQuery(new[] { cmd, cmd2 }).ConfigureAwait(false) > 0) { Counter.TweetDeleted.Increment(); }
                }
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
            
            //画像の存在確認をするやつ
            var TryDownloadBlock = new ActionBlock<IEnumerable<MediaInfo>>(async (media) =>
            {
                if(media.Count() == 0) { return; }
                foreach (var m in media)
                {
                    using (var req = new HttpRequestMessage(HttpMethod.Head, m.media_url))
                    {
                        Counter.MediaTotal.Increment();
                        req.Headers.Referrer = new Uri(m.tweet_url);
                        using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                        {
                            if (!res.IsSuccessStatusCode
                                && (res.StatusCode == HttpStatusCode.NotFound
                                    || res.StatusCode == HttpStatusCode.Gone
                                    || res.StatusCode == HttpStatusCode.Forbidden))
                            {
                                Counter.MediaGone.Increment();
                            }
                            else { return; }
                        }
                    }
                }
                //ここまでたどり着いたら画像が全滅ってこと
                RemoveTweetBlock.Post(media.First().source_tweet_id);

            }, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = DownloadConcurrency << 1,
                MaxDegreeOfParallelism = DownloadConcurrency
            });

            //画像転載の疑いがあるツイだけ選ぶやつ
            var CheckTweetBlock = new ActionBlock<(long tweet_id, long[] media_id)>(async (t) =>
            {
                //ハッシュ値が同じで古い奴
                using (var mediacmd = new MySqlCommand(@"SELECT NOT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
WHERE dcthash = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
                //ハッシュ値がちょっと違って古い奴
                using (var mediacmd2 = new MySqlCommand(@"SELECT NOT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
JOIN dcthashpairslim h ON h.hash_large = m.dcthash
WHERE h.hash_small = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
                using (var mediacmd3 = new MySqlCommand(@"SELECT NOT EXISTS(
SELECT * FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet t USING (tweet_id)
JOIN user u USING (user_id)
JOIN dcthashpairslim h ON h.hash_small = m.dcthash
WHERE h.hash_large = (SELECT dcthash FROM media WHERE media_id = @media_id)
AND t.tweet_id < @tweet_id
AND u.isprotected IS FALSE);"))
                {
                    mediacmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = t.tweet_id;
                    mediacmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = t.tweet_id;
                    mediacmd3.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = t.tweet_id;
                    var mediaparam = mediacmd.Parameters.Add("@media_id", MySqlDbType.Int64);
                    var mediaparam2 = mediacmd2.Parameters.Add("@media_id", MySqlDbType.Int64);
                    var mediaparam3 = mediacmd3.Parameters.Add("@media_id", MySqlDbType.Int64);
                    foreach (long mid in t.media_id)
                    {
                        mediaparam.Value = mid;
                        mediaparam2.Value = mid;
                        mediaparam3.Value = mid;
                        //「存在する」時だけ次の画像に進める
                        if (await SelectCount(mediacmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false) != 0
                            && await SelectCount(mediacmd2, IsolationLevel.ReadUncommitted).ConfigureAwait(false) != 0
                            && await SelectCount(mediacmd3, IsolationLevel.ReadUncommitted).ConfigureAwait(false) != 0)
                        { return; }
                    }
                }
                //存在確認をする画像の情報を取得(汚い
                using (var cmd = new MySqlCommand(@"SELECT
m.media_id, mt.media_url, u.screen_name
FROM media m
LEFT JOIN media_downloaded_at md ON m.media_id = md.media_id
JOIN media_text mt ON m.media_id = mt.media_id
JOIN tweet t ON m.source_tweet_Id = t.tweet_id
JOIN user u USING (user_id)
WHERE m.source_tweet_id = @tweet_id;"))
                {
                    cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = t.tweet_id;

                    var medialist = new List<MediaInfo>();
                    await ExecuteReader(cmd, (r) =>
                    {
                        medialist.Add(new MediaInfo()
                        {
                            media_id = r.GetInt64(0),
                            media_url = r.GetString(1),
                            screen_name = r.GetString(2),
                            source_tweet_id = t.tweet_id,
                        });
                    }).ConfigureAwait(false);
                    await TryDownloadBlock.SendAsync(medialist).ConfigureAwait(false);
                }
            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = Environment.ProcessorCount << 1, MaxDegreeOfParallelism = Environment.ProcessorCount });


            using (var cmd = new MySqlCommand(@"SELECT o.tweet_id, t.media_id
FROM tweet o USE INDEX (PRIMARY)
JOIN tweet_media t USING (tweet_id)
WHERE o.retweet_id IS NULL
AND @tweet_id <= o.tweet_id
ORDER BY o.tweet_id
LIMIT 1000;"))
            {
                var tweet_param = cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64);

                //ここから始めるんじゃ(
                long last_tweet_id = SnowFlake.SecondinSnowFlake(Begin, false);
                long exclude_tweet_id = SnowFlake.SecondinSnowFlake(Exclude, false);
                tweet_param.Value = last_tweet_id;

                var media_id = new List<long>();
                List<(long t, long m)> Table = new List<(long t, long m)>();
                do
                {
                    Table.Clear();
                    await ExecuteReader(cmd, (r) => Table.Add((r.GetInt64(0), r.GetInt64(1))), IsolationLevel.ReadUncommitted).ConfigureAwait(false);
                    foreach (var t in Table)
                    {
                        long tweet_id = t.t;
                        if (last_tweet_id < tweet_id)
                        {
                            if (exclude_tweet_id < tweet_id) { break; }
                            if (media_id.Count > 0)
                            {
                                await CheckTweetBlock.SendAsync((last_tweet_id, media_id.ToArray())).ConfigureAwait(false);
                                media_id.Clear();
                            }
                            last_tweet_id = tweet_id;
                            Counter.LastTweetID = tweet_id;
                        }
                        else if (tweet_id < last_tweet_id) { throw new InvalidOperationException(); }
                        media_id.Add(t.m);
                    }
                    if (Table.Count > 0) { tweet_param.Value = Table.Last().t; }
                } while (Table.Count > 0 && last_tweet_id < exclude_tweet_id);

                CheckTweetBlock.Complete();
                await CheckTweetBlock.Completion.ConfigureAwait(false);
                TryDownloadBlock.Complete();
                await TryDownloadBlock.Completion.ConfigureAwait(false);
                RemoveTweetBlock.Complete();
                await RemoveTweetBlock.Completion.ConfigureAwait(false);
            }
        }

        public struct MediaInfo
        {
            public long media_id { get; set; }
            public long source_tweet_id { get; set; }
            public string screen_name { get; set; }
            public string media_url { get; set; }
            public string tweet_url { get { return "https://twitter.com/" + screen_name + "/status/" + source_tweet_id.ToString(); } }
        }
    }

    static class Counter
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

        public static long LastTweetID;

        //structだからreadonlyにすると更新されなくなるよ
        public static CounterValue MediaGone = new CounterValue();
        public static CounterValue MediaTotal = new CounterValue();
        public static CounterValue TweetDeleted = new CounterValue();
        public static CounterValue TweetToDelete = new CounterValue();
        public static void PrintReset()
        {
            Console.WriteLine("{0} Tweet ID: {1} ({2})", DateTime.Now, LastTweetID, SnowFlake.DatefromSnowFlake(LastTweetID));
            Console.WriteLine("{0} {1} / {2} Media Gone",DateTime.Now, MediaGone.Get(), MediaTotal.Get()); 
            Console.WriteLine("{0} {1} / {2} Tweet Deleted", DateTime.Now, TweetDeleted.Get(), TweetToDelete.Get()); 
        }

        public static void AutoRefresh()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(60000).ConfigureAwait(false);
                    PrintReset();
                }
            });
        }
    }
}
