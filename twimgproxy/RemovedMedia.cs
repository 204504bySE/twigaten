using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static twimgproxy.DBHandler;
using static twimgproxy.twimgStatic;

namespace twimgproxy
{
    public class RemovedMedia
    {
        const int RemoveBatchSize = 128;

        public RemovedMedia()
        {
            RemovedMediaBlock = new ActionBlock<MediaInfo>(async (m) =>
            {
                //ひとまず何もしないようにしておく
                return;
                //一応もう一度ダウンロードして確認するよ
                if (await CheckRemoved(m).ConfigureAwait(false)) { RemoveTweetBatch.Post(m.source_tweet_id); }
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
            RemoveTweetBatch.LinkTo(RemoveTweetBlock);
        }
        public ActionBlock<MediaInfo> RemovedMediaBlock { get; }
        BatchBlock<long> RemoveTweetBatch = new BatchBlock<long>(RemoveBatchSize);
        readonly ActionBlock<long[]> RemoveTweetBlock = new ActionBlock<long[]>(async (m) =>
        {
            foreach (var tweet_id in m.Distinct())
            {
                //もっと古い公開ツイートがある場合だけ消そうな
                return;

                Counter.TweetToDelete.Increment();
                Counter.TweetDeleted.Add(await DB.RemoveDeletedTweet(tweet_id).ConfigureAwait(false));
            }
        }, new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true });

        static async Task<bool> CheckRemoved(MediaInfo m)
        {
            try
            {
                using (var req = new HttpRequestMessage(HttpMethod.Head, m.media_url))
                {
                    req.Headers.Referrer = new Uri(m.tweet_url);
                    using (var res = await Http.SendAsync(req).ConfigureAwait(false))
                    {
                        //凍結は403が返ってくるけど鍵垢と区別できないのでとりあえず放置
                        return res.StatusCode == HttpStatusCode.NotFound || res.StatusCode == HttpStatusCode.Gone;
                    }
                }
            }
            catch { return false; }   //失敗したときはほっとく
        }
    }
}
