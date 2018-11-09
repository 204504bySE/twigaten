using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static twimgproxy.DBHandlerView;
using static twimgproxy.twimgStatic;

namespace twimgproxy
{
    public class RemovedMedia
    {
        const int RemoveBatchSize = 16;

        public BatchBlock<long> RemoveTweetQueue { get; } = new BatchBlock<long>(RemoveBatchSize);
        readonly ActionBlock<long[]> RemoveTweetBlock = new ActionBlock<long[]>(async (batch) =>
        {
            foreach (long tweet_id in batch.Distinct())
            {
                //もっと古い公開ツイートがある場合だけ消そうな
                if (await DB.AllHaveOlderMedia(tweet_id).ConfigureAwait(false))
                {
                    Counter.TweetDeleted.Add(await DBCrawl.RemoveDeletedTweet(tweet_id).ConfigureAwait(false));
                }
            }
        }, new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true });

        public RemovedMedia()
        {
            RemoveTweetQueue.LinkTo(RemoveTweetBlock);
        }

        //やっぱいいや
        /*
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
        */
    }
}
