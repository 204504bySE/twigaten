using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static twiview.DBHandlerTwimg;
using static twiview.twimgStatic;

namespace twiview
{
    public class RemovedMedia
    {
        const int RemoveBatchSize = 16;

        public void Enqueue(long tweet_id) { RemoveTweetQueue.Post(tweet_id); }
        readonly BatchBlock<long> RemoveTweetQueue = new BatchBlock<long>(RemoveBatchSize);
        readonly ActionBlock<long[]> RemoveTweetBlock = new ActionBlock<long[]>(async (batch) =>
        {
            foreach (long tweet_id in batch.Distinct())
            {
                Counter.TweetToDelete.Increment();
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
    }
}
