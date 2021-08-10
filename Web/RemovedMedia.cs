using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Twigaten.Web
{
    public class RemovedMedia
    {
        const int RemoveBatchSize = 16;
        static readonly DBHandler DB = DBHandler.Instance;

        public void Enqueue(long tweet_id) { RemoveTweetQueue.Post(tweet_id); }
        readonly BatchBlock<long> RemoveTweetQueue = new BatchBlock<long>(RemoveBatchSize);
        readonly ActionBlock<long[]> RemoveTweetBlock = new ActionBlock<long[]>(async (batch) =>
        {
            foreach (long tweet_id in batch.Distinct())
            {
                Counter.TweetToCheckDelete.Increment();
                //もっと古い公開ツイートがある場合だけ消そうな
                if (await DB.AllHaveOlderMedia(tweet_id).ConfigureAwait(false))
                {
                    Counter.TweetToDelete.Increment();
                    if (await DB.RemoveDeletedTweet(tweet_id).ConfigureAwait(false)) { Counter.TweetDeleted.Increment(); }
                }
            }
        }, new ExecutionDataflowBlockOptions() { SingleProducerConstrained = true });

        public RemovedMedia()
        {
            RemoveTweetQueue.LinkTo(RemoveTweetBlock);
        }
    }
}
