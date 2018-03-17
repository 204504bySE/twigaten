using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net;
using System.Reactive.Linq;
using System.Reactive.Concurrency;
using CoreTweet;
using CoreTweet.Streaming;
using twitenlib;

namespace twidown
{
    class UserStreamer
    {
        // 各TokenのUserstreamをもう使わないんだよなー
        public Tokens Token { get; }
        public bool NeedRestMyTweet { get; set; }   //次のconnect時にRESTでツイートを取得する
        public bool ConnectWaiting { get; set; }    //UserStreamerManager.ConnectBlockに入っているかどうか
        readonly TweetTimeList TweetTime = new TweetTimeList();
        long? LastTweetID;

        //Singleton
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public UserStreamer(Tokens t)
        {
            Token = t;
            Token.ConnectionOptions.DisableKeepAlive = false;
            Token.ConnectionOptions.UseCompression = true;
        }

        //最近受信したツイートの時刻を一定数保持する
        //Userstreamの場合は実際に受信した時刻を使う
        class TweetTimeList
        {
            readonly SortedSet<DateTimeOffset> TweetTime = new SortedSet<DateTimeOffset>();
            static readonly Config config = Config.Instance;
            public void Add(DateTimeOffset Time)
            {
                TweetTime.Add(Time);
                while (TweetTime.Count > config.crawl.StreamSpeedTweets) { TweetTime.Remove(TweetTime.Min); }
            }

            public int Count { get { return TweetTime.Count; } }

            //config.crawl.UserStreamTimeoutTweets個前のツイートの時刻を返すってわけ
            public DateTimeOffset Min { get { return TweetTime.Count > 0 ? TweetTime.Min : DateTimeOffset.Now; } }
            public DateTimeOffset Max { get { return TweetTime.Count > 0 ? TweetTime.Max : DateTimeOffset.Now; } }
        }

        DateTimeOffset? PostponedTime;    //ロックされたアカウントが再試行する時刻
        public void PostponeRetry() { PostponedTime = DateTimeOffset.Now.AddSeconds(config.crawl.LockedTokenPostpone); }
        public void PostponeRetry(int Seconds) { PostponedTime = DateTimeOffset.Now.AddSeconds(Seconds); }
        public bool PostPoned()
        {
            if (PostponedTime == null) { return false; }
            else if (DateTimeOffset.Now > PostponedTime.Value) { return true; }
            else { PostponedTime = null; return false; }
        }

        //tokenの有効性を確認して自身のプロフィールも取得
        //Revokeの可能性があるときだけ呼ぶ
        public enum TokenStatus { Success, Failure, Revoked, Locked, Canceled }
        public TokenStatus VerifyCredentials()
        {
            try
            {
                //Console.WriteLine("{0} {1}: Verifying token", DateTime.Now, Token.UserId);
                db.StoreUserProfile(Token.Account.VerifyCredentialsAsync().Result);
                //Console.WriteLine("{0} {1}: Token verification success", DateTime.Now, Token.UserId);
                return TokenStatus.Success;
            }
            catch (WebException ex)
            {
                if (ex.Status == WebExceptionStatus.ProtocolError
                    && ex.Response is HttpWebResponse wr)
                {
                    if (wr.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                        return TokenStatus.Revoked;
                    }
                    else
                    {
                        Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, ex.Status, ex.Message);
                        return TokenStatus.Failure;
                    }
                }
                else
                {
                    Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, ex.Status, ex.Message);
                    return TokenStatus.Failure;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, ex.Message);
                return TokenStatus.Failure;
            }
        }

        DateTimeOffset? LastRecieveRestTimeline;
        public TokenStatus RecieveRestTimeline()
        {
            //RESTで取得してツイートをDBに突っ込む
            //各ツイートの時刻をTweetTimeに格納
            DateTimeOffset Now = DateTimeOffset.Now;

            //必要なさそうならやらないだけ
            if (LastRecieveRestTimeline.HasValue && 
                (Now - LastRecieveRestTimeline.Value < TimeSpan.FromMinutes(1)
                || (TweetTime.Count > 2 && Now - LastRecieveRestTimeline < TweetTime.Max - TweetTime.Min)))
            { return TokenStatus.Canceled; }
            try
            {
                CoreTweet.Core.ListedResponse<Status> Timeline;
                if (LastTweetID.HasValue) { Timeline = Token.Statuses.HomeTimelineAsync(since_id => LastTweetID, count => 200, tweet_mode => TweetMode.Extended).Result; }
                else { Timeline = Token.Statuses.HomeTimelineAsync(count => 200, tweet_mode => TweetMode.Extended).Result; }

                //Console.WriteLine("{0} {1}: Handling {2} RESTed timeline", DateTime.Now, Token.UserId, Timeline.Count);
                for (int i = 0; i < Timeline.Count; i++)
                {
                    UserStreamerStatic.HandleTweetRest(Timeline[i], Token, true);
                    TweetTime.Add(Timeline[i].CreatedAt);
                }                
                if (Timeline.Count == 0) { TweetTime.Add(Now); }
                //Console.WriteLine("{0} {1}: REST timeline success", DateTime.Now, Token.UserId);
                if (Timeline.Count < 200) { LastTweetID = Timeline.Max().Id; } else { LastTweetID = null; } //取得漏れだ！
                return TokenStatus.Success;
            }
            catch (Exception e)
            {
                if (e is WebException ex
                    && ex.Status == WebExceptionStatus.ProtocolError
                    && ex.Response is HttpWebResponse wr)
                {
                    if (wr.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                        return TokenStatus.Revoked;
                    }
                    else if (wr.StatusCode == HttpStatusCode.Forbidden)
                    {
                        Console.WriteLine("{0} {1}: Locked", DateTime.Now, Token.UserId);
                        return TokenStatus.Locked;
                    }
                    else
                    {
                        Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, ex.Status, ex.Message);
                        return TokenStatus.Failure;
                    }
                }
                else
                {
                    Console.WriteLine("{0} {1}: {2} {3}", DateTime.Now, Token.UserId, e);
                    return TokenStatus.Failure;
                }
            }
        }

        public void RestMyTweet()
        {
            //RESTで取得してツイートをDBに突っ込む
            try
            {
                CoreTweet.Core.ListedResponse<Status> Tweets = Token.Statuses.UserTimelineAsync(user_id => Token.UserId, count => 200, tweet_mode => TweetMode.Extended).Result;

                //Console.WriteLine("{0} {1}: Handling {2} RESTed tweets", DateTime.Now, Token.UserId, Tweets.Count);
                foreach (Status s in Tweets)
                {   //ここでRESTをDBに突っ込む
                    UserStreamerStatic.HandleTweetRest(s, Token, false);
                }
                //Console.WriteLine("{0} {1}: REST tweets success", DateTime.Now, Token.UserId);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST tweets failed: {2}", DateTime.Now, Token.UserId, e.Message);
            }
        }

        public void RestFriends()
        {
            long[] friends = RestCursored(RestCursorMode.Friend);
            if (friends != null)
            {
                db.StoreFriends(friends, Token.UserId);
                //Console.WriteLine("{0} {1}: REST friends success", DateTime.Now, Token.UserId);
            }
            else { Console.WriteLine("{0} {1}: REST friends failed", DateTime.Now, Token.UserId); }
        }

        public void RestBlock()
        {
            long[] blocks = RestCursored(RestCursorMode.Block);
            if (blocks != null)
            {
                db.StoreBlocks(blocks, Token.UserId);
                //Console.WriteLine("{0} {1}: REST blocks success", DateTime.Now, Token.UserId);
            }
            else { Console.WriteLine("{0} {1}: REST blocks failed", DateTime.Now, Token.UserId); }
        }

        enum RestCursorMode { Friend, Block }
        long[] RestCursored(RestCursorMode Mode)
        {
            try
            {
                switch (Mode)
                {
                    case RestCursorMode.Block:
                        return Token.Blocks.IdsAsync(user_id => Token.UserId).Result.ToArray();
                    case RestCursorMode.Friend:
                        return Token.Friends.IdsAsync(user_id => Token.UserId).Result.ToArray();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST {2}s failed: {3}", DateTime.Now, Token.UserId, Mode.ToString(), e.Message);
            }
            return null;
        }
    }
}
