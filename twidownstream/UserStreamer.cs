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

namespace twidownstream
{
    class UserStreamer : IDisposable
    {
        // 各TokenのUserstreamを受信したり仕分けたりする

        public Exception e { get; private set; }
        public bool StreamConnected { get; private set; }
        public Tokens Token { get; }
        public bool NeedRestMyTweet { get; set; }   //次のconnect時にRESTでツイートを取得する
        public bool ConnectWaiting { get; set; }    //UserStreamerManager.ConnectBlockに入っているかどうか
        IDisposable StreamSubscriber;
        DateTimeOffset LastStreamingMessageTime = DateTimeOffset.Now;
        long LastReceivedTweetId;
        readonly TweetTimeList TweetTime = new TweetTimeList();

        //Singleton
        static readonly Config config = Config.Instance;
        static readonly DBHandler db = DBHandler.Instance;

        public UserStreamer(Tokens t)
        {
            Token = t;
            Token.ConnectionOptions.DisableKeepAlive = false;
            Token.ConnectionOptions.UseCompression = true;
            Token.ConnectionOptions.UseCompressionOnStreaming = true;
        }

        public void Dispose()
        {
            DisconnectStream();
            e = null;
            StreamConnected = false;
            GC.SuppressFinalize(this);
        }

        ~UserStreamer()
        {
            Dispose();
        }

        //最近受信したツイートの時刻を一定数保持する
        //Userstreamの場合は実際に受信した時刻を使う
        class TweetTimeList
        {
            readonly SortedSet<DateTimeOffset> TweetTime = new SortedSet<DateTimeOffset>();
            static readonly Config config = Config.Instance;
            int AddCount;
            public void Add(DateTimeOffset Time)
            {
                TweetTime.Add(Time);
                AddCount++;
                RemoveOldAndRefresh();
            }
            public void AddRange(DateTimeOffset[] Time)
            {
                if (Time.Length >= config.crawl.StreamSpeedTweets)
                {
                    TweetTime.Clear();
                    AddCount = 0;
                }
                foreach(DateTimeOffset t in Time)
                {
                    TweetTime.Add(t);
                    AddCount++;
                }
                RemoveOldAndRefresh();
            }

            void RemoveOldAndRefresh()
            {
                //あまり使ってなかったら古い要素を消す
                if (AddCount < config.crawl.StreamSpeedTweets << 4)
                {
                    while (TweetTime.Count > config.crawl.StreamSpeedTweets)
                    {
                        TweetTime.Remove(TweetTime.Min);
                    }
                }
                //たくさん使ってたら作り直す
                else
                {
                    DateTimeOffset[] Temp = TweetTime.TakeLast(config.crawl.StreamSpeedTweets).ToArray();
                    TweetTime.Clear();
                    foreach (DateTimeOffset t in Temp) { TweetTime.Add(t); }
                    AddCount = 0;
                }
            }

            //config.crawl.UserStreamTimeoutTweets個前のツイートの時刻を返すってわけ
            public DateTimeOffset Min
            {
                get
                {
                    if (TweetTime.Count > 0) { return TweetTime.Min; }
                    else { return DateTimeOffset.Now; }
                }
            }
            public DateTimeOffset Max
            {
                get
                {
                    if (TweetTime.Count > 0) { return TweetTime.Max; }
                    else { return DateTimeOffset.Now; }
                }
            }
            public int Count { get { return TweetTime.Count; } }
        }

        DateTimeOffset? PostponedTime;    //ロックされたアカウントが再試行する時刻
        public void PostponeConnect() { PostponedTime = DateTimeOffset.Now.AddSeconds(config.crawl.LockedTokenPostpone); }
        bool IsPostponed()
        {
            if (PostponedTime == null) { return false; }
            else if (DateTimeOffset.Now > PostponedTime.Value) { return true; }
            else { PostponedTime = null; return false; }
        }

        public enum NeedConnectResult
        {
            StreamConnected,         //Stream接続済み→不要(Postponedもこれ)
            JustNeeded,       //必要だけど↓の各処理は不要
            Verify,       //VerifyCredentialsが必要
            RestOnly,    //TLが遅いからRESTだけにして
            Postponed   //ロックされてるから何もしない
        }

        //これを外部から叩いて再接続の必要性を確認
        public NeedConnectResult NeedConnect()
        {
            if (IsPostponed()) { return NeedConnectResult.Postponed; }
            else if (e != null)
            {
                if (e is TwitterException t)
                {
                    if (t.Status == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                        return NeedConnectResult.Verify;
                    }
                    else { return NeedConnectResult.JustNeeded; }
                }
                else if (e is ParsingException) { return NeedConnectResult.JustNeeded; }
                else if (e is WebException ex
                    && ex.Status == WebExceptionStatus.ProtocolError
                    && ex.Response is HttpWebResponse wr
                    && wr.StatusCode == HttpStatusCode.Unauthorized)
                { return NeedConnectResult.Verify; }
                else { return NeedConnectResult.JustNeeded; }
            }
            else if (StreamSubscriber == null)
            {
                //一度もTLを取得してないときはVerifyCredentials()してプロフィールを取得させる
                if (TweetTime.Count == 0) { return NeedConnectResult.Verify; }
                //TLが遅いアカウントはRESTだけでいいや
                else if (NeedStreamSpeed() != NeedStreamResult.Stream) { return NeedConnectResult.RestOnly; }
                else { return NeedConnectResult.JustNeeded; }
            }
            /*
            else if ((DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds
                > Math.Max(config.crawl.UserStreamTimeout, (LastStreamingMessageTime - TweetTime.Min).TotalSeconds))
            {
                //Console.WriteLine("{0} {1}: No streaming message for {2} sec.", DateTime.Now, Token.UserId, (DateTimeOffset.Now - LastStreamingMessageTime).TotalSeconds.ToString("#"));
                return NeedRetryResult.JustNeeded;
            }
            */
            return NeedConnectResult.StreamConnected;
        }
        public enum NeedStreamResult
        {
            Stream,
            Hysteresis,
            RestOnly
        }
        public NeedStreamResult NeedStreamSpeed()
        {
            int TotalSeconds = (int)((TweetTime.Max - TweetTime.Min).TotalSeconds);
            if (TotalSeconds < config.crawl.StreamSpeedSeconds) { return NeedStreamResult.Stream; }
            //ヒステリシスを用意する
            else if (TotalSeconds < config.crawl.StreamSpeedSeconds << 1) { return NeedStreamResult.Hysteresis; }
            else { return NeedStreamResult.RestOnly; }
        }


        //tokenの有効性を確認して自身のプロフィールも取得
        //Revokeの可能性があるときだけ呼ぶ
        public enum TokenStatus { Success, Failure, Revoked, Locked }
        public async ValueTask<TokenStatus> VerifyCredentials()
        {
            try
            {
                //Console.WriteLine("{0} {1}: Verifying token", DateTime.Now, Token.UserId);
                await db.StoreUserProfile(await Token.Account.VerifyCredentialsAsync());
                //Console.WriteLine("{0} {1}: Token verification success", DateTime.Now, Token.UserId);
                return TokenStatus.Success;
            }
            catch (Exception e)
            {
                if (e is TwitterException t)
                {
                    if (t.Status == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: VerifyCredentials Unauthorized", DateTime.Now, Token.UserId);
                        return TokenStatus.Revoked;
                    }
                    else { return TokenStatus.Failure; }
                }
                else if (e is ParsingException) { return TokenStatus.Failure; }
                else if (e is WebException ex
                    && ex.Status == WebExceptionStatus.ProtocolError
                    && ex.Response is HttpWebResponse wr)
                {
                    if (wr.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: VerifyCredentials Unauthorized", DateTime.Now, Token.UserId);
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
                    Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, e);
                    return TokenStatus.Failure;
                }
            }
        }
        
        public void RecieveStream()
        {
            StreamSubscriber?.Dispose(); StreamSubscriber = null;
            e = null;
            LastStreamingMessageTime = DateTimeOffset.Now;
            TweetTime.Add(LastStreamingMessageTime);
            StreamConnected = true;
            StreamSubscriber = Token.Streaming.UserAsObservable()
                //.ObserveOn(Scheduler.Immediate)
                //.SubscribeOn(Scheduler.CurrentThread)
                .Subscribe(
                    async (StreamingMessage m) =>
                    {
                        DateTimeOffset now = DateTimeOffset.Now;
                        LastStreamingMessageTime = now;
                        if (m.Type == MessageType.Create)
                        {
                            LastReceivedTweetId = (m as StatusMessage).Status.Id;
                            TweetTime.Add(now);
                        
                        }
                        await HandleStreamingMessage(m);
                    },
                    (Exception ex) =>
                    {
                        if (ex is TaskCanceledException)
                        {
                            //Console.WriteLine(ex);
                            //Console.WriteLine(ex.InnerException);
                            //Environment.Exit(1);    //つまり自殺
                        }
                        e = ex;
                        DisconnectStream();
                        //Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, ex.Message);
                    },
                    () => { DisconnectStream(); } //接続中のRevokeはこれ
                );
        }

        public void DisconnectStream() { StreamSubscriber?.Dispose(); StreamSubscriber = null; }

        public async ValueTask<TokenStatus> RecieveRestTimelineAuto()
        {
            //TLが遅い分は省略
            if(TweetTime.Count >= 2 && TweetTime.Max - TweetTime.Min > DateTimeOffset.Now - TweetTime.Max) { return TokenStatus.Success; }
            //RESTで取得してツイートをDBに突っ込む
            //各ツイートの時刻をTweetTimeに格納
            try
            {
                CoreTweet.Core.ListedResponse<Status> Timeline;
                if (LastReceivedTweetId != 0) {
                     Timeline = await Token.Statuses.HomeTimelineAsync
                        (count => 200, tweet_mode => TweetMode.Extended, since_id => LastReceivedTweetId);
                }
                else
                {
                    Timeline = await Token.Statuses.HomeTimelineAsync
                        (count => 200, tweet_mode => TweetMode.Extended);
                }

                //Console.WriteLine("{0} {1}: Handling {2} RESTed timeline", DateTime.Now, Token.UserId, Timeline.Count);
                foreach(Status s in Timeline)
                {
                    await UserStreamerStatic.HandleTweetRest(s, Token);
                    if(s.Id > LastReceivedTweetId) { LastReceivedTweetId = s.Id; }
                }
                TweetTime.AddRange(Timeline.Select(s => s.CreatedAt).ToArray());
                if (Timeline.Count == 0) { TweetTime.Add(DateTimeOffset.Now); }
                //Console.WriteLine("{0} {1}: REST timeline success", DateTime.Now, Token.UserId);
                return TokenStatus.Success;
            }
            catch (Exception e)
            {
                if (e is TwitterException t)
                {
                    if (t.Status == HttpStatusCode.Unauthorized)
                    {
                        Console.WriteLine("{0} {1}: Unauthorized", DateTime.Now, Token.UserId);
                        return TokenStatus.Revoked;
                    }
                    else { return TokenStatus.Failure; }
                }
                else if (e is ParsingException) { return TokenStatus.Failure; }
                else if (e is WebException ex
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
                    Console.WriteLine("{0} {1}: {2}", DateTime.Now, Token.UserId, e);
                    return TokenStatus.Failure;
                }
            }
        }

        public async Task RestMyTweet()
        {
            //RESTで取得してツイートをDBに突っ込む
            try
            {
                CoreTweet.Core.ListedResponse<Status> Tweets = await Token.Statuses.UserTimelineAsync(user_id => Token.UserId, count => 200, tweet_mode => TweetMode.Extended);

                //Console.WriteLine("{0} {1}: Handling {2} RESTed tweets", DateTime.Now, Token.UserId, Tweets.Count);
                foreach (Status s in Tweets)
                {   //ここでRESTをDBに突っ込む
                    await UserStreamerStatic.HandleTweetRest(s, Token);
                }
                //Console.WriteLine("{0} {1}: REST tweets success", DateTime.Now, Token.UserId);
            }
            catch (Exception e)
            {
                Console.WriteLine("{0} {1}: REST tweets failed: {2}", DateTime.Now, Token.UserId, e.Message);
            }
        }

        public async ValueTask<int> RestBlock()
        {
            long[] blocks = (await Token.Blocks.IdsAsync(user_id => Token.UserId)).ToArray();
            if (blocks != null) { await db.StoreBlocks(blocks, Token.UserId); }
            return blocks.Length;
        }

        public async ValueTask<int> RestFriend()
        {
            long[] friends = (await Token.Friends.IdsAsync(user_id => Token.UserId)).ToArray();
            if (friends != null) { await db.StoreFriends(friends, Token.UserId); }
            return friends.Length;
        }

        async Task HandleStreamingMessage(StreamingMessage x)
        {
            switch (x.Type)
            {
                case MessageType.Create:
                    await UserStreamerStatic.HandleStatusMessage((x as StatusMessage).Status, Token);
                    break;
                case MessageType.DeleteStatus:
                    await UserStreamerStatic.HandleDeleteMessage(x as DeleteMessage);
                    break;
                case MessageType.Friends:
                    //UserStream接続時に届く(10000フォロー超だと届かない)
                    await db.StoreFriends(x as FriendsMessage, Token.UserId);
                    //Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
                    break;
                case MessageType.Disconnect:
                    //届かないことの方が多い
                    Console.WriteLine("{0} {1}: DisconnectMessage({2})", DateTime.Now, Token.UserId, (x as DisconnectMessage).Code);
                    break;
                case MessageType.Event:
                    await HandleEventMessage(x as EventMessage);
                    break;
                case MessageType.Warning:
                    if ((x as WarningMessage).Code == "FOLLOWS_OVER_LIMIT")
                    {
                        if (await RestFriend() > 0) { Console.WriteLine("{0} {1}: REST friends success", DateTime.Now, Token.UserId); }
                        //Console.WriteLine("{0} {1}: Stream connected", DateTime.Now, Token.UserId);
                    }
                    break;
            }
        }

        async Task HandleEventMessage(EventMessage x)
        {
            switch (x.Event)
            {
                case EventCode.Follow:
                case EventCode.Unfollow:
                case EventCode.Unblock:
                    if (x.Source.Id == Token.UserId) { await db.StoreEvents(x); }
                    break;
                case EventCode.Block:
                    if (x.Source.Id == Token.UserId || x.Target.Id == Token.UserId) { await db.StoreEvents(x); }
                    break;
                case EventCode.UserUpdate:
                    if (x.Source.Id == Token.UserId) { await db.StoreUserProfile(await Token.Account.VerifyCredentialsAsync()); }
                    break;
            }
        }
    }
}
