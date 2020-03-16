using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MySql.Data.MySqlClient;
using Twigaten.Lib;

namespace Twigaten.Web.DBHandler
{
    public class DBView : Lib.DBHandler
    {
        public DBView() : base("view", "", config.database.Address, config.database.Protocol, 11, 40, 600) { }
        public async Task<TweetData._user> SelectUser(long user_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(GetUsersHead + @"WHERE user_id = @user_id"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                var ret = await GetUsers(cmd).ConfigureAwait(false);
                if (0 < ret.Length) { return ret[0]; }
                else { return null; }
            }
        }

        public async Task<string> SelectUserLoginToken(long user_id)
        {
            string LoginToken = null;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT logintoken FROM viewlogin WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteReader(cmd, (r) => LoginToken = r.GetString(0)).ConfigureAwait(false);
            }
            return LoginToken;
        }

        public async Task<int> DeleteUserLoginString(long user_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"DELETE FROM viewlogin WHERE user_id = @user_id"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ユーザー検索のモード選択
        /// </summary>
        public enum SelectUserLikeMode
        { 
            ///<summary>表示可能(フォロー外の鍵垢を除外する)</summary>
            Show,
            ///<summary>フォローしているアカウントのみ(サインインしてない場合は虚無になるのでUIで選択禁止させる)</summary>
            Following,
            ///<summary>全て</summary>
            All
        }
        /// <summary>
        /// screen_nameでアカウントを検索する
        /// </summary>
        /// <param name="Pattern">screen_name(前方一致, 先頭の"@"は付けない)</param>
        /// <param name="login_user_id"></param>
        /// <param name="Mode">検索モード</param>
        /// <param name="Limit">返す最大件数</param>
        /// <returns></returns>
        public async Task<TweetData._user[]> SelectUserLike(string Pattern, long? login_user_id, SelectUserLikeMode Mode, int Limit)
        {
            System.Text.StringBuilder cmdBuilder = new System.Text.StringBuilder(GetUsersHead);
            cmdBuilder.Append(@"WHERE screen_name LIKE @screen_name ");
            switch (Mode)
            {
                case SelectUserLikeMode.Show:
                    cmdBuilder.Append(@"AND (isprotected = 0 OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = user_id))");
                    break;
                case SelectUserLikeMode.Following:
                    cmdBuilder.Append("AND EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = u.user_id)");
                    break;
                default:
                    break;
            }
            cmdBuilder.Append(" LIMIT @limit;");
            using (MySqlCommand cmd = new MySqlCommand(cmdBuilder.ToString()))
            {
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = Pattern;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@limit", MySqlDbType.Int64).Value = Limit;
                return await GetUsers(cmd).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// GetUsers()で使うSQL文の先頭部分
        /// </summary>
        const string GetUsersHead = @"SELECT
user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description FROM user ";
        /// <summary>
        /// Userテーブルを取得してオブジェクトに詰めるだけのやっつけメソッド
        /// </summary>
        /// <param name="cmd">GetUsersHeadで始まるSQL文を含むMySqlCommand</param>
        /// <returns></returns>
        async Task<TweetData._user[]> GetUsers(MySqlCommand cmd)
        {
            var users = new List<TweetData._user>();
            await ExecuteReader(cmd, (r) =>
            {
                var tmpuser = new TweetData._user()
                {
                    user_id = r.GetInt64(0),
                    name = r.GetString(1),
                    screen_name = r.GetString(2),
                    isprotected = r.GetBoolean(3),
                    local_profile_image_url = r.GetString(4),
                    is_default_profile_image = r.GetBoolean(5),
                    location = r.IsDBNull(6) ? null : r.GetString(6),
                    description_html = r.IsDBNull(7) ? null : LocalText.TextToLink(r.GetString(7))
                };
                tmpuser.local_profile_image_url = LocalText.ProfileImageUrl(tmpuser, r.GetBoolean(5));
                users.Add(tmpuser);
            }).ConfigureAwait(false);
            return users.ToArray();
        }

        /// <summary>
        /// そのscreen_nameのアカウントが1つだけ見つかればそのIDを返す
        /// </summary>
        /// <param name="target_screen_name">検索対象のscreen_name(先頭の"@"はつけない</param>
        public async Task<long?> SelectID_Unique_screen_name(string target_screen_name)
        {
            long? ret = null;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT user_id FROM user WHERE screen_name = @screen_name LIMIT 2;"))
            {
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = target_screen_name;
                await ExecuteReader(cmd, r =>
                {
                    //アカウントが2個出てきた場合もnullにする
                    if (!r.IsDBNull(0)) { ret = ret.HasValue ? null as long? : r.GetInt64(0); }
                }).ConfigureAwait(false);
            }
            return ret;
        }

        /// <summary>
        /// 特定のハッシュ値の画像を含むツイートのうち、表示可能かつ最も古いやつの(tweet_id, media_id)
        /// </summary>
        /// <param name="dcthash"></param>
        /// <param name="login_user_id"></param>
        /// <returns>(tweet_id, media_id) 見つからなければnull</returns>
        public async Task<(long tweet_id, long media_id)?> HashtoTweet(long? dcthash, long? login_user_id)
        {
            if (dcthash == null) { return null; }

            //外側にORDER BYがないので
            //ハッシュ完全一致→距離1→距離2,3くらい の優先度になる(たぶん)
            using (MySqlCommand cmd = new MySqlCommand(@"
(SELECT o.tweet_id, m.media_id
FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE m.dcthash = @dcthash
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM media m
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE m.dcthash IN (@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM dcthashpairslim p 
JOIN media m ON p.hash_large = m.dcthash
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE p.hash_small IN (@dcthash,@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = o.user_id))
ORDER BY o.created_at LIMIT 1
) UNION ALL (
SELECT o.tweet_id, m.media_id
FROM dcthashpairslim p 
JOIN media m ON p.hash_small = m.dcthash
JOIN tweet_media USING (media_id)
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
WHERE p.hash_large IN (@dcthash,@0,@1,@2,@3,@4,@5,@6,@7,@8,@9,@10,@11,@12,@13,@14,@15,@16,@17,@18,@19,@20,@21,@22,@23,@24,@25,@26,@27,@28,@29,@30,@31,@32,@33,@34,@35,@36,@37,@38,@39,@40,@41,@42,@43,@44,@45,@46,@47,@48,@49,@50,@51,@52,@53,@54,@55,@56,@57,@58,@59,@60,@61,@62,@63)
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = o.user_id))
ORDER BY o.created_at LIMIT 1
);"))
            {
                cmd.Parameters.Add("@dcthash", MySqlDbType.Int64).Value = dcthash;
                for(int i = 0; i < 64; i++)
                {
                    cmd.Parameters.Add("@" + i.ToString(), MySqlDbType.Int64).Value = dcthash ^ (1L << i);
                }
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;

                (long, long)? ret = null;
                await ExecuteReader(cmd, (r) => ret = (r.GetInt64(0), r.GetInt64(1))).ConfigureAwait(false);
                return ret;
            }
        }

        /// <summary>
        /// そのツイートが存在するかどうか
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <returns></returns>
        public async Task<bool> ExistTweet(long tweet_id)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(tweet_id) FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return await SelectCount(cmd).ConfigureAwait(false) >= 1;
            }
        }

        /// <summary>
        /// tweet_idのツイートがRTだったら元ツイートのIDを返す
        /// RTじゃない/そもそも存在しない ときはnull
        /// </summary>
        public async Task<long?> SourceTweetRT(long tweet_id)
        {
            using(MySqlCommand cmd = new MySqlCommand(@"SELECT retweet_id FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                long? ret = null;
                await ExecuteReader(cmd, (r) => ret = r.GetInt64(0)).ConfigureAwait(false);
                return ret;
            }
        }

        /// <summary>
        /// 特定のツイートの各画像とその類似画像
        /// 鍵かつフォロー外なら何も出ない
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <param name="login_user_id"></param>
        /// <param name="SimilarLimit">類似画像の最大件数(古い順)</param>
        /// <returns></returns>

        public async Task<SimilarMediaTweet[]> SimilarMediaTweet(long tweet_id, long? login_user_id, int SimilarLimit = 3)
        {
            using (MySqlCommand cmd = new MySqlCommand(SimilarMediaHeadRT + @"
FROM tweet o
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE o.tweet_id = @tweet_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id));"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                return await TableToTweet(cmd, login_user_id, SimilarLimit, true).ConfigureAwait(false);
            }
        }

        const int MultipleMediaOffset = 3;  //複画は今のところ4枚まで これを同ページに収めたいマン
        /// <summary>
        /// 指定したユーザーのタイムライン上のツイートをずらりと
        /// 鍵かつフォロー外なら何も出ない
        /// </summary>
        /// <param name="target_user_id"></param>
        /// <param name="login_user_id"></param>
        /// <param name="LastTweet"></param>
        /// <param name="TweetCount">最大件数(複画がラストに来ると増える)</param>
        /// <param name="SimilarLimit">類似画像の枚数上限(古い順)</param>
        /// <param name="GetRetweet">RTを含める</param>
        /// <param name="ShowNoDup">類似画像がない</param>
        /// <param name="Before">true→LastTweetより古いツイートを検索する/false→(同文)新しいツイート(同文)</param>
        /// <returns></returns>
        public async Task<SimilarMediaTweet[]> SimilarMediaTimeline(long target_user_id, long? login_user_id, long LastTweet, int TweetCount, int SimilarLimit, bool GetRetweet, bool ShowNoDup, bool Before)
        {
            //鍵垢のTLはフォローしてない限り表示しない
            //未登録のアカウントもここで弾かれる
            TweetData._user TargetUserInfo = await SelectUser(target_user_id).ConfigureAwait(false);
            if (TargetUserInfo != null && TargetUserInfo.isprotected && login_user_id != target_user_id)
            {
                if (login_user_id == null) { return new SimilarMediaTweet[0]; }
                using (MySqlCommand cmd = new MySqlCommand(@"SELECT COUNT(*) FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id"))
                {
                    cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                    cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                    switch (await SelectCount(cmd).ConfigureAwait(false))
                    {
                        case 0:
                            return new SimilarMediaTweet[0];
                        case -1:
                            throw new Exception("SelectCount() failed.");
                        //それ以外は↓の処理を続行する
                    }
                }
            }

            const long QueryRangeSnowFlake = 90 * 1000 * SnowFlake.msinSnowFlake;
            long QuerySnowFlake = LastTweet;
            long NowSnowFlake = SnowFlake.Now(true);
            long NoTweetSnowFlake = 0;
            const long NoTweetLimitSnowFlake = 86400 * 1000 * SnowFlake.msinSnowFlake;
            const int GiveupMilliSeconds = 5000;
            Stopwatch sw = new Stopwatch();
            sw.Start();

            CancellationTokenSource CancelToken = new CancellationTokenSource();
            ExecutionDataflowBlockOptions op = new ExecutionDataflowBlockOptions()
            {
                CancellationToken = CancelToken.Token,
                MaxDegreeOfParallelism = Environment.ProcessorCount,
                SingleProducerConstrained = true
            };
            string QueryText;
            if (GetRetweet)
            {
                QueryText = SimilarMediaHeadRT + @"
FROM friend f 
JOIN user ou ON f.friend_id = ou.user_id
JOIN tweet o USE INDEX (PRIMARY) ON ou.user_id = o.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(rt.tweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE " + (ShowNoDup ? "" : @"(
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
) AND") + @"
o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
AND f.user_id = @target_user_id
AND (@login_user_id = @target_user_id
    OR ou.isprotected = 0 
    OR ou.user_id = @login_user_id
    OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id)
)
AND NOT EXISTS (SELECT * FROM block WHERE user_id = @login_user_id AND target_id = rt.user_id)
AND NOT EXISTS(
    SELECT *
    FROM friend fs
    JOIN user ous ON fs.user_id = ous.user_id
    JOIN tweet os ON ous.user_id = os.user_id
    WHERE os.retweet_id = rt.tweet_id
    AND fs.user_id = @target_user_id
    AND (@login_user_id = @target_user_id
        OR (ous.isprotected = 0
        OR ous.user_id = @login_user_id
        OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ous.user_id))
    )
    AND o.tweet_id < os.tweet_id
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
            }
            else
            {
                QueryText = SimilarMediaHeadnoRT + @"
FROM friend f 
JOIN user ou ON f.friend_id = ou.user_id
JOIN tweet o USE INDEX (PRIMARY) ON ou.user_id = o.user_id
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE " + (ShowNoDup ? "" : @"(
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
) AND") + @"
f.user_id = @target_user_id
AND o.tweet_id BETWEEN " + (Before ? "@time - @timerange AND @time" : "@time AND @time + @timerange") + @"
AND o.retweet_id IS NULL
AND (@login_user_id = @target_user_id
    OR ou.user_id = @login_user_id
    OR ou.isprotected = 0
    OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = ou.user_id)
)
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
            }

            var GetTimelineBlock = new TransformBlock<int, SimilarMediaTweet[]>(
                async (int i) =>
                {
                    using (MySqlCommand cmd = new MySqlCommand(QueryText))
                    {
                        cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                        cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                        cmd.Parameters.Add("@time", MySqlDbType.Int64).Value = (Before ? QuerySnowFlake - QueryRangeSnowFlake * i : QuerySnowFlake + QueryRangeSnowFlake * i);
                        cmd.Parameters.Add("@timerange", MySqlDbType.Int64).Value = QueryRangeSnowFlake;
                        //類似画像が表示できない画像を弾くときだけ多めに取得する
                        cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = ShowNoDup ? TweetCount : TweetCount + MultipleMediaOffset;
                        return await TableToTweet(cmd, login_user_id, SimilarLimit, ShowNoDup).ConfigureAwait(false);
                    }
                }, op);
            int PostedCount = 0;
            for (; PostedCount <= Environment.ProcessorCount; PostedCount++)
            {
                GetTimelineBlock.Post(PostedCount);
            }

            var ret = new List<SimilarMediaTweet>();
            int RecievedCount = 0;
            do
            {
                var Tweets = GetTimelineBlock.Receive();
                RecievedCount++;

                //ツイートがない期間が続いたら打ち切る
                if (Tweets.Length > 0) { NoTweetSnowFlake = 0; } 
                else { NoTweetSnowFlake += QueryRangeSnowFlake; }

                //必要数が取得できたら打ち切る 複画の分だけ多めに取得する
                foreach (var tweet in Tweets)
                {
                    ret.Add(tweet);
                    if (ret.Count >= TweetCount + MultipleMediaOffset) { break; }
                }

                if (Before || QuerySnowFlake + QueryRangeSnowFlake * (PostedCount - 1) < NowSnowFlake)   //未来は取得しない
                {
                    GetTimelineBlock.Post(PostedCount);
                    PostedCount++;
                }
            } while (PostedCount > RecievedCount && (ret.Count < TweetCount)
                && NoTweetSnowFlake < NoTweetLimitSnowFlake
                && sw.ElapsedMilliseconds < GiveupMilliSeconds);
            CancelToken.Cancel();

            if (!Before) { ret.Reverse(); }
            //TableToTweetで類似画像が表示できないやつが削られるので
            //多めに拾ってきて溢れた分を捨てる
            //あと複画は件数超えても同ページに入れる
            for (int i = TweetCount; i < ret.Count; i++)
            {
                if (ret[i].tweet.tweet_id != ret[TweetCount - 1].tweet.tweet_id) { return ret.Take(i - 1).ToArray(); }
            }
            return ret.ToArray();
        }

        /// <summary>
        /// 指定したユーザーのツイートをずらりと
        /// 鍵かつフォロー外なら何も出ない
        /// </summary>
        /// <param name="target_user_id"></param>
        /// <param name="login_user_id"></param>
        /// <param name="LastTweet"></param>
        /// <param name="TweetCount">最大件数(複画がラストに来ると増える)</param>
        /// <param name="SimilarLimit">類似画像の枚数上限(古い順)</param>
        /// <param name="GetRetweet">RTを含める</param>
        /// <param name="ShowNoDup">類似画像がない</param>
        /// <param name="Before">true→LastTweetより古いツイートを検索する/false→(同文)新しいツイート(同文)</param>
        /// <returns></returns>
        public async Task<SimilarMediaTweet[]> SimilarMediaUser(long target_user_id, long? login_user_id, long LastTweet, int TweetCount, int SimilarLimit, bool GetRetweet, bool ShowNoDup, bool Before)
        {
            SimilarMediaTweet[] ret;
            using (MySqlCommand cmd = new MySqlCommand())
            {
                if (GetRetweet)
                {
                    cmd.CommandText = SimilarMediaHeadRT + @"
FROM tweet o USE INDEX (user_id)
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
JOIN user ou ON o.user_id = ou.user_id
LEFT JOIN tweet rt ON o.retweet_id = rt.tweet_id
LEFT JOIN user ru ON rt.user_id = ru.user_id
JOIN tweet_media t ON COALESCE(o.retweet_id, o.tweet_id) = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text rtt ON rt.tweet_id = rtt.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<" : ">") + @" @lasttweet" + (ShowNoDup ? "" : @"
AND (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)") + @"
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
                }
                else
                {
                    cmd.CommandText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (user_id)
JOIN user ou ON o.user_id = ou.user_id
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE ou.user_id = @target_user_id
AND (ou.isprotected = 0 OR ou.user_id = @login_user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @login_user_id AND friend_id = @target_user_id))
AND o.tweet_id " + (Before ? "<" : ">") + @" @lasttweet
AND o.retweet_id IS NULL" + (ShowNoDup ? "" : @"
AND (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)") + @"
ORDER BY o.tweet_id " + (Before ? "DESC" : "ASC") + " LIMIT @limitplus;";
                }
                cmd.Parameters.Add("@target_user_id", MySqlDbType.Int64).Value = target_user_id;
                cmd.Parameters.Add("@login_user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@lasttweet", MySqlDbType.Int64).Value = LastTweet;
                //類似画像が表示できない画像を弾くときだけ多めに取得する
                cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = ShowNoDup ? TweetCount : TweetCount + MultipleMediaOffset;
                var a = cmd.CommandText;
                ret = await TableToTweet(cmd, login_user_id, SimilarLimit, ShowNoDup).ConfigureAwait(false);
            }
            if (!Before) { ret = ret.Reverse().ToArray(); }
            //TableToTweetで類似画像が表示できないやつが削られるので
            //多めに拾ってきて溢れた分を捨てる
            //あと複画は件数超えても同ページに入れる
            for (int i = TweetCount; i < ret.Length; i++)
            {
                if(ret[i].tweet.tweet_id != ret[TweetCount - 1].tweet.tweet_id) { return ret.Take(i - 1).ToArray(); }
            }
            return ret;
        }

        public enum TweetOrder 
        {
            ///<summary>新しい順</summary>
            New, 
            ///<summary>人気順(ふぁぼ+RT数)</summary>
            Featured
        }
        /// <summary>
        /// 「人気のツイート」とかいうようわからん機能を作ってしまいおつらい
        /// </summary>
        /// <param name="SimilarLimit">類似画像の最大取得数</param>
        /// <param name="BeginSnowFlake">ツイートの古い方の限界</param>
        /// <param name="EndSnowFlake">ツイートの新しい方の限界</param>
        /// <param name="Order"></param>
        /// <returns></returns>
        public async Task<SimilarMediaTweet[]> SimilarMediaFeatured(int SimilarLimit, long BeginSnowFlake, long EndSnowFlake, TweetOrder Order)
        {
            int RangeCount = Environment.ProcessorCount;
            long QuerySnowFlake = BeginSnowFlake;
            long QueryRangeSnowFlake = (EndSnowFlake - BeginSnowFlake) / RangeCount;

            string QueryText = SimilarMediaHeadnoRT + @"
FROM tweet o USE INDEX (PRIMARY)
JOIN user ou USING (user_id)
JOIN tweet_media t ON o.tweet_id = t.tweet_id
JOIN media m ON t.media_id = m.media_id
LEFT JOIN tweet_text ot ON o.tweet_id = ot.tweet_id
LEFT JOIN media_text mt ON m.media_id = mt.media_id
WHERE (
    EXISTS (SELECT * FROM media WHERE dcthash = m.dcthash AND media_id != m.media_id)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_small = m.dcthash)
    OR EXISTS (SELECT * FROM dcthashpairslim WHERE hash_large = m.dcthash)
)
AND o.tweet_id BETWEEN @begin AND @end
AND (o.favorite_count >= 250 AND o.retweet_count >= 250)
AND ou.isprotected = 0
ORDER BY (o.favorite_count + o.retweet_count) DESC
LIMIT 50;";

            var QueryBlock = new TransformBlock<int, SimilarMediaTweet[]>(async (i) =>
            {
                using (MySqlCommand cmd = new MySqlCommand(QueryText))
                {
                    cmd.Parameters.Add("@begin", MySqlDbType.Int64).Value = QuerySnowFlake + QueryRangeSnowFlake * i;
                    cmd.Parameters.Add("@end", MySqlDbType.Int64).Value = QuerySnowFlake + QueryRangeSnowFlake * (i + 1) - 1;
                    return await TableToTweet(cmd, null, SimilarLimit).ConfigureAwait(false);
                }
            }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
            for (int i = 0; i < RangeCount; i++)
            {
                QueryBlock.Post(i);
            }
            QueryBlock.Complete();

            var ret = new List<SimilarMediaTweet>();
            for(int i = 0; i < RangeCount; i++)
            {
                ret.AddRange(await QueryBlock.ReceiveAsync().ConfigureAwait(false));
            }

            switch (Order)
            {
                case TweetOrder.New:
                        //ふぁぼ+RT数
                    return ret.OrderByDescending(s => s.tweet.favorite_count + s.tweet.retweet_count)
                        .Take(50)
                        //ツイの時刻
                        .OrderByDescending(s => s.tweet.created_at)
                        .ToArray();
                case TweetOrder.Featured:
                default:
                    //ふぁぼ+RT数
                    return ret.OrderByDescending(s => s.tweet.favorite_count + s.tweet.retweet_count)
                        .Take(50)
                        .ToArray();
            }
        }

        ///<summary>TabletoTweetに渡すリレーションの形式
        ///ou:user o:tweet ot:tweet_text
        ///rt:RT元ツイのtweet ru:RT元ツイのuser
        ///m:media
        /// </summary>
        const string SimilarMediaHeadRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
rt.tweet_id, ru.user_id, ru.name, ru.screen_name, ru.profile_image_url, ru.is_default_profile_image, ru.isprotected,
rt.created_at, rtt.text, rt.favorite_count, rt.retweet_count,
m.media_id, mt.media_url, mt.type,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_large = media.dcthash
        WHERE hash_small = m.dcthash)
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_small = media.dcthash
        WHERE hash_large = m.dcthash)
    + (SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";
        ///<summary>TabletoTweetに渡すリレーションの形式
        ///ou:user o:tweet ot:tweet_text
        ///m:media
        /// </summary>
        const string SimilarMediaHeadnoRT = @"SELECT
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url, ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
m.media_id, mt.media_url, mt.type,
(SELECT COUNT(media_id) FROM media WHERE dcthash = m.dctHash) - 1
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_large = media.dcthash
        WHERE hash_small = m.dcthash)
    + (SELECT COUNT(media_id) FROM dcthashpairslim
        JOIN media ON hash_small = media.dcthash
        WHERE hash_large = m.dcthash)
    + (SELECT COUNT(tweet_id) FROM tweet_media WHERE media_id = m.media_id) - 1";

        /// <summary>
        /// SimilarMediaHead(no)RTで始まるコマンドを処理してオブジェクトに詰め込む
        /// ついでに類似画像も取得してオブジェクトに詰め込む
        /// </summary>
        /// <param name="cmd">SimilarMediaHead(no)RT で始まるSQL文を含むMySqlCommand</param>
        /// <param name="login_user_id"></param>
        /// <param name="SimilarLimit">取得する類似画像の上限</param>
        /// <param name="GetIsolated">「ぼっち画像」を含める</param>
        /// <param name="GetSimilars">与えられたツイに対して類似画像を取得するかどうか</param>
        /// <returns></returns>
        async Task<SimilarMediaTweet[]> TableToTweet(MySqlCommand cmd, long? login_user_id, int SimilarLimit, bool GetIsolated = false, bool GetSimilars = true)
        {
            ActionBlock<SimilarMediaTweet> GetSimilarsBlock = null;
            if (GetSimilars)
            {
                GetSimilarsBlock = new ActionBlock<SimilarMediaTweet>(async (rettmp) =>
                {
                    //この中でTableToTweet()が呼ばれる
                    rettmp.Similars = await SimilarMedia(rettmp.media.media_id, SimilarLimit, (rettmp.tweet.retweet ?? rettmp.tweet).tweet_id, login_user_id).ConfigureAwait(false);
                }, new ExecutionDataflowBlockOptions() { MaxDegreeOfParallelism = Environment.ProcessorCount });
            }

            var TweetList = new List<SimilarMediaTweet>();
            await ExecuteReader(cmd, (r) =>
            {
                SimilarMediaTweet rettmp = new SimilarMediaTweet();
                rettmp.tweet.user.user_id = r.GetInt64(0);
                rettmp.tweet.user.name = r.GetString(1);
                rettmp.tweet.user.screen_name = r.GetString(2);
                rettmp.tweet.user.local_profile_image_url = r.GetString(3);
                rettmp.tweet.user.isprotected =r.GetBoolean(4);
                rettmp.tweet.tweet_id = r.GetInt64(6);
                rettmp.tweet.created_at = DateTimeOffset.FromUnixTimeSeconds(r.GetInt64(7));
                rettmp.tweet.text = r.IsDBNull(8) ? null :r.GetString(8);
                rettmp.tweet.favorite_count = r.GetInt32(9);
                rettmp.tweet.retweet_count = r.GetInt32(10);

                rettmp.tweet.text_html = LocalText.TextToLink(rettmp.tweet.text);
                //アイコンが鯖内にあってもなくてもそれの絶対パスに置き換える
                rettmp.tweet.user.local_profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.user, r.GetBoolean(4));

                if (!r.IsDBNull(11)) //RTなら元ツイートが入っている
                {
                    rettmp.tweet.retweet = new TweetData._tweet();
                    rettmp.tweet.retweet.tweet_id = r.GetInt64(11);
                    rettmp.tweet.retweet.user.user_id = r.GetInt64(12);
                    rettmp.tweet.retweet.user.name = r.GetString(13);
                    rettmp.tweet.retweet.user.screen_name = r.GetString(14);
                    rettmp.tweet.retweet.user.local_profile_image_url = r.GetString(15);
                    rettmp.tweet.retweet.user.isprotected = r.GetBoolean(17);
                    rettmp.tweet.retweet.created_at = DateTimeOffset.FromUnixTimeSeconds(r.GetInt64(18));
                    rettmp.tweet.retweet.text = r.GetString(19);
                    rettmp.tweet.retweet.favorite_count = r.GetInt32(20);
                    rettmp.tweet.retweet.retweet_count = r.GetInt32(21);

                    rettmp.tweet.retweet.text_html = LocalText.TextToLink(rettmp.tweet.retweet.text);
                    //アイコンが鯖内にあってもなくてもそれの絶対パスに置き換える
                    rettmp.tweet.retweet.user.local_profile_image_url = LocalText.ProfileImageUrl(rettmp.tweet.retweet.user, r.GetBoolean(16));
                }
                rettmp.media.media_id = r.GetInt64(22);
                rettmp.media.orig_media_url = r.GetString(23);
                rettmp.media.type = r.GetString(24);
                rettmp.media.local_media_url = LocalText.MediaUrl(rettmp.media);
                rettmp.SimilarMediaCount = r.IsDBNull(25) ? -1 : r.GetInt64(25);    //COUNTはNOT NULLじゃない
                rettmp.ExistsMoreMedia = SimilarLimit < rettmp.SimilarMediaCount;

                TweetList.Add(rettmp);
                if (GetSimilars) { GetSimilarsBlock.Post(rettmp); }
            }).ConfigureAwait(false);

            if (GetSimilars)
            {
                GetSimilarsBlock.Complete();
                await GetSimilarsBlock.Completion.ConfigureAwait(false);

                if (GetIsolated) { return TweetList.ToArray(); }
                else { return TweetList.Where(rettmp => rettmp.Similars.Length > 0).ToArray(); }
            }
            else { return TweetList.ToArray(); }
        }

        /// <summary>
        /// 特定の画像に対する類似画像とそれが含まれるツイートを返す
        /// except_tweet_idを除く
        /// </summary>
        /// <param name="media_id"></param>
        /// <param name="SimilarLimit">類似画像の個数の上限</param>
        /// <param name="except_tweet_id">除外するツイート(自分自身が類似画像として検索されるのを防いでくれ)</param>
        /// <param name="login_user_id"></param>
        /// <returns></returns>

        async Task<SimilarMediaTweet[]> SimilarMedia(long media_id, int SimilarLimit, long except_tweet_id, long? login_user_id = null)
        {
            //先に画像のハッシュ値を取得する #ウンコード
            long media_hash;
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT dcthash FROM media WHERE media_id = @media_id"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                media_hash = await SelectCount(cmd).ConfigureAwait(false);
            }

            //リレーションの形式はSimilarMediaHeadNoRTに準じる
            using (MySqlCommand cmd = new MySqlCommand(@"SELECT 
ou.user_id, ou.name, ou.screen_name, ou.profile_image_url,  ou.is_default_profile_image, ou.isprotected,
o.tweet_id, o.created_at, ot.text, o.favorite_count, o.retweet_count,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
a.media_id, a.media_url, a.type,
NULL
FROM(
    SELECT t.tweet_id, m.media_id, mt.media_url, mt.type
    FROM ((
            SELECT media_id FROM media 
            WHERE dcthash = @media_hash
            ORDER BY media_id LIMIT @limitplus
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_large = media.dcthash
            WHERE p.hash_small = @media_hash
            ORDER BY media.media_id LIMIT @limitplus
        ) UNION ALL (
            SELECT media.media_id FROM media
            JOIN dcthashpairslim p on p.hash_small = media.dcthash
            WHERE p.hash_large = @media_hash
            ORDER BY media.media_id LIMIT @limitplus
        ) ORDER BY media_id LIMIT @limitplus
    ) AS i
    JOIN media m ON i.media_id = m.media_id
    JOIN tweet_media t ON m.media_id = t.media_id
    LEFT JOIN media_text mt ON m.media_id = mt.media_id
ORDER BY t.tweet_id LIMIT @limitplus
) AS a
JOIN tweet o USING (tweet_id)
JOIN user ou USING (user_id)
LEFT JOIN tweet_text ot USING (tweet_id)
WHERE (ou.isprotected = 0 OR ou.user_id = @user_id OR EXISTS (SELECT * FROM friend WHERE user_id = @user_id AND friend_id = o.user_id))
AND o.tweet_id != @except_tweet_id
ORDER BY o.tweet_id
LIMIT @limit"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = login_user_id;
                cmd.Parameters.Add("@media_hash", MySqlDbType.Int64).Value = media_hash;
                cmd.Parameters.Add("@except_tweet_id", MySqlDbType.Int64).Value = except_tweet_id;
                cmd.Parameters.Add("@limit", MySqlDbType.Int64).Value = SimilarLimit;
                cmd.Parameters.Add("@limitplus", MySqlDbType.Int64).Value = SimilarLimit << 2;
                return await TableToTweet(cmd, login_user_id, SimilarLimit, false, false).ConfigureAwait(false);
            }
        }
    }
}