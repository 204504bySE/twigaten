using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using MySql.Data.MySqlClient;
using System.Data;
using CoreTweet;
using CoreTweet.Streaming;

using twitenlib;
using System.Diagnostics;
using System.Threading;

namespace twidownstream
{
    class DBHandler : twitenlib.DBHandler
    {
        readonly int pid = Process.GetCurrentProcess().Id;

        private DBHandler() : base("crawl", "", config.database.Address, config.database.Protocol, 10, (uint)Config.Instance.crawl.MaxDBConnections) { }
        private static readonly DBHandler _db = new DBHandler();
        //singletonはこれでインスタンスを取得して使う
        public static DBHandler Instance
        {
            get { return _db; }
        }

        int Selfpid = Process.GetCurrentProcess().Id;

        public enum SelectTokenMode
        {
            All,
            CurrentProcess,
            RestInStreamer
        }

        public async Task<Tokens[]> Selecttoken(SelectTokenMode Mode)
        {
            string cmdstr = @"SELECT
user_id, token, token_secret
FROM token
NATURAL JOIN crawlprocess
";
            switch (Mode)
            {
                case SelectTokenMode.CurrentProcess:
                    cmdstr += "WHERE pid = @pid;"; break;
                case SelectTokenMode.RestInStreamer:
                    cmdstr += "WHERE rest_needed IS TRUE;"; break;
                case SelectTokenMode.All:
                    cmdstr += ";"; break;
            }
            using (var cmd = new MySqlCommand(cmdstr))
            {
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = Selfpid;

                var ret = new List<Tokens>();
                if (await ExecuteReader(cmd, (r) =>
                 {
                     ret.Add(Tokens.Create(config.token.ConsumerKey,
                         config.token.ConsumerSecret,
                         r.GetString(1),
                         r.GetString(2),
                         r.GetInt64(0)));
                 }).ConfigureAwait(false)) { return ret.ToArray(); }
                else { return new Tokens[0]; }//一応全取得に成功しない限り返さない
            }

        }

        public async Task<Tokens[]> SelectAlltoken()
        //全tokenを返す
        {
            using (var cmd = new MySqlCommand(@"SELECT
user_id, token, token_secret
FROM token
NATURAL JOIN crawlprocess
WHERE pid = @pid;"))
            {
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value =  Selfpid;

                var ret = new List<Tokens>();
                if (await ExecuteReader(cmd, (r) =>
                {
                    ret.Add(Tokens.Create(config.token.ConsumerKey,
                        config.token.ConsumerSecret,
                        r.GetString(1),
                        r.GetString(2),
                        r.GetInt64(0)));
                }).ConfigureAwait(false)) { return ret.ToArray(); }
                else { return new Tokens[0]; }//一応全取得に成功しない限り返さない
            }
        }
        ///<summary>twidownrestでツイートを取得して欲しいやつ
        ///と思ったが毎日無条件で取得してる上にどこからも使われてない</summary>
        public async Task<int> StoreRestNeedtoken(long user_id)
        {
            using (var cmd = new MySqlCommand(@"UPDATE crawlprocess SET rest_needed = TRUE WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id",MySqlDbType.Int64).Value =  user_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
        ///<summary>こっちは新規登録直後のトークンのREST取得後にちゃんと使ってる</summary>
        public async Task<int> StoreRestDonetoken(long user_id)
        {
            using (var cmd = new MySqlCommand(@"UPDATE crawlprocess SET rest_needed = FALSE WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        ///<summary>無効化されたっぽいTokenを消す</summary>
        public async Task<int> DeleteToken(long user_id)
        {
            using (var cmd = new MySqlCommand(@"DELETE FROM crawlprocess WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
            using (var cmd = new MySqlCommand(@"DELETE FROM token WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        ///<summary>本文の短縮URLを全て展開する</summary>
        static string ExpandUrls(Status x)
        {
            string ret;

            if (x.ExtendedTweet == null) { ret = x.FullText ?? x.Text; } else { ret = x.ExtendedTweet.FullText; }

            foreach (MediaEntity m in x.ExtendedEntities.Media)
            {
                ret = ret.Replace(m.Url, m.ExpandedUrl);
            }
            UrlEntity[] Urls;
            if (x.ExtendedTweet == null) { Urls = x.Entities.Urls; } else { Urls = x.ExtendedTweet.Entities.Urls; }
            foreach (UrlEntity u in Urls)
            {
                ret = ret.Replace(u.Url, u.ExpandedUrl);
            }
            return ret;
        }

        public async Task<int> StoreUserProfile(UserResponse ProfileResponse)
        //ログインユーザー自身のユーザー情報を格納
        //Tokens.Account.VerifyCredentials() の戻り値を投げて使う
        {
            using (var cmd = new MySqlCommand(@"INSERT
INTO user (user_id, name, screen_name, isprotected, location, description) 
VALUES (@user_id, @name, @screen_name, @isprotected, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, location=@location, description=@description;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = ProfileResponse.Id;
                cmd.Parameters.Add("@name", MySqlDbType.VarChar).Value =  ProfileResponse.Name;
                cmd.Parameters.Add("@screen_name",MySqlDbType.VarChar).Value = ProfileResponse.ScreenName;
                cmd.Parameters.Add("@isprotected", MySqlDbType.Byte).Value = ProfileResponse.IsProtected;
                //こっちではアイコンはダウンロードしてないし更新もしない
                cmd.Parameters.Add("@location", MySqlDbType.TinyText).Value = ProfileResponse.Location;
                cmd.Parameters.Add("@description", MySqlDbType.Text).Value = ProfileResponse.Description;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        //アイコンを取得する必要があるかどうか返す
        //  「保存されている」アイコンの元URLとNewProfileImageUrlが一致しない
        //  updated_at IS NULL (アイコンが保存されていない)
        //  そもそもアカウントの情報が保存されていない
        //  stringは古いアイコンのURL(trueの場合のみ)
        //  卵アイコンかどうかは考慮しない(updated_atしか見ない
        public struct ProfileImageInfo
        {
            public bool NeedDownload;
            public bool isDefaultProfileImage;
            public string OldProfileImageUrl;
        }

        public async Task<ProfileImageInfo> NeedtoDownloadProfileImage(long user_id, string NewProfileImageUrl)
        {
            using (var cmd = new MySqlCommand(@"SELECT profile_image_url, updated_at, is_default_profile_image FROM user WHERE user_id = @user_id;"))
            {
                cmd.Parameters.AddWithValue("@user_id", user_id);
                bool HasRow = false;
                string profile_image_url = null;
                long? updated_at = null;
                bool is_default_profile_image = false;
                if (await ExecuteReader(cmd, (r) =>
                {
                    HasRow = true;
                    profile_image_url = r.GetString(0);
                    updated_at = r.IsDBNull(1) ? null as long? : r.GetInt64(1);
                    is_default_profile_image = r.GetBoolean(2);
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false))
                {
                    //そのユーザーの情報がないという状況が謎だがとりあえず取得する
                    if (!HasRow) { return new ProfileImageInfo { NeedDownload = true }; }
                    //画像が変わってるか流されてたら普通に取得
                    else if (!updated_at.HasValue || profile_image_url != NewProfileImageUrl)
                    {
                        return new ProfileImageInfo
                        {
                            NeedDownload = true,
                            OldProfileImageUrl = profile_image_url,
                            isDefaultProfileImage = is_default_profile_image
                        };
                    }
                    //画像は流されてないし変わってもいないので取得不要
                    else { return new ProfileImageInfo { NeedDownload = false }; }
                }
                //DBの処理に失敗したらやめとく
                else { return new ProfileImageInfo { NeedDownload = false }; }
            }
        }

        public async Task<int> StoreUser(Status x, bool IconDownloaded, bool ForceUpdate = true)
        {
            //DBにユーザーを入れる RTは先にやらないとキー制約が
            
            if (x.Entities.Media == null) { return 0; }    //画像なしツイートは捨てる
            using (var cmd = new MySqlCommand())
            {

                if (IconDownloaded) {
                    cmd.CommandText = @"INSERT
INTO user (user_id, name, screen_name, isprotected, profile_image_url, updated_at, is_default_profile_image, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @updated_at, @is_default_profile_image, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, profile_image_url=@profile_image_url, updated_at=@updated_at, is_default_profile_image=@is_default_profile_image, location=@location, description=@description;"; }
                else if (ForceUpdate)
                {
                    //アイコンを取得しなかった時用, insertは未保存アカウントかつアイコン取得失敗時のみ
                    cmd.CommandText = @"INSERT
INTO user (user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @is_default_profile_image, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, 
updated_at=(CASE
WHEN updated_at IS NOT NULL THEN @updated_at
ELSE NULL
END),
location=@location, description=@description;";
                }
                else
                {
                    //RESTで取得した場合はアイコンが変わらない限り何も更新したくない
                    cmd.CommandText = @"INSERT IGNORE
INTO user (user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @is_default_profile_image, @location, @description);";
                }
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = x.User.Id;
                cmd.Parameters.Add("@name", MySqlDbType.VarChar).Value = x.User.Name;
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = x.User.ScreenName;
                cmd.Parameters.Add("@isprotected", MySqlDbType.Byte).Value = x.User.IsProtected;
                cmd.Parameters.Add("@location", MySqlDbType.TinyText).Value = x.User.Location;
                cmd.Parameters.Add("@description", MySqlDbType.Text).Value = x.User.Description;
                //卵アイコンではupdated_atは無意味なのでnullに
                cmd.Parameters.Add("@updated_at", MySqlDbType.Int64).Value = x.User.IsDefaultProfileImage ? null as long? : DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                //↓アイコンを保存したときだけだけ更新される
                cmd.Parameters.Add("@profile_image_url", MySqlDbType.Text).Value = x.User.ProfileImageUrlHttps ?? x.User.ProfileImageUrl;
                cmd.Parameters.Add("@is_default_profile_image", MySqlDbType.Byte).Value = x.User.IsDefaultProfileImage;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        ///<summary>
        ///DBにツイートを入れる 先ににstoreuserしないとキー制約が
        ///もちろんRT元→RTの順で呼ばないとキー制約が
        ///</summary>
        public async Task<int> StoreTweet(Status x, bool update)
        {
            if (x.Entities.Media == null) { return 0; }    //画像なしツイートは捨てる
            using (var cmd = new MySqlCommand())
            {
                if (update) //同じツイートがあったらふぁぼRT数を更新する
                {
                    cmd.CommandText = @"INSERT
INTO tweet (tweet_id, user_id, created_at, text, retweet_id, retweet_count, favorite_count)
VALUES(@tweet_id, @user_id, @created_at, @text, @retweet_id, @retweet_count, @favorite_count)
ON DUPLICATE KEY UPDATE retweet_count=@retweet_count, favorite_count=@favorite_count;";
                }
                else
                {
                    cmd.CommandText = @"INSERT IGNORE
INTO tweet (tweet_id, user_id, created_at, text, retweet_id, retweet_count, favorite_count)
VALUES(@tweet_id, @user_id, @created_at, @text, @retweet_id, @retweet_count, @favorite_count);";
                }

                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = x.Id;
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = x.User.Id;
                cmd.Parameters.Add("@created_at", MySqlDbType.Int64).Value = x.CreatedAt.ToUnixTimeSeconds();
                cmd.Parameters.Add("@text", MySqlDbType.Text).Value = x.RetweetedStatus == null ? ExpandUrls(x) : null;
                cmd.Parameters.Add("@retweet_id", MySqlDbType.Int64).Value = x.RetweetedStatus == null ? null as long? : x.RetweetedStatus.Id;
                cmd.Parameters.Add("@retweet_count", MySqlDbType.Int32).Value = x.RetweetCount;
                cmd.Parameters.Add("@favorite_count", MySqlDbType.Int32).Value = x.FavoriteCount;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
        ///<summary> 消されたツイートをDBから消す 戻り値は削除に失敗したツイート Counterもここで処理する</summary>
        public async Task<List<long>> StoreDelete(long[] DeleteID)
        {
            List<long> ret = new List<long>();
            if (DeleteID == null || DeleteID.Length == 0) { return ret; }
            const int BulkUnit = 100;
            const string head = @"DELETE FROM tweet WHERE tweet_id IN";
            int i = 0, j;
            Counter.TweetToDelete.Add(DeleteID.Length);
            Array.Sort(DeleteID);

            if (DeleteID.Length >= BulkUnit)
            {
                using (var cmd = new MySqlCommand(BulkCmdStrIn(BulkUnit, head)))
                {
                    for (j = 0; j < BulkUnit; j++)
                    {
                        cmd.Parameters.Add("@" + j.ToString(), MySqlDbType.Int64);
                    }                    
                    for (i = 0; i < DeleteID.Length / BulkUnit; i++)
                    {
                        for (j = 0; j < BulkUnit; j++)
                        {
                            cmd.Parameters["@" + j.ToString()].Value = DeleteID[BulkUnit * i + j];
                        }
                        int DeletedCount = await ExecuteNonQuery(cmd).ConfigureAwait(false);
                        if (DeletedCount >= 0) { Counter.TweetDeleted.Add(DeletedCount); }
                        else { foreach (long f in DeleteID.Skip(BulkUnit * i).Take(BulkUnit)) { ret.Add(f); } }
                        
                    }
                }
            }
            if (DeleteID.Length % BulkUnit != 0)
            {
                using (var cmd = new MySqlCommand(BulkCmdStrIn(DeleteID.Length % BulkUnit, head)))
                {
                    for (j = 0; j < DeleteID.Length % BulkUnit; j++)
                    {
                        cmd.Parameters.Add('@' + j.ToString(), MySqlDbType.Int64).Value = DeleteID[BulkUnit * i + j];
                    }
                    int DeletedCount = await ExecuteNonQuery(cmd).ConfigureAwait(false);
                    if (DeletedCount >= 0) { Counter.TweetDeleted.Add(DeletedCount); }
                    else { foreach (long f in DeleteID.Skip(BulkUnit * i)) { ret.Add(f); } }
                }
            }
            return ret;
        }

        public Task<int> StoreFriends(FriendsMessage x, long UserID)
        {
            return StoreFriends(x.Friends, UserID);
        }

        public async Task<int> StoreFriends(long[] x, long UserID)
        //<summary>
        //UserStream接続時のフォローしている一覧を保存する
        //自分自身も入れる
        {
            const int BulkUnit = 1000;
            const string head = @"INSERT IGNORE INTO friend (user_id, friend_id) VALUES";
            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            MySqlCommand cmdtmp;
            int i, j;

            MySqlCommand deletecmd = new MySqlCommand(@"DELETE FROM friend WHERE user_id = @user_id;");
            deletecmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = UserID;
            cmdList.Add(deletecmd);

            MySqlCommand selfcmd = new MySqlCommand(@"INSERT IGNORE INTO friend (user_id, friend_id) VALUES (@user, @user);");
            selfcmd.Parameters.Add("@user", MySqlDbType.Int64).Value = UserID;
            cmdList.Add(selfcmd);

            string BulkInsertCmdFull = "";
            for (i = 0; i < x.Length / BulkUnit; i++)
            {
                if (i == 0) { BulkInsertCmdFull = BulkCmdStr(BulkUnit, 2, head); }
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.Add("@a" + j.ToString(), MySqlDbType.Int64).Value = UserID;
                    cmdtmp.Parameters.Add("@b" + j.ToString(), MySqlDbType.Int64).Value = x[BulkUnit * i + j];
                }
                cmdList.Add(cmdtmp);
            }
            if (x.Length % BulkUnit != 0)
            {
                cmdtmp = new MySqlCommand(BulkCmdStr(x.Length % BulkUnit, 2, head));
                for (j = 0; j < x.Length % BulkUnit; j++)
                {
                    cmdtmp.Parameters.Add("@a" + j.ToString(), MySqlDbType.Int64).Value = UserID;
                    cmdtmp.Parameters.Add("@b" + j.ToString(), MySqlDbType.Int64).Value = x[BulkUnit * i + j];
                }
                cmdList.Add(cmdtmp);
            }
            return await ExecuteNonQuery(cmdList).ConfigureAwait(false);
        }

        public async Task<int> StoreBlocks(long[] x, long UserID)
        //<summary>
        //ブロックしている一覧を保存する
        {
            const int BulkUnit = 1000;
            const string head = @"INSERT IGNORE INTO block (user_id, target_id) VALUES";
            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            MySqlCommand cmdtmp;
            int i, j;

            MySqlCommand deletecmd = new MySqlCommand(@"DELETE FROM block WHERE user_id = @user_id;");
            deletecmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = UserID;
            cmdList.Add(deletecmd);

            string BulkInsertCmdFull = "";
            for (i = 0; i < x.Length / BulkUnit; i++)
            {
                if (i == 0) { BulkInsertCmdFull = BulkCmdStr(BulkUnit, 2, head); }
                cmdtmp = new MySqlCommand(BulkInsertCmdFull);
                for (j = 0; j < BulkUnit; j++)
                {
                    cmdtmp.Parameters.Add("@a" + j.ToString(), MySqlDbType.Int64).Value = UserID;
                    cmdtmp.Parameters.Add("@b" + j.ToString(), MySqlDbType.Int64).Value = x[BulkUnit * i + j];
                }
                cmdList.Add(cmdtmp);
            }
            if (x.Length % BulkUnit != 0)
            {
                cmdtmp = new MySqlCommand(BulkCmdStr(x.Length % BulkUnit, 2, head));
                for (j = 0; j < x.Length % BulkUnit; j++)
                {
                    cmdtmp.Parameters.Add("@a" + j.ToString(), MySqlDbType.Int64).Value = UserID;
                    cmdtmp.Parameters.Add("@b" + j.ToString(), MySqlDbType.Int64).Value = x[BulkUnit * i + j];
                }
                cmdList.Add(cmdtmp);
            }
            return await ExecuteNonQuery(cmdList).ConfigureAwait(false);
        }

        public async Task<bool> ExistTweet(long tweet_id)
        {
            using (var cmd = new MySqlCommand(@"SELECT COUNT(tweet_id) FROM tweet WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return await SelectCount(cmd, IsolationLevel.ReadUncommitted).ConfigureAwait(false) >= 1;
            }
        }

        //true→Mediaにmedia_idが載ってる false→載ってない null→source_tweet_idがない
        public async Task<bool?> ExistMedia_source_tweet_id(long media_id)
        {
            bool HasRow = false;
            long? result = null;
            using (var cmd = new MySqlCommand(@"SELECT source_tweet_id FROM media WHERE media_id = @media_id;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                if(await ExecuteReader(cmd, (r) => 
                {
                    HasRow = true;
                    result = r.IsDBNull(0) ? null as long? : r.GetInt64(0);
                }, IsolationLevel.ReadUncommitted).ConfigureAwait(false))
                {
                    if (!HasRow) { return false; }  //DBが詰まるとあああ #とは
                    else if (!result.HasValue) { return null; }
                    else { return true; }
                }
                else { return false; }
            }
        }

        //source_tweet_idを更新するためだけ
        public async Task<int> UpdateMedia_source_tweet_id(MediaEntity m, Status x)
        {
            using (var cmd = new MySqlCommand(@"UPDATE IGNORE media SET
source_tweet_id = if (EXISTS (SELECT * FROM tweet WHERE tweet_id = @source_tweet_id), @source_tweet_id, source_tweet_id)
WHERE media_id = @media_id;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = m.Id;
                cmd.Parameters.Add("@source_tweet_id", MySqlDbType.Int64).Value = m.SourceStatusId ?? x.Id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async Task<int> StoreMedia(MediaEntity m, Status x, long hash)
        {
            MySqlCommand[] cmd = new MySqlCommand[] { new MySqlCommand(@"INSERT IGNORE 
INTO media (media_id, source_tweet_id, type, media_url, dcthash) 
VALUES(@media_id, @source_tweet_id, @type, @media_url, @dcthash) 
ON DUPLICATE KEY UPDATE
source_tweet_id = if (EXISTS (SELECT * FROM tweet WHERE tweet_id = @source_tweet_id), @source_tweet_id, source_tweet_id),
dcthash = @dcthash;"),
            new MySqlCommand(@"INSERT IGNORE
INTO media_downloaded_at
VALUES(@media_id, @downloaded_at)") };
            cmd[0].Parameters.Add("@media_id", MySqlDbType.Int64).Value = m.Id;
            cmd[0].Parameters.Add("@source_tweet_id", MySqlDbType.Int64).Value = m.SourceStatusId ?? x.Id;
            cmd[0].Parameters.Add("@type", MySqlDbType.VarChar).Value = m.Type;
            cmd[0].Parameters.Add("@media_url", MySqlDbType.Text).Value = m.MediaUrlHttps ?? m.MediaUrl;
            cmd[0].Parameters.Add("@dcthash", MySqlDbType.Int64).Value = hash;

            cmd[1].Parameters.Add("@media_id", MySqlDbType.Int64).Value = m.Id;
            cmd[1].Parameters.Add("@downloaded_at", MySqlDbType.Int64).Value = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            int ret = await ExecuteNonQuery(cmd).ConfigureAwait(false) >> 1;
            return ret + await Storetweet_media(x.Id, m.Id).ConfigureAwait(false);
        }

        public async Task<int> Storetweet_media(long tweet_id, long media_id)
        {
            using (var cmd = new MySqlCommand(@"INSERT IGNORE INTO tweet_media VALUES(@tweet_id, @media_id)"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async Task<int> StoreEvents(EventMessage x)
        {
            //Eventを問答無用にDBに反映する
            //入れる必要があるイベントの仕分けはstreamer側で

            List<MySqlCommand> cmdList = new List<MySqlCommand>();
            switch (x.Event)
            {
                case EventCode.Follow:
                    cmdList.Add(new MySqlCommand(@"INSERT IGNORE INTO friend VALUES (@source, @target);"));
                    break;
                case EventCode.Unfollow:
                    cmdList.Add(new MySqlCommand(@"DELETE FROM friend WHERE user_id = @source AND friend_id = @target;"));
                    break;
                case EventCode.Block:
                    cmdList.Add(new MySqlCommand(@"DELETE IGNORE FROM friend WHERE (user_id = @source AND friend_id = @target) OR (user_id = @target AND friend_id = @source);"));
                    cmdList.Add(new MySqlCommand(@"INSERT IGNORE INTO block VALUES (@source, @target);"));
                    break;
                case EventCode.Unblock:
                    cmdList.Add(new MySqlCommand(@"DELETE FROM block WHERE user_id = @source AND target_id = @target;"));
                    break;
            }
            if (cmdList.Count < 1) { return 0; }
            foreach (MySqlCommand cmd in cmdList)
            {
                cmd.Parameters.Add("@source", MySqlDbType.Int64).Value = x.Source;
                cmd.Parameters.Add("@target", MySqlDbType.Int64).Value = x.Target;
            }
            return await ExecuteNonQuery(cmdList).ConfigureAwait(false);
        }

        static readonly int ThisPid = Process.GetCurrentProcess().Id;
        //MySQLが落ちてpidが消えてたら自殺したい
        public async Task<bool> ExistThisPid()
        {
            using(MySqlCommand cmd = new MySqlCommand("SELECT EXISTS(SELECT * FROM crawlprocess WHERE pid = @pid);"))
            {
                cmd.Parameters.Add("@pid", MySqlDbType.Int32).Value = ThisPid;
                return await SelectCount(cmd).ConfigureAwait(false) != 0;   //DBにアクセスできなかったときは存在することにする
            }
        }
    }
}
