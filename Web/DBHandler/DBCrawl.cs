using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace Twigaten.Web
{
    /// <summary>
    /// クローラーのフリを強いられているんだ！
    /// TwimgController絡みの処理でしか使わないはず
    /// </summary>
    public partial class DBHandler : Lib.DBHandler
    {
        /// <summary>
        /// 指定されたツイをDBから消すだけ
        /// </summary>
        /// <param name="tweet_id"></param>
        /// <returns></returns>
        public async Task<bool> RemoveDeletedTweet(long tweet_id)
        {
            using (var cmd = new MySqlCommand(@"DELETE FROM tweet WHERE tweet_id = @tweet_id;"))
            using (var cmd2 = new MySqlCommand(@"DELETE FROM tweet_text WHERE tweet_id = @tweet_id;"))
            {
                cmd.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                cmd2.Parameters.Add("@tweet_id", MySqlDbType.Int64).Value = tweet_id;
                return await ExecuteNonQuery(new[] { cmd, cmd2 }).ConfigureAwait(false) > 0;
            }
        }
        /// <summary>
        /// media_downloaded_atに現在時刻を書き込むだけ
        /// </summary>
        /// <param name="media_id"></param>
        /// <returns></returns>
        public async Task<int> StoreMedia_downloaded_at(long media_id)
        {
            using (var cmd = new MySqlCommand(@"INSERT INTO media_downloaded_at (media_id, downloaded_at) VALUES (@media_id, @downloaded_at)
ON DUPLICATE KEY UPDATE downloaded_at=@downloaded_at;"))
            {
                cmd.Parameters.Add("@media_id", MySqlDbType.Int64).Value = media_id;
                cmd.Parameters.Add("@downloaded_at", MySqlDbType.Int64).Value = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
        /// <summary>
        /// user_updated_atに現在時刻を書き込むだけ
        /// 初期アイコンになってるアカウントに対してやっちゃダメ
        /// </summary>
        /// <param name="user_id"></param>
        /// <returns></returns>
        public async Task<int> StoreUser_updated_at(long user_id)
        {
            using (var cmd = new MySqlCommand(@"INSERT INTO user_updated_at (user_id, updated_at) VALUES (@user_id, @updated_at)
ON DUPLICATE KEY UPDATE updated_at=@updated_at;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                cmd.Parameters.Add("@updated_at", MySqlDbType.Int64).Value = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
    }
}
