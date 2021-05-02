using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CoreTweet;
using MySqlConnector;
using Twigaten.Lib;

namespace Twigaten.Web
{
    public partial class DBHandler : Lib.DBHandler
    {
        /// <summary>
        /// TokenをDBに保存するだけの簡単なお仕事
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async Task<int> InsertNewtoken(Tokens token)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT
INTO token (user_id, token, token_secret) VALUES (@user_id, @token, @token_secret)
ON DUPLICATE KEY UPDATE token=@token, token_secret=@token_secret;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = token.UserId;
                cmd.Parameters.Add("@token", MySqlDbType.VarChar).Value = token.AccessToken;
                cmd.Parameters.Add("@token_secret", MySqlDbType.VarChar).Value = token.AccessTokenSecret;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public enum VerifytokenResult { New, Exist, Modified }
        //revokeされてたものはNewと返す
        public async Task<VerifytokenResult> Verifytoken(Tokens token)
        {
            Tokens dbToken = null;
            using (MySqlCommand cmd = new MySqlCommand("SELECT token, token_secret FROM token WHERE user_id = @user_id;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = token.UserId;
                await ExecuteReader(cmd, (r) => dbToken = new Tokens() { AccessToken = r.GetString(0), AccessTokenSecret = r.GetString(1) }).ConfigureAwait(false);
            }
            if (dbToken == null) { return VerifytokenResult.New; }
            else if (dbToken.AccessToken == token.AccessToken
                && dbToken.AccessTokenSecret == token.AccessTokenSecret)
            { return VerifytokenResult.Exist; }
            else { return VerifytokenResult.Modified; }
        }

        public async Task<int> StoreUserProfile(UserResponse ProfileResponse)
        //ログインユーザー自身のユーザー情報を格納
        //Tokens.Account.VerifyCredentials() の戻り値を投げて使う
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO
user (user_id, name, screen_name, isprotected, profile_image_url, is_default_profile_image, location, description)
VALUES (@user_id, @name, @screen_name, @isprotected, @profile_image_url, @is_default_profile_image, @location, @description)
ON DUPLICATE KEY UPDATE name=@name, screen_name=@screen_name, isprotected=@isprotected, location=@location, description=@description;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = ProfileResponse.Id;
                cmd.Parameters.Add("@name", MySqlDbType.VarChar).Value = ProfileResponse.Name;
                cmd.Parameters.Add("@screen_name", MySqlDbType.VarChar).Value = ProfileResponse.ScreenName;
                cmd.Parameters.Add("@isprotected", MySqlDbType.Byte).Value = ProfileResponse.IsProtected;
                cmd.Parameters.Add("@profile_image_url", MySqlDbType.Text).Value = ProfileResponse.ProfileImageUrlHttps ?? ProfileResponse.ProfileImageUrl;
                cmd.Parameters.Add("@is_default_profile_image", MySqlDbType.Byte).Value = ProfileResponse.IsDefaultProfileImage;
                cmd.Parameters.Add("@location", MySqlDbType.TinyText).Value = ProfileResponse.Location;
                cmd.Parameters.Add("@description", MySqlDbType.Text).Value = ProfileResponse.Description;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }

        public async Task<int> StoreUserLoginToken(long user_id, string logintoken)
        {
            using (MySqlCommand cmd = new MySqlCommand(@"INSERT INTO viewlogin (user_id, logintoken) VALUES (@user_id, @logintoken) ON DUPLICATE KEY UPDATE logintoken=@logintoken;"))
            {
                cmd.Parameters.Add("@user_id", MySqlDbType.Int64).Value = user_id;
                cmd.Parameters.Add("@logintoken", MySqlDbType.VarChar).Value = logintoken;

                return await ExecuteNonQuery(cmd).ConfigureAwait(false);
            }
        }
    }
}
