using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using CoreTweet;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Twigaten.Lib;
using Twigaten.Web.DBHandler;
using Twigaten.Web.Parameters;

namespace Twigaten.Web.Controllers
{
    [Route("auth")]
    public class AuthController : ControllerBase
    {
        static readonly Config config = Config.Instance;

        /// <summary>
        /// 「Twitterでサインイン」を始める
        /// </summary>
        /// <returns></returns>
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [HttpGet("login")]
        public ActionResult Login()
        {
            //"{TwitterApiKey}", "{TwitterApiKeySecret}", "http://mydomain.com:63543/AuthCallback/Twitter"
            var OAuthSession = OAuth.Authorize(config.token.ConsumerKey, config.token.ConsumerSecret, config.web.CallBackUrl);

            // セッション情報にOAuthSessionの内容を保存
            HttpContext.Session.Set(nameof(OAuthSession), JsonSerializer.SerializeToUtf8Bytes(OAuthSession));
            return Redirect(OAuthSession.AuthorizeUri.OriginalString);
        }

        [HttpGet("logout")]
        public async Task<ActionResult> Logout(LoginParameters p)
        {
            await p.InitValidate(HttpContext).ConfigureAwait(false);
            p.Logout(true);
            return Redirect("/");
        }

        public class TwitterCallbackParameters : LoginParameters
        {
            [FromQuery]
            public string oauth_token { get; set; }
            [FromQuery]
            public string oauth_verifier { get; set; }
            /// <summary>
            /// セッションから都度読み込む
            /// 書き込み時はセッションに直接書き込む(JsonSerializer.SerializeToUtf8Bytesを使う)
            /// </summary>
            public OAuth.OAuthSession OAuthSession
                => Context.Session.TryGetValue(nameof(OAuthSession), out var Bytes)
                    ? JsonSerializer.Deserialize<OAuth.OAuthSession>(Bytes)
                    : null;


            //(新規)ログインの処理
            public async Task<DBToken.VerifytokenResult> StoreNewLogin(Tokens Token, HttpContext Context)
            {
                DBToken.VerifytokenResult vt = await DB.DBToken.Verifytoken(Token).ConfigureAwait(false);
                if (vt != DBToken.VerifytokenResult.Exist)
                {
                    if (await DB.DBToken.InsertNewtoken(Token).ConfigureAwait(false) < 1)
                    {
                        throw (new Exception("トークンの保存に失敗しました"));
                    }
                }
                UserResponse SelfUserInfo = Token.Account.VerifyCredentials();
                await DB.DBToken.StoreUserProfile(SelfUserInfo).ConfigureAwait(false);
                ScreenName = SelfUserInfo.ScreenName;
                Context.Session.SetString(nameof(ScreenName), ScreenName);

                var LoginToken = LoginTokenEncrypt.NewToken();
                if (await DB.DBToken.StoreUserLoginToken(Token.UserId, LoginToken.Hash44).ConfigureAwait(false) < 1) { throw new Exception("トークンの保存に失敗しました"); }

                ID = Token.UserId;
                base.LoginToken = LoginToken.Text88;

                SetCookie(nameof(ID), Token.UserId.ToString());
                SetCookie(nameof(LoginParameters.LoginToken), LoginToken.Text88);
                ID = Token.UserId;
                base.LoginToken = LoginToken.Text88;

                return vt;
            }
        }

        /// <summary>
        /// Twitterでアクセスを許可するとこいつにアクセストークンが飛んでくる
        /// </summary>
        /// <param name="p"></param>
        /// <returns></returns>
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [HttpGet("callback")]
        public async Task<ActionResult> TwitterCallback(TwitterCallbackParameters p)
        {
            await p.InitValidate(HttpContext).ConfigureAwait(false);

            // tokenをDBに保存
            Tokens token = p.OAuthSession.GetTokens(p.oauth_verifier);
            var VeryfyTokenResult = await p.StoreNewLogin(token, HttpContext).ConfigureAwait(false);

            //すでにサインインしてたユーザーならそいつのページに飛ばす
            if (VeryfyTokenResult == DBToken.VerifytokenResult.Exist) { return Redirect("/users/" + token.UserId.ToString()); }
            else 
            {
                //新規ユーザーはツイート等を取得させる
                //セッションに認証用の項目を用意して1回しか実行させないようにする
                HttpContext.Session.Set(nameof(FirstProcess), new byte[0]);
                return Redirect("/auth/first"); 
            }
        }

        /// <summary>
        /// 新規ユーザーのツイート等を取得する
        /// 完了後はそいつのページに飛ばす
        /// </summary>
        /// <returns></returns>
        [HttpGet("firstprocess")]
        public async Task<ActionResult> FirstProcess()
        {
            var Params = new LoginParameters();
            await Params.InitValidate(HttpContext).ConfigureAwait(false);
            if (!Params.ID.HasValue) { return Redirect("/"); }

            //セッション内の認証用の項目を確認する
            if (!HttpContext.Session.TryGetValue(nameof(FirstProcess), out var _)){ return Redirect("/"); }
            HttpContext.Session.Remove(nameof(FirstProcess));

            await CrawlManager.Run(Params.ID.Value).ConfigureAwait(false);
            return Redirect("/users/" + Params.ID.ToString());
        }
    }
}