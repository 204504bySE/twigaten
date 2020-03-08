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
            //HttpContext.Session.SetString(nameof(OAuthSession), JsonConvert.SerializeObject(OAuthSession));            
            return Redirect(OAuthSession.AuthorizeUri.OriginalString);
        }

        [HttpGet("logout")]
        public async Task<ActionResult> Logout(LoginParameters p)
        {
            await p.InitValidate(HttpContext).ConfigureAwait(false);
            p.Logout(true);
            return LocalRedirect("/");
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

            public OAuth.OAuthSession OAuthSession()
                => JsonSerializer.Deserialize<OAuth.OAuthSession>(Context.Session.Get(nameof(OAuthSession)));
                //=> JsonConvert.DeserializeObject<OAuth.OAuthSession>(Context.Session.GetString(nameof(OAuthSession)));

            //(新規)ログインの処理
            public async Task<DBToken.VerifytokenResult> StoreNewLogin(Tokens Token)
            {
                DBToken.VerifytokenResult vt = await DB.DBToken.Verifytoken(Token).ConfigureAwait(false);
                if (vt != DBToken.VerifytokenResult.Exist)
                {
                    if (await DB.DBToken.InsertNewtoken(Token).ConfigureAwait(false) < 1)
                    {
                        throw (new Exception("トークンの保存に失敗しました"));
                    }
                }
                var SelfUserInfo = await Token.Account.VerifyCredentialsAsync().ConfigureAwait(false);
                await DB.DBToken.StoreUserProfile(SelfUserInfo).ConfigureAwait(false);

                var NewToken = LoginTokenEncrypt.NewToken();
                if (await DB.DBToken.StoreUserLoginToken(Token.UserId, NewToken.Hash44).ConfigureAwait(false) < 1) { throw new Exception("トークンの保存に失敗しました"); }

                //ここでCookieにも保存する
                ID = Token.UserId;
                LoginToken = NewToken.Text88;
                ScreenName = SelfUserInfo.ScreenName;

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
            Tokens token = p.OAuthSession().GetTokens(p.oauth_verifier);
            var VeryfyTokenResult = await p.StoreNewLogin(token).ConfigureAwait(false);

            //すでにサインインしてたユーザーならそいつのページに飛ばす
            if (VeryfyTokenResult == DBToken.VerifytokenResult.Exist) { return LocalRedirect("/users/" + token.UserId.ToString()); }
            else 
            {
                //新規ユーザーはツイート等を取得させる
                //セッションに認証用の項目を用意して1回しか実行させないようにする
                HttpContext.Session.Set(nameof(FirstProcess), new byte[] { 0 });
                return LocalRedirect("/auth/first"); 
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
            if (!Params.ID.HasValue) { return LocalRedirect("/"); }

            //セッション内の認証用の項目を確認する
            // auth/first.cshtml でもやってる
            if (HttpContext.Session.TryGetValue(nameof(FirstProcess), out var Bytes)
                && 1 <= Bytes.Length && Bytes[0] == 0)
            {
                //なんでもいいので0以外の値を入れる
                HttpContext.Session.Set(nameof(FirstProcess), new byte[] { 255 });

                await CrawlManager.Run(Params.ID.Value).ConfigureAwait(false);
                return LocalRedirect("/users/" + Params.ID.ToString());
            }
            else { return LocalRedirect("/"); }
        }
    }
}