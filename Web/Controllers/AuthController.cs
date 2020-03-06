using System;
using System.Collections.Generic;
using System.Linq;
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

        // GET: Auth
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
        //http://nakaji.hatenablog.com/entry/2014/09/19/024341
        [HttpGet("callback")]
        public async Task<ActionResult> TwitterCallback(TwitterCallbackParameters p)
        {
            await p.InitValidate(HttpContext).ConfigureAwait(false);
            try
            {
                Tokens token = p.OAuthSession.GetTokens(p.oauth_verifier);
                // token から AccessToken と AccessTokenSecret を永続化しておくとか、
                // セッション情報に格納しておけば毎回認証しなくて良いかも

                var VeryfyTokenResult = await p.StoreNewLogin(token, HttpContext).ConfigureAwait(false);

                //127.0.0.1だとInvalid Hostnameされる localhostだとおｋ
                if (VeryfyTokenResult == DBToken.VerifytokenResult.Exist) { return Redirect("/users/" + token.UserId.ToString()); }
                else { return Redirect("/"); }
            }
            catch { return Redirect("/"); }
            finally
            {
                //セッションはOAuthにしか使ってないので消す
                HttpContext.Session.Clear();
            }
        }

        [HttpGet("logout")]
        public async Task<ActionResult> Logout(LoginParameters p)
        {
            await p.InitValidate(HttpContext).ConfigureAwait(false);
            p.Logout(true);
            return Redirect("/");
        }
    }
}