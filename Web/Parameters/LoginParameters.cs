using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Twigaten.Lib;
using static Twigaten.Web.DBHandler.DB;

namespace Twigaten.Web.Parameters
{
    ///<summary>URLやCookieから引数を受け取ったりする
    ///Controllerごとに引数を足したりした派生クラスは各Controllerのファイルでおｋ</summary>
    public class LoginParameters
    {
        /// <summary>これを与えないとCookieなどは読めない#ウンコード</summary>
        protected HttpContext Context { get; private set; }

        long? _ID;
        ///<summary>
        ///サインインしたアカウントのTwitter ID(Cookie)
        ///セッションにも"ID"として書き込む
        ///</summary>
        public long? ID 
        {
            get { return _ID; }
            set 
            {
                _ID = value;
                if (value.HasValue)
                {
                    SetCookie(nameof(ID), value.Value.ToString(), false);
                } 
                else { ClearCookie(nameof(ID)); } 
            }
        }

        string _LoginToken;
        ///<summary>サインイン認証用のランダムな文字列(Cookie)</summary>
        public string LoginToken 
        {
            get { return _LoginToken; }
            set { if (value != null) { SetCookie(nameof(LoginToken), value, true); } else { ClearCookie(nameof(LoginToken)); } }
        }
        ///<summary>アカウント名(Session)</summary>
        public string ScreenName 
        {
            get { return TryGetCookie(nameof(ScreenName), out string ret) ? ret : null; }
            set { if (value != null) { SetCookie(nameof(ScreenName), value, false); } else { ClearCookie(nameof(ScreenName)); } }
        }

        /// <summary>
        /// HttpContextを受け取って各パラメーターを使えるようにする
        /// LoginTokenの検査もここで行う
        /// override先ではTryGetCookieなども使ってCookieを読む
        /// </summary>
        /// <param name="_Context"></param>
        /// <returns></returns>
        public virtual async Task InitValidate(HttpContext _Context)
        {
            Context = _Context;

            //Cookieと由来のパラメーターを読み込む
            _ID = TryGetCookie(nameof(ID), out string IDStr) && long.TryParse(IDStr, out long __ID) ? __ID : null as long?;
            _LoginToken = TryGetCookie(nameof(LoginToken), out string __LoginToken) ? __LoginToken : null;

            //ログイン確認
            if (ID != null)
            {
                if (LoginToken != null && LoginTokenEncrypt.VerifyToken(LoginToken, await DBView.SelectUserLoginToken(ID.Value).ConfigureAwait(false)))
                {
                    //Cookieの有効期限を延長する
                    ID = ID;
                    LoginToken = LoginToken;
                    if (ScreenName == null) { ScreenName = (await DBView.SelectUser(ID.Value).ConfigureAwait(false)).screen_name; }
                }
                else
                {
                    //新しい端末/ブラウザでログインすると他のログインは無効になる
                    Logout();
                }
            }
        }

        /// <summary>
        /// サインアウトに伴いCookieを消す処理
        /// </summary>
        /// <param name="Manually"></param>
        public void Logout(bool Manually = false)
        {
            Context.Session.Clear();
            ID = null;
            ScreenName = null;
            LoginToken = null;

            //overrideでは解決できない #ウンコード
            if (Manually)
            {
                ClearCookie("Featured_Order");
                ClearCookie("TLUser_Count");
                ClearCookie("TLUser_RT");
                ClearCookie("TLUser_Show0");
            }
            //これはログインしてないと「フォローしている」が使えないので消す
            ClearCookie("UserSearch_LikeMode");
        }

        /// <summary>
        /// 開発環境ならhttpsじゃなくてもCookieを使えるようにする
        /// </summary>
        public static bool IsDevelopment { get; set; }

        /// <summary>
        /// Cookieの読み込みをちょっと楽にする
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        protected bool TryGetCookie(string Name, out string Value) { return Context.Request.Cookies.TryGetValue(Name, out Value); }

        /// <summary>
        /// Cookieに所定のオプションを付けて書き込む
        /// </summary>
        /// <param name="Name"></param>
        /// <param name="Value"></param>
        /// <param name="Ephemeral">ブラウザを閉じたら消すやつ</param>
        protected void SetCookie(string Name, string Value, bool HttpOnly)
        {
            Context.Response.Cookies.Append(Name, Value, new CookieOptions()
            {
                HttpOnly = HttpOnly,
                Secure = !IsDevelopment,
                Expires = DateTimeOffset.UtcNow.AddYears(1)
            });
        }

        /// <summary>
        /// Cookieを消すだけ
        /// </summary>
        /// <param name="Name"></param>
        protected void ClearCookie(string Name)
        {
            Context.Response.Cookies.Append(Name, "", new CookieOptions()
            {
                HttpOnly = false,
                Secure = IsDevelopment,
                Expires = DateTimeOffset.FromUnixTimeSeconds(0)
            });
        }
    }

    /// <summary>
    /// logintokenテーブルの認証用文字列を暗号化したりする
    /// </summary>
    static class LoginTokenEncrypt
    {
        static readonly RNGCryptoServiceProvider RNG = new RNGCryptoServiceProvider();
        static readonly SHA256 SHA = SHA256.Create();
        ///<summary>88文字のCookie用文字列と44文字のDB用文字列を生成</summary>
        public static (string Text88, string Hash44) NewToken()
        {
            byte[] random = new byte[64];
            RNG.GetBytes(random);            
            return (Convert.ToBase64String(random), Convert.ToBase64String(SHA.ComputeHash(random)));
        }
        ///<summary>88文字のCookie用文字列と44文字のDB用文字列を照合</summary>
        public static bool VerifyToken(string Text88, string Hash44)
        {
            if(Text88 == null || Hash44 == null) { return false; }
            return SHA.ComputeHash(Convert.FromBase64String(Text88)).SequenceEqual(Convert.FromBase64String(Hash44));
        }
    }
}
