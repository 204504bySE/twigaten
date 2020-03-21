'use strict';

// 検索設定のCookieを復元するやつ(複数タブで設定を不揃いにしても大丈夫)
var twigatenCookies = twigatenCookies || {};
(function () {
    const cookieOption = twigatenCookies.cookieOption = { expires: 365, sameSite: 'lax', secure: true };
    const UserSearch_LikeMode = Cookies.get('UserSearch_LikeMode');
    const Featured_Order = Cookies.get('Featured_Order');
    const TLUser_Count = Cookies.get('TLUser_Count');
    const TLUser_RT = Cookies.get('TLUser_RT');
    const TLUser_Show0 = Cookies.get('TLUser_Show0');
    //こいつを呼ぶ
    twigatenCookies.set = () => {
        if (UserSearch_LikeMode) { Cookies.set('UserSearch_LikeMode', UserSearch_LikeMode, cookieOption); }
        if (Featured_Order) { Cookies.set('Featured_Order', Featured_Order, cookieOption); }
        if (TLUser_Count) { Cookies.set('TLUser_Count', TLUser_Count, cookieOption); }
        if (TLUser_RT) { Cookies.set('TLUser_RT', TLUser_RT, cookieOption); }
        if (TLUser_Show0) { Cookies.set('TLUser_Show0', TLUser_Show0, cookieOption); }
    };
})();

(function () {
    // navbar初期化
    const screenName = Cookies.get('ScreenName');
    if (screenName) {
        document.getElementById('menu-screenname').textContent = '@' + screenName;
    }
    const userId = Cookies.get('ID');
    if (userId) {
        const menuLogin = document.getElementById('menu-login');
        menuLogin.classList.add('is-hidden');
        menuLogin.classList.remove('is-flex');
        //「自分のツイート」
        const menuMyTweet = document.getElementById('menu-mytweet');
        menuMyTweet.setAttribute('href', '/users/' + userId);

        //ドロップダウン
        const menuUser = document.getElementById('menu-user');
        menuUser.classList.add('is-flex');
        menuUser.classList.remove('is-hidden');

        const menuDropDown = document.getElementById('menu-user-dropdown');
        menuUser.addEventListener('touchstart', () => { menuDropDown.classList.add('is-block'); });
        menuUser.addEventListener('click', () => { menuDropDown.classList.add('is-block'); });
        menuUser.addEventListener('mouseenter', () => { menuDropDown.classList.add('is-block'); });
        menuUser.addEventListener('mouseleave', () => { menuDropDown.classList.remove('is-block'); });
    }
    else {
        Cookies.remove('ScreenName');
    }


    //検索ボックス
    Array.prototype.forEach.call(document.getElementsByClassName('twigaten-search'), (x) => {
        x.addEventListener('submit', (event) => {
            event.preventDefault();
            twigatenCookies.set();

            const queryText = event.currentTarget.elements['q'].value.trim();

            // (?<=twitter\.com\/.+?\/status(es)?\/)\d+
            const statusMatch = new RegExp('(?:twitter\\.com\\/.+?\\/status(?:es)?\\/)(\\d+)').exec(queryText);
            if (statusMatch && 2 <= statusMatch.length) {
                location.href = '/tweet/' + statusMatch[1];
            }
            else {
                // (?<=twitter\.com\/|@|^)[_\w]+
                const screenNameMatch = new RegExp('(?:twitter\\.com\\/|@|^)([_\\w]+)(?=$|\/)').exec(queryText);
                if (screenNameMatch && 2 <= screenNameMatch.length) {
                    event.currentTarget.setAttribute('action', '/search/user');
                }
                else { event.currentTarget.setAttribute('action', '/search/'); }
                event.currentTarget.submit();
            }
        });
    });

    //ハンバーガーメニュー
    const menuBurgered = document.getElementById('menu-burgered');
    document.getElementById('menu-burger').addEventListener('click', () => {
        menuBurgered.classList.toggle('is-active');
    });

    // クリックしたらCookieをセットして移動/リロードするやつ
    // <a>は data-keyとdata-valueなCookieをセット → hrefがあれば移動,なければリロード
    Array.prototype.forEach.call(document.getElementsByClassName('twigaten-cookie-click'), (x) => {
        x.addEventListener('click', (event) => {
            event.preventDefault();
            twigatenCookies.set();
            if (event.currentTarget.dataset.key) {
                Cookies.set(event.currentTarget.dataset.key, event.currentTarget.dataset.value, twigatenCookies.cookieOption);
            }
            const href = event.currentTarget.getAttribute('href');
            if (href) {
                location.href = href;
            }
            else {
                location.reload();
            }
        });
    });
    // <select>は <select data-key="">と<option value="">を参照する
    Array.prototype.forEach.call(document.getElementsByClassName('twigaten-cookie-select'), (x) => {
        x.addEventListener('change', (event) => {
            event.preventDefault();
            twigatenCookies.set();
            if (event.currentTarget.dataset.key) {
                Cookies.set(event.currentTarget.dataset.key, event.currentTarget.value, twigatenCookies.cookieOption);
            }
            location.reload();
        });
    });
})();