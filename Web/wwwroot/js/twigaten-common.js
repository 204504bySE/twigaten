"use strict";
var twigatenCookies = twigatenCookies || {};
(function () {
    const cookieOption = twigatenCookies.cookieOption = { expires: 365, sameSite: 'strict', secure: location.protocol === 'https:' };    
    const UserLikeMode = Cookies.get('UserLikeMode');
    const Order = Cookies.get('Order');
    const Count = Cookies.get('Count');
    const RT = Cookies.get('RT');
    const Show0 = Cookies.get('Show0');
    // 検索設定のCookieを復元する(複数タブで設定を不揃いにしても大丈夫)
    twigatenCookies.set = () => {
        if (UserLikeMode) { Cookies.set('UserLikeMode', UserLikeMode, cookieOption); }
        if (Order) { Cookies.set('Order', Order, cookieOption); }
        if (Count) { Cookies.set('Count', Count, cookieOption); }
        if (RT) { Cookies.set('RT', RT, cookieOption); }
        if (Show0) { Cookies.set('Show0', Show0, cookieOption); }
    };
})();

(function () {
    // navbar初期化
    const screenName = Cookies.get('ScreenName');
    if (screenName) {
        document.getElementById('menu-login').style.display = 'none';
        document.getElementById('menu-screenname').textContent = '@' + screenName;
        const userId = Cookies.get('ID');
        if (userId) {
            //「自分のツイート」
            const menuMyTweet = document.getElementById('menu-mytweet');
            menuMyTweet.setAttribute('href', '/users/' + userId);

            //ドロップダウン
            const menuUser = document.getElementById('menu-user');
            const menuDropDown = document.getElementById('menu-user-dropdown');
            menuUser.addEventListener('click', () => { menuDropDown.classList.toggle('is-block'); });
            menuUser.addEventListener('mouseenter', () => { menuDropDown.classList.add('is-block'); });
            menuUser.addEventListener('mouseleave', () => { menuDropDown.classList.remove('is-block'); });
        }
        else { Cookies.remove('ScreenName'); }
    }
    else { document.getElementById('menu-user').style.display = 'none'; }

    //検索ボックス
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-search"), (x) => {
        x.addEventListener('submit', (event) => {
            event.preventDefault();
            twigatenCookies.set();
            const href = event.currentTarget.getAttribute('action');
            let url = href ? new URL(event.currentTarget.getAttribute('action'), location.href) : location.href;
            url.searchParams.append('date', new Date(event.currentTarget.elements['date'].value) / 1000);
            location.href = url;
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
            Cookies.set(event.currentTarget.dataset.key, event.currentTarget.dataset.value, twigatenCookies.cookieOption);
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
            Cookies.set(event.currentTarget.dataset.key, event.currentTarget.value, twigatenCookies.cookieOption);
            location.reload();
        });
    });
})();