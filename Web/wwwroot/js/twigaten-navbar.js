"use strict";

//set menubar
const screenName = Cookies.get('ScreenName');
if (screenName) {
    document.getElementById('menu-login').style.display = 'none';
    document.getElementById('menu-screenname').textContent = '@' + screenName;
    const userId = Cookies.get('ID');
    if (userId) {
        document.getElementById('menu-mytweet').setAttribute('href', '/users/' + userId);
        document.getElementById('menu-timeline').setAttribute('href', '/timeline/' + userId);

        //set event for dropdown
        const menuUser = document.getElementById('menu-user');
        const menuDropDown = document.getElementById('menu-user-dropdown');
        menuUser.addEventListener('click', () => { menuDropDown.classList.toggle('is-block'); });
        menuUser.addEventListener('mouseenter', () => { menuDropDown.classList.add('is-block'); });
        menuUser.addEventListener('mouseleave', () => { menuDropDown.classList.remove('is-block'); });
    }
    else { Cookies.remove('ScreenName'); }
}
else { document.getElementById('menu-user').style.display = 'none'; }

//set event of the humberger menu
const menuBurgered =  document.getElementById('menu-burgered');
document.getElementById('menu-burger').addEventListener('click', () => {
    menuBurgered.classList.toggle('is-active');
});
