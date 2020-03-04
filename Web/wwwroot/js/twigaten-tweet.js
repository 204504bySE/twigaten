// set cookie then navigate for search
Array.prototype.forEach.call(document.getElementsByClassName('twigaten-cookie-href'), function (x) {
    x.addEventListener('click', twigatenCookies.setNavigate);
});
Array.prototype.forEach.call(document.getElementsByClassName('twigaten-cookie-reload'), function (x) {
    x.addEventListener('click', twigatenCookies.setReload);
});

//convert unixtime
Array.prototype.forEach.call(document.getElementsByClassName("twigaten-unixtime"), function (x) {
    x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
});