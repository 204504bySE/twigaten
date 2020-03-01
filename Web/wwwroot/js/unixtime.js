function unixTimeToLocalTime() {
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-unixtime"), function (x) {
        x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
    })
}
if (document.readyState !== 'loading') {
    unixTimeToLocalTime();
} else {
    document.addEventListener('DOMContentLoaded', unixTimeToLocalTime, false);
}