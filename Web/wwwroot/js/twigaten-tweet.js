"use strict";
(function () {
    //convert unixtime
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-unixtime"), function (x) {
        x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
    });
})();