'use strict';
(function () {
    //convert unixtime
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-unixtime"), (x) => {
        x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
    });
})();