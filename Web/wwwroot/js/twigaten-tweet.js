'use strict';
(function () {
    //convert unixtime
    Array.prototype.forEach.call(document.getElementsByClassName('twigaten-unixtime'), (x) => {
        x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
    });
    // Before loading lazyload...
    // Set the options to make LazyLoad self-initialize
    window.lazyLoadOptions = { threshold: 0 };
    // Listen to the initialization event and get the instance of LazyLoad
    window.addEventListener(
        'LazyLoad::Initialized',
        function (event) {
            const lazyLoadInstance = event.detail.instance;
            if (document.readyState === 'loading') {
                window.addEventListener('DOMContentLoaded', () => {
                    lazyLoadInstance.update();
                });
            }
            else { lazyLoadInstance.update(); }
        },
        false
    );
})();