'use strict';
(function () {
  // Set the options to make LazyLoad self-initialize
  window.lazyLoadOptions = {
    //fallback to blurhash when the image is not found
    callback_error: (element) => {
      const url = element.getAttribute('data-src');
      if (url.indexOf('/thumb/') < 0) { return; }
      fetch(url + '/blurhash')
        .then((res) => res.ok
          && res.text()
            .then((hash) => {
              const pixels = decodeBlurHash(hash, 150, 150);
              const imageData = new ImageData(pixels, 150, 150);
              const canvas = document.createElement('canvas');
              canvas.width = 150;
              canvas.height = 150;
              const ctx = canvas.getContext('2d');
              ctx.putImageData(imageData, 0, 0);
              element.parentNode.replaceChild(canvas, element);
            })
        );
    }
  };

  // Listen to the initialization event and get the instance of LazyLoad
  window.addEventListener(
    'LazyLoad::Initialized',
    function (event) {
      const lazyLoadInstance = event.detail.instance;
      if (document.readyState === 'loading') {
        window.addEventListener('DOMContentLoaded', function () {
          lazyLoadInstance.update();
        });
      }
      else { lazyLoadInstance.update(); }
    },
    false
  );

  //convert unixtime
  Array.prototype.forEach.call(document.getElementsByClassName('twigaten-unixtime'), function (x) {
    x.textContent = new Date(x.dataset.unixtime * 1000).toLocaleString();
  });
})();
