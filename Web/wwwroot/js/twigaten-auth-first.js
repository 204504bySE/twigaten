﻿'use strict';
(function () {
    const animation = () => {
        var frames = document.getElementById('animation-frames').getElementsByTagName('span');
        var frameIndex = 0;
        function nextFrame() {
            frames[frameIndex].classList.add('is-hidden');
            frameIndex++;
            if (frames.length <= frameIndex) { frameIndex = 0; }
            frames[frameIndex].classList.remove('is-hidden');
            setTimeout(nextFrame, 200);
        };
        nextFrame();
        location.href = '/auth/firstprocess';
    };
    if (document.readyState == 'loading') { window.addEventListener('load', animation); }
    else { animation(); }
})();