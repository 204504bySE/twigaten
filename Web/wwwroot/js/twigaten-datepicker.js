"use strict";
(function () {
    flatpickr('.flatpickr', {
        minDate: new Date(1288834974657),
        maxDate: new Date(),
        defaultDate: new Date(),
        enableTime: true,
        time_24hr: true
    });    
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-datepicker"), (x) => {
        x.addEventListener('submit', (event) => {
            event.preventDefault();
            if (event.currentTarget.elements['date'].value) {
                twigatenCookies.set();
                const href = event.currentTarget.getAttribute('action');
                let url = href ? new URL(event.currentTarget.getAttribute('action'), location.href) : location.href;
                url.searchParams.append('date', new Date(event.currentTarget.elements['date'].value) / 1000);
                location.href = url;
            }
        });
    });
})();