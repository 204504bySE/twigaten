"use strict";
(function () {
    flatpickr('.flatpickr', {
        minDate: new Date(1288834974657),
        maxDate: new Date(),
        enableTime: true,
        time_24hr: true
    });
    Array.prototype.forEach.call(document.getElementsByClassName("twigaten-datepicker-form"), (x) => {
        x.addEventListener('submit', (event) => {
            event.preventDefault();
            twigatenCookies.set();
            const href = event.currentTarget.getAttribute('action');
            let url = href ? new URL(event.currentTarget.getAttribute('action'), location.href) : location.href;
            url.searchParams.append('date', event.currentTarget.elements['date'].value);
            location.href = url;
        });
    });
})();