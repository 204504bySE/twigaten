﻿'use strict';
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
  //LazyLoad(vanilla-lazyload) @license MIT
  //lazyload.iife.min.js
  var LazyLoad = function () { "use strict"; function t() { return (t = Object.assign || function (t) { for (var e = 1; e < arguments.length; e++) { var n = arguments[e]; for (var r in n) Object.prototype.hasOwnProperty.call(n, r) && (t[r] = n[r]) } return t }).apply(this, arguments) } var e = "undefined" != typeof window, n = e && !("onscroll" in window) || "undefined" != typeof navigator && /(gle|ing|ro)bot|crawl|spider/i.test(navigator.userAgent), r = e && "IntersectionObserver" in window, a = e && "classList" in document.createElement("p"), o = { elements_selector: "img", container: n || e ? document : null, threshold: 300, thresholds: null, data_src: "src", data_srcset: "srcset", data_sizes: "sizes", data_bg: "bg", data_poster: "poster", class_loading: "loading", class_loaded: "loaded", class_error: "error", load_delay: 0, auto_unobserve: !0, callback_enter: null, callback_exit: null, callback_reveal: null, callback_loaded: null, callback_error: null, callback_finish: null, use_native: !1 }, s = function (t, e) { var n, r = new t(e); try { n = new CustomEvent("LazyLoad::Initialized", { detail: { instance: r } }) } catch (t) { (n = document.createEvent("CustomEvent")).initCustomEvent("LazyLoad::Initialized", !1, !1, { instance: r }) } window.dispatchEvent(n) }, i = function (t, e) { return t.getAttribute("data-" + e) }, c = function (t, e, n) { var r = "data-" + e; null !== n ? t.setAttribute(r, n) : t.removeAttribute(r) }, l = function (t) { return "true" === i(t, "was-processed") }, u = function (t, e) { return c(t, "ll-timeout", e) }, d = function (t) { return i(t, "ll-timeout") }, f = function (t) { for (var e, n = [], r = 0; e = t.children[r]; r += 1)"SOURCE" === e.tagName && n.push(e); return n }, _ = function (t, e, n) { n && t.setAttribute(e, n) }, v = function (t, e) { _(t, "sizes", i(t, e.data_sizes)), _(t, "srcset", i(t, e.data_srcset)), _(t, "src", i(t, e.data_src)) }, g = { IMG: function (t, e) { var n = t.parentNode; n && "PICTURE" === n.tagName && f(n).forEach((function (t) { v(t, e) })); v(t, e) }, IFRAME: function (t, e) { _(t, "src", i(t, e.data_src)) }, VIDEO: function (t, e) { f(t).forEach((function (t) { _(t, "src", i(t, e.data_src)) })), _(t, "poster", i(t, e.data_poster)), _(t, "src", i(t, e.data_src)), t.load() } }, h = function (t, e) { var n, r, a = e._settings, o = t.tagName, s = g[o]; if (s) return s(t, a), e.loadingCount += 1, void (e._elements = (n = e._elements, r = t, n.filter((function (t) { return t !== r })))); !function (t, e) { var n = i(t, e.data_src), r = i(t, e.data_bg); n && (t.style.backgroundImage = 'url("'.concat(n, '")')), r && (t.style.backgroundImage = r) }(t, a) }, m = function (t, e) { a ? t.classList.add(e) : t.className += (t.className ? " " : "") + e }, b = function (t, e) { a ? t.classList.remove(e) : t.className = t.className.replace(new RegExp("(^|\\s+)" + e + "(\\s+|$)"), " ").replace(/^\s+/, "").replace(/\s+$/, "") }, p = function (t, e, n, r) { t && (void 0 === r ? void 0 === n ? t(e) : t(e, n) : t(e, n, r)) }, E = function (t, e, n) { t.addEventListener(e, n) }, y = function (t, e, n) { t.removeEventListener(e, n) }, w = function (t, e, n) { y(t, "load", e), y(t, "loadeddata", e), y(t, "error", n) }, I = function (t, e, n) { var r = n._settings, a = e ? r.class_loaded : r.class_error, o = e ? r.callback_loaded : r.callback_error, s = t.target; b(s, r.class_loading), m(s, a), p(o, s, n), n.loadingCount -= 1, 0 === n._elements.length && 0 === n.loadingCount && p(r.callback_finish, n) }, k = function (t, e) { var n = function n(a) { I(a, !0, e), w(t, n, r) }, r = function r(a) { I(a, !1, e), w(t, n, r) }; !function (t, e, n) { E(t, "load", e), E(t, "loadeddata", e), E(t, "error", n) }(t, n, r) }, A = ["IMG", "IFRAME", "VIDEO"], L = function (t, e) { var n = e._observer; z(t, e), n && e._settings.auto_unobserve && n.unobserve(t) }, z = function (t, e, n) { var r = e._settings; !n && l(t) || (A.indexOf(t.tagName) > -1 && (k(t, e), m(t, r.class_loading)), h(t, e), function (t) { c(t, "was-processed", "true") }(t), p(r.callback_reveal, t, e)) }, O = function (t) { var e = d(t); e && (clearTimeout(e), u(t, null)) }, N = function (t, e, n) { var r = n._settings; p(r.callback_enter, t, e, n), r.load_delay ? function (t, e) { var n = e._settings.load_delay, r = d(t); r || (r = setTimeout((function () { L(t, e), O(t) }), n), u(t, r)) }(t, n) : L(t, n) }, C = function (t) { return !!r && (t._observer = new IntersectionObserver((function (e) { e.forEach((function (e) { return function (t) { return t.isIntersecting || t.intersectionRatio > 0 }(e) ? N(e.target, e, t) : function (t, e, n) { var r = n._settings; p(r.callback_exit, t, e, n), r.load_delay && O(t) }(e.target, e, t) })) }), { root: (e = t._settings).container === document ? null : e.container, rootMargin: e.thresholds || e.threshold + "px" }), !0); var e }, M = ["IMG", "IFRAME"], R = function (t) { return Array.prototype.slice.call(t) }, x = function (t, e) { return function (t) { return t.filter((function (t) { return !l(t) })) }(R(t || function (t) { return t.container.querySelectorAll(t.elements_selector) }(e))) }, T = function (t) { var e = t._settings, n = e.container.querySelectorAll("." + e.class_error); R(n).forEach((function (t) { b(t, e.class_error), function (t) { c(t, "was-processed", null) }(t) })), t.update() }, F = function (n, r) { var a; this._settings = function (e) { return t({}, o, e) }(n), this.loadingCount = 0, C(this), this.update(r), a = this, e && window.addEventListener("online", (function (t) { T(a) })) }; return F.prototype = { update: function (t) { var e, r = this, a = this._settings; (this._elements = x(t, a), !n && this._observer) ? (function (t) { return t.use_native && "loading" in HTMLImageElement.prototype }(a) && ((e = this)._elements.forEach((function (t) { -1 !== M.indexOf(t.tagName) && (t.setAttribute("loading", "lazy"), z(t, e)) })), this._elements = x(t, a)), this._elements.forEach((function (t) { r._observer.observe(t) }))) : this.loadAll() }, destroy: function () { var t = this; this._observer && (this._elements.forEach((function (e) { t._observer.unobserve(e) })), this._observer = null), this._elements = null, this._settings = null }, load: function (t, e) { z(t, this, e) }, loadAll: function () { var t = this; this._elements.forEach((function (e) { L(e, t) })) } }, e && function (t, e) { if (e) if (e.length) for (var n, r = 0; n = e[r]; r += 1)s(t, n); else s(t, e) }(F, window.lazyLoadOptions), F }();

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
