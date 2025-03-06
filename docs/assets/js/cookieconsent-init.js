import "https://cdn.jsdelivr.net/gh/orestbida/cookieconsent@v3.1.0/dist/cookieconsent.umd.js";

CookieConsent.run({
    guiOptions: {
        consentModal: {
            layout: "box inline",
            position: "middle center",
            equalWeightButtons: false,
        },
        preferencesModal: {
            layout: "box",
            position: "middle center",
            equalWeightButtons: false,
            flipButtons: false,
        },
    },
    autoShow: true,
    manageScriptTags: true,
    revision: 1,
    autoClearCookies: true,
    disablePageInteraction: true,
    categories: {
        necessary: {
            enabled: true,
            readOnly: true,
        },
        analytics: {
            enabled: true,
            readOnly: false,
        },
    },
    language: {
        default: "en",
        translations: {
            en: {
                consentModal: {
                    title:
                        '<div style="display:flex;flex-direction:column;align-items:center;"><img style="padding-bottom: 14px;" src="data:image/png;base64,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" /><div>Hello fellow axolotl, it\'s cookie time!</div></div>',
                    description:
                        'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
                    acceptAllBtn: "Accept all",
                    showPreferencesBtn: "Settings",
                    revisionMessage:
                        "<br><br> Dear user, terms and conditions have changed since the last time you visited!",
                },
                preferencesModal: {
                    title: "Cookie settings",
                    acceptAllBtn: "Accept all",
                    acceptNecessaryBtn: "Reject all",
                    savePreferencesBtn: "Save current selection",
                    closeIconLabel: "Close",
                    sections: [
                        {
                            title: "Cookie usage",
                            description:
                                'Our website uses essential cookies to ensure its proper operation and tracking cookies to understand how you interact with it. The latter will be set only after consent. Please see our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a>.',
                        },
                        {
                            title: "Strictly necessary cookies",
                            description:
                                "These cookies are strictly necessary for the website to function. They are usually set to handle only your actions in response to a service request, such as setting your privacy preferences, navigating between pages, and setting your preferred version. You can set your browser to block these cookies or to alert you to their presence, but some parts of the website will not function without them. These cookies do not store any personally identifiable information.",
                            linkedCategory: "necessary",
                        },
                        {
                            title: "Analytics & Performance cookies",
                            description:
                                "These cookies are used for analytics and performance metering purposes. They are used to collect information about how visitors use our website, which helps us improve it over time. They do not collect any information that identifies a visitor. The information collected is aggregated and anonymous.",
                            linkedCategory: "analytics",
                        },
                        {
                            title: "More information",
                            description:
                                'For more information about cookie usage, privacy, and how we use the data we collect, please refer to our <a href="https://lakefs.io/privacy-policy/" target="_blank">privacy policy</a> and <a href="https://lakefs.io/terms-of-use/" target="_blank">terms of use</a>.',
                        },
                    ],
                },
            },
        },
    },
});


var SignalsSDK;
(() => {
    "use strict";
    var e = {};
    (() => {
        e.g = (() => {
            if ("object" == typeof globalThis) return globalThis;
            try {
                return this || Function("return this")()
            } catch (e) {
                if ("object" == typeof window) return window
            }
        })()
    })(), (() => {
        let r;
        var a = "object", s = function () {
            }, l = "undefined" != typeof process ? process : {},
            d = (l.env && l.env.NODE_ENV, "undefined" != typeof document);

        function f(e, r) {
            return r.charAt(0)[e]() + r.slice(1)
        }

        d && window.location.hostname, null != l.versions && l.versions.node, "undefined" != typeof Deno && Deno.core, "object" == typeof self && self.constructor && self.constructor.name, d && "nodejs" === window.name || "undefined" != typeof navigator && void 0 !== navigator.userAgent && (navigator.userAgent.includes("Node.js") || navigator.userAgent.includes("jsdom"));
        var h = f.bind(null, "toUpperCase"), p = f.bind(null, "toLowerCase");

        function v(e, r) {
            void 0 === r && (r = !0);
            var a, s = function (e) {
                return null === e
            }(e) ? h("null") : "object" == typeof e ? g((a = e).constructor) ? a.constructor.name : null : Object.prototype.toString.call(e).slice(8, -1);
            return r ? p(s) : s
        }

        function m(e, r) {
            return typeof r === e
        }

        var g = m.bind(null, "function"), w = m.bind(null, "string"), y = m.bind(null, "undefined");

        function b(e) {
            return null === e
        }

        m.bind(null, "boolean"), m.bind(null, "symbol");

        function I(e, r) {
            if ("object" != typeof r || null === r) return !1;
            if (r instanceof e) return !0;
            var a, s, l = v(new e(""));
            if ((s = r) instanceof Error || w(s.message) && s.constructor && "number" === v(a = s.constructor.stackTraceLimit) && !isNaN(a)) for (; r;) {
                if (v(r) === l) return !0;
                r = Object.getPrototypeOf(r)
            }
            return !1
        }

        function S(e, r) {
            var a, s, l = e instanceof Element || e instanceof HTMLDocument;
            return l && r ? (a = e, void 0 === (s = r) && (s = ""), a && a.nodeName === s.toUpperCase()) : l
        }

        function E(e) {
            var r = [].slice.call(arguments, 1);
            return function () {
                return e.apply(void 0, [].slice.call(arguments).concat(r))
            }
        }

        I.bind(null, TypeError), I.bind(null, SyntaxError), E(S, "form"), E(S, "button"), E(S, "input"), E(S, "select");
        var T = "__global__",
            U = typeof self === a && self.self === self && self || typeof e.g === a && e.g.global === e.g && e.g || void 0;
        U[T] || (U[T] = {});
        var k = function () {
            if (void 0 !== k) return k;
            var e = "cookiecookie";
            try {
                D(e, e), (k = -1 !== document.cookie.indexOf(e)) ? D(e, "", -1) : delete U[T][e]
            } catch (e) {
                k = !1
            }
            return k
        }();

        function D(e, r, a, s, l, d) {
            if ("undefined" != typeof window) {
                var f = arguments.length > 1;
                return !1 === k && (f ? U[T][e] = r : U[T][e]), f ? document.cookie = e + "=" + encodeURIComponent(r) + (a ? "; expires=" + new Date(+new Date + 1e3 * a).toUTCString() + (s ? "; path=" + s : "") + (l ? "; domain=" + l : "") + (d ? "; secure" : "") : "") : decodeURIComponent((("; " + document.cookie).split("; " + e + "=")[1] || "").split(";")[0])
            }
        }

        function A(e) {
            for (var r = [], a = e.length >>> 0; a--;) r[a] = e[a];
            return r
        }

        function _(e, r) {
            return e.reduce(function (e, a) {
                var s = function (e) {
                    if (o(e)) return [e];
                    if (!u(e)) throw Error("Selector must be string");
                    return A(document.querySelectorAll(e))
                }(a);
                if (!s.length) return r && console.log(a + " not found"), e;
                var l = s.filter(function (e) {
                    return !(!e || !n(e, t) && !n(e, i)) || (console.log("Selector passed in not a valid " + t), !1)
                });
                return e.concat(l)
            }, [])
        }

        function F(e) {
            return !o(e) && typeof e === c
        }

        var L = {"[object HTMLCollection]": !0, "[object NodeList]": !0, "[object RadioNodeList]": !0},
            P = {button: !0, fieldset: !0, reset: !0, submit: !0}, j = {checkbox: !0, radio: !0}, O = /^\s+|\s+$/g,
            V = Array.prototype.slice, R = Object.prototype.toString, C = "Invalid form";

        function N(e, r) {
            var a = null, s = e.type;
            if ("select-one" === s) return e.options.length && (a = e.options[e.selectedIndex].value), a;
            if ("select-multiple" === s) {
                a = [];
                for (var l = 0, d = e.options.length; l < d; l++) e.options[l].selected && a.push(e.options[l].value);
                return 0 === a.length && (a = null), a
            }
            return "file" === s && "files" in e ? e.multiple ? 0 === (a = V.call(e.files)).length && (a = null) : a = e.files[0] : j[s] ? e.checked && (a = e.value) : a = r ? e.value.replace(O, "") : e.value, a
        }

        function x(e) {
            return decodeURIComponent(e)
        }

        function z(e) {
            return encodeURIComponent(e)
        }

        function M(e) {
            return "__proto__" === e || "constructor" === e
        }

        var $ = "true", q = "false";

        function H(e) {
            if (!e) return "";
            var r = x(e);
            return "_null" === r ? null : "_true" === r ? $ : "_false" === r ? q : r === $ || r !== q && (0 * +r == 0 ? +r : r)
        }

        var B = /.*/, W = {"#": "hash", "?": "search"},
            Z = /^(https?)?(?:[\:\/]*)([a-z0-9\.-]*)(?:\:(\d+))?(\/[^?#]*)?(?:\?([^#]*))?(?:#(.*))?$/i;

        function K(e) {
            var r, a;
            return y(e) && d ? window.location : (void 0 === (r = e) && (r = ""), {
                href: (a = r.match(Z))[0] || "",
                protocol: a[1] || "",
                hostname: a[2] || "",
                port: a[3] || "",
                pathname: a[4] || "",
                search: a[5] || "",
                hash: a[6] || ""
            })
        }

        function Q(e, r, a, s) {
            var l = J(r || s, e);
            if (!0 === a) return l;
            var d = function (e) {
                void 0 === e && (e = "");
                for (var r, a, s = {}, l = e.split("&"); (r = l.shift()) && !M(a = (r = r.split("=")).shift());) s[a] = void 0 !== s[a] ? [].concat(s[a], H(r.shift())) : H(r.shift());
                return s
            }(l);
            return a ? d[a] : d
        }

        function J(e, r) {
            var a = K(e)[W[r]];
            return a.charAt(0) === r ? a.substring(1) : a
        }

        function G(e, r, a) {
            void 0 === r && (r = B);
            var s = K(a), l = e + J(a, e), f = r instanceof RegExp;
            if (!f && -1 === l.indexOf(r)) return a;
            var h = RegExp("(\\&|\\" + e + ")(" + (f ? r.source : "\\b" + r + "\\b") + ')(=?[_A-Za-z0-9"+=.\\/\\-@%]+)?', "g"),
                p = l.replace(h, "").replace(/^&/, e), v = s.href.replace(l, p);
            if (d && !a && s.href !== v) {
                var m = window.history;
                if (!m && !m.replaceState) return;
                m.replaceState({}, "", v)
            }
            return v
        }

        function X(e, r) {
            var a, s = Object.create(null);
            r = function (e) {
                if (!w(e)) return !1;
                var r, a = e.match(Z);
                return a && (a[2].indexOf(".") > -1 || (void 0 === (r = a[2]) && (r = ""), -1 !== r.indexOf("localhost") || -1 !== r.indexOf("127.0.0.1")))
            }(r) || !r ? J(r, e) : r;
            for (var l = /([^&=]+)=?([^&]*)/g; a = l.exec(r);) {
                var d = x(a[1]), f = x(a[2]);
                "[]" === d.substring(d.length - 2) ? (s[d = d.substring(0, d.length - 2)] || (s[d] = [])).push(f) : s[d] = "" === f || f
            }
            for (var h in s) {
                var p = h.split("[");
                p.length > 1 && (function (e, r, a) {
                    for (var s = r.length - 1, l = 0; l < s; ++l) {
                        var d = r[l];
                        if (M(d)) break;
                        d in e || (e[d] = {}), e = e[d]
                    }
                    e[r[s]] = a
                }(s, p.map(function (e) {
                    return e.replace(/[?[\]\\ ]/g, "")
                }), s[h]), delete s[h])
            }
            return s
        }

        Q.bind(null, "?"), Q.bind(null, "?", null), G.bind(null, "?"), Q.bind(null, "#"), Q.bind(null, "#", null), G.bind(null, "#"), X.bind(null, "?"), X.bind(null, "#");
        let Y = {randomUUID: "undefined" != typeof crypto && crypto.randomUUID && crypto.randomUUID.bind(crypto)},
            ee = new Uint8Array(16), et = [];
        for (let e = 0; e < 256; ++e) et.push((e + 256).toString(16).slice(1));
        let en = function (e, a, s) {
            if (Y.randomUUID && !a && !e) return Y.randomUUID();
            let l = (e = e || {}).random || (e.rng || function () {
                if (!r && !(r = "undefined" != typeof crypto && crypto.getRandomValues && crypto.getRandomValues.bind(crypto))) throw Error("crypto.getRandomValues() not supported. See https://github.com/uuidjs/uuid#getrandomvalues-not-supported");
                return r(ee)
            })();
            if (l[6] = 15 & l[6] | 64, l[8] = 63 & l[8] | 128, a) {
                s = s || 0;
                for (let e = 0; e < 16; ++e) a[s + e] = l[e];
                return a
            }
            return function (e, r = 0) {
                return et[e[r + 0]] + et[e[r + 1]] + et[e[r + 2]] + et[e[r + 3]] + "-" + et[e[r + 4]] + et[e[r + 5]] + "-" + et[e[r + 6]] + et[e[r + 7]] + "-" + et[e[r + 8]] + et[e[r + 9]] + "-" + et[e[r + 10]] + et[e[r + 11]] + et[e[r + 12]] + et[e[r + 13]] + et[e[r + 14]] + et[e[r + 15]]
            }(l)
        };
        var er = {i8: "0.2.10"};

        function ei(e, r) {
            for (let a of r) for (let r of e.keys()) if (r.match(a)) {
                let a = e.get(r);
                if ("string" == typeof a && "" !== a) return a
            }
        }

        function eo(e, r, a) {
            for (let s of a) for (let a of e.querySelectorAll("input, textarea")) for (let e of r) if (a.hasAttribute(e)) {
                let r = a.getAttribute(e);
                if (r?.match(s)) {
                    let e = a.value;
                    if ("string" == typeof e && "" !== e) return e
                }
            }
        }

        let ea = /^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,6}$/,
            es = e => "nodeType" in e && e.nodeType === Node.ELEMENT_NODE;

        function el(e) {
            let r = (es(e) && "ownerDocument" in e ? e.ownerDocument : window.document).defaultView;
            return e instanceof r.HTMLFormElement
        }

        class eu {
            instance;

            constructor(e) {
                this.instance = e;
                let r = (e, r, a) => {
                    e.querySelectorAll(r).forEach(a), new MutationObserver(e => {
                        for (let s of e) for (let e of s.addedNodes) es(e) && (e.matches(r) && a(e), e.querySelectorAll(r).forEach(a))
                    }).observe(e.body, {childList: !0, subtree: !0})
                }, a = e => {
                    (function (e = document) {
                        return new Promise(r => {
                            "complete" === e.readyState || "interactive" === e.readyState ? r() : e.addEventListener("DOMContentLoaded", () => r())
                        })
                    })(e).then(() => r(e, "form", e => e.addEventListener("submit", this.onSubmit.bind(this)))).catch(() => {
                    })
                };
                a(document), r(document, "iframe", e => {
                    let r = e.contentDocument;
                    r && a(r)
                })
            }

            onSubmit(e) {
                if (null != e.target && el(e.target)) {
                    var r;
                    let a = ei(new FormData(r = e.target), [/^email$/i, /email/i]) ?? eo(r, ["autocomplete"], [/^email$/]) ?? eo(r, ["placeholder"], [/email/i]) ?? function (e, r) {
                        for (let a of r) for (let r of e.querySelectorAll("input, textarea")) {
                            let e = r.value;
                            if ("string" == typeof e && e.match(a)) return e
                        }
                    }(r, [ea]), s = function (e) {
                        var r;
                        let a = new FormData(e);
                        return ei(a, [/^name$/i]) ?? ("" !== (r = [eo(e, ["autocomplete"], [/^given-name$/]) ?? ei(a, [/first.*name/i, /fname/i, /given.*name/i]) ?? eo(e, ["placeholder"], [/first.*name/i, /given.*name/i]), eo(e, ["autocomplete"], [/^family-name$/]) ?? ei(a, [/last.*name/i, /lname/i, /surname/i]) ?? eo(e, ["placeholder"], [/last.*name/i, /surname/i])].filter(e => null != e).join(" ")) ? r : null) ?? eo(e, ["autocomplete"], [/^name$/]) ?? ei(a, [/name/i]) ?? eo(e, ["placeholder"], [/name/i])
                    }(e.target);
                    this.instance.form({...null != a ? {email: a} : {}, ...null != s ? {name: s} : {}})
                }
            }
        }

        var ec = "EventListener";

        function ed(e) {
            return function (r, a, l, f) {
                var h = l || s, p = f || !1;
                if (!d) return h;
                var v = ef(a), m = ef(r, !0);
                if (!m.length) throw Error("noElements");
                if (!v.length) throw Error("noEvent");
                var g = [];
                return function e(r) {
                    r && (g = []);
                    for (var a = r ? "add" + ec : "remove" + ec, s = 0; s < m.length; s++) {
                        var l = m[s];
                        g[s] = r ? p && p.once ? function (e, r) {
                            var a;
                            return function () {
                                return e && (a = e.apply(r || this, arguments), e = null), a
                            }
                        }(h) : h : g[s] || h;
                        for (var d = 0; d < v.length; d++) l[a] ? l["on" + v[d]] = r ? g[s] : null : l[a](v[d], g[s], p)
                    }
                    return e.bind(null, !r)
                }(e)
            }
        }

        function ef(e, r) {
            if (w(e)) return r ? ef(document.querySelectorAll(e)) : e.split(e.indexOf(",") > -1 ? "," : " ").map(function (e) {
                return e.trim()
            });
            if (NodeList.prototype.isPrototypeOf(e)) {
                for (var a = [], s = e.length >>> 0; s--;) a[s] = e[s];
                return a
            }
            var l = e ? "array" === v(e) ? e : [e] : [];
            return r ? l.map(function (e) {
                return w(e) ? ef(e, !0) : e
            }).flat() : l
        }

        var eh = ed("Event");

        function ep(e, r) {
            var a, s;
            return d && g(window[e]) ? (a = window[e], void 0 === (s = window) && (s = null), g(a) ? function () {
                a.apply(s, arguments), r.apply(s, arguments)
            } : r) : window[e] = r
        }

        ed(), ep.bind(null, "onerror"), ep.bind(null, "onload");
        var ev = "undefined" == typeof window, em = "hidden",
            eg = ["mousemove", "mousedown", "touchmove", "touchstart", "touchend", "keydown"];

        function ew(e, r) {
            return r ? 0 : Math.round((new Date - e) / 1e3)
        }

        class ey {
            currentUrl;
            instance;
            currentFocusTime;
            aggregateFocusTime;
            activityTracker;
            heartbeatInterval;
            idleTimeout;

            constructor(e, {heartbeatInterval: r, idleTimeout: a = 15} = {}) {
                this.instance = e, this.currentUrl = null, this.currentFocusTime = 0, this.aggregateFocusTime = 0, this.heartbeatInterval = r, this.idleTimeout = a;
                let s = history.pushState;
                history.pushState = (...e) => {
                    s.apply(history, e), this.onPageChange()
                };
                let l = history.replaceState;
                history.replaceState = (...e) => {
                    l.apply(history, e), this.onReplaceState()
                }, window.addEventListener("popstate", () => this.onPageChange()), this.activityTracker = function (e) {
                    var r, a, s = e.onIdle, l = e.onWakeUp, d = e.onHeartbeat, f = e.timeout,
                        h = void 0 === f ? 1e4 : f, p = e.throttle, v = !1, m = !1, g = new Date, w = function () {
                            return clearTimeout(r)
                        };

                    function y(e) {
                        w(), d && !v && d(ew(g), e), l && v && (v = !1, l(ew(a), e), g = new Date), r = setTimeout(function () {
                            v = !0, s && (a = new Date, s(ew(g), e))
                        }, h)
                    }

                    var b = function (e, r) {
                        void 0 === r && (r = {});
                        var a = function (e, r) {
                            var a = this, s = !1;
                            return function (l) {
                                s || (e.call(a, l), s = !0, setTimeout(function () {
                                    return s = !1
                                }, r))
                            }
                        }(e, r.throttle || 1e4), s = [];

                        function l() {
                            return s = [function (e) {
                                if (ev) return !1;
                                var r = function () {
                                    return ev || em in document ? em : ["webkit", "moz", "ms", "o"].reduce(function (e, r) {
                                        var a = r + "Hidden";
                                        return !e && a in document ? a : e
                                    }, null)
                                }(), a = "".concat(r.replace(/[H|h]idden/, ""), "visibilitychange"), s = function () {
                                    return e(!!document[r])
                                }, l = function () {
                                    return document.addEventListener(a, s)
                                };
                                return l(), function () {
                                    return document.removeEventListener(a, s), l
                                }
                            }(function (e) {
                                e || a({type: "tabVisible"})
                            })].concat(eg.map(function (e) {
                                return eh(document, e, a)
                            })).concat(eh(window, "load", a)).concat(eh(window, "scroll", a, {
                                capture: !0,
                                passive: !0
                            })), d
                        }

                        function d() {
                            s.map(function (e) {
                                return e()
                            })
                        }

                        return l(), function () {
                            return d(), l
                        }
                    }(y, {throttle: void 0 === p ? 2e3 : p});
                    return {
                        disable: function () {
                            m = !0, v = !1, w();
                            var e = b();
                            return function () {
                                return m = !1, g = new Date, y({type: "load"}), e()
                            }
                        }, getStatus: function () {
                            return {isIdle: v, isDisabled: m, active: v ? 0 : ew(g, m), idle: v ? ew(a, m) : 0}
                        }
                    }
                }({
                    onIdle: e => {
                        this.onPageFocus(e)
                    },
                    onHeartbeat: null != this.heartbeatInterval ? e => {
                        this.onPageFocus(e)
                    } : void 0,
                    onWakeUp: () => {
                        this.aggregateFocusTime += this.currentFocusTime, this.currentFocusTime = 0
                    },
                    timeout: 1e3 * this.idleTimeout,
                    throttle: null != this.heartbeatInterval ? 1e3 * this.heartbeatInterval : void 0
                }), window.addEventListener("beforeunload", () => {
                    this.onLeavingPageFocus()
                }), this.onPageChange()
            }

            resetActivityTracker() {
                this.aggregateFocusTime = 0, this.currentFocusTime = 0, this.activityTracker.disable()()
            }

            onPageFocus(e) {
                e >= 0 && e <= 525 ? (this.currentFocusTime = e, this.instance.pageFocus(this.aggregateFocusTime + this.currentFocusTime)) : (this.instance.pageFocus(this.aggregateFocusTime), this.resetActivityTracker())
            }

            onLeavingPageFocus() {
                if (this.currentUrl) {
                    let e = this.activityTracker.getStatus();
                    e.isIdle || this.onPageFocus(e.active)
                }
            }

            onPageChange() {
                this.onLeavingPageFocus(), this.currentUrl = new URL(window.location.href), this.instance.page(), this.resetActivityTracker()
            }

            onReplaceState() {
                let e = new URL(window.location.href);
                (null == this.currentUrl || this.currentUrl.hostname !== e.hostname || this.currentUrl.pathname !== e.pathname || this.currentUrl.search !== e.search) && this.onPageChange()
            }
        }

        let eb = {USER_ID: "signals-sdk-user-id", SESSION_ID: "signals-sdk-session-id"},
            eI = {EMAIL: "cr_email", EMAIL_BASE64: "cr_e", UTM_ID: "utm_id"};

        function eS(e) {
            let r = document.createElement("input");
            if (r.type = "email", r.required = !0, r.value = e, !("function" == typeof r.checkValidity ? r.checkValidity() : /\S+@\S+\.\S+/.test(e))) return !1;
            let a = e.split("@")[0];
            return !(null == a || /\*{3,}$/.test(a))
        }

        function eE(e, r, a = ["string"]) {
            return Object.fromEntries(Object.entries(e).filter(([e, s]) => r.includes(e) && a.includes(typeof s)))
        }

        class eT {
            siteId;
            userId;
            sessionId;
            apiHost;
            pageTracker = null;
            formTracker = null;
            static version = er.i8;
            lastPageViewEvent = null;

            constructor(e, {apiHost: r = "https://api.cr-relay.com", autoTracking: a = !0} = {}) {
                this.siteId = e, this.apiHost = r, this.userId = this.initializeUserId(), this.sessionId = this.initializeSessionId();
                try {
                    this.identifyWithQueryParams()
                } catch (e) {
                    console.error("Failed to identify the visitor with query params")
                }
                a && (this.pageTracker = new ey(this), this.formTracker = new eu(this))
            }

            initializeStoredId(e, r, a) {
                let s = D(e), l = "" !== s ? s : r;
                return D(e, l, a, "/", "." + K(window.location.href).hostname.split(".").slice(-2).join("."), !1), l
            }

            initializeSessionId() {
                return this.initializeStoredId(eb.SESSION_ID, en(), 1800)
            }

            initializeUserId() {
                return this.initializeStoredId(eb.USER_ID, en(), 31536e3)
            }

            identifyWithQueryParams() {
                let e;
                let r = new URLSearchParams(window.location.search), a = r.get(eI.EMAIL), s = r.get(eI.EMAIL_BASE64),
                    l = r.get(eI.UTM_ID), d = r => {
                        try {
                            let a = atob(r);
                            eS(a) && (e = a)
                        } catch (e) {
                        }
                    };
                null != a && eS(a) ? e = a : null != s ? d(s) : null != l && d(l), null != e && this.identify({email: e})
            }

            traceApiUrl(e) {
                return new URL(`v1/site/${e}/batch`, this.apiHost)
            }

            async sendEvent(e) {
                let r = this.traceApiUrl(this.siteId).toString(), a = JSON.stringify([e]);
                try {
                    if (navigator.sendBeacon && "function" == typeof navigator.sendBeacon && navigator.sendBeacon(r, a)) return;
                    await fetch(r, {
                        method: "POST",
                        body: a,
                        keepalive: !0,
                        headers: {"Content-Type": "application/json"}
                    })
                } catch (e) {
                    console.warn("Failed to send web visit event:", e)
                }
            }

            page(e) {
                let r = {
                    eventType: "page_view",
                    url: "string" == typeof e ? e : e?.url ?? window.location.href,
                    userId: this.userId,
                    sessionId: this.sessionId,
                    sdkVersion: eT.version,
                    pageViewId: en()
                };
                this.lastPageViewEvent = r, this.sendEvent(r)
            }

            pageFocus(e) {
                if (null == this.lastPageViewEvent) return;
                let r = {
                    eventType: "page_focus",
                    pageViewId: this.lastPageViewEvent.pageViewId,
                    focusTime: e,
                    url: this.lastPageViewEvent.url,
                    userId: this.userId,
                    sessionId: this.sessionId,
                    sdkVersion: eT.version
                };
                this.sendEvent(r)
            }

            identify(e) {
                let r = {
                    eventType: "identity",
                    userId: this.userId,
                    sessionId: this.sessionId,
                    sdkVersion: eT.version, ...eE(e, ["email", "name"])
                };
                this.sendEvent(r)
            }

            form(e) {
                let r = el(e) ? function (e, r) {
                    if (void 0 === r && (r = {trim: !1}), !e || !e.elements) throw Error(C);
                    for (var a, s = {}, l = [], d = {}, f = 0, h = e.elements.length; f < h; f++) {
                        var p = e.elements[f];
                        P[p.type] || p.disabled || (a = p.name || p.id) && !d[a] && (l.push(a), d[a] = !0)
                    }
                    for (var v = 0, m = l.length; v < m; v++) {
                        var g = function (e, r, a) {
                            if (void 0 === a && (a = {trim: !1}), !e) throw Error(C);
                            if (!r && "[object String]" !== R.call(r)) throw Error("Field name is required by getInputData");
                            var s = e.elements[r];
                            if (!s || s.disabled) return null;
                            if (!L[R.call(s)]) return N(s, a.trim);
                            for (var l = [], d = !0, f = 0, h = s.length; f < h; f++) if (!s[f].disabled) {
                                d && "radio" !== s[f].type && (d = !1);
                                var p = N(s[f], a.trim);
                                null != p && (l = l.concat(p))
                            }
                            return d && 1 === l.length ? l[0] : l.length > 0 ? l : null
                        }(e, a = l[v], r);
                        null != g && (s[a] = g)
                    }
                    return s
                }(e) : e, a = {
                    eventType: "form",
                    url: window.location.href,
                    userId: this.userId,
                    sessionId: this.sessionId,
                    sdkVersion: eT.version, ...eE(r, ["email", "name"])
                };
                this.sendEvent(a)
            }
        }

        !function (e, r) {
            if ("string" != typeof e || "" === e) throw Error("siteId is required");
            if (void 0 !== window.signals && window.signals instanceof eT) throw Error("SignalsSDK already loaded");
            null == r && (r = {});
            let a = new eT(e, r);
            if (Array.isArray(window.signals)) for (let [e, r] of window.signals) try {
                a[e](...r)
            } catch (r) {
                console.warn(`Error calling ${e}`, r)
            }
            window.signals = a
        }(null != window.signals && "_siteId" in window.signals ? window.signals._siteId : function () {
            let e = document.currentScript?.getAttribute("src");
            return null != e ? e.split("/").at(-2) : ""
        }(), null != window.signals && "_opts" in window.signals ? window.signals._opts : void 0)
    })(), SignalsSDK = {}
})();