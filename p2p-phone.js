/* Phone side of Photo Mirror when the page is hosted on the web: instead of talking to a local server,
   every request goes straight to your computer's browser over an encrypted peer-to-peer link. */
(function () {
'use strict';
if (!document.querySelector('meta[name="pm-mode"]')) return;
const LS = 'pm_p2p', RELAY0 = 'https://ntfy.sh';
const realFetch = self.fetch.bind(self), RealES = self.EventSource, RealXHR = self.XMLHttpRequest;
const load = () => { try { return JSON.parse(localStorage.getItem(LS) || 'null'); } catch (e) { return null; } };
const save = c => { try { localStorage.setItem(LS, JSON.stringify(c)); } catch (e) {} };

/* the code on the QR sits after the # so it never reaches any server; keep it, then tidy the address */
(function () {
  const p = new URLSearchParams(location.hash.replace(/^#/, ''));
  if (p.get('t') && p.get('k')) save({ t: p.get('t'), k: p.get('k'), r: p.get('r') || RELAY0, s: p.get('s') !== '0' });
  if (location.hash) { try { history.replaceState(null, '', location.pathname + location.search); } catch (e) {} }
})();

let link = null, busy = false, delay = 1500, feeds = [], waiters = [];
async function connect() {
  if (link || busy) return; const c = load(); if (!c) return;
  busy = true;
  try {
    const r = await PMP.dial({ relay: c.r, topic: c.t, key: c.k, stun: c.s, ua: navigator.userAgent });
    const l = new PMP.Link(r.dc, r.pc, 1, r.early);
    l.onevent = (e, d) => {
      if (e === 'welcome') { try { const w = JSON.parse(d); save({ t: w.t, k: w.k, r: c.r, s: c.s }); } catch (x) {} }
      else if (e !== 'ka') feeds.forEach(f => f.push(e, d));
    };
    l.onclose = () => { if (link === l) link = null; later(); };
    link = l; delay = 1500; waiters.splice(0).forEach(w => w());
  } catch (e) { console.warn('Photo Mirror: could not connect:', e && e.message); delay = Math.min(Math.round(delay * 1.6), 12000); }
  busy = false; if (!link) later();
}
let timer = 0;
function later() { clearTimeout(timer); timer = setTimeout(connect, delay); }
function waitLink(ms) {
  if (link) return Promise.resolve();
  return new Promise((res, rej) => { const t = setTimeout(() => rej(new TypeError('offline')), ms); waiters.push(() => { clearTimeout(t); res(); }); });
}
setInterval(() => { if (link) { link.emit('ka', ''); if (Date.now() - link.seen > 30000) link.close(); } }, 8000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) { delay = 800; if (!link) { clearTimeout(timer); connect(); } } });
addEventListener('online', () => { delay = 800; if (!link) { clearTimeout(timer); connect(); } });

self.fetch = async function (input, init) {
  const url = typeof input === 'string' ? input : input.url;
  if (!url.startsWith('/') && !url.startsWith('./')) return realFetch(input, init);
  init = init || {};
  if (!load()) return new Response('Not paired', { status: 401 });
  connect(); await waitLink(9000);
  return link.request((init.method || 'GET').toUpperCase(), url, { body: init.body });
};

/* uploads with a progress number */
self.XMLHttpRequest = function () {
  const x = { upload: {}, status: 0, responseText: '', onload: null, onerror: null, _m: 'GET', _u: '' };
  x.open = (m, u) => { x._m = m; x._u = u; };
  x.send = async body => {
    try {
      if (!load()) { x.status = 401; x.onload && x.onload(); return; }
      connect(); await waitLink(9000);
      const r = await link.request(x._m, x._u, { body, onprogress: (a, b) => x.upload.onprogress && x.upload.onprogress({ lengthComputable: true, loaded: a, total: b }) });
      x.status = r.status; x.responseText = await r.text(); x.onload && x.onload();
    } catch (e) { x.onerror && x.onerror(); }
  };
  return x;
};

self.EventSource = function (url, o) {
  if (url !== '/phone-events') return new RealES(url, o);
  const h = {};
  const f = { addEventListener(t, fn) { (h[t] = h[t] || []).push(fn); }, close() { feeds = feeds.filter(x => x !== f); }, push(t, d) { (h[t] || []).forEach(fn => { try { fn({ type: t, data: d }); } catch (e) { console.error(e); } }); } };
  feeds.push(f); connect(); return f;
};

})();
