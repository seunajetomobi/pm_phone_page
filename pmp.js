/* Photo Mirror peer-to-peer toolkit. Used by the browser extension (the computer side) and by the
   hosted phone page. It finds the other side through a small relay, then talks directly (WebRTC).
   The relay only ever sees encrypted handshake messages, never photos or files. */
(function (G) {
'use strict';
const enc = new TextEncoder(), dec = new TextDecoder();
const sleep = ms => new Promise(r => setTimeout(r, ms));

/* ---------- encoding and encryption (AES-GCM, key travels only inside the QR code) ---------- */
const b64 = {
  enc(u) { let s = ''; for (let i = 0; i < u.length; i += 0x8000) s += String.fromCharCode.apply(null, u.subarray(i, i + 0x8000)); return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); },
  dec(s) { s = s.replace(/-/g, '+').replace(/_/g, '/'); while (s.length % 4) s += '='; const r = atob(s), u = new Uint8Array(r.length); for (let i = 0; i < r.length; i++) u[i] = r.charCodeAt(i); return u; }
};
const rnd = n => b64.enc(crypto.getRandomValues(new Uint8Array(n)));
const keys = new Map();
const ckey = k => { let p = keys.get(k); if (!p) { p = crypto.subtle.importKey('raw', b64.dec(k), 'AES-GCM', false, ['encrypt', 'decrypt']); keys.set(k, p); } return p; };
async function seal(k, obj) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, await ckey(k), enc.encode(JSON.stringify(obj))));
  const o = new Uint8Array(12 + ct.length); o.set(iv); o.set(ct, 12); return b64.enc(o);
}
async function unseal(k, s) {
  try { const u = b64.dec(s); return JSON.parse(dec.decode(await crypto.subtle.decrypt({ name: 'AES-GCM', iv: u.subarray(0, 12) }, await ckey(k), u.subarray(12)))); } catch (e) { return null; }
}

/* ---------- relay (ntfy-style: POST text to /topic, listen on /topic/sse) ---------- */
function relay(base) {
  base = String(base).replace(/\/+$/, '');
  return {
    base,
    async publish(topic, text) { const r = await fetch(base + '/' + topic, { method: 'POST', body: text }); if (!r.ok) throw new Error('relay ' + r.status); },
    subscribe(topic, onmsg, onstate) {
      let closed = false, es = null, ok = false, fire;
      const ready = new Promise(r => { fire = r; });
      es = new EventSource(base + '/' + topic + '/sse');
      es.onopen = () => { ok = true; fire(true); onstate && onstate(true); };
      es.onerror = () => { ok = false; onstate && onstate(false); };
      es.onmessage = e => { try { const j = JSON.parse(e.data); if (j.event === 'message' && typeof j.message === 'string') onmsg(j.message); } catch (x) {} };
      return { ready, get up() { return ok; }, close() { closed = true; es.close(); } };
    }
  };
}


/* one connection for any number of topics (browsers allow only a few open streams per server) */
function hub(base) {
  base = String(base).replace(/\/+$/, '');
  const subs = new Map(); let es = null, timer = 0, up = null;
  const state = v => { up = v; subs.forEach(s => s.onstate && s.onstate(v)); };
  const reopen = () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      if (es) { es.close(); es = null; }
      const ts = [...subs.keys()]; if (!ts.length) return;
      es = new EventSource(base + '/' + ts.join(',') + '/sse');
      es.onopen = () => state(true); es.onerror = () => state(false);
      es.onmessage = e => { try { const j = JSON.parse(e.data); if (j.event === 'message' && typeof j.message === 'string') { const s = subs.get(j.topic); if (s) s.onmsg(j.message); } } catch (x) {} };
    }, 60);
  };
  return {
    get up() { return up; },
    subscribe(topic, onmsg, onstate) { subs.set(topic, { onmsg, onstate }); reopen(); return { close() { subs.delete(topic); reopen(); } }; },
    close() { clearTimeout(timer); subs.clear(); if (es) es.close(); }
  };
}

const ice = stun => ({ iceServers: stun === false ? [] : [{ urls: 'stun:stun.l.google.com:19302' }] });
function gathered(pc, ms) {
  return new Promise(res => {
    if (pc.iceGatheringState === 'complete') return res();
    const t = setTimeout(done, ms);
    function done() { clearTimeout(t); pc.removeEventListener('icegatheringstatechange', chk); res(); }
    function chk() { if (pc.iceGatheringState === 'complete') done(); }
    pc.addEventListener('icegatheringstatechange', chk);
  });
}

/* ---------- phone side: dial the computer ---------- */
async function dial(o) {
  const rl = relay(o.relay), id = rnd(6), pc = new RTCPeerConnection(ice(o.stun)), dc = pc.createDataChannel('pm');
  let sub = null;
  try {
    let got; const answer = new Promise(r => { got = r; });
    sub = rl.subscribe(o.topic, async t => { const m = await unseal(o.key, t); if (m && m.t === 'answer' && m.id === id) got(m); });
    if (!await Promise.race([sub.ready, sleep(6000).then(() => false)])) throw new Error('relay');
    await pc.setLocalDescription(await pc.createOffer());
    await gathered(pc, 3500);
    const offer = await seal(o.key, { t: 'offer', id, sdp: pc.localDescription.sdp, ua: o.ua || '' });
    const early = []; dc.onmessage = e => early.push(e.data);
    const opened = new Promise((res, rej) => { dc.onopen = () => res(); dc.onclose = () => rej(new Error('closed')); });
    opened.catch(() => {});
    let m = null;
    for (let i = 0; i < 8 && !m; i++) { await rl.publish(o.topic, offer); m = await Promise.race([answer, sleep(2500).then(() => null)]); }
    if (!m) throw new Error('no-answer');
    await pc.setRemoteDescription({ type: 'answer', sdp: m.sdp });
    await Promise.race([opened, sleep(15000).then(() => { throw new Error('connect'); })]);
    sub.close();
    return { pc, dc, early };
  } catch (e) { if (sub) sub.close(); try { pc.close(); } catch (x) {} throw e; }
}

/* ---------- computer side: answer an offer ---------- */
async function answer(o) {                           // o: {relay, topic, key, offer, stun}
  const rl = relay(o.relay), pc = new RTCPeerConnection(ice(o.stun)), early = [];
  const dcp = new Promise((res, rej) => {
    const t = setTimeout(() => { try { pc.close(); } catch (e) {} rej(new Error('timeout')); }, 30000);
    pc.ondatachannel = e => { const c = e.channel; c.onmessage = x => early.push(x.data); const go = () => { clearTimeout(t); res(c); }; if (c.readyState === 'open') go(); else c.onopen = go; };
  });
  dcp.catch(() => {});
  await pc.setRemoteDescription({ type: 'offer', sdp: o.offer.sdp });
  await pc.setLocalDescription(await pc.createAnswer());
  await gathered(pc, 3500);
  await rl.publish(o.topic, await seal(o.key, { t: 'answer', id: o.offer.id, sdp: pc.localDescription.sdp }));
  const dc = await dcp;
  return { pc, dc, early };
}

/* ---------- request / response / events over one data channel ---------- */
const CH = 16384;
class Link {
  constructor(dc, pc, parity, early) {
    this.dc = dc; this.pc = pc; this.par = parity; this.n = 0; this.pend = new Map(); this.inc = new Map();
    this.onrequest = null; this.onevent = null; this.onclose = null; this.open = dc.readyState === 'open'; this.seen = Date.now();
    dc.binaryType = 'arraybuffer'; dc.bufferedAmountLowThreshold = 262144;
    dc.onmessage = e => { this.seen = Date.now(); this._msg(e.data); };
    dc.onclose = () => this._closed(); dc.onerror = () => {};
    (early || []).forEach(d => this._msg(d));
    if (pc) pc.onconnectionstatechange = () => { if (pc.connectionState === 'failed' || pc.connectionState === 'closed') this._closed(); };
  }
  _id() { this.n += 2; return this.n + this.par; }
  async _tx(u8) {
    while (this.dc.bufferedAmount > 1048576) {
      if (this.dc.readyState !== 'open') throw new Error('closed');
      await new Promise(r => { this.dc.onbufferedamountlow = () => { this.dc.onbufferedamountlow = null; r(); }; setTimeout(r, 1000); });
    }
    if (this.dc.readyState !== 'open') throw new Error('closed');
    this.dc.send(u8);
  }
  async _body(id, blob, prog) {
    let sent = 0;
    for (let off = 0; off < blob.size; off += 262144) {
      const big = new Uint8Array(await blob.slice(off, off + 262144).arrayBuffer());
      for (let i = 0; i < big.length; i += CH) {
        const n = Math.min(CH, big.length - i), f = new Uint8Array(4 + n);
        new DataView(f.buffer).setUint32(0, id); f.set(big.subarray(i, i + n), 4);
        await this._tx(f); sent += n; if (prog) prog(sent, blob.size);
      }
    }
  }
  _text(kind, obj) { if (this.dc.readyState !== 'open') throw new Error('closed'); this.dc.send(kind + JSON.stringify(obj)); }
  toBlob(b) { return b instanceof Blob ? b : new Blob(b == null ? [] : [b]); }
  async request(method, path, o) {
    o = o || {}; const id = this._id(), body = this.toBlob(o.body);
    const p = new Promise((res, rej) => { this.pend.set(id, { res, rej, parts: [], got: 0, head: null }); });
    try { this._text('R', { id, m: method, p: path, h: o.headers || {}, len: body.size }); await this._body(id, body, o.onprogress); }
    catch (e) { const x = this.pend.get(id); this.pend.delete(id); if (x) x.rej(e); }
    return p;
  }
  emit(ev, data) { try { this._text('E', { e: ev, d: data }); } catch (e) {} }
  _msg(d) {
    if (typeof d === 'string') {
      const k = d[0], j = JSON.parse(d.slice(1));
      if (k === 'R') { this.inc.set(j.id, { h: j, parts: [], got: 0 }); if (!j.len) this._req(j.id); }
      else if (k === 'S') { const x = this.pend.get(j.id); if (x) { x.head = j; if (!j.len) this._done(j.id); } }
      else if (k === 'E') { this.onevent && this.onevent(j.e, j.d); }
      return;
    }
    const u = new Uint8Array(d), id = new DataView(d).getUint32(0), mine = (id & 1) === this.par;
    const x = mine ? this.pend.get(id) : this.inc.get(id); if (!x) return;
    x.parts.push(u.subarray(4)); x.got += u.length - 4;
    if (mine) { if (x.head && x.got >= x.head.len) this._done(id); } else if (x.got >= x.h.len) this._req(id);
  }
  _done(id) {
    const x = this.pend.get(id); this.pend.delete(id); if (!x) return;
    const h = x.head, body = h.len ? new Blob(x.parts, { type: (h.h && h.h['Content-Type']) || '' }) : null;
    x.res(new Response(body, { status: h.st, headers: h.h || {} }));
  }
  async _req(id) {
    const x = this.inc.get(id); this.inc.delete(id); if (!x) return;
    let r;
    try { r = await this.onrequest({ method: x.h.m, path: x.h.p, headers: x.h.h || {}, body: new Blob(x.parts) }); }
    catch (e) { r = { status: 500, headers: {}, body: 'Error' }; }
    try { const b = this.toBlob(r.body); this._text('S', { id, st: r.status, h: r.headers || {}, len: b.size }); await this._body(id, b); } catch (e) {}
  }
  _closed() {
    if (!this.open) return; this.open = false;
    for (const x of this.pend.values()) x.rej(new Error('closed')); this.pend.clear();
    try { this.dc.close(); } catch (e) {} try { this.pc && this.pc.close(); } catch (e) {}
    this.onclose && this.onclose();
  }
  close() { this._closed(); }
}

G.PMP = { b64, rnd, seal, unseal, relay, hub, dial, answer, Link, sleep };
})(typeof self !== 'undefined' ? self : this);
