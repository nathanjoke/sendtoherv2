'use strict';
// sendtoher — ส่งไฟล์ด้วยรหัส 4 หลัก ไม่มีโฆษณา ไม่ต้องติดตั้งแพ็กเกจเพิ่ม (Node.js 18+)
//
// มี 2 โหมด:
//  1) direct (ค่าเริ่มต้น): ส่งตรงระหว่างเบราว์เซอร์ด้วย WebRTC (P2P)
//     เซิร์ฟเวอร์ทำแค่จับคู่ (signaling ผ่าน Server-Sent Events)
//     ถ้าต่อตรงไม่ได้ จะส่งผ่านเซิร์ฟเวอร์แบบ stream ทันที (relay) ไม่พักไฟล์ลงดิสก์
//  2) link: อัปโหลดไฟล์มาพักไว้บนเซิร์ฟเวอร์ชั่วคราว ผู้รับมาโหลดทีหลังได้
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
const MAX_BYTES = (Number(process.env.MAX_MB) || 4096) * 1024 * 1024; // โหมด link: ขนาดรวมสูงสุดต่อรหัส
const MAX_TTL_MIN = Number(process.env.MAX_TTL_MIN) || 60;
const SITE_KEY = process.env.SITE_KEY || '';
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const STUN_URLS = (process.env.STUN_URLS || 'stun:stun.l.google.com:19302,stun:stun.cloudflare.com:3478').split(',').map(s => s.trim()).filter(Boolean);
const TURN_URL = process.env.TURN_URL || '';
const TURN_USER = process.env.TURN_USER || '';
const TURN_PASS = process.env.TURN_PASS || '';
const MAX_FAILS = 8;
const FAIL_WINDOW = 15 * 60 * 1000;
const MAX_SESSIONS = 2000;
const IDLE_UPLOAD_MS = 30 * 60 * 1000;
const SENDER_GRACE_MS = 20 * 1000;
const INDEX = path.join(__dirname, 'public', 'index.html');

const ICE = [{ urls: STUN_URLS }];
if (TURN_URL) ICE.push({ urls: TURN_URL.split(','), username: TURN_USER, credential: TURN_PASS });

fs.mkdirSync(DATA_DIR, { recursive: true });
for (const f of fs.readdirSync(DATA_DIR)) if (/^[a-f0-9]{32}$/.test(f)) fs.rmSync(path.join(DATA_DIR, f), { force: true });

const sessions = new Map(); // pin -> session (kind: 'store' | 'direct')
const fails = new Map();

const now = () => Date.now();
const rnd = n => crypto.randomBytes(n).toString('hex');
const newPin = () => {
  for (let i = 0; i < 100; i++) {
    const p = String(crypto.randomInt(0, 10000)).padStart(4, '0');
    if (!sessions.has(p)) return p;
  }
  return null;
};
const clientIp = req => {
  if (TRUST_PROXY) { const x = req.headers['x-forwarded-for']; if (x) return String(x).split(',')[0].trim(); }
  return req.socket.remoteAddress || '?';
};
const isBlocked = ip => { const f = fails.get(ip); return !!(f && f.reset > now() && f.n >= MAX_FAILS); };
const addFail = ip => { let f = fails.get(ip); if (!f || f.reset < now()) f = { n: 0, reset: now() + FAIL_WINDOW }; f.n++; fails.set(ip, f); };
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const cleanName = raw => {
  let n = ''; try { n = decodeURIComponent(String(raw || '')); } catch { n = String(raw || ''); }
  n = n.replace(/[\\/]/g, '_').replace(/[\x00-\x1f\x7f]/g, '').trim().slice(0, 200);
  return n && n !== '.' && n !== '..' ? n : 'file';
};
const disposition = name => `attachment; filename="${name.replace(/[^\x20-\x7e]|["\\]/g, '_')}"; filename*=UTF-8''${encodeURIComponent(name)}`;

// ---------- Server-Sent Events ----------
function openSse(res) {
  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.write('retry: 3000\n\n');
  const hb = setInterval(() => res.write(': hb\n\n'), 15000);
  res.on('close', () => clearInterval(hb));
}
const push = (res, ev, data) => { if (res && !res.writableEnded) res.write(`event: ${ev}\ndata: ${JSON.stringify(data || {})}\n\n`); };

function destroy(pin, reason = 'gone') {
  const s = sessions.get(pin);
  if (!s) return;
  sessions.delete(pin);
  if (s.kind === 'store') {
    for (const f of s.files) fs.rm(f.path, { force: true }, () => {});
    for (const p of s.partial) fs.rm(p, { force: true }, () => {});
  } else {
    clearTimeout(s.graceTimer);
    push(s.receiver, 'gone', { reason }); push(s.sender, 'gone', { reason });
    if (s.receiver) s.receiver.end();
    if (s.sender) s.sender.end();
    for (const r of s.relays.values()) { if (r.upload) r.upload.destroy(); r.res.destroy(); }
  }
}

setInterval(() => {
  const t = now();
  for (const [pin, s] of sessions) {
    if (s.kind === 'store') {
      if (s.ready && s.expires < t) destroy(pin);
      else if (!s.ready && s.lastActivity < t - IDLE_UPLOAD_MS) destroy(pin);
    } else if (s.expires < t && !s.receiver && !s.relays.size) destroy(pin, 'expired');
  }
  for (const [ip, f] of fails) if (f.reset < t) fails.delete(ip);
}, 10000).unref();

function json(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}
function readJson(req, limit = 4096) {
  return new Promise(resolve => {
    let d = '';
    req.on('data', c => { d += c; if (d.length > limit) { req.destroy(); resolve({}); } });
    req.on('end', () => { try { resolve(JSON.parse(d || '{}')); } catch { resolve({}); } });
    req.on('error', () => resolve({}));
  });
}
const checkSiteKey = req => !SITE_KEY || safeEq(req.headers['x-site-key'] || '', SITE_KEY);
const ttlOf = v => Math.min(MAX_TTL_MIN, Math.max(1, Math.round(Number(v) || 10)));

// ผู้รับ: ตรวจรหัสพร้อมนับครั้งที่เดาผิด
function lookupPin(req, res, pin) {
  const ip = clientIp(req);
  if (isBlocked(ip)) { json(res, 429, { error: 'too_many_tries' }); return null; }
  const s = /^\d{4}$/.test(pin || '') ? sessions.get(pin) : null;
  const ok = s && (s.kind === 'store' ? s.ready && s.expires > now() : !!s.sender || !!s.graceTimer);
  if (!ok) { addFail(ip); json(res, 404, { error: 'not_found' }); return null; }
  return s;
}

async function handle(req, res) {
  const url = new URL(req.url, 'http://local');
  const parts = url.pathname.split('/').filter(Boolean);
  const m = req.method;

  if (m === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
      'Content-Security-Policy': "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data: blob:; frame-ancestors 'none'",
    });
    return fs.createReadStream(INDEX).pipe(res);
  }
  if (parts[0] !== 'api') return json(res, 404, { error: 'not_found' });

  // ================= DIRECT (P2P + relay) =================
  if (parts[1] === 'direct') {
    // สร้างรหัส
    if (m === 'POST' && parts.length === 2) {
      if (!checkSiteKey(req)) return json(res, 401, { error: 'site_key' });
      if (sessions.size >= MAX_SESSIONS) return json(res, 503, { error: 'busy' });
      const body = await readJson(req, 64 * 1024);
      const files = (Array.isArray(body.files) ? body.files : []).slice(0, 500).map(f => ({ name: cleanName(encodeURIComponent(String(f && f.name || ''))), size: Math.max(0, Number(f && f.size) || 0) }));
      if (!files.length) return json(res, 400, { error: 'no_files' });
      const pin = newPin();
      if (!pin) return json(res, 503, { error: 'busy' });
      const ttl = ttlOf(body.ttl);
      sessions.set(pin, { kind: 'direct', pin, token: rnd(24), files, ttl, expires: now() + ttl * 60000, sender: null, graceTimer: null, rid: null, receiver: null, relays: new Map() });
      const s = sessions.get(pin);
      s.graceTimer = setTimeout(() => { if (!s.sender) destroy(pin, 'sender_left'); }, SENDER_GRACE_MS);
      return json(res, 200, { pin, token: s.token, ice: ICE, expiresAt: s.expires, now: now() });
    }

    const pin = parts[2];
    const s = sessions.get(pin);
    const isSender = s && s.kind === 'direct' && safeEq(url.searchParams.get('token') || req.headers['x-token'] || '', s.token);
    const ridParam = url.searchParams.get('rid') || req.headers['x-rid'] || '';
    const isReceiver = s && s.kind === 'direct' && s.rid && safeEq(ridParam, s.rid);

    // ช่องรับเหตุการณ์ (SSE) ของทั้งสองฝั่ง
    if (m === 'GET' && parts[3] === 'events') {
      if (url.searchParams.has('token')) {
        if (!isSender) return json(res, 404, { error: 'not_found' });
        if (s.sender) s.sender.end();
        clearTimeout(s.graceTimer); s.graceTimer = null;
        openSse(res); s.sender = res;
        push(res, 'hello', { receiver: !!s.receiver });
        res.on('close', () => {
          if (s.sender !== res) return;
          s.sender = null;
          s.graceTimer = setTimeout(() => { if (!s.sender) destroy(pin, 'sender_left'); }, SENDER_GRACE_MS);
        });
        return;
      }
      const t = lookupPin(req, res, pin);
      if (!t) return;
      if (t.kind !== 'direct') return json(res, 400, { error: 'wrong_mode' });
      if (!/^[a-f0-9]{16,64}$/.test(ridParam)) return json(res, 400, { error: 'bad_rid' });
      if (t.receiver && !safeEq(ridParam, t.rid || '')) return json(res, 409, { error: 'busy' });
      if (t.receiver) t.receiver.end();
      t.rid = ridParam; openSse(res); t.receiver = res;
      t.expires = Math.max(t.expires, now() + 6 * 3600 * 1000);
      push(res, 'hello', { files: t.files, ice: ICE });
      push(t.sender, 'peer', {});
      res.on('close', () => {
        if (t.receiver !== res) return;
        t.receiver = null; t.rid = null;
        push(t.sender, 'peer-left', {});
      });
      return;
    }

    if (!s || s.kind !== 'direct') return json(res, 404, { error: 'not_found' });

    // ส่งต่อข้อความ signaling (SDP / ICE / สถานะ) ไปอีกฝั่ง
    if (m === 'POST' && parts[3] === 'signal') {
      if (!isSender && !isReceiver) return json(res, 403, { error: 'forbidden' });
      const body = await readJson(req, 64 * 1024);
      const target = isSender ? s.receiver : s.sender;
      if (!target) return json(res, 409, { error: 'peer_offline' });
      push(target, 'signal', body.data || {});
      return json(res, 200, { ok: true });
    }

    // relay: ผู้รับดาวน์โหลดไฟล์ที่ i -> เซิร์ฟเวอร์ขอให้ผู้ส่งอัปโหลดมาแล้วต่อท่อตรงถึงกันทันที
    if (m === 'GET' && parts[3] === 'relay') {
      if (!isReceiver) return json(res, 403, { error: 'forbidden' });
      const i = Number(parts[4]); const f = Number.isInteger(i) ? s.files[i] : null;
      if (!f) return json(res, 404, { error: 'not_found' });
      if (!s.sender) return json(res, 409, { error: 'sender_offline' });
      const id = rnd(12);
      res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Content-Length': f.size, 'Content-Disposition': disposition(f.name), 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
      s.relays.set(id, { res, i, upload: null });
      res.on('close', () => { const r = s.relays.get(id); if (r && r.upload) r.upload.destroy(); s.relays.delete(id); });
      push(s.sender, 'pull', { id, i });
      return;
    }
    if (m === 'PUT' && parts[3] === 'relay') {
      if (!isSender) return json(res, 403, { error: 'forbidden' });
      const r = s.relays.get(parts[4]);
      if (!r) { req.resume(); return json(res, 404, { error: 'not_found' }); }
      r.upload = req;
      const size = s.files[r.i].size;
      let sent = 0, last = 0;
      req.on('data', c => {
        sent += c.length;
        if (sent > size) { req.destroy(); r.res.destroy(); return; }
        if (!r.res.write(c)) { req.pause(); r.res.once('drain', () => req.resume()); }
        if (now() - last > 400) { last = now(); push(s.receiver, 'relay-progress', { i: r.i, bytes: sent }); }
      });
      req.on('end', () => {
        r.res.end(); s.relays.delete(parts[4]);
        push(s.receiver, 'relay-progress', { i: r.i, bytes: sent, done: true });
        json(res, 200, { ok: true });
      });
      req.on('error', () => r.res.destroy());
      req.on('aborted', () => r.res.destroy());
      return;
    }
    if (m === 'DELETE' && parts.length === 3) {
      if (!isSender) return json(res, 403, { error: 'forbidden' });
      destroy(pin, 'cancelled'); return json(res, 200, { ok: true });
    }
    return json(res, 404, { error: 'not_found' });
  }

  // ================= LINK (อัปโหลดพักไว้) =================
  if (parts[1] === 'send') {
    if (m === 'POST' && parts.length === 2) {
      if (!checkSiteKey(req)) return json(res, 401, { error: 'site_key' });
      if (sessions.size >= MAX_SESSIONS) return json(res, 503, { error: 'busy' });
      const body = await readJson(req);
      const pin = newPin();
      if (!pin) return json(res, 503, { error: 'busy' });
      const s = { kind: 'store', pin, token: rnd(24), ttl: ttlOf(body.ttl), oneTime: !!body.oneTime, ready: false, expires: 0, lastActivity: now(), files: [], partial: new Set(), bytes: 0, downloaded: new Set(), downloads: 0 };
      sessions.set(pin, s);
      return json(res, 200, { pin, token: s.token, maxBytes: MAX_BYTES, ttl: s.ttl });
    }
    const pin = parts[2];
    const s = sessions.get(pin);
    if (!s || s.kind !== 'store' || !safeEq(req.headers['x-token'] || '', s.token)) return json(res, 404, { error: 'not_found' });

    if (m === 'PUT' && parts[3] === 'files') {
      if (s.ready) return json(res, 409, { error: 'already_sent' });
      const len = Number(req.headers['content-length']);
      if (len && s.bytes + len > MAX_BYTES) { req.resume(); return json(res, 413, { error: 'too_large', maxBytes: MAX_BYTES }); }
      const name = cleanName(req.headers['x-filename']);
      const file = path.join(DATA_DIR, rnd(16));
      s.partial.add(file);
      const out = fs.createWriteStream(file);
      let size = 0, failed = false;
      const abort = code => {
        if (failed) return; failed = true;
        out.destroy(); s.partial.delete(file); s.bytes -= size;
        fs.rm(file, { force: true }, () => {});
        if (code && !res.headersSent) json(res, code, { error: code === 413 ? 'too_large' : 'upload_failed', maxBytes: MAX_BYTES });
        req.destroy();
      };
      req.on('data', c => { size += c.length; s.bytes += c.length; s.lastActivity = now(); if (s.bytes > MAX_BYTES) abort(413); });
      req.on('aborted', () => abort(0));
      req.on('error', () => abort(0));
      out.on('error', () => abort(500));
      out.on('finish', () => {
        if (failed) return;
        if (!sessions.has(pin)) { fs.rm(file, { force: true }, () => {}); return json(res, 410, { error: 'gone' }); }
        s.partial.delete(file);
        s.files.push({ name, size, path: file });
        json(res, 200, { ok: true, index: s.files.length - 1 });
      });
      return req.pipe(out);
    }
    if (m === 'POST' && parts[3] === 'done') {
      if (!s.files.length) return json(res, 400, { error: 'no_files' });
      if (!s.ready) { s.ready = true; s.expires = now() + s.ttl * 60000; }
      return json(res, 200, { expiresAt: s.expires, now: now() });
    }
    if (m === 'GET' && parts[3] === 'status') return json(res, 200, { received: s.downloaded.size, total: s.files.length, expiresAt: s.expires, now: now() });
    if (m === 'DELETE' && parts.length === 3) { destroy(pin); return json(res, 200, { ok: true }); }
    return json(res, 404, { error: 'not_found' });
  }

  // ================= ผู้รับ: ตรวจรหัส =================
  if (parts[1] === 'recv' && m === 'GET') {
    const s = lookupPin(req, res, parts[2]);
    if (!s) return;
    if (parts.length === 3) {
      if (s.kind === 'direct') return json(res, 200, { mode: 'direct', files: s.files, busy: !!s.receiver, now: now() });
      return json(res, 200, { mode: 'store', files: s.files.map(f => ({ name: f.name, size: f.size })), expiresAt: s.expires, now: now(), oneTime: s.oneTime });
    }
    if (s.kind !== 'store') return json(res, 400, { error: 'wrong_mode' });
    const i = Number(parts[3]); const f = Number.isInteger(i) ? s.files[i] : null;
    if (!f) return json(res, 404, { error: 'not_found' });
    res.writeHead(200, { 'Content-Type': 'application/octet-stream', 'Content-Length': f.size, 'Content-Disposition': disposition(f.name), 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
    const rs = fs.createReadStream(f.path);
    rs.on('error', () => res.destroy());
    res.on('finish', () => {
      s.downloads++; s.downloaded.add(i);
      if (s.oneTime && s.downloaded.size >= s.files.length) setTimeout(() => destroy(s.pin), 2000);
    });
    return rs.pipe(res);
  }
  return json(res, 404, { error: 'not_found' });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch(err => { console.error(err); if (!res.headersSent) json(res, 500, { error: 'server' }); else res.destroy(); });
});
server.requestTimeout = 0;
server.headersTimeout = 60000;
server.keepAliveTimeout = 65000;
server.listen(PORT, HOST, () => {
  console.log(`sendtoher พร้อมใช้งานที่ http://localhost:${PORT}`);
  console.log(`โหมด link เก็บไฟล์ที่ ${DATA_DIR} | สูงสุด ${Math.round(MAX_BYTES / 1048576)} MB | STUN: ${STUN_URLS.join(', ')}${TURN_URL ? ' | TURN: เปิด' : ''}${SITE_KEY ? ' | เปิดใช้รหัสผู้ส่ง' : ''}`);
});
