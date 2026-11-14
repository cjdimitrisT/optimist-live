/* Optimist Academy — live server for online races with friends (WebSocket, no extra packages).
   Start with: node server.js      It only passes messages between the players of one room. */
'use strict';
const http = require('http'), crypto = require('crypto');
const VERSION = 5, PORT = process.env.PORT || 3000, MAX_PLAYERS = 6, MAX_FRAME = 40000;
const rooms = new Map(), clients = new Set();
let nextId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(`Optimist Academy live server: OK\nVersion: ${VERSION}\nConnected: ${clients.size} · Rooms: ${rooms.size}\n`);
});

/* ---------- minimal WebSocket ---------- */
function frame(op, data) {
  const p = Buffer.isBuffer(data) ? data : Buffer.from(data || ''), n = p.length;
  const h = n < 126 ? Buffer.from([0x80 | op, n]) : n < 65536 ? Buffer.from([0x80 | op, 126, n >> 8, n & 255]) : Buffer.concat([Buffer.from([0x80 | op, 127, 0, 0, 0, 0]), Buffer.from([(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255])]);
  return Buffer.concat([h, p]);
}
const send = (c, m) => { if (!c.closed) try { c.sock.write(frame(1, JSON.stringify(m))); } catch (e) { } };
server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') return sock.destroy();
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { sock, id: nextId++, nick: '', room: null, alive: true, closed: false, voice: false, fin: false, buf: Buffer.alloc(0), frag: [] };
  clients.add(c);
  const close = () => { if (c.closed) return; c.closed = true; clients.delete(c); leave(c); try { sock.destroy(); } catch (e) { } };
  sock.on('data', d => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > MAX_FRAME * 2) return close();
    for (;;) {
      const b = c.buf; if (b.length < 2) return;
      const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80; let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = b.readUInt32BE(6); off = 10; if (b.readUInt32BE(2) !== 0) return close(); }
      if (len > MAX_FRAME || !masked) return close();
      if (b.length < off + 4 + len) return;
      const mask = b.subarray(off, off + 4), p = Buffer.from(b.subarray(off + 4, off + 4 + len));
      for (let i = 0; i < p.length; i++) p[i] ^= mask[i & 3];
      c.buf = b.subarray(off + 4 + len);
      if (op === 8) return close();
      if (op === 9) { try { sock.write(frame(10, p)); } catch (e) { } continue; }
      if (op === 10) { c.alive = true; continue; }
      if (op === 1 || op === 0) {
        c.frag.push(p); if (c.frag.reduce((a, x) => a + x.length, 0) > MAX_FRAME) return close();
        if (fin) { const txt = Buffer.concat(c.frag).toString('utf8'); c.frag = []; let m = null; try { m = JSON.parse(txt); } catch (e) { } if (m && typeof m === 'object') { try { onMsg(c, m); } catch (e) { console.error(e); } } }
      }
    }
  });
  sock.on('close', close); sock.on('error', close);
});
setInterval(() => { for (const c of clients) { if (!c.alive) { c.closed || c.sock.destroy(); continue; } c.alive = false; try { c.sock.write(frame(9, '')); } catch (e) { } } }, 30000);

/* ---------- rooms ---------- */
const roomMsg = r => ({ t: 'room', code: r.code, host: r.host.id, racing: r.racing, cfg: r.cfg, race: r.raceNo, of: r.cfg.races, members: r.members.map(x => ({ id: x.id, nick: x.nick, voice: x.voice })) });
const cast = (r, m, except) => { for (const x of r.members) if (x !== except) send(x, m); };
function newCode() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; for (;;) { let s = ''; for (let i = 0; i < 4; i++) s += A[crypto.randomInt(A.length)]; if (!rooms.has(s)) return s; } }
function leave(c) {
  const r = c.room; if (!r) return; c.room = null;
  r.members = r.members.filter(x => x !== c);
  if (!r.members.length) { rooms.delete(r.code); return; }
  if (r.host === c) r.host = r.members[0];
  cast(r, { t: 'gone', id: c.id }); cast(r, roomMsg(r));
}
const num = (v, a, b) => Math.max(a, Math.min(b, Number(v) || 0));
// a designed course chosen by the host: checked here, then sent to everybody in the room
function course(c) {
  if (!c || typeof c !== 'object' || !Array.isArray(c.marks) || c.marks.length < 2 || c.marks.length > 5) return null;
  const txt = (v, n) => String(v == null ? '' : v).replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, n);
  return { name: txt(c.name, 24) || '?', by: txt(c.by, 14), marks: c.marks.map(k => { k = k || {}; const m = { x: num(k.x, -24, 80), z: num(k.z, -46, 46) }; if (k.x2 != null && k.z2 != null) { m.x2 = num(k.x2, -24, 80); m.z2 = num(k.z2, -46, 46); } return m; }), sx: num(c.sx, -14, 60), sz: num(c.sz, -36, 36), sl: num(c.sl == null ? 7 : c.sl, 5, 22), w: num(c.w, 0.3, 0.8), wd: Math.round(num(c.wd, -30, 30)), sh: num(c.sh, 0, 20), gu: num(c.gu, 0, 0.2), n: Math.round(num(c.n, 0, 5)) };
}
const nums = (a, n) => Array.isArray(a) && a.length <= n && a.every(v => typeof v === 'number' && isFinite(v));
function onMsg(c, m) {
  const r = c.room;
  if (m.t === 'join') {
    if (r) return;
    c.nick = String(m.nick || '').replace(/[<>&"'`\\]/g, '').replace(/\s+/g, ' ').trim().slice(0, 14) || 'Sailor';
    let room;
    if (m.create) { room = { code: newCode(), members: [], host: c, racing: false, raceNo: 0, cfg: { lvl: 0, bots: true, hints: true, races: 1 } }; rooms.set(room.code, room); }
    else {
      room = rooms.get(String(m.room || '').toUpperCase().replace(/[^A-Z]/g, '').slice(0, 4));
      if (!room) return send(c, { t: 'error', code: 'noroom' });
      if (room.members.length >= MAX_PLAYERS) return send(c, { t: 'error', code: 'full' });
      if (room.racing) return send(c, { t: 'error', code: 'started' });
    }
    c.room = room; c.voice = false; c.fin = false; room.members.push(c);
    send(c, { t: 'you', id: c.id }); cast(room, roomMsg(room));
    return;
  }
  if (!r) return;
  if (m.t === 'cfg' && r.host === c && !r.racing) { r.cfg = { lvl: Math.round(num(m.lvl, 0, 5)), bots: !!m.bots, hints: !!m.hints, races: Math.round(num(m.races, 1, 5)) || 1, crs: course(m.crs) }; r.raceNo = 0; return cast(r, roomMsg(r)); }
  if (m.t === 'start' && r.host === c && !r.racing) {   // a series: race 1..n, then the next start begins a new series
    if (r.raceNo >= r.cfg.races) r.raceNo = 0;
    r.racing = true; for (const x of r.members) x.fin = false; cast(r, { t: 'start', cfg: r.cfg, order: r.members.map(x => x.id), race: r.raceNo, of: r.cfg.races }); return cast(r, roomMsg(r));
  }
  if (m.t === 'end' && r.host === c && r.racing) { r.racing = false; r.raceNo++; return cast(r, roomMsg(r)); }
  if (m.t === 'bfin' && r.host === c && r.racing) return cast(r, { t: 'bfin', i: Math.round(num(m.i, 0, 4)), time: num(m.time, 0, 36000) }, c);   // a bot of the host finished
  if (m.t === 'pos' && r.racing) {
    if (!nums(m.d, 10)) return;
    const o = { t: 'pos', from: c.id, d: m.d };
    if (r.host === c && Array.isArray(m.b) && m.b.length <= 5 && m.b.every(x => nums(x, 8))) o.b = m.b;
    return cast(r, o, c);
  }
  if (m.t === 'fin' && r.racing && !c.fin) {
    c.fin = true; cast(r, { t: 'fin', from: c.id, nick: c.nick, time: num(m.time, 0, 36000) });
    if (r.members.every(x => x.fin)) { r.racing = false; r.raceNo++; cast(r, roomMsg(r)); }
    return;
  }
  if (m.t === 'bp' && r.racing && r.host !== c) return send(r.host, { t: 'bp', i: Math.round(num(m.i, 0, 4)) });   // a bot fouled this player: the host makes it do its turns
  if (m.t === 'voice') { c.voice = !!m.on; return cast(r, roomMsg(r)); }
  if (m.t === 'rtc') {   // voice chat: pass the message on, only inside the same room
    const to = r.members.find(x => x.id === m.to && x !== c); if (!to) return;
    const sdp = m.sdp && typeof m.sdp.sdp === 'string' && m.sdp.sdp.length < 20000 && ['offer', 'answer'].includes(m.sdp.type) ? { type: m.sdp.type, sdp: m.sdp.sdp } : null;
    const ice = m.ice && typeof m.ice.candidate === 'string' && m.ice.candidate.length < 2000 ? { candidate: m.ice.candidate, sdpMid: m.ice.sdpMid, sdpMLineIndex: m.ice.sdpMLineIndex } : null;
    if (sdp || ice) send(to, { t: 'rtc', from: c.id, sdp, ice });
    return;
  }
  if (m.t === 'leave') leave(c);
}
server.listen(PORT, () => console.log('Optimist Academy live server on port ' + PORT));
