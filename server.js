// 꼰대력 테스트 서버 (무료, Node 기본 모듈만 사용)
// - kkondae.html 제공
// - /api : 방(room) 단위로 플레이어·채팅·결과를 저장하고, 변경 알림은 SSE(/api/events)로 보냅니다.
// - /api/random/* : 랜덤 채팅 대기열. 두 사람이 모이면 새 방 코드를 만들어 짝지어 줍니다.
// 내 PC에서는 127.0.0.1(터널로만 연결). Render 같은 호스팅에서는 HOST 환경변수로 0.0.0.0을 줍니다.
const http = require("http"), fs = require("fs"), path = require("path"), crypto = require("crypto");
const PORT = Number(process.env.PORT) || 2500, HOST = process.env.HOST || (process.env.PORT ? "0.0.0.0" : "127.0.0.1"); // 배포 환경(PORT 지정)에서는 외부 접속 허용
const FILE = path.join(__dirname, "kkondae.html");
const LOGO = path.join(__dirname, "logo.png");
const DATA = path.join(__dirname, "data.json"); // 방 데이터 저장 파일 (서버를 껐다 켜도 남아 있음)

// 컬렉션은 rooms/방코드/(players|msgs|meta) 형식만, 문서 id는 안전한 문자만 허용
const COL_RE = /^rooms\/[A-Z0-9]{3,6}\/(players|msgs|meta)$/;
const ID_RE = /^(?!__proto__$)[A-Za-z0-9_.-]{1,80}$/;
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const isObj = v => v && typeof v === "object" && !Array.isArray(v);
const merge = (a, b) => { const o = { ...a }; for (const k in b) o[k] = isObj(b[k]) && isObj(o[k]) ? merge(o[k], b[k]) : b[k]; return o; };

let db = {};
try { db = JSON.parse(fs.readFileSync(DATA, "utf8")) || {}; } catch (e) { db = {}; }
let saveTimer = null;
function saveSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DATA, JSON.stringify(db), () => {}), 300);
}

const streams = new Map(); // 방 코드 -> 그 방을 보고 있는 SSE 연결들
function notify(room) {
  const set = streams.get(room);
  if (set) for (const r of set) r.write("data: 1\n\n");
}

// ----- 랜덤 채팅 대기열 (메모리에만 보관) -----
const WAIT_MS = 120000, MATCH_MS = 300000, PACKS = ["work", "school", "family"];
const queue = [];           // { uid, pack, t } 기다리는 사람
const matched = new Map();  // uid -> { room, t } 짝이 정해졌지만 아직 알려주지 않은 사람
const CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";
function newRoomCode() {
  for (;;) {
    const code = Array.from(crypto.randomBytes(6), b => CODE_CHARS[b % CODE_CHARS.length]).join("");
    if (!db["rooms/" + code + "/meta"]) return code;
  }
}
function pruneLobby() {
  const now = Date.now();
  for (let i = queue.length - 1; i >= 0; i--) if (now - queue[i].t > WAIT_MS) queue.splice(i, 1);
  for (const [k, v] of matched) if (now - v.t > MATCH_MS) matched.delete(k);
}

function json(r, code, obj) {
  r.writeHead(code, { "Content-Type": "application/json;charset=utf-8", "Cache-Control": "no-store" });
  r.end(JSON.stringify(obj));
}
function readJson(req, cb) {
  let s = "", over = false;
  req.on("data", c => { if (over) return; s += c; if (s.length > 64 * 1024) { over = true; cb(new Error("too big")); } });
  req.on("end", () => { if (over) return; try { cb(null, JSON.parse(s || "{}")); } catch (e) { cb(e); } });
}

function api(req, r, url) {
  const p = url.pathname, q = url.searchParams;

  // 변경 알림 (방 단위)
  if (p === "/api/events" && req.method === "GET") {
    const room = q.get("room") || "";
    if (!/^[A-Z0-9]{3,6}$/.test(room)) return json(r, 400, { error: "bad-room" });
    r.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store", "Connection": "keep-alive" });
    r.write(": connected\n\n");
    if (!streams.has(room)) streams.set(room, new Set());
    streams.get(room).add(r);
    const ka = setInterval(() => r.write(": ping\n\n"), 25000);
    req.on("close", () => {
      clearInterval(ka);
      const set = streams.get(room);
      if (set) { set.delete(r); if (!set.size) streams.delete(room); }
    });
    return;
  }

  // 문서 1개 읽기
  if (p === "/api/get" && req.method === "GET") {
    const col = q.get("col") || "", id = q.get("id") || "";
    if (!COL_RE.test(col) || !ID_RE.test(id)) return json(r, 400, { error: "bad-path" });
    const c = db[col] || {};
    const has = hasOwn(c, id);
    return json(r, 200, { exists: has, data: has ? c[id] : null });
  }

  // 컬렉션 전체 읽기 (정렬·개수 제한은 브라우저에서 처리)
  if (p === "/api/list" && req.method === "GET") {
    const col = q.get("col") || "";
    if (!COL_RE.test(col)) return json(r, 400, { error: "bad-path" });
    const c = db[col] || {};
    return json(r, 200, { docs: Object.keys(c).map(id => ({ id, data: c[id] })) });
  }

  // 쓰기: set / update(병합) / delete
  if (p === "/api/write" && req.method === "POST") {
    return readJson(req, (err, b) => {
      if (err || !b || typeof b !== "object") return json(r, 400, { error: "bad-json" });
      const { op, col, id, data } = b;
      if (!COL_RE.test(col) || !ID_RE.test(id)) return json(r, 400, { error: "bad-path" });
      if (op !== "delete" && !isObj(data)) return json(r, 400, { error: "bad-data" });
      if (op !== "set" && op !== "update" && op !== "delete") return json(r, 400, { error: "bad-op" });
      const c = (db[col] = db[col] || {});
      if (op === "set") c[id] = data;
      else if (op === "update") {
        if (!hasOwn(c, id)) return json(r, 404, { error: "not-found" });
        c[id] = merge(c[id], data);
      } else {
        if (hasOwn(c, id)) delete c[id];
        if (!Object.keys(c).length) delete db[col];
      }
      saveSoon();
      notify(col.split("/")[1]);
      return json(r, 200, { ok: true });
    });
  }

  // 랜덤 채팅: 대기열에 들어가기. 기다리는 사람이 있으면 바로 짝지어 방을 만든다
  if (p === "/api/random/join" && req.method === "POST") {
    return readJson(req, (err, b) => {
      if (err || !b) return json(r, 400, { error: "bad-json" });
      const uid = String(b.uid || "");
      const pack = PACKS.includes(b.pack) ? b.pack : "work";
      if (!ID_RE.test(uid)) return json(r, 400, { error: "bad-uid" });
      pruneLobby();
      const idx = queue.findIndex(x => x.uid !== uid);
      if (idx >= 0) {
        const other = queue.splice(idx, 1)[0];
        const room = newRoomCode();
        db["rooms/" + room + "/meta"] = { info: { round: 1, pack: other.pack, seed: Math.floor(Math.random() * 1e9) + 1 } };
        saveSoon();
        matched.set(other.uid, { room, t: Date.now() });
        return json(r, 200, { room });
      }
      const mine = queue.findIndex(x => x.uid === uid);
      if (mine >= 0) queue.splice(mine, 1);
      queue.push({ uid, pack, t: Date.now() });
      return json(r, 200, { waiting: true });
    });
  }

  // 랜덤 채팅: 짝이 정해졌는지 확인 (기다리는 중 / 방 코드 / 대기 만료)
  if (p === "/api/random/status" && req.method === "GET") {
    const uid = q.get("uid") || "";
    if (!ID_RE.test(uid)) return json(r, 400, { error: "bad-uid" });
    pruneLobby();
    const m = matched.get(uid);
    if (m) { matched.delete(uid); return json(r, 200, { room: m.room }); }
    if (queue.some(x => x.uid === uid)) return json(r, 200, { waiting: true });
    return json(r, 200, { gone: true });
  }

  // 랜덤 채팅: 대기 취소
  if (p === "/api/random/cancel" && req.method === "POST") {
    return readJson(req, (err, b) => {
      const uid = b && String(b.uid || "");
      const i = queue.findIndex(x => x.uid === uid);
      if (i >= 0) queue.splice(i, 1);
      return json(r, 200, { ok: true });
    });
  }

  return json(r, 404, { error: "not-found" });
}

http.createServer((q, r) => {
  const url = new URL(q.url || "/", "http://localhost");
  if (url.pathname.startsWith("/api/")) return api(q, r, url);
  if (q.method !== "GET" && q.method !== "HEAD") { r.writeHead(405); return r.end(); }
  if (url.pathname === "/logo.png") { // 로고 이미지 (배경 투명)
    r.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "public, max-age=3600", "X-Content-Type-Options": "nosniff" });
    if (q.method === "HEAD") return r.end();
    return fs.createReadStream(LOGO).on("error", () => r.destroy()).pipe(r);
  }
  if (url.pathname !== "/" && url.pathname !== "/kkondae.html") { r.writeHead(404); return r.end("404"); }
  r.writeHead(200, { "Content-Type": "text/html;charset=utf-8", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff" });
  if (q.method === "HEAD") return r.end();
  fs.createReadStream(FILE).on("error", () => r.destroy()).pipe(r);
}).listen(PORT, HOST, () => console.log("http://localhost:" + PORT));
