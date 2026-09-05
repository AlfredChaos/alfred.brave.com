// Brave IM 测试客户端（dev-task §6：单页原生 JS，无框架无构建）。
// 客户端义务（D15）：消息按 conv 内 seq 排序去重；断线重连按 lastSeq 补拉；按 msg_id 幂等。
// 交互范式对齐微信桌面版：单聊/群聊同级统一会话列表；名字在气泡上方、时间在气泡下方；
// 输入框 Enter 发送（Shift+Enter 换行）、可拖高；@所有人/@all 与 @名字 用关键字解析，
// 不做独立按钮（seq 仅内部使用，不在 UI 体现）。
"use strict";

// ---------- 全局状态 ----------
const S = {
  gwBase: localStorage.getItem("gw") || "http://127.0.0.1:37001",
  token: null, uid: null, name: null,
  ws: null, wsAddr: null,
  // convId -> { lastSeq, seen: Map(msgId), pending: Map(cliMsgId->el) }
  convs: new Map(),
  // 当前打开的会话：{ convId, kind: 'single'|'group', name, peerUid?, gid?, isOwner? }
  active: null,
  // 会话列表：convId -> { kind, name, peerUid?, ownerUid?, lastPreview, lastTs, unread }
  sessions: new Map(),
  // 群原始数据：gid -> {gid, name, owner_uid, member_count}
  groupsRaw: new Map(),
  feedCursor: "",
  // uid -> 用户名（搜索/登录/群消息渐进缓存；消息流显示名而非裸 uid）
  usernames: JSON.parse(localStorage.getItem("usernames") || "{}"),
};

// 会话持久化（#7：刷新丢登录态）——token 存 sessionStorage，关窗即失效，安全与便利折中
function saveSession(wsAddr) {
  sessionStorage.setItem("brave-session", JSON.stringify({ token: S.token, uid: S.uid, name: S.name, wsAddr }));
}
function restoreSession() {
  try { return JSON.parse(sessionStorage.getItem("brave-session") || "null"); } catch { return null; }
}

// ---------- 工具 ----------
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
function now() { return new Date().toLocaleTimeString("zh-CN", { hour12: false }); }

async function api(method, path, body) {
  const opt = { method, headers: { "Content-Type": "application/json" } };
  if (S.token) opt.headers["Authorization"] = "Bearer " + S.token;
  if (body !== undefined) opt.body = JSON.stringify(body);
  const resp = await fetch(S.gwBase + path, opt);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data.error || resp.status);
    err.status = resp.status;
    throw err;
  }
  return data;
}

// 头像色块：按用户名 hash 取色（无头像数据源的克制替代）
const AV_COLORS = ["g", "b", "o", "p", "t"];
function avatarCls(name) { let h = 0; for (const c of String(name)) h = (h * 31 + c.codePointAt(0)) >>> 0; return AV_COLORS[h % AV_COLORS.length]; }
function avatarHtml(name, cls) { return `<span class="avatar ${cls || avatarCls(name)}">${esc(String(name || "?").slice(0, 1).toUpperCase())}</span>`; }

// ---------- 初始化 ----------
function initView() {
  $("gw-input").value = S.gwBase;
  $("ws-override-toggle").onchange = () => $("ws-override-row").classList.toggle("hidden");
  $("btn-test-gw").onclick = testGateway;
  $("btn-register").onclick = () => account("register");
  $("btn-login").onclick = () => account("login");
  $("btn-close-settings").onclick = () => switchTab("chat"); // 登录后从设置页返回（未登录时该按钮隐藏）
  document.querySelectorAll("nav .tab").forEach(btn => btn.onclick = () => switchTab(btn.dataset.tab));
  // 刷新后恢复会话（#7）：token 仍有效则直接进入，失效由首个 API 调用报错兜底
  const sess = restoreSession();
  if (sess && sess.token) {
    S.token = sess.token; S.uid = sess.uid; S.name = sess.name;
    enterApp(sess.wsAddr);
  }
  $("btn-search-users").onclick = () => searchUsers($("chat-search").value.trim());
  $("chat-search").addEventListener("keydown", e => { if (e.key === "Enter") searchUsers($("chat-search").value.trim()); });
  // 聊天输入：Enter 发送、Shift+Enter 换行；无发送按钮（对齐微信）
  $("chat-input").addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendCurrent(); }
  });
  // 输入框高度拖拽：手柄在顶边右上角、向上拖增高（底边贴窗口底部不动）。
  // 不用 textarea 原生 resize —— 右下角手柄向下拖放大，对贴底输入框是反直觉的
  initComposerResize();
  $("btn-new-group").onclick = () => $("group-create-row").classList.toggle("hidden");
  $("btn-create-group").onclick = createGroup;
  // 通讯录
  $("btn-search-users2").onclick = () => loadContacts($("contact-search").value.trim());
  $("contact-search").addEventListener("keydown", e => { if (e.key === "Enter") loadContacts($("contact-search").value.trim()); });
  // 朋友圈
  $("btn-publish").onclick = publishPost;
  $("btn-feed-more").onclick = () => loadFeed(true);
}

async function testGateway() {
  $("gw-test-result").textContent = "测试中…";
  S.gwBase = $("gw-input").value.replace(/\/$/, "");
  localStorage.setItem("gw", S.gwBase);
  try {
    const resp = await fetch(S.gwBase + "/v1/index"); // DefaultIndex 挂在 /v1 分组下
    $("gw-test-result").textContent = resp.ok ? "✅ 网关可达" : "⚠️ 返回 " + resp.status;
  } catch (e) {
    $("gw-test-result").textContent = "❌ 无法连接：" + e.message;
  }
}

async function account(action) {
  const name = $("acct-user").value.trim(), pass = $("acct-pass").value;
  try {
    if (action === "register") {
      await api("POST", "/v1/register", { user_name: name, email: name + "@brave.local", password: pass });
      $("acct-result").textContent = "注册成功，自动登录…";
    }
    const data = await api("POST", "/v1/login", { user_name: name, password: pass });
    S.token = data.token; S.uid = data.uid; S.name = data.user_name;
    S.usernames[data.uid] = data.user_name;
    localStorage.setItem("usernames", JSON.stringify(S.usernames));
    localStorage.setItem("uid", S.uid);
    let wsAddr = data.ws_addr;
    if ($("ws-override-toggle").checked && $("ws-override-input").value) {
      wsAddr = $("ws-override-input").value; // 调试覆盖
    }
    saveSession(wsAddr);
    enterApp(wsAddr);
  } catch (e) {
    $("acct-result").textContent = e.status === 409 ? "❌ 用户名已被占用，换一个试试" : "❌ " + e.message;
  }
}

function enterApp(wsAddr) {
  $("settings-view").classList.add("hidden");
  $("btn-close-settings").classList.add("hidden"); // 已登录：设置页可通过导航再进
  $("app-view").classList.remove("hidden");
  // 导航头像位只放名字首字，悬浮提示完整身份
  $("whoami").textContent = String(S.name || "?").slice(0, 1).toUpperCase();
  $("whoami").title = `${S.name} (${S.uid.slice(0, 8)}…)`;
  connectWS(wsAddr);
  refreshSessions();
  loadFeed(false);
}

// ---------- WebSocket（心跳 30s + 重连补拉） ----------
function wsDot(on, tip) {
  const dot = $("ws-dot");
  if (!dot) return;
  dot.classList.toggle("on", on);
  dot.classList.toggle("off", !on);
  dot.title = tip;
}

function connectWS(wsAddr) {
  S.wsAddr = wsAddr;
  const url = "ws://" + wsAddr + "/ws/" + S.uid;
  wsDot(false, "连接中…");
  const ws = new WebSocket(url);
  S.ws = ws;
  ws.onopen = () => {
    wsDot(true, "已连接 " + wsAddr);
    S.heartbeat = setInterval(() => send("heartbeat", {}), 30000);
    // 断线重连/首连：对所有已知会话按 lastSeq 补拉（D15 客户端义务）
    for (const [convId] of S.convs) pullHistory(convId);
  };
  ws.onmessage = (ev) => {
    let frame; try { frame = JSON.parse(ev.data); } catch { return; }
    if (frame.code !== undefined) return;            // 受理回执 {code:200}
    if (frame.cmd === "msg") onMessage(frame.data);
    if (frame.cmd === "ack") onAck(frame.data);
  };
  ws.onclose = () => {
    clearInterval(S.heartbeat);
    wsDot(false, "连接断开，重连中…");
    setTimeout(() => connectWS(S.wsAddr), 3000);
  };
}

function send(cmd, data) {
  if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify({ cmd, data }));
}

// 显示名：优先已缓存的用户名，降级 uid 前 8 位
function displayName(uid) {
  return S.usernames[uid] || uid.slice(0, 8);
}

// ---------- 会话列表（单聊 + 群聊同级） ----------
async function refreshSessions() {
  try {
    const [convData, groupData] = await Promise.all([
      api("GET", "/v1/conversations"),
      api("GET", "/v1/groups").catch(() => ({ groups: [] })),
    ]);
    S.groupsRaw = new Map((groupData.groups || []).map(g => [g.gid, g]));
    for (const c of convData.conversations || []) {
      if (S.sessions.has(c.conv_id)) continue; // 已有会话保留未读/预览等本地状态
      if (c.type === "group") {
        const g = S.groupsRaw.get(c.conv_id);
        S.sessions.set(c.conv_id, {
          kind: "group", name: g ? g.name : c.conv_id, ownerUid: g ? g.owner_uid : "",
          lastPreview: "", lastTs: 0, unread: 0,
        });
      } else {
        const peerUid = (c.members || []).find(u => u !== S.uid) || "";
        const name = displayName(peerUid);
        S.usernames[peerUid] = name; // 尽量缓存
        S.sessions.set(c.conv_id, {
          kind: "single", name, peerUid, lastPreview: "", lastTs: 0, unread: 0,
        });
      }
    }
    localStorage.setItem("usernames", JSON.stringify(S.usernames));
    renderSessionList();
  } catch (e) { console.warn("refresh sessions failed", e); }
}

// 会话列表项时间：今天 HH:MM，昨天标"昨天"，更早 M/D
function fmtListTime(ts) {
  if (!ts) return "";
  const d = new Date(ts), nowD = new Date();
  const sameDay = d.toDateString() === nowD.toDateString();
  if (sameDay) return d.toLocaleTimeString("zh-CN", { hour12: false, hour: "2-digit", minute: "2-digit" });
  const yesterday = new Date(nowD); yesterday.setDate(nowD.getDate() - 1);
  if (d.toDateString() === yesterday.toDateString()) return "昨天";
  return `${d.getMonth() + 1}/${d.getDate()}`;
}

function renderSessionList() {
  const ul = $("session-list");
  ul.innerHTML = "";
  const items = [...S.sessions.entries()].sort((a, b) => (b[1].lastTs || 0) - (a[1].lastTs || 0));
  for (const [convId, s] of items) {
    const li = document.createElement("li");
    li.className = "sess" + (S.active && S.active.convId === convId ? " active" : "");
    li.innerHTML = `${avatarHtml(s.name, s.kind === "group" ? "g" : "")}
      <div class="min-w-0">
        <b>${esc(s.name)}</b>
        <div class="sub prev">${esc(s.lastPreview || (s.kind === "group" ? "群聊" : "发消息打个招呼吧"))}</div>
      </div>
      <div class="sess-right">
        <span class="sess-time">${fmtListTime(s.lastTs)}</span>
        ${s.unread ? `<span class="unread">${s.unread > 99 ? "99+" : s.unread}</span>` : ""}
      </div>`;
    li.onclick = () => openSession(convId);
    ul.appendChild(li);
  }
}

// touchSession 收到/发出消息时更新会话列表条目（预览 + 未读 + 置顶）
function touchSession(convId, preview, fromUid) {
  const s = S.sessions.get(convId);
  if (!s) return;
  s.lastPreview = (fromUid === S.uid ? "我： " : "") + preview.slice(0, 40);
  s.lastTs = Date.now();
  const isActive = S.active && S.active.convId === convId;
  if (!isActive && fromUid !== S.uid) s.unread = (s.unread || 0) + 1;
  renderSessionList();
}

// openSession 统一打开单聊/群聊
async function openSession(convId) {
  const s = S.sessions.get(convId);
  if (!s) return;
  S.active = {
    convId, kind: s.kind, name: s.name,
    peerUid: s.peerUid, gid: s.kind === "group" ? convId : undefined,
    isOwner: s.ownerUid === S.uid,
  };
  s.unread = 0;
  $("conv-title").textContent = s.kind === "group"
    ? `${s.name}（${(S.groupsRaw.get(convId)?.member_count) || "?"}人）${s.ownerUid === S.uid ? " · 群主" : ""}`
    : s.name;
  const el = $("chat-log"); el.innerHTML = "";
  ensureConv(convId);
  await renderLogTo(el, convId);
  renderSessionList();
}

// 从通讯录/搜索打开单聊：确保会话存在 → 刷新列表 → 打开
async function openChatWith(user) {
  try {
    const conv = await api("POST", "/v1/conversations", { to_uid: user.uid });
    S.usernames[user.uid] = user.user_name;
    localStorage.setItem("usernames", JSON.stringify(S.usernames));
    await refreshSessions();
    await openSession(conv.conv_id);
    switchTab("chat");
  } catch (e) { alert(e.message); }
}

// ---------- 消息接收 ----------
function onMessage(p) {
  const conv = ensureConv(p.conv_id);
  if (conv.seen.has(p.msg_id)) return;               // 幂等去重
  // 自己发的消息经群 fanout 回推：本地 sendCurrent 已渲染（pending 靠 ack 置 ✓✓），
  // 这里只推进 seq/seen，不重复渲染
  if (p.from_uid === S.uid) {
    conv.seen.set(p.msg_id, true);
    conv.lastSeq = Math.max(conv.lastSeq, p.seq);
    return;
  }
  conv.seen.set(p.msg_id, true);
  if (p.seq > conv.lastSeq + 1 && conv.lastSeq > 0) pullHistory(p.conv_id); // 空洞 → 补拉
  conv.lastSeq = Math.max(conv.lastSeq, p.seq);
  if (p.type === "system_event") {
    if (S.active && S.active.convId === p.conv_id) logTo($("chat-log"), "sys", "📣 " + esc(eventText(p.content)));
    return;
  }
  const from = displayName(p.from_uid);
  S.usernames[p.from_uid] = from; // 渐进缓存
  const text = textOf(p.content);
  // 会话列表更新（预览/未读/置顶）+ 未打开该会话时 toast
  touchSession(p.conv_id, text, p.from_uid);
  if (!S.active || S.active.convId !== p.conv_id) {
    toast(`${from}：${text.slice(0, 40)}`, "新消息");
  }
  renderPeerLine($("chat-log"), p, text, now());
}

function onAck(a) {
  const conv = S.convs.get(a.conv_id);
  const pending = conv && conv.pending.get(a.cli_msg_id);
  if (pending) {
    pending.querySelector(".tick").textContent = "✓✓";
    pending.querySelector(".tick").classList.remove("pending");
    conv.pending.delete(a.cli_msg_id);
  }
}

async function pullHistory(convId) {
  try {
    const conv = ensureConv(convId);
    const data = await api("GET", `/v1/conversations/${convId}/messages?after_seq=${conv.lastSeq}&limit=50`);
    for (const m of data.messages || []) {
      if (conv.seen.has(m.msg_id)) continue;
      conv.seen.set(m.msg_id, true);
      conv.lastSeq = Math.max(conv.lastSeq, m.seq);
      if (!(S.active && S.active.convId === convId)) continue;
      renderMsgLine($("chat-log"), m);
    }
  } catch (e) { /* 会话不属于自己等场景静默 */ }
}

// ---------- 发送（Enter 驱动，@关键字解析） ----------
function sendCurrent() {
  if (!S.active) return alert("先选择一个会话");
  const ta = $("chat-input");
  const text = ta.value.trim();
  if (!text) return;
  const isGroup = S.active.kind === "group";
  let content = { text };
  if (isGroup) {
    // @所有人 / @all：关键字解析，不做独立按钮；权限矩阵由 persist 校验，前端预检提示
    if (/@(所有人|all|everyone)\b/i.test(text)) {
      if (!S.active.isOwner) return alert("仅群主可以 @所有人");
      content.mention_all = true;
    }
    // @名字 → mentions 必须是 uid（persist 按成员表校验）；解析不出 uid 的名字
    // 仅保留视觉高亮、不进 mentions，避免无谓的毒消息拒收
    const nameToUid = {};
    for (const [uid, n] of Object.entries(S.usernames)) nameToUid[n] = uid;
    const rawNames = [...text.matchAll(/@([\u4e00-\u9fa5A-Za-z0-9_]{1,20})/g)]
      .map(m => m[1]).filter(n => !/^(所有人|all|everyone)$/i.test(n));
    const mentionUids = [...new Set(rawNames.map(n => nameToUid[n]).filter(Boolean))];
    if (mentionUids.length) content.mentions = mentionUids;
  }
  const cliMsgId = "m-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8);
  send("msg", isGroup
    ? { group: true, conv_id: S.active.convId, cli_msg_id: cliMsgId, content }
    : { conv_id: S.active.convId, to_uid: S.active.peerUid, cli_msg_id: cliMsgId, content });
  const conv = ensureConv(S.active.convId);
  const el = renderMine($("chat-log"), highlightMentions(esc(text)), cliMsgId);
  conv.pending.set(cliMsgId, el);
  touchSession(S.active.convId, text, S.uid);
  ta.value = "";
}

// @关键字高亮：一次扫描避免嵌套替换；@所有人/@all 一类样式，@名字另一类
function highlightMentions(escapedText) {
  return escapedText.replace(/@(所有人|all|everyone)|@([\u4e00-\u9fa5A-Za-z0-9_]{1,20})/gi,
    (m0, all) => all !== undefined
      ? `<mark class="mention-all">${m0}</mark>`
      : `<mark class="mention">${m0}</mark>`);
}

// ---------- 消息渲染（名字在气泡上方、时间在气泡下方、无 seq） ----------
// peer 实时消息
function renderPeerLine(el, p, text, time) {
  const isGroup = p.type === "group";
  const line = document.createElement("div");
  line.className = "line peer";
  const nameHtml = isGroup ? `<div class="msg-name">${esc(displayName(p.from_uid))}</div>` : "";
  line.innerHTML = `<span class="bub-av"></span>
    <div class="msg-col">
      ${nameHtml}
      <div class="bubble">${highlightMentions(esc(text))}${p.mention ? `<mark class="mention-all"> @我</mark>` : ""}</div>
      <div class="msg-meta">${time}</div>
    </div>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

// mine 实时消息（tick 发送态）。行是 row-reverse（主轴从右起）：头像 DOM 在前
// 才能贴窗口右缘，气泡在其左侧 —— 对齐微信（此前 DOM 顺序反了：气泡贴右缘）
function renderMine(el, html, cliMsgId) {
  const line = document.createElement("div");
  line.className = "line mine";
  line.innerHTML = `<span class="bub-av"></span><div class="msg-col">
      <div class="bubble" data-cli="${cliMsgId}">${html}</div>
      <div class="msg-meta">${now()} <span class="tick pending">✓</span></div>
    </div>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
  return line;
}

// 历史消息（补拉/打开会话）：与实时同结构，但用消息真实 created_at、不带发送态 tick
function renderMsgLine(el, m) {
  if (m.type === "system_event") {
    logTo(el, "sys", "📣 " + esc(eventText(m.content)));
    return;
  }
  const time = m.created_at ? new Date(m.created_at).toLocaleTimeString("zh-CN", { hour12: false }) : now();
  const mine = m.from_uid === S.uid;
  const line = document.createElement("div");
  line.className = "line " + (mine ? "mine" : "peer");
  if (mine) {
    // row-reverse 下头像 DOM 在前 → 贴窗口右缘（对齐微信）
    line.innerHTML = `<span class="bub-av"></span><div class="msg-col">
        <div class="bubble">${highlightMentions(esc(textOf(m.content)))}</div>
        <div class="msg-meta">${time}</div>
      </div>`;
  } else {
    const isGroup = m.type === "group";
    const nameHtml = isGroup ? `<div class="msg-name">${esc(displayName(m.from_uid))}</div>` : "";
    line.innerHTML = `<span class="bub-av"></span>
      <div class="msg-col">
        ${nameHtml}
        <div class="bubble">${highlightMentions(esc(textOf(m.content)))}</div>
        <div class="msg-meta">${time}</div>
      </div>`;
  }
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function textOf(content) {
  try { const c = typeof content === "string" ? JSON.parse(content) : content; return c.text ?? JSON.stringify(c); }
  catch { return String(content); }
}

function ensureConv(convId) {
  if (!S.convs.has(convId)) S.convs.set(convId, { lastSeq: 0, seen: new Map(), pending: new Map() });
  return S.convs.get(convId);
}

// ---------- 用户搜索 / 好友 ----------
// 搜索全部用户（渲染到指定列表，带 +好友 按钮）
async function searchUsersInto(ul, kw) {
  const data = await api("GET", "/v1/users").catch(() => ({ users: [] }));
  ul.innerHTML = "";
  for (const u of data.users || []) {
    if (u.uid === S.uid) continue;
    if (kw && !u.user_name.includes(kw)) continue;
    S.usernames[u.uid] = u.user_name;
    const li = document.createElement("li");
    li.innerHTML = `${avatarHtml(u.user_name)}<div class="min-w-0"><b>${esc(u.user_name)}</b><div class="sub">${esc(u.uid.slice(0, 13))}…</div></div>
      <button class="friend-btn" data-uid="${esc(u.uid)}" data-name="${esc(u.user_name)}" title="加好友">+ 好友</button>`;
    li.onclick = () => openChatWith(u);
    li.querySelector(".friend-btn").onclick = (ev) => { ev.stopPropagation(); addFriend(ev.currentTarget); };
    ul.appendChild(li);
  }
  localStorage.setItem("usernames", JSON.stringify(S.usernames));
}

function searchUsers(kw) {
  searchUsersInto($("user-list"), kw);
}

// 好友列表（通讯录主视图）
async function loadFriends() {
  const data = await api("GET", "/v1/friends").catch(() => ({ friends: [] }));
  const ul = $("friend-list");
  ul.innerHTML = "";
  for (const f of data.friends || []) {
    S.usernames[f.friend_uid] = f.friend_name;
    const li = document.createElement("li");
    li.innerHTML = `${avatarHtml(f.friend_name)}<div class="min-w-0"><b>${esc(f.friend_name)}</b><div class="sub">${esc(f.friend_uid.slice(0, 13))}…</div></div>
      <span class="sub">发消息 ›</span>`;
    li.onclick = () => openChatWith({ uid: f.friend_uid, user_name: f.friend_name });
    ul.appendChild(li);
  }
  if (!ul.children.length) {
    ul.innerHTML = `<li class="muted">还没有好友 — 用上方"找用户"搜索并添加。</li>`;
  }
  localStorage.setItem("usernames", JSON.stringify(S.usernames));
}

// 通讯录视图：有关键字搜全用户，否则显示好友列表
function loadContacts(kw) {
  if (kw) searchUsersInto($("friend-list"), kw);
  else loadFriends();
}

// addFriend 加好友（朋友圈 fanout 按好友关系分发，无好友 = 动态不可见）
async function addFriend(btn) {
  btn.disabled = true;
  try {
    await api("POST", "/v1/friends", { user_name: btn.dataset.name });
    btn.textContent = "✓ 好友";
    btn.classList.add("done");
  } catch (e) {
    btn.disabled = false;
    alert(e.status === 409 ? "已经是好友了" : "加好友失败：" + e.message);
    if (e.status === 409) { btn.textContent = "✓ 好友"; btn.classList.add("done"); }
  }
}

// ---------- 群 ----------
async function createGroup() {
  const name = $("group-name-input").value.trim();
  if (!name) return;
  try {
    // 以当前单聊对象为初始成员（测试路径最短）；无人选中则建只有自己的群
    const members = S.active && S.active.kind === "single" ? [S.active.peerUid] : [];
    const g = await api("POST", "/v1/groups", { name, members });
    $("group-create-row").classList.add("hidden");
    $("group-name-input").value = "";
    await refreshSessions();
    await openSession(g.gid);
  } catch (e) { alert(e.message); }
}

// ---------- 朋友圈 ----------
async function publishPost() {
  const text = $("post-text").value.trim();
  if (!text) return;
  try {
    await api("POST", "/v1/feed", { text });
    $("post-text").value = "";
    loadFeed(false);
  } catch (e) { alert(e.message); }
}

async function loadFeed(more) {
  try {
    const url = "/v1/feed?limit=10" + (more && S.feedCursor ? "&cursor=" + encodeURIComponent(S.feedCursor) : "");
    const data = await api("GET", url);
    S.feedCursor = data.next_cursor || "";
    if (!more) $("feed-list").innerHTML = "";
    for (const p of data.posts || []) {
      const div = document.createElement("div");
      div.className = "post";
      const ts = new Date(p.created_at).toLocaleString("zh-CN", { hour12: false });
      div.innerHTML = `
        <div class="author">${esc(p.author_name)} <span class="muted">${esc(ts)}</span></div>
        <div class="text">${esc(textOf(p.content))}</div>
        <div class="actions">
          <button class="like-btn">👍 <span>${p.like_cnt}</span></button>
          <span>💬 ${p.comment_cnt}</span>
        </div>`;
      div.querySelector(".like-btn").onclick = async (ev) => {
        const btn = ev.currentTarget; // await 之后事件对象的 currentTarget 会被置 null，先存引用
        const r = await api("POST", `/v1/feed/${p.post_id}/like`).catch(() => null);
        if (!r) return alert("点赞失败，请重试");
        btn.querySelector("span").textContent = Number(btn.querySelector("span").textContent) + (r.liked ? 1 : -1);
      };
      $("feed-list").appendChild(div);
    }
    $("btn-feed-more").disabled = !S.feedCursor;
  } catch (e) { console.warn("feed load failed", e); }
}

// ---------- 基础渲染 ----------
function logTo(el, cls, html) {
  const line = document.createElement("div");
  line.className = "line " + (cls === "mine" ? "mine" : cls === "sys" ? "event" : "peer");
  line.innerHTML = cls === "sys" ? html : "";
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
  return line;
}

async function renderLogTo(el, convId) {
  const data = await api("GET", `/v1/conversations/${convId}/messages?after_seq=0&limit=50`).catch(() => ({ messages: [] }));
  el.innerHTML = "";
  for (const m of data.messages || []) {
    renderMsgLine(el, m);
  }
}

function eventText(content) {
  try {
    const c = typeof content === "string" ? JSON.parse(content) : content;
    const names = { group_create: "群已创建", member_join: "新成员加入", member_kick: "有成员被移出群聊",
      member_quit: "有成员退出群聊", group_rename: "群已改名", announcement_set: "群公告已更新",
      pin_set: "消息已置顶", pin_unset: "取消置顶", group_dismiss: "群已解散" };
    return names[c.event] || c.event;
  } catch { return ""; }
}

// 输入框高度拖拽：向上拖增高（高度 = 起点 + 位移差），上限 60% 视口、下限 56px。
// 拖拽期间禁用文本选择，避免鼠标划过消息流时选中文字。
function initComposerResize() {
  const handle = $("chat-resize"), ta = $("chat-input");
  if (!handle || !ta) return;
  handle.addEventListener("mousedown", (e) => {
    e.preventDefault();
    const startY = e.clientY, startH = ta.getBoundingClientRect().height;
    const maxH = Math.floor(window.innerHeight * 0.6), minH = 56;
    const onMove = (ev) => {
      const h = Math.min(maxH, Math.max(minH, Math.round(startH + (startY - ev.clientY))));
      ta.style.height = h + "px";
    };
    const onUp = () => {
      document.body.classList.remove("resizing");
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    document.body.classList.add("resizing");
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  });
}

// ---------- Tab 切换 ----------
function switchTab(name) {
  document.querySelectorAll("nav .tab").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tabpane").forEach(p => p.classList.add("hidden"));
  $("tab-" + name)?.classList.remove("hidden");
  if (name === "settings") {
    $("settings-view").classList.remove("hidden");
    $("app-view").classList.add("hidden");
    $("btn-close-settings").classList.remove("hidden"); // 已登录可返回，未登录时 enterApp 会再藏起
  } else {
    $("settings-view").classList.add("hidden");
    $("app-view").classList.remove("hidden");
  }
  if (name === "contacts") loadContacts($("contact-search").value.trim());
  if (name === "feed") loadFeed(false); // 切到朋友圈刷新时间线
  if (name === "chat") renderSessionList();
}

// ---------- 轻量 toast ----------
function toast(text, title) {
  let box = document.getElementById("toast-box");
  if (!box) { box = document.createElement("div"); box.id = "toast-box"; document.body.appendChild(box); }
  const el = document.createElement("div");
  el.className = "toast";
  el.innerHTML = `${title ? `<b>${esc(title)}</b>` : ""}<div>${esc(text)}</div>`;
  box.appendChild(el);
  setTimeout(() => { el.classList.add("show"); }, 10);
  setTimeout(() => {
    el.classList.remove("show");
    setTimeout(() => el.remove(), 300);
  }, 5000);
}

initView();
