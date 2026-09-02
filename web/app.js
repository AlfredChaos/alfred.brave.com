// Brave IM 测试客户端（dev-task §6：单页原生 JS，无框架无构建）。
// 客户端义务（D15）：消息按 conv 内 seq 排序去重；断线重连按 lastSeq 补拉；按 msg_id 幂等。
"use strict";

// ---------- 全局状态 ----------
const S = {
  gwBase: localStorage.getItem("gw") || "http://127.0.0.1:37001",
  token: null, uid: null, name: null,
  ws: null, wsAddr: null,
  // 会话状态：convId -> { lastSeq, seen: Map(msgId), pending: Map(cliMsgId->el) }
  convs: new Map(),
  activeChat: null,      // {convId, peerName, peerUid}
  activeGroup: null,     // {gid, name, isOwner}
  feedCursor: "",
};

// ---------- 工具 ----------
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
function now() { return new Date().toLocaleTimeString(); }

async function api(method, path, body) {
  const opt = { method, headers: { "Content-Type": "application/json" } };
  if (S.token) opt.headers["Authorization"] = "Bearer " + S.token;
  if (body !== undefined) opt.body = JSON.stringify(body);
  const resp = await fetch(S.gwBase + path, opt);
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.error || resp.status);
  return data;
}

// ---------- 设置页 ----------
function initView() {
  $("gw-input").value = S.gwBase;
  $("ws-override-toggle").onchange = () => $("ws-override-row").classList.toggle("hidden");
  $("btn-test-gw").onclick = testGateway;
  $("btn-register").onclick = () => account("register");
  $("btn-login").onclick = () => account("login");
  document.querySelectorAll("nav .tab").forEach(btn => btn.onclick = () => switchTab(btn.dataset.tab));
  $("btn-search-users").onclick = searchUsers;
  $("chat-input").addEventListener("keydown", e => { if (e.key === "Enter") sendChat(); });
  $("btn-send-chat").onclick = sendChat;
  $("btn-create-group").onclick = createGroup;
  $("btn-add-member").onclick = addMember;
  $("group-msg-input").addEventListener("keydown", e => { if (e.key === "Enter") sendGroup(); });
  $("btn-send-group").onclick = sendGroup;
  $("btn-publish").onclick = publishPost;
  $("btn-feed-more").onclick = () => loadFeed(true);
}

async function testGateway() {
  $("gw-test-result").textContent = "测试中…";
  S.gwBase = $("gw-input").value.replace(/\/$/, "");
  localStorage.setItem("gw", S.gwBase);
  try {
    const resp = await fetch(S.gwBase + "/index");
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
    localStorage.setItem("uid", S.uid);
    let wsAddr = data.ws_addr;
    if ($("ws-override-toggle").checked && $("ws-override-input").value) {
      wsAddr = $("ws-override-input").value; // 调试覆盖
    }
    enterApp(wsAddr);
  } catch (e) {
    $("acct-result").textContent = "❌ " + e.message;
  }
}

function enterApp(wsAddr) {
  $("settings-view").classList.add("hidden");
  $("app-view").classList.remove("hidden");
  $("whoami").textContent = `${S.name} (${S.uid.slice(0, 8)}…)`;
  connectWS(wsAddr);
  loadFeed(false);
}

// ---------- WebSocket（心跳 30s + 重连补拉） ----------
function connectWS(wsAddr) {
  S.wsAddr = wsAddr;
  const url = "ws://" + wsAddr + "/ws/" + S.uid;
  log("sys", `连接 ${url}`);
  const ws = new WebSocket(url);
  S.ws = ws;
  ws.onopen = () => {
    log("sys", "已连接 " + wsAddr);
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
    log("sys", "连接断开，3 秒后重连…");
    setTimeout(() => connectWS(S.wsAddr), 3000);
  };
}

function send(cmd, data) {
  if (S.ws && S.ws.readyState === 1) S.ws.send(JSON.stringify({ cmd, data }));
}

// 消息接收：按 seq 排序去重（D15），已有序号补空洞走补拉
function onMessage(p) {
  const conv = ensureConv(p.conv_id);
  if (conv.seen.has(p.msg_id)) return;               // 幂等去重
  conv.seen.set(p.msg_id, true);
  if (p.seq > conv.lastSeq + 1 && conv.lastSeq > 0) pullHistory(p.conv_id); // 空洞 → 补拉
  conv.lastSeq = Math.max(conv.lastSeq, p.seq);
  const mention = p.mention ? " @我" : "";
  log("peer", `${esc(p.from_uid.slice(0, 8))}: ${esc(textOf(p.content))}${mention}`, p.conv_id, p.seq);
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
      const mine = m.from_uid === S.uid;
      log(mine ? "mine" : "peer", esc(textOf(m.content)), convId, m.seq, mine ? "" : esc(m.from_uid.slice(0, 8)));
    }
  } catch (e) { /* 会话不属于自己等场景静默（列表刷新时会纠正） */ }
}

function textOf(content) {
  try { const c = typeof content === "string" ? JSON.parse(content) : content; return c.text ?? JSON.stringify(c); }
  catch { return String(content); }
}

function ensureConv(convId) {
  if (!S.convs.has(convId)) S.convs.set(convId, { lastSeq: 0, seen: new Map(), pending: new Map() });
  return S.convs.get(convId);
}

// ---------- 单聊 ----------
async function searchUsers() {
  const kw = $("chat-search").value.trim();
  const data = await api("GET", "/v1/users").catch(() => ({ users: [] }));
  const ul = $("user-list"); ul.innerHTML = "";
  for (const u of data.users || []) {
    if (u.uid === S.uid) continue;
    if (kw && !u.user_name.includes(kw)) continue;
    const li = document.createElement("li");
    li.innerHTML = `<b>${esc(u.user_name)}</b><div class="sub">${esc(u.uid.slice(0, 13))}…</div>`;
    li.onclick = () => openChatWith(u);
    ul.appendChild(li);
  }
}

async function openChatWith(user) {
  try {
    const conv = await api("POST", "/v1/conversations", { to_uid: user.uid });
    S.activeChat = { convId: conv.conv_id, peerName: user.user_name, peerUid: user.uid };
    $("conv-title").textContent = `与 ${user.user_name} 的会话（conv ${conv.conv_id.slice(0, 8)}…）`;
    const el = $("chat-log"); el.innerHTML = ""; ensureConv(conv.conv_id);
    // 渲染已有历史（补拉语义：本地 lastSeq=0 全量拉第一页）
    await pullHistory(conv.conv_id);
    // pullHistory 渲染进全局 log 前先清空 —— 简化：直接重渲
    renderLogTo(el, conv.conv_id);
    logTo(el, "sys", "会话就绪，发消息试试（跨 CS 投递会经 Kafka + gRPC）");
  } catch (e) { alert(e.message); }
}

function sendChat() {
  if (!S.activeChat) return alert("先选一个用户");
  const text = $("chat-input").value.trim();
  if (!text) return;
  const cliMsgId = "c-" + Date.now() + "-" + Math.random().toString(36).slice(2, 8);
  send("msg", { conv_id: S.activeChat.convId, to_uid: S.activeChat.peerUid, cli_msg_id: cliMsgId, content: { text } });
  const conv = ensureConv(S.activeChat.convId);
  const el = renderMine($("chat-log"), esc(text), cliMsgId);
  conv.pending.set(cliMsgId, el);
  $("chat-input").value = "";
}

// ---------- 群聊 ----------
async function refreshGroups() {
  const data = await api("GET", "/v1/groups").catch(() => ({ groups: [] }));
  const ul = $("group-list"); ul.innerHTML = "";
  for (const g of data.groups || []) {
    const li = document.createElement("li");
    const owner = g.owner_uid === S.uid;
    li.innerHTML = `<b>${esc(g.name)}</b> <span class="sub">${g.member_count} 人${owner ? " · 我是群主" : ""}</span><div class="sub">${esc(g.gid)}</div>`;
    li.onclick = () => openGroup(g, owner);
    ul.appendChild(li);
  }
}

async function createGroup() {
  const name = $("group-name-input").value.trim();
  if (!name) return;
  try {
    // 以当前单聊对象为初始成员（测试路径最短）；无人选中则建只有自己的群
    const members = S.activeChat ? [S.activeChat.peerUid] : [];
    const g = await api("POST", "/v1/groups", { name, members });
    alert("建群成功 gid=" + g.gid);
    refreshGroups();
  } catch (e) { alert(e.message); }
}

async function addMember() {
  if (!S.activeGroup) return alert("先选群");
  const name = $("group-add-input").value.trim();
  try {
    const users = await api("GET", "/v1/users");
    const target = (users.users || []).find(u => u.user_name === name);
    if (!target) return alert("用户不存在");
    await api("POST", `/v1/groups/${S.activeGroup.gid}/members`, { uids: [target.uid] });
    refreshGroups();
  } catch (e) { alert(e.message); }
}

function openGroup(g, isOwner) {
  S.activeGroup = { gid: g.gid, name: g.name, isOwner };
  $("group-title").textContent = `${g.name}（gid ${g.gid}）${isOwner ? " · 群主" : " · 成员"}`;
  const el = $("group-log"); el.innerHTML = "";
  ensureConv(g.gid);
  pullHistory(g.gid).then(() => renderLogTo(el, g.gid));
}

function sendGroup() {
  if (!S.activeGroup) return alert("先选群");
  const text = $("group-msg-input").value.trim();
  if (!text) return;
  const mentionAll = $("mention-all").checked;
  if (mentionAll && !S.activeGroup.isOwner) return alert("仅群主可以 @所有人（§5 权限矩阵，persist 会拒收）");
  const cliMsgId = "g-" + Date.now();
  const content = { text };
  if (mentionAll) content.mention_all = true;
  send("msg", { group: true, conv_id: S.activeGroup.gid, cli_msg_id: cliMsgId, content });
  const conv = ensureConv(S.activeGroup.gid);
  const el = renderMine($("group-log"), esc(text) + (mentionAll ? " @所有人" : ""), cliMsgId);
  conv.pending.set(cliMsgId, el);
  $("group-msg-input").value = "";
}

// ---------- 朋友圈 ----------
async function publishPost() {
  const text = $("post-text").value.trim();
  if (!text) return;
  try {
    const r = await api("POST", "/v1/feed", { text });
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
      div.innerHTML = `
        <div class="author">${esc(p.author_name)} <span class="muted">${new Date(p.created_at).toLocaleString()}</span></div>
        <div class="text">${esc(textOf(p.content))}</div>
        <div class="actions">
          <button class="like-btn">👍 <span>${p.like_cnt}</span></button>
          <span>💬 ${p.comment_cnt}</span>
        </div>`;
      div.querySelector(".like-btn").onclick = async (ev) => {
        const r = await api("POST", `/v1/feed/${p.post_id}/like`).catch(() => null);
        if (r) ev.currentTarget.querySelector("span").textContent = Number(ev.currentTarget.querySelector("span").textContent) + (r.liked ? 1 : -1);
      };
      $("feed-list").appendChild(div);
    }
    $("btn-feed-more").disabled = !S.feedCursor;
  } catch (e) { console.warn("feed load failed", e); }
}

// ---------- 渲染 ----------
const LOGSTORE = new Map(); // convId -> [{cls, html}]，切会话时重渲（简化实现）

function logTo(el, cls, html) {
  const line = document.createElement("div");
  line.className = "line " + (cls === "mine" ? "mine" : cls === "sys" ? "event" : "");
  line.innerHTML = cls === "mine"
    ? `<div class="bubble">${html} <span class="tick pending">✓</span><div class="meta">${now()}</div></div>`
    : cls === "sys" ? html : `<div class="bubble">${html}<div class="meta">${now()}</div></div>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function log(cls, html, convId, seq, from) {
  const target = S.activeChat && convId === S.activeChat.convId ? $("chat-log")
    : S.activeGroup && convId === S.activeGroup.gid ? $("group-log") : null;
  if (!target) return;
  const meta = seq ? `<div class="meta">seq ${seq}${from ? " · " + from : ""}</div>` : "";
  logTo(target, cls, cls === "sys" ? html : html + (cls === "mine" ? "" : "") + (seq ? meta : ""));
}

function renderMine(el, html, cliMsgId) {
  const line = document.createElement("div");
  line.className = "line mine";
  line.innerHTML = `<div class="bubble" data-cli="${cliMsgId}">${html} <span class="tick pending">✓</span><div class="meta">${now()}</div></div>`;
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
  return line;
}

// 切换会话时按 LOGSTORE 重渲（简化：直接重拉最新页）
async function renderLogTo(el, convId) {
  const conv = ensureConv(convId);
  const data = await api("GET", `/v1/conversations/${convId}/messages?after_seq=0&limit=50`).catch(() => ({ messages: [] }));
  el.innerHTML = "";
  for (const m of data.messages || []) {
    const mine = m.from_uid === S.uid;
    if (m.type === "system_event") { logTo(el, "sys", "📣 " + eventText(m.content)); continue; }
    logTo(el, mine ? "mine" : "peer", esc(textOf(m.content)));
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

// ---------- Tab 切换 ----------
function switchTab(name) {
  document.querySelectorAll("nav .tab").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".tabpane").forEach(p => p.classList.add("hidden"));
  $("tab-" + name).classList.remove("hidden");
  if (name === "group") refreshGroups();
  if (name === "settings") { $("settings-view").classList.remove("hidden"); $("app-view").classList.add("hidden"); }
  else { $("settings-view").classList.add("hidden"); $("app-view").classList.remove("hidden"); }
}

initView();
