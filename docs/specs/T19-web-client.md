# T19 · 简易测试客户端 + 压测骨架

## 1. 目标与范围

**做**：
1. `web/`（index.html + app.js + style.css，原生 JS 无框架无构建），gateway 静态托管 `/web/`：
   - 设置页（首个界面）：网关地址（localStorage，默认 http://127.0.0.1:37001）、WS 地址覆盖开关（调试）、连接测试按钮 ✅
   - 注册/登录（token+ws_addr→WS）✅
   - 单聊：找用户/建会话/实时收发/ACK 打勾（cli_msg_id 匹配 + ✓→✓✓）✅
   - 群聊：建群/拉人/发消息/@所有人（仅群主，UI 拦截 + persist 权威）✅
   - 朋友圈：发布/浏览（cursor 分页）/点赞 ✅
   - **D15 客户端义务**：消息按 conv 内 seq 排序去重（seen 集合 + 空洞触发补拉 after_seq）✅；30s 心跳 + 断线 3s 重连补拉 ✅
2. `tools/stress/`：hold（连接保持）与 storm（消息风暴）两模式骨架 + README（用法/诚实边界，不产出数字）。

**不做**：浏览器自动化验证（环境无浏览器），以 API 级等价验收覆盖并如实声明。

## 2. 验收记录

- API 级全流程（本地栈真实运行）：feed 发布（经网关代理）→ 加好友 → fanout 收件箱 → B 时间线见 A 的帖 → 点赞；建群 → 成员看到 system_event 流；`/web/` 静态资源 200×3 ✅
- `go run ./tools/e2e` 回归 PASS（3/3 稳定）✅
- 压测骨架：`go build ./tools/stress` 通过；**未执行**（明示）✅
- **真实浏览器验证（自动化 IAB）**：设置页/注册/自动登录/带 token 的用户列表在真实浏览器渲染与调用全部正常 ✅；
  WebSocket 腿被自动化环境封锁——页面内 `new WebSocket` 对两个 CS 端口均 error+close 1006、CS 日志无到达记录，
  而同 URL 的 Go 客户端握手/心跳/在线 kv 全部成功 ⇒ 属 webview 环境限制而非代码缺陷（证据留档）。
  **浏览器双人 WS 全流程仍未完成**，由 e2e（双 Go WS 连不同 CS）+ API 级检查作协议等价覆盖。
- 浏览器尝试的真实收益：暴露并修复 `joker/api/websocket.go` 升级后 `c.JSON` 写 hijacked 连接的 panic（每次升级必触发）。

## 3. Review 记录

- 无框架/无构建链（§9.5）✅；D15 客户端义务逐条实现 ✅
- 浏览器双人验收未做（诚实声明）：等价物为 e2e 脚本双 WS 连不同 CS 的全流程 + API 级群/feed 检查
