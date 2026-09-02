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
- `go run ./tools/e2e` 回归 PASS ✅
- 压测骨架：`go build ./tools/stress` 通过；**未执行**（明示）✅

## 3. Review 记录

- 无框架/无构建链（§9.5）✅；D15 客户端义务逐条实现 ✅
- 浏览器双人验收未做（诚实声明）：等价物为 e2e 脚本双 WS 连不同 CS 的全流程 + API 级群/feed 检查
