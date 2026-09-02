# Alfred Brave · 分布式即时通讯系统（求职练手项目）

> **本项目为求职面试练手项目**，用于实践分布式即时通讯系统设计。架构参考 [link1st/gowebsocket](https://github.com/link1st/gowebsocket)（Gin + WebSocket + gRPC 分布式 IM）的思想，代码为独立实现。目标架构设计见 `docs/target-architecture.html`（v4），决策记录见 `docs/architecture-decisions.md`（D01–D22）。
>
> **诚实声明**：下文区分【已实现】与【设计/计划中】；压测与生产部署未在真实环境执行（服务器未购买），不存在任何编造的性能数字。

## 快速开始（本地全栈）

```bash
make local-up                                  # PG/etcd/Kafka + 双 Joker + 全部 worker
open http://127.0.0.1:37001/web/               # 测试客户端（两窗口两账号 = 跨 CS 全流程）
go run ./tools/e2e -gateway http://127.0.0.1:37001   # 脚本化端到端验收
```

消息链路：`发送方 ─WS→ Joker-A ─produce chat.msg→ persist(事务落库+seq) ─produce chat.push→ deliver(查 online kv) ─gRPC→ Joker-B ─WS→ 收信方；送达 → chat.ack → 发送方打勾`。

## 【已实现】（每条可指到代码）

| 能力 | 位置 | 验证 |
|---|---|---|
| 网关：token 鉴权（HMAC）、etcd 选 CS 下发 ws_addr | `server/`、`internal/token` | 单测 + e2e |
| Chat Server：WS 连接管理、客户端心跳（30s）+ 6 分钟超时清理 | `joker/exchange` | 单测（D14） |
| cmd 注册式路由（login/heartbeat/msg） | `joker/exchange/router.go` | 单测 |
| Kafka 主干：chat.msg/chat.push/chat.ack/chat.notify/feed.fanout（分区数 §8） | `internal/chat`、`internal/kafka` | 集成 |
| persist：事务{kv seq 自增 → INSERT → last_seq}，确定性 msg_id 幂等重放（无 seq 空洞） | `worker/persist` | 集成 |
| deliver：online kv 30s TTL 缓存 + not-found 即时校正 + gRPC 投递 | `worker/deliver`、`joker/relay` | 集成（双节点漂移） |
| ACK 链路：送达回执广播回发送方所在 CS | `joker/exchange/ack.go` | 集成 |
| 在线状态：Joker 唯一写 online:{uid}（含漂移保护条件删）；GHOST 对账 60s | `worker/ghost` | 集成 |
| 群聊：groups/group_members、owner-only 权限矩阵、群事件即消息（D09）、投递层写扩散（仅在线成员扇出）、@/@all 权威校验、成员时间窗历史 | `database/group.go`、`server/api/group.go`、`worker/persist` | 14 用例矩阵 + 集成 |
| 朋友圈：发布同步落库 + 异步 fanout（500/批）、inbox+pull merge 读取（cursor 分页、tombstone 过滤、hydrate）、点赞/评论 + 计数定时聚合 | `feed/`、`worker/fanout` | 集成 |
| 离线通知：chat.notify + push worker（60s 同人合并、20/h 频控）——**厂商通道为 mock（演示级）** | `worker/push` | 单测 |
| 存储：PostgreSQL 15 + pgx v5 + goose(postgres)；kv 表兼作 KV（online/seq） | `database/` | 集成 |
| 本地 compose 全栈（双 Joker 验证跨 CS）+ 端到端验收脚本 + Web 测试客户端 + 压测骨架 | `deploy/local/`、`tools/e2e`、`web/`、`tools/stress/` | e2e PASS |

## 【设计/计划中】（未实现，面试表述用"设计方案"）

- **生产三节点部署**：`deploy/prod/` 脚本写好**未执行**（服务器未购买）；PG 流复制/Kafka RF=3/etcd 三成员均为脚本态。
- **压测数字**：`tools/stress` 仅骨架；容量表（§9）是推导假设（×20 峰值系数为包络），待真机压测校准。
- **内核调优**：`docs/kernel-tuning.md` 只读（真机到手后执行）。
- D15 心跳 seq 对账的服务端比对段（客户端按 seq 补拉已实现）。
- PG 读写分流（feed 读走 replica）、DLQ 死信 topic、消息未读数/已读回执/撤回（D10 明确不做）。

## 技术栈

Go 1.21 · Gin · gorilla/websocket · segmentio/kafka-go · google.golang.org/grpc · PostgreSQL 15 + jackc/pgx v5 · pressly/goose v3 · etcd v3.5 · viper/urfave-cli · logrus

## 更多文档

- 任务规格与验收记录：`docs/specs/`（T01–T19 逐任务四阶段）
- 架构与决策：`docs/target-architecture.html`、`docs/architecture-decisions.md`
- 面试参考：`docs/distributed-im-system-design-interview-reference.md`
- 部署：`deploy/local/README.md`（本地）、`deploy/prod/README.md`（生产，未执行）
