# 最终交接报告 · Alfred Brave 目标架构落地（T01–T19）

> 依据 `docs/dev-task-prompt.md` 自主推进的完整交付记录。每个任务的 spec、红→绿记录、
> 验收输出、Review 清单见 `docs/specs/T*.md`；本报告只做全局汇总与诚实边界。

## 1. 已完成任务表（19/19，每任务独立 commit）

| # | 任务 | commit | 核心验证 |
|---|---|---|---|
| T01 | 修既有 bug（心跳/rand.Seed/校验/vet） | 2830bb6 | 单测（超时判定/清理/校验/分发） |
| T02 | PG 迁移（pgx v5 + goose + CRUD 改写） | 3293c1b | 集成：CRUD 等价/双边好友/kv 语义/50 并发 seq 无重号 |
| T03 | 网关化（token + ws_addr + 删 cloudware） | a9bd55b | token 5 用例 + 中间件 401/放行 |
| T04 | 在线状态（online kv + GHOST 对账） | 47c1bf8 | 单测 + 集成（真 PG 闭环/漂移保护） |
| T05 | Kafka + persist（事务 seq + 幂等重放） | be69aa2 | 集成：重放无空洞/100 条经真 Kafka 严格递增 |
| T06 | 投递闭环（gRPC RelayMessage + TTL 缓存） | a4794f2 | 集成：双 gRPC 节点真漂移 not-found 校正 |
| T07 | ACK 链路（广播消费组） | 546eb70 | 集成：真 Kafka 广播只命中本机发送者 |
| T08 | 本地全链路验收（compose + e2e 脚本） | c8f9418 | **E2E PASS**（跨 CS/ACK/落库/重放） |
| T09 | feed 存储 + 发布 + 网关代理 | 3258c07 | 集成：发布落库 + fanout 事件 key 正确 |
| T10 | fanout worker（500/批 inbox） | 2bfab6a | 集成：3 好友扇出/重放幂等/big_v 跳过 |
| T11 | feed 读取（merge + cursor） | 76f84ad | 集成：merge 顺序/分页无重叠/tombstone |
| T12 | 点赞/评论 + 计数聚合 | 8f4445f | 集成：toggle/计数归零（红灯修复零值兜底） |
| T13 | 群模型 + 管理 API（权限矩阵） | c2464a9 | 集成：14 用例矩阵 + 事件进 seq 流 |
| T14 | 群投递（在线扇出 + 顺序域切换） | 8ab2013 | 集成：仅在线扇出/拒收矩阵/seq 无空洞 |
| T15 | @ 机制（权威校验 + mention 标记） | 864e164 | 集成：@all owner-only/@ 成员标记 |
| T16 | 离线通知骨架（mock 厂商通道） | 99301b1 | 单测：合并/频控/互不串扰 |
| T17 | 本地 compose 收尾 | 249e050 | 12 容器全栈 + e2e 回归 |
| T18 | 生产三节点脚本（**未执行**） | 5d40b36 | bash -n + 结构自查 |
| T19 | Web 客户端 + 压测骨架 | 249e050 | API 级全流程 + 静态托管 200 |

## 2. 端到端验收记录（本地真实运行）

```
$ go run ./tools/e2e -gateway http://127.0.0.1:37001     # 连续 3 次稳定通过
  A -> 127.0.0.1:37202, B -> 127.0.0.1:37002 (cross-CS verified)
  B received: conv=... seq=1 · A ack: msg_id=... seq=1
  replay → history 1 row（重放不重复）
  E2E PASS: register/login/ws_addr/cross-cs deliver/ack/persist/replay-idempotent
```
在线状态实测：连接期 `online:{uid}={"cs":"127.0.0.1:37002","addr":"cs-1:37012"}`，断开后 0 条。
群/feed 实测：建群→成员见 system_event 流；feed 发布（经网关代理）→ fanout → 好友时间线 → 点赞。

## 3. 全量门禁（最终回归）

- `gofmt -l .` 空 · `go vet ./...` 0 告警 · `go build ./...` 通过
- `go test ./...` → 9 包 ok（单元）
- `go test -tags=integration ./... -count=1` → **11 包全部 ok**（PG/Kafka/双 gRPC 节点真实依赖）

## 4. 已验证 / 未验证清单（诚实边界）

**已验证**（真实运行 + 断言）：
- 消息链路全环：WS→chat.msg→事务落库（seq/幂等）→chat.push→kv 路由→gRPC→WS 帧→ACK→重放不重复
- 跨 CS 投递（两个 Joker 容器）、路由陈旧 not-found 校正（双 gRPC 节点漂移）、GHOST 清理
- 群聊权限矩阵 14 用例、群事件 seq 流、在线扇出、@ 权威校验、成员时间窗
- feed 发布/扇出/读取分页/tombstone/点赞计数；离线通知合并与频控（注入时钟）
- 本地 compose 12 容器全栈 + Web 客户端静态托管

**未验证 / 未执行**（不得宣称）：
1. **生产脚本**（deploy/prod）：服务器未购买，bootstrap/verify 从未在真实环境跑过。
2. **压测**：tools/stress 仅骨架；**零性能数字**（无 TPS/延迟/连接容量结论；架构文档容量表是推导假设）。
3. **厂商推送通道**：push worker 到 mock（日志）为止，"演示级"。
4. **浏览器双人验收**：Web 客户端未做真人双窗口操作，以 API/协议级等价验收覆盖（e2e 双 WS + 群/feed API 检查）。
5. **D15 心跳 seq 对账服务端段**：客户端按 seq 补拉/去重已实现；心跳携带 last_seqs 的服务端比对未实现（设计项）。
6. **PG 读写分流 / patroni 自动切换 / 网关 LB**：生产简化项（deploy/prod README 列明 5 条）。
7. **DLQ 死信 topic**：毒消息策略为 log+skip+commit；*.dlq 未实现（T05/T16 spec 声明的全局遗留）。
8. **Send chan 缓冲**：保持现有 1000；kernel-tuning.md §5 目标 16~32 属压测阶段联动项（本阶段该文档只读不用）。

## 5. 红灯修复记录（TDD 有效性抽样）

- T02：NextSeq 初始 next:0 → 首条消息 seq=0（测试红灯）→ 初始 1。
- T12：动作清零后 recount 空结果不触发 ON CONFLICT、旧计数残留 → unnest+LEFT JOIN 零值兜底。
- T14：system_event 信封未解析扇出目标（红灯）→ ActiveMembers 无状态校验解析（解散事件必须能投出）。
- T16：合并逻辑首版 pending 队列在 send 后状态丢失（每条都发）→ badge 计数模型。
- T17：feed 服务缺席本地栈；gin 通配路由 /v1/feed 307 → 双路由注册。

## 6. 给用户的下一步建议

1. **买服务器** → 按 `deploy/prod/README.md` checklist 执行 bootstrap + verify（脚本就绪，占位符填 IP 即可）；
2. **内核调优** → 按 `docs/kernel-tuning.md` 分档执行 sysctl/ulimit（真机到手第一步）；
3. **压测闭环** → `tools/stress` hold/storm + kernel-tuning 记录模板，产出真实 TPS/延迟/连接数，回填架构文档容量表（校准 ×20 峰值系数）；
4. 遗留功能按需补：DLQ、D15 服务端 seq 对账、PG 读分流、真厂商通道（接口已就位）。
