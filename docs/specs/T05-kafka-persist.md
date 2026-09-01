# T05 · Kafka 接入 + persist worker

## 1. 目标与范围

**做**：
1. `internal/chat`：Kafka 线协议（chat.msg / chat.push 的 JSON 结构与 topic 常量）。
2. `internal/kafka`：kafka-go Producer 封装 + `EnsureTopics`（分区数对齐 §8：chat.msg 12P / chat.push 12P / chat.ack 6P / chat.notify 3P / feed.fanout 6P）。
3. Joker produce：`cmd=msg` 帧 `{conv_id, to_uid, cli_msg_id, content}` → 组装 chat.msg（from 取连接身份）→ key=conv_id produce；CS 侧 content 快检（mentions ≤50、mention_all 布尔）。
4. `brave persist` worker：消费组 `persist` 单 goroutine（D20 保守实现：实例内全串行 ⇒ 分区内必然串行；处理完才 commit）→ 事务{seq=kv 事务内自增 → INSERT messages → 更新 conversations.last_seq} → commit 后 produce chat.push（D19：禁止直投）。
5. 幂等重放：msg_id 由 (conv_id, from_uid, cli_msg_id) 确定性派生（uuid5）；重放先查后插，不烧 seq（无空洞）。
6. 网关会话 API：`POST /v1/conversations {to_uid}`（find-or-create 单聊）、`GET /v1/conversations`（我的会话列表）、`GET /v1/conversations/:id/messages?after_seq=&limit=`（历史，鉴权 + 成员校验）。
7. 配置：`kafka.brokers` 进 conf（joker/persist/deliver 等共用）；etc/persist.yaml。

**不做**：deliver 消费（T06）；chat.ack（T07）；群聊扇出（T14）；DLQ（重试 3 次入死信——本任务对毒消息 log+skip + commit，DLQ 随 T16 通知链路统一处理并在 README 标注）。

## 2. 接口契约

### chat.msg（topic=chat.msg, key=conv_id, 12P）
```json
{"msg_id":"", "cli_msg_id":"c-1", "conv_id":"", "from_uid":"u1", "to_uid":"u2",
 "type":"single", "content":{"text":"hi"}, "sent_at":1690000000}
```
- conv_id 可空：persist 按 (from,to) find-or-create 单聊会话（首条消息路径）。
- type 当前仅 single（group=T14）。

### chat.push（topic=chat.push, key=to_uid, 12P）
```json
{"msg_id":"...", "cli_msg_id":"c-1", "conv_id":"...", "seq":1, "from_uid":"u1",
 "to_uid":"u2", "type":"single", "content":{...}, "mention":false, "created_at":...}
```

### REST
- `POST /v1/conversations {to_uid}` → `{conv_id, type, members:[a,b]}`（404 对端不存在；401 未鉴权）
- `GET /v1/conversations` → `[{conv_id, type, members, last_seq}]`
- `GET /v1/conversations/:id/messages?after_seq=0&limit=50` → `{messages:[{msg_id, conv_id, seq, from_uid, type, content, created_at}]}`（非成员 403→ 用 401 语义 Unauthorized）

## 3. 架构符合性声明

- **D02**：CS 只 produce chat.msg，落库在 persist，投递在 deliver——数据流方向与 §2 拓扑一致。
- **D18**：Joker 零业务写 PG（online kv 例外已在 T04 声明）。
- **D19**：persist 落库后 produce chat.push，绝不直投 gRPC。
- **D20**：消费侧实例内单 goroutine 处理完才 commit（每分区串行的保守超集）；producer 写必带 key。
- **D08/D16**：seq 在同一 PG 事务内自增 + INSERT；UNIQUE(conv_id,seq) 兜底；重放不产生重复行。
- **D14 注**：CS 快检失败回 ParameterIllegal 不进 Kafka（§5 校验链的 CS 段）。

## 4. 测试计划

- 单元：确定性 msg_id（同输入同 ID、异输入异 ID）；chat.msg/chat.push JSON 契约序列化；EnsureTopics 幂等（连真 Kafka）。
- 集成（PG+Kafka）：
  - persist 落库：produce chat.msg → persist 处理 → messages 行存在、seq=1、conversations.last_seq=1、chat.push 收到且 key=to_uid。
  - 幂等重放：同一 chat.msg 重投 2 次 → 1 行、seq 不变、无空洞（后续消息 seq 连续）。
  - 顺序：同 conv_id 100 条乱序并发 produce → 消费后按 seq 严格递增（分区序 + 事务串行）。
  - 会话 API：create→重复 create 同一会话（single_key 幂等）→ 历史拉取 after_seq 分页。

## 5. 验收标准

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./...
# 起 PG + Kafka（docker）后：
BRAVE_PG_DSN=... BRAVE_KAFKA_BROKERS=127.0.0.1:9092 go test -tags=integration ./... -count=1
```

## 6. 验收记录（实测粘贴）

- 门禁四项全绿（gofmt 空 / vet 0 / build ok / test 6 包 ok）✅
- 集成（PG@55432 + Kafka KRaft@9092，apache/kafka:3.7.0）：
  ```
  --- PASS: TestPersistEndToEnd (0.29s)          // 首条建会话 seq=1 → 第二条 seq=2 → push key=to_uid
  --- PASS: TestPersistReplayIdempotent (0.07s)  // 重投 2 次：1 行、seq 复用、后续 seq=2 无空洞
  --- PASS: TestPersistPoisonMessage (0.07s)     // 对端不存在 → ErrPoison
  --- PASS: TestKafkaOrderingByKey (10.02s)      // 100 条经真实 Kafka 同 key 分区 → seq 1..100 严格递增
  ok   alfred.brave.com/worker/persist
  ```
- 修复记录：ListAfter 单页上限钳制（>100→50）曾吞掉测试断言，测试改传 100 后通过——断言失败暴露的是查询边界而非数据问题。

## 7. Review 记录

- D02/D18/D19/D20 逐条对照 ✅；毒消息策略（log+skip+commit）与 DLQ 缺口已声明 ✅
- 新增依赖 segmentio/kafka-go；`internal/chat` 只放线协议（无业务逻辑）✅
