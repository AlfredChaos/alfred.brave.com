# T07 · ACK 链路（chat.ack → CS → 发送者）

## 1. 目标与范围

**做**：
1. deliver 送达回调 produce chat.ack（key=from_uid）——已在 T06 随投递闭环落地（§2 图注"送达→chat.ack"）。
2. Joker 消费 chat.ack：每实例独立消费组（cs-ack-{ServiceId}，广播语义）→ 本机连接的发送者推 `{cmd:"ack", data:{msg_id, cli_msg_id, conv_id, seq}}`；无关 ack 跳过；Send 满丢弃（seq 补拉兜底，D15）。

**不做**：客户端 UI 打勾（T19 实现，按 cli_msg_id 匹配 + msg_id 幂等）；消息级重发。

## 2. 接口契约

WS 下行帧：`{"cmd":"ack","data":{"msg_id":"...","cli_msg_id":"c-1","conv_id":"...","seq":7,"from_uid":"...","to_uid":"..."}}`

## 3. 架构符合性声明

- §3 步骤 13-15 全链：deliver→chat.ack→CS-A 消费→WS 推发送者。
- D15：ack 非唯一正确性来源（可丢），客户端 seq 对账是终极兜底。
- 广播消费组选型：每 CS 独立 group 取代"投递回指定 CS"，避免 deliver 需要知道发送者所在节点（发送者路由不在投递职责内）。

## 4. 测试计划

集成（真实 Kafka）：本机发送者收到正确 ack 帧；无关用户的 ack 不投给本机其他连接；消费者随 ctx 退出。

## 5. 验收标准

门禁四项 + `go test -tags=integration ./joker/exchange/ -run TestConsumeAcks` 全绿。

## 6. 验收记录（实测粘贴）

```
--- PASS: TestConsumeAcksIntegration (2.35s)
ok   alfred.brave.com/joker/exchange
```
门禁四项全绿 ✅

## 7. Review 记录

- ack 丢弃策略（Send 满不阻塞消费者）+ D15 兜底声明 ✅；无新增全局 ✅
