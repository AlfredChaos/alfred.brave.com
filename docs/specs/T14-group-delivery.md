# T14 · 群聊投递（persist 权威校验 + 在线成员扇出 + 顺序域切换）

## 1. 目标与范围

**做**：
1. persist 三分支：single（原路径）/ group（权威校验：群存活 + 发送者有效成员）/ system_event（预写消息幂等跳过落库，直接扇出）。
2. 投递写扩散（D08）：群消息落库 1 份 → ActiveMembers 批量查 online kv（新增 GetManyBatch）→ **仅在线成员** produce chat.push（1:M）；离线成员不扇出不标记，上线按 seq 补拉。
3. 顺序域切换（§5 底注）：入队 key=gid（同群同分区 → persist 串行 → seq 无空洞）；扇出 key=uid（同接收者有序）。
4. CS 帧：`group` 布尔字段区分群聊；content 内 mentions/mention_all 由 CS 解析进信封（客户端无法伪造信封与内容不一致）。
5. 群历史可见性（约束⑦）：ListAfterForGroup 按 [joined_at, left_at) 时间窗过滤（网关历史 API 接线在 T19 客户端联调时验证）。
6. system_event 扇出目标取 ActiveMembers **不做状态校验**——解散事件必须在群 dismissed 后仍能投出（末条消息语义）。

**不做**：@ 权威校验（T15）；mention 标志按成员展开的字段已就位（MentionAll || mentions 含该成员），校验补齐在 T15。

## 2. 架构符合性声明

- **D08**：存储不扩散（1 行）/ 投递写扩散（在线 M 份 push）——两个顺序域一次 Kafka 写入完成切换。
- **D09**：群事件与普通消息共享 seq 流；预写幂等由 msg_id 主键快查实现（不烧 seq）。
- **D19/D20**：扇出经 chat.push 单一路径；同 gid 消息经分区串行消费，seq 严格递增（测试覆盖）。

## 3. 测试计划（全部集成，真实 PG）

在线扇出（3 人群 1 在线 → 1 行存储 + 1 条 push 且 key=成员）；拒收矩阵（非成员/未知群/解散后）；事件信封幂等扇出（Rename 预写 → 不重复落库 → 在线成员收到 system_event push）；同 gid 10 条 seq=2..11 严格无空洞。

## 4. 验收记录

```
--- PASS: TestGroupFanoutOnlineOnly (0.07s)
--- PASS: TestGroupRejectedSenders (0.05s)
--- PASS: TestGroupSystemEventEnvelopeFanout (0.04s)   // 红灯修复：事件信封未解析扇出目标
--- PASS: TestGroupOrderingSameGid (0.05s)
ok   alfred.brave.com/worker/persist（含 T05 全部用例回归通过）
```
门禁四项全绿（8 包）✅

## 5. Review 记录

- 解散事件不做状态校验的取舍已注明（否则末条消息投不出）✅
- 离线成员零扇出（补拉兜底），无未读标记（D10）✅
