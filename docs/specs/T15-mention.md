# T15 · @ 机制

## 1. 目标与范围

**做**：persist 权威校验（§5 校验链第二级）：`mention_all=true → sender.role=owner`；
`mentions 每个 uid ∈ 当前有效成员`；失败**整条拒绝**（不留半条消息）。投递展开（第三级）已在 T14 落地：
`mention = mention_all || mentions ∋ 接收者`，被 @ 成员的 chat.push 带 `mention:true`。
CS 格式快检（第一级：mentions≤50、布尔）T05 已实现。

**不做**：@ 消息的额外推送通道（§5："无额外推送通道"，mention 标记即全部）。

## 2. 架构符合性声明

- §5 三级校验链完整：CS 快检（不进 Kafka）→ persist 权威（事务串行原子判定）→ 投递展开。
- 信封的 mentions/mention_all 由 CS 从 content 解析（T14），客户端无法伪造信封与内容不一致。

## 3. 测试计划

member @all 拒收；owner @all 放行且在线成员 push mention=true；@ 非成员整条拒收；@ 成员 push mention=true；普通消息 mention=false。

## 4. 验收记录

```
--- PASS: TestMentionAllOwnerOnly (0.06s)
--- PASS: TestMentionMembersOnly (0.04s)
--- PASS: TestMentionNoMarkWithoutAt (0.04s)
```
门禁四项全绿（8 包）✅
