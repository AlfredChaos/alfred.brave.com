# T13 · 群聊数据模型与管理 API

## 1. 目标与范围

**做**：
1. 迁移 `20260902000002_groups.sql`：groups / group_members（§5 DDL：role 二值、joined_at/left_at、状态 tombstone、500 上限校验路径索引）。
2. `internal/snowflake`：最小雪花 ID（41+3+10，时钟回拨保护）——gid 生成（D16 承诺落地）。
3. `database.GroupStore`：建群/拉人/踢人/退群/改名/公告/置顶/解散，全部**事务内**（群变更 + 会话表同步 + system_event 消息进 seq 流，D09）；owner-only 权限矩阵在事务内权威校验（§5："并发管理操作天然串行化"）。
4. 网关群管理 REST（POST/GET /v1/groups、members、quit、PATCH、announcement、pinned、DELETE）+ 错误映射（403/409/400/404）；管理成功后把预写事件投进 chat.msg（persist 幂等扇出，T14 实现）。
5. conversations 表同步维护（conv_id=gid；成员变更同步 conversation_members）。

**不做**：群消息投递校验与扇出（T14）；@ 权威校验（T15）；普通成员拉人/群主转让/禁言（§5 明确不做）。

## 2. 接口契约

8 个端点 + 权限矩阵见 §5；错误语义：403 owner-only / 409 满员·已解散 / 400 目标非法 / 404 不存在。

## 3. 架构符合性声明

- **D09 群事件即消息**：管理事务同时改群表 + INSERT system_event（共享 seq 流）；投递经 chat.msg→persist 单一路径（D19）。
- **D16**：gid 雪花 ID；worker_id 可配（多副本不撞号）。
- **§5 约束⑦**：joined_at/left_at 字段就位（历史可见性过滤在读取层实现，T14 注明）。

## 4. 测试计划

- 单元：雪花 ID 唯一/递增/并发/workerID 越界（10000 次 + 50×200 并发无重复）。
- 集成（PG）：权限矩阵 14 用例逐行（owner can / member cannot × 拉人/踢人/改名/公告/置顶/退群/解散 + 不可踢群主 + 群主不可退群）；群事件进 seq 流（建群 seq=1、拉人 seq=2）；解散 tombstone + 末条 dismiss 事件。

## 5. 验收记录

```
--- PASS: TestGroupPermissionMatrix (0.13s)   // 14/14 子用例
--- PASS: TestGroupEventMessagesAsSeqStream (0.03s)
--- PASS: TestGroupDismissBlocksPersist (0.03s)
--- PASS: TestNextUniqueAndIncreasing / TestConcurrentUnique / TestWorkerIDRange
```
门禁四项全绿 ✅

## 6. Review 记录

- 权限校验位置：网关只透传身份，权威校验在 GroupStore 事务（§5 权限矩阵注释对齐）✅
- 群事件 produce 失败 fail-open（消息已落库，实时推损失，打开会话按 seq 可见）——诚实声明 ✅
