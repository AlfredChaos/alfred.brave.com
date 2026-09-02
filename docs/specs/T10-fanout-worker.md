# T10 · fanout-worker

## 1. 目标与范围

**做**：`brave fanout`——消费 feed.fanout（group=fanout，单 goroutine 处理完才 commit）→
帖子存在性/tombstone 检查 → big_v 跳过 → 好友列表（30s 本地缓存）→ 批量写 feed_inbox（500/批，ON CONFLICT 幂等）。

**不做**：feed.notify 红点（§6 步骤 10 可选项，不做）；删除的 inbox 物理清理（读取时过滤 tombstone，T11）。

## 2. 架构符合性声明

- **§6 步骤 7-9**：fanout 异步于发布（发布 P99 不受影响）；好友缓存 30s（架构图注明）。
- **D19 同源纪律**：处理完才 commit；inbox 写幂等，重投安全。
- 发布者自身不进自己的 inbox（约定：读自己 timeline 走 pull 自己 uid，T11 实现）。

## 3. 测试计划

集成（PG）：3 好友 → 3 inbox 行 + 发布者无行；重放不重复；big_v 跳过。

## 4. 验收记录

```
--- PASS: TestFanoutWritesInbox (0.04s)
--- PASS: TestFanoutSkipsBigV (0.05s)
ok   alfred.brave.com/worker/fanout
```
门禁四项全绿 ✅
