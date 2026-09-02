# T12 · 简单动作（点赞/评论 + counters 异步聚合）

## 1. 目标与范围

**做**：
1. `POST /v1/feed/:id/like`（toggle：无则加/有则删）；`POST /v1/feed/:id/comments {text}`；`GET /v1/feed/:id/comments`（正序 50，作者 hydrate）。
2. 计数异步聚合最小版：fanout worker 进程内定时任务（30s）重算"近 5 分钟有动作"帖子的计数（全量重算幂等，多实例无害）。

**不做**：评论的树状回复/分页；counter 实时推送。

## 2. 架构符合性声明

- §6 "点赞/评论计数走异步聚合，不在发布链路"：动作只写 post_actions（事实源），计数是派生视图。
- 取舍声明：受 PK(post_id,uid,action) 约束，评论一人一帖一条（重复覆盖），最简版。

## 3. 测试计划

集成（PG）：点赞→计数 1；取消→重算→0；评论写入→列表→计数 1。
**红灯修复**：动作清零后 recount 的 SELECT 无行不触发 ON CONFLICT，旧计数残留——改 unnest 驱动 + LEFT JOIN 零值兜底后转绿。

## 4. 验收记录

```
--- PASS: TestLikeToggleAndCounters (0.03s)
--- PASS: TestCommentFlow (0.01s)
ok   alfred.brave.com/feed/api   （含 T09/T11 全部用例 7/7）
```
门禁四项全绿 ✅

## 5. Review 记录

- 聚合放 fanout worker 进程（feed 域后台 worker 两职责：扩散 + 计数），无新增部署单元 ✅
