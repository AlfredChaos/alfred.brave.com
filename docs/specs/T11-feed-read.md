# T11 · feed 读取链路

## 1. 目标与范围

**做**：`GET /v1/feed?cursor=&limit=`——解析 cursor → inbox 游标分页 → pull（big_v 好友 ∪ 自己）→
merge 去重倒序 → tombstone 过滤 → hydrate（内容/作者/计数批量）→ 返回 + next_cursor。
cursor 为 base64url(createdAtNano:postID) 不透明串。P99 断言不做（无压测，spec 明示），仅功能测试。

**不做**：feed 读走 replica 分流（生产拓扑项，T18 注明）；feed.notify 红点（可选不做）。

## 2. 架构符合性声明

- **§6 读路径 9 步**逐条对应实现；hybrid 一条规则：inbox(push) + big_v/自己(pull)。
- 自己的帖子不经 inbox（T10 约定），读取时并入 pull 集合。
- tombstone 读取时过滤，inbox 不物理清理（§6 明示）。

## 3. 测试计划

集成（PG）：merge（inbox 帖 + big_v pull 帖 + 自己帖同页、倒序）；cursor 分页（两页无重叠、覆盖全部、耗尽返回空 next）；tombstone 过滤。

## 4. 验收记录

```
--- PASS: TestFeedReadMerge (0.02s)
--- PASS: TestFeedReadCursorPagination (0.02s)
--- PASS: TestFeedReadTombstoneFilter (0.02s)
ok   alfred.brave.com/feed/api
```
门禁四项全绿 ✅

## 5. Review 记录

- 诚实边界：未做 P99<150ms 断言（需压测）；pull 集合每读查一次 friends（5000 上限内，30s 缓留待压测后加）✅
