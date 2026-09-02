# T09 · 朋友圈存储 + 发布

## 1. 目标与范围

**做**：
1. 迁移 `20260902000001_feed.sql`：posts / feed_inbox / post_actions / post_counters（§7 DDL + 索引）。
2. `database.FeedStore`：CreatePost / GetPosts / PullByAuthors(big_v) / InsertInbox(批) / PageInbox / CountFriendsByOwner。
3. `brave feed` 服务（:37003，etcd kind=feed 注册）：`POST /v1/feed`——同步落库返回 post_id，异步 produce feed.fanout（key=pub_uid）；big_v 判定 = 好友数 > 5000。
4. 网关 `/v1/feed/*` 透明转发（ReverseProxy + feed 服务表随机选点，鉴权透传双端校验）。
5. 前置重构：etcd 服务注册带类型（`services/{kind}/{uuid}`），网关选 CS 只看 kind=cs，feed 代理只看 kind=feed，GHOST 对账对齐 kind=cs。

**不做**：fanout worker（T10）；读取（T11）；点赞评论（T12）。

## 2. 接口契约

`POST /v1/feed {text, media[]}` → 200 `{post_id, is_big_v, created_at}`；400 空文本/超长（>2000）；401 未鉴权；503 feed 不可用（网关侧）。

## 3. 架构符合性声明

- **§6 写路径步骤 1-6**：网关只鉴权+转发；发布同步落库返回（不等 fanout）。
- **D05/D07**：friends 点查计数判定 big_v；阈值一条规则（§6"不搞复杂策略"）。
- fanout produce 失败不阻塞发布（fail-open，损失单次 push，读侧 pull 兜底）——诚实记录的取舍。

## 4. 测试计划

集成（PG+Kafka）：发布 → posts 行 + content JSONB 语义正确 + fanout 事件进 topic 且 key=pub_uid；鉴权（无 token 401 / 空文本 400）。

## 5. 验收记录（实测粘贴）

```
--- PASS: TestPublishPost (9.2s)    // posts 落库 + feed.fanout 事件（key=pub_uid）验证
--- PASS: TestPublishAuth (0.01s)
ok   alfred.brave.com/feed/api
```
门禁：gofmt 空 / vet 0 / build ok / go test 7 包绿 ✅

## 6. Review 记录

- etcd kind 改造波及 joker 注册/网关监听/ghost 对账，全部切换 ✅
- feed-api 与网关共享 secret 双端鉴权（网关转发不重写 Authorization）✅
