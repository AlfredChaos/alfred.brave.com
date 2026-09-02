# T17 · 本地 compose（deploy/local）

## 1. 目标与范围

**做**：`deploy/local/docker-compose.yml` + `README.md` + Makefile（local-up/down/logs/rebuild）——§4 规格全项落地（该交付在 T08 验收时提前建成，本任务收尾文档与 feed 域补齐）：
- 单节点中间件（PG/etcd/Kafka 512m，topic 显式创建 §8 分区数）+ gateway/persist/deliver/feed-api/fanout/push/ghost + **joker×2**（37002/37202 · 37012/37212）。
- advertise 双地址解耦（浏览器 ws_addr=127.0.0.1，deliver 走内网名）。
- healthcheck + depends_on 条件编排；migrate 一次性容器。

## 2. 验收记录（真实运行）

- `docker compose ps` → 12 容器全部 Up（pg/etcd/kafka healthy）✅
- `go run ./tools/e2e` → **E2E PASS**（注册→登录→跨 CS→ACK→落库→重放不重复）✅
- feed 经网关代理：发布/时间线（含 fanout 收件箱）/点赞 全通 ✅（修复记录：gin 通配路由 307 → 双路由注册）
- 群：建群/拉人/成员可见事件消息 ✅
- `/web/` 静态客户端 200（index/app.js/style.css）✅
- 端口/服务清单对照 §4 逐项 ✅

## 3. Review 记录

- 红灯修复两则：feed 服务缺席本地栈（§4 服务清单漏配）；feed 代理 307 重定向 ✅
