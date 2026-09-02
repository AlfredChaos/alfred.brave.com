# 本地开发栈（T17 / §4 规格）

原则：**本地跑通即可**——单副本中间件、无高可用、无内核调优。唯一"多副本"是 Joker ×2（验证跨 CS 投递）。

## 快速开始

```bash
make local-up        # = cd deploy/local && docker compose up -d --build（首次 ~2min）
make local-logs      # 跟踪全部日志
make local-down      # 停栈（保留数据卷）
make local-rebuild   # 改代码后全量重建业务容器
```

就绪后：
- 测试客户端：http://127.0.0.1:37001/web/ （两浏览器窗口两账号 = 跨 CS 全流程）
- 端到端验收：`go run ./tools/e2e -gateway http://127.0.0.1:37001`

## 服务清单（§4）

| 服务 | 镜像 | 端口（宿主机） | 说明 |
|---|---|---|---|
| postgres | postgres:15 | – | 数据卷；migrate 一次性容器跑 goose |
| etcd | coreos/etcd v3.5.5 | – | 单节点，服务注册表（cs/feed 分 kind） |
| kafka | apache/kafka 3.7.0（KRaft，heap 512m） | – | 单 broker；kafka-init 显式建 5 topic（§8 分区数） |
| gateway | brave:local | **37001** | REST 入口 + /web 静态客户端 + /v1/feed/* 转发 |
| cs-1 / cs-2 | brave:local | **37002/37012** · **37202/37212** | WS + gRPC；advertise=127.0.0.1（浏览器直连）、grpc_advertise=容器名（内网投递） |
| persist / deliver | brave:local | – | 消费组 worker |
| feed-api / fanout / push | brave:local | 37003（feed，未映射宿主） | feed 域 |
| ghost | brave:local | – | 在线状态对账（60s） |

## 关键实现注意（§4 对照）

- 双 joker 宿主机端口映射 37002/37202、37012/37212；登录返回的 `ws_addr=127.0.0.1:37x02` 浏览器可直连。
- 对外/对内地址解耦：`ADVERTISE_HOST`（etcd 注册 + ws_addr + kv.cs）与 `GRPC_ADVERTISE_HOST`（kv.addr，deliver 拨接）分离。
- 服务间走 compose 网络名；`PROJECT_PATH=/app` + etc/*.tmpl 由 entrypoint envsubst 渲染（沿用原模式）。
- healthcheck + `depends_on.condition`：PG/etcd/Kafka 就绪 + migrate 成功后才起业务。

## 已知边界（诚实声明）

- 单副本中间件：Kafka RF=1（无冗余）、etcd 单点、PG 单机——与生产栈（deploy/prod）语义差异见各自 README；
- topic 分区数与生产一致（12/12/6/6/3）但 RF=1；
- 压测请用 `tools/stress` 骨架 + kernel-tuning.md 模板，本地数字不代表集群容量。
