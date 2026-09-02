# 生产三节点部署（T18 交付物）

> **诚实声明**：3 台 4C16G 服务器**未购买**。本目录脚本写完整、自洽、可审阅，但**从未在真实环境执行**。
> 部署拓扑、组件清单、端口、副本策略均对齐架构文档 §9。

## 机器清单（IP 为占位符）

| 节点 | 规格 | 角色（§9） |
|---|---|---|
| node-1 | 4C16G + 200G SSD | PG **primary** · kafka-broker-1 · etcd-1 · gateway · cs-1 · persist · **migrate/kafka-init（一次性）** |
| node-2 | 4C16G + 200G SSD | PG replica · kafka-broker-2 · etcd-2 · gateway · cs-2 · feed-api · persist(二副本) · fanout |
| node-3 | 4C16G + 200G SSD | PG replica · kafka-broker-3 · etcd-3 · cs-3 · feed-api · deliver×2 · push · ghost |

关键副本策略：
- **PG**：bitnami/postgresql 流复制（repl 账号 env 配置），primary 写、replica 备份/读分流（读分流为演进项：当前所有服务指向 primary，见"已知简化"）。
- **Kafka**：KRaft 3 broker + 3 controller，RF=3、`min.insync.replicas=2`（acks=all 语义），topic 由 `kafka-init` 显式创建（分区数 §8）。
- **etcd**：`initial-cluster` 三成员，lease 60s。
- **persist 双实例**（node1+node2）消费组分摊分区；**deliver 双实例**（node3）。

## 首次执行步骤 checklist

```bash
# 0. 运维机准备：ssh 免密到三台 root；本仓库一份
cp .env.example .env && vi .env        # 填 NODE1/2/3_IP 与三个密码（必改：AUTH_SECRET/PG 密码）

# 1. 一键部署（装 docker → 分发 → 构建 → node1 先起 → node2/3）
./bootstrap.sh /path/to/repo

# 2. 健康巡检（端口 / etcd / kafka topic / PG 复制 / 容器存活）
./verify.sh
```

## 与 kernel-tuning.md 的衔接（真机到手后）

**先调优、后压测、后宣称任何数字**（kernel-tuning.md 只读参照）：
1. 三节点执行 kernel-tuning.md 的 sysctl/ulimit 分档脚本（文件句柄、somaxconn、tcp_mem 等）；
2. `tools/stress` 压测（连接保持 + 消息风暴两模式），按 kernel-tuning.md 的记录模板留档；
3. 校准架构文档 §9 容量表里的峰值系数（×20 是包络假设）。

## 已知简化（评审要点，不隐瞒）

1. **PG 读写未分流**：feed 读按架构应走 node2/3 replica，当前全服务 DSN 指向 primary——
   分流需要 per-service DSN + 只读延迟权衡，留待真机联调（compose 里 `POSTGRES_HOST` 单值）。
2. **无 patroni/自动故障切换**：primary 宕机需手工 `pg_ctl promote`（README 记录步骤即可）；
3. **网关无 LB**：两个 gateway 直接暴露，客户端应配置两个地址或前置 Nginx/云 LB（架构允许，网关无状态）；
4. **镜像经本地构建分发**（bootstrap 每节点 build），未接 registry——真机阶段建议加 harbor/ghcr；
5. bitnami/postgresql 与本地 compose 的 postgres:15 官方镜像不同：bitnami 原生支持 env 化流复制，
   是"脚本可审阅、无手工步骤"的取舍。
