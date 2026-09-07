# systemd 部署形态（压测专用）

> 场景：3×4C16G 真机压测（stress-plan.md）。业务进程 systemd 直跑（省 dockerd/shim
> ~300–500MB/台，连接轴 node-1/2 可整停 docker）；**PG 原生**装 node-1（消息轴瓶颈
> 直调优）；etcd/kafka 留 docker（原生要 JDK 不划算）。
> 与 deploy/prod（docker compose 全容器）的差异是**有意的**：本形态为压测实验拓扑，
> 砍掉了 PG 副本与统一容器化——README 即取舍记录。

## 角色分配（对齐 prod compose profiles）

| 节点 | systemd（brave 二进制） | docker | 原生 |
|---|---|---|---|
| node-1 | gateway · cs · persist ·（migrate 一次性） | 混部时 etcd-1 + broker-1 | **PG primary** |
| node-2 | gateway · cs · feed · fanout · persist | 混部时 etcd-2 + broker-2 | — |
| node-3 | cs · feed · deliver · push · ghost | 混部时 etcd-3 + broker-3；**2+1' 时 etcd 单点 + 单 broker** | — |

## 首次部署

```bash
# 0. 运维机（或本机仓库根）：交叉编译打包（默认 linux/amd64，arm 机器 GOARCH=arm64）
deploy/systemd/build.sh
scp dist/brave-systemd-linux-amd64.tar.gz root@<nodeN>:/opt/

# 1. 每台目标机：解包 + 安装
ssh root@<nodeN> "mkdir -p /opt/brave && tar xzf /opt/brave-systemd-linux-amd64.tar.gz -C /opt/brave"
ssh root@<nodeN> "cd /opt/brave/systemd && ./install.sh nodeN"    # node1|node2|node3

# 2. 每台编辑 /etc/brave/env（真实 IP/密码/本机 advertise）+ 中间件 env
vi /etc/brave/env                      # brave 服务变量
cp /opt/brave/systemd/middleware/env.middleware.node1.example /opt/brave/systemd/middleware/.env.middleware
cd /opt/brave/systemd && ./render-config.sh

# 3. node-1 原生 PG（一次性，见下节），然后起迁移
systemctl start brave-migrate

# 4. 三台中间件（混部起步）
cd /opt/brave/systemd/middleware && set -a && source .env.middleware && set +a \
  && docker compose -f docker-compose.middleware.yml up -d
# 建 topic（任一台）：
docker run --rm --network host apache/kafka:3.7.0 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 --create --topic chat.msg --partitions 12 --replication-factor 3
# … chat.push 12P / feed.fanout 6P / chat.ack 6P / chat.notify 3P（RF=3）

# 5. 起业务
systemctl start brave-gateway brave-cs brave-persist   # 按本机角色表
```

## node-1 原生 PostgreSQL（一次性）

```bash
apt install -y postgresql-15
systemctl edit --full postgresql   # 或直接改 /etc/postgresql/15/main/
#   shared_buffers = 768MB        # 压测档（中间件同机，不给 25% 默认）
#   max_connections = 300         # 业务侧 ~10 实例 × 池 4-8 + 余量
#   listen_addresses = '*'
# pg_hba.conf 追加：host all all <三台网段>/24 md5
sudo -u postgres psql -c "CREATE USER brave WITH PASSWORD '...'; CREATE DATABASE brave OWNER brave;"
systemctl restart postgresql
```

## 拓扑切换（混部 ↔ 2+1'）

```bash
# 混部 → 2+1'（S3 完成后切连接轴）：
# node-3：换单点中间件 env（env.middleware.single.example）+ down/up 重建（etcd/kafka 数据可清）
#   RF 3→1 需重建 topic（RF1 版）：kafka-init 命令同上但 --replication-factor 1
# node-3：cs 切限容变体
systemctl disable --now brave-cs && systemctl enable --now brave-cs-node3
# node-1/2：整停 docker（连接轴纯赚）
systemctl stop docker.socket docker
# node-1/2 的 brave-cs 继续（LimitNOFILE=800000 已在 unit）
```

- 切换会重建 etcd（服务注册）与 kafka topic——cs 的注册维持循环会自动重注册（已修），
  压测数据分阶段独立，无跨阶段依赖；
- 2+1' 时 `/etc/brave/env` 的 `KAFKA_BROKERS/ETCD_ENDPOINTS` 要改成仅 node-3，
  `render-config.sh` 重渲染后 `systemctl restart` 相关服务。

## 坑清单（按重要度）

1. **LimitNOFILE 写在 unit 里**（cs=800000 / worker=500000）——pam limits 对 systemd
   不生效，这是"调了 ulimit 仍 too many open files"的头号来源；
2. `MemoryMax=5G`（brave-cs-node3）触顶被 OOM kill + 自动重启——这是**预期的红线信号**，
   S1 拐点记录就看它；
3. journald 默认限速 1000 条/30s 会丢日志——压测期这是保护不是故障；查完整日志用
   `journalctl -u brave-cs --no-pager`；
4. 改 `/etc/brave/env` 后必须 `render-config.sh` + `systemctl restart brave-*`（env
   只进进程环境，yaml 才是 viper 实际读的）；
5. `brave-migrate` 是 oneshot：重跑幂等（goose 版本表）；PG 重装后先 drop database。
