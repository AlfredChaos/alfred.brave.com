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

> 执行顺序（2026-09-07 拍板）：**S1/S2 连接轴（2+1'）先行**——首次部署中间件只上 node-3 单点；
> S3 消息轴前才切混部（node-1/2 补装 docker，见「拓扑切换」）。

```bash
# 0. 运维机（或本机仓库根）：交叉编译打包（默认 linux/amd64，arm 机器 GOARCH=arm64）
deploy/systemd/build.sh     # 产物含 brave + stress 两个二进制
scp dist/brave-systemd-linux-amd64.tar.gz ubuntu@<nodeN>:/tmp/

# 1. 每台目标机：解包 + 安装 + 内核调优（腾讯云 ubuntu@ 免密 sudo）
ssh ubuntu@<nodeN> "sudo mkdir -p /opt/brave && sudo tar xzf /tmp/brave-systemd-linux-amd64.tar.gz -C /opt/brave && sudo chown -R ubuntu:ubuntu /opt/brave"
ssh ubuntu@<nodeN> "cd /opt/brave/systemd && sudo ./install.sh nodeN"    # node1|node2|node3
#    内核调优（tuning/ 两档，参数出处 kernel-tuning.md §3——node-1/2 连接档、node-3 保守档）：
ssh ubuntu@<node1|2> "sudo cp /opt/brave/systemd/tuning/sysctl-node12.conf /etc/sysctl.d/99-zz-brave.conf && sudo cp tuning/limits-node12.conf /etc/security/limits.d/99-brave.conf && sudo sysctl --system"
ssh ubuntu@<node3>   "sudo cp /opt/brave/systemd/tuning/sysctl-node3.conf  /etc/sysctl.d/99-zz-brave.conf && sudo cp tuning/limits-node3.conf  /etc/security/limits.d/99-brave.conf && sudo sysctl --system"
#    ⚠️ 云镜像 /etc/sysctl.conf 预置 somaxconn=4096，sysctl --system 时它最后应用会压掉
#    任何 99-*.conf——必须 sed 改掉该行（见坑清单 #7）

# 2. 每台编辑 /etc/brave/env（真实 IP/密码/本机 advertise）+ 中间件 env
vi /etc/brave/env                      # brave 服务变量
cp /opt/brave/systemd/middleware/env.middleware.single.example /opt/brave/systemd/middleware/.env.middleware   # 2+1'：node-3 单点
cd /opt/brave/systemd && ./render-config.sh

# 3. node-1 原生 PG（一次性，见下节），然后起迁移
systemctl start brave-migrate

# 4. 中间件（2+1' 起步：**仅 node-3**，单点 etcd + 单 broker）
cd /opt/brave/systemd/middleware && set -a && source .env.middleware && set +a \
  && docker compose -f docker-compose.middleware.yml up -d
# 建 topic（node-3，RF=1 单点版）：
docker run --rm --network host apache/kafka:3.7.0 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 --create --topic chat.msg --partitions 12 --replication-factor 1
# … chat.push 12P / feed.fanout 6P / chat.ack 6P / chat.notify 3P（RF=1）

# 5. 起业务
systemctl start brave-gateway brave-cs brave-persist   # 按本机角色表
```

## node-1 原生 PostgreSQL（一次性）

```bash
apt install -y postgresql        # Ubuntu 26.04 = PG 18（无 15 包）
systemctl edit --full postgresql   # 或直接改 /etc/postgresql/18/main/
#   shared_buffers = 768MB        # 压测档（中间件同机，不给 25% 默认）
#   max_connections = 300         # 业务侧 ~10 实例 × 池 4-8 + 余量
#   listen_addresses = '*'
# pg_hba.conf 追加：host all all 172.16.16.0/24 scram-sha-256
sudo -u postgres psql -c "CREATE USER brave WITH PASSWORD '...'; CREATE DATABASE brave OWNER brave;"
systemctl restart postgresql
```

## 拓扑切换（2+1' ↔ 混部）

```bash
# 2+1' → 混部（S3 消息轴前切换）：
# node-1/2：装 docker（apt install -y docker.io docker-compose-v2）+ 起本机 etcd-N/broker-N
#   ⚠️ 装 docker 会加载 nf_conntrack（默认表项 65536）——必须随后
#   sysctl net.netfilter.nf_conntrack_max=1048576 并压测期盯 nf_conntrack_count
# 三台 .env.middleware 换三节点版（KAFKA_BROKERS/ETCD_ENDPOINTS 指向三台）+ down/up；
#   topic RF 1→3 需重建（RF3 版建 topic 命令同上 --replication-factor 3）
# node-3 cs 切回标准 unit：systemctl disable --now brave-cs-node3 && systemctl enable --now brave-cs

# 混部 → 2+1'（S5 后复测连接轴）：
# node-3：换单点中间件 env（env.middleware.single.example）+ down/up 重建（etcd/kafka 数据可清）
#   RF 3→1 需重建 topic（RF1 版）：kafka-init 命令同上但 --replication-factor 1
# node-3：cs 切限容变体
systemctl disable --now brave-cs && systemctl enable --now brave-cs-node3
# node-1/2：整停 docker（连接轴纯赚；nf_conntrack 随模块卸载退出）
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
6. **云镜像 `/etc/sysctl.conf` 最后应用**（sysctl --system 的 Debian 约定）——腾讯云镜像预置
   `somaxconn=4096` 会压掉任何 `99-*.conf` 里的同名项，drop-in 改名也赢不了，必须直接 sed
   改 `/etc/sysctl.conf`（2026-09-07 真机踩坑实证）；
7. 登录 shell 的 nofile 走 `/etc/security/limits.d/99-brave.conf`（tuning/ 两档），但 pam_limits
   在**会话建立时**读取——同一条 ssh 里"装文件+验证 ulimit"会看到旧值 1024，新开会话才是真值。
