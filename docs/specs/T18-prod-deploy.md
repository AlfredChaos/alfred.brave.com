# T18 · 生产三节点脚本（写好不执行）

## 1. 目标与范围

**做**（§5 规格）：`deploy/prod/`——
1. `README.md`：机器清单、角色分配、首次执行 checklist、kernel-tuning 衔接、**已知简化清单**。
2. `.env.example`：NODE1/2/3_IP 占位符 + 集群共享配置（PG 复制密码/AUTH_SECRET/Kafka heap）。
3. `docker-compose.prod.yml`：统一 compose + profile（node1/node2/node3）差异，逐节点对齐 §9：
   PG 流复制（bitnami env 化）、Kafka RF=3 min.insync=2、etcd 三成员、persist×2 跨节点、deliver×2、feed-api×2、ghost；kafka-init 显式建 topic（§8 分区数）。
4. `bootstrap.sh`：装 docker → rsync 分发 → node1 先起（primary/迁移/topic）→ 等 pg_isready → node2/3；占位符 IP 未改则拒绝执行。
5. `verify.sh`：TCP 端口（37001/37002/37012/9092）、etcd endpoint health ×3、kafka topic 存在性与 RF=3、PG 两副本 streaming、容器存活。

**明确不包含**（§5）：内核调优参数（kernel-tuning.md 执行阶段）；任何性能断言。
**诚实声明**：服务器未购买，全部脚本**未在真实环境验证**（bash -n 语法检查通过；compose 未跑）。

## 2. 架构符合性声明

- §9 节点拓扑逐项对齐（含 CPU/内存预算对应的组件分布）。
- D11：persist/deliver 拆分部署且双实例跨节点（消费组 rebalance 自动接管）。
- D22：Kafka heap 1G（混部压缩档）。

## 3. 验收记录

- `bash -n bootstrap.sh verify.sh` → 语法通过 ✅
- compose YAML 结构自查（profiles/env 继承/卷清单）✅
- **未执行**：无真实服务器（本任务的明确边界）✅

## 4. Review 记录

- 已知简化 5 条列在 README（读分流/故障切换/LB/镜像分发/bitnami 选型）——不隐瞒 ✅
- 占位符防呆（.env 默认 IP 拒绝执行）✅
