# 压测真机清单（3×4C16G 腾讯云 CVM）

> 2026-09-07 租机当天实测落档。数据全部来自 SSH 实际采集（非控制台抄录），
> 采集命令见文末附录。机器为压测 campaign（[stress-plan.md](stress-plan.md)）专用，
> 硬件数字与该方案"3×4C16G"前提一致；后续任何压测报告引用硬件配置以本文档为准。

## 1. 机器清单

| 节点 | 内网 IP | 主机名 | MAC |
|---|---|---|---|
| node-1 | 172.16.16.9 | VM-16-9-ubuntu | 52:54:00:84:73:29 |
| node-2 | 172.16.16.13 | VM-16-13-ubuntu | 52:54:00:07:d1:56 |
| node-3 | 172.16.16.17 | VM-16-17-ubuntu | 52:54:00:9c:d4:f9 |

- 2026-09-07 角色已按内网序拍板并**随装机落地**（node-1 装 PG、node-3 装 docker、node-2 零安装，见 §6）；如需改排需重装对应组件。
- 公网 IP 与 SSH 密钥存放路径属私有部署信息，不入公开仓库；内网 IP 保留作拓扑编排标识（压测方案与部署脚本引用）。
- 登录：密钥认证 `ssh -i <密钥路径> ubuntu@<节点>`。**用户名是 ubuntu，root 被拒**；ubuntu 免密 sudo。密钥文件权限须为 600——0644 会被 ssh 拒载（云厂商下发密钥的常见坑）。

## 2. 硬件与系统（三台完全一致）

| 项 | 值 |
|---|---|
| CPU | AMD EPYC 7K62 48-Core（虚拟化切出）4 vCPU，**1 线程/核（无 SMT）**，单 NUMA 节点 |
| 缓存 | L1d 128 KiB / L2 16 MiB（4 实例）/ L3 16 MiB（1 实例） |
| 内存 | 15978592 kB（~15.2 GiB），开机占用仅 ~600–700 MB |
| 磁盘 | /dev/vda 100G（virtio，ROTA 标志位不可信），vda3 = 98G，已用 ~5.6G，**空闲 ~89G** |
| Swap | /swap.img 2G（文件型，当前 0 使用） |
| 系统 | **Ubuntu 26.04 LTS（resolute）**，内核 **7.0.0-14-generic**，systemd **259** |
| 虚拟化 | KVM（systemd-detect-virt = kvm） |
| cgroup | **v2（cgroup2fs）** —— systemd MemoryMax / unit 资源限制直接生效，无需传统 blkio/memory 子系统配置 |
| 网络 | eth0 单网卡，MTU 1500，172.16.16.0/**20**（注意是 /20 网段，非 /24） |
| 时区/NTP | Asia/Shanghai，三台 NTPSynchronized=yes（跨机日志时间可比） |

**内核启动参数（云镜像预置，对压测有实际影响的三条）**：

```
processor.max_cstate=1 intel_idle.max_cstate=1   # C-state 限深 → 延迟抖动更小
spec_rstack_overflow=off                          # 关 Spectre 缓解 → 系统调用/上下文切换更快
crashkernel=...8G-16G:256M                        # kdump 预留 256M（已含在 15G 内）
```

## 3. 网络实测

内网互通（2026-09-07 实测 ping，3 包）：

| 路径 | RTT min/avg/max (ms) |
|---|---|
| 16.9 ↔ 16.13 | 0.187 / 0.239 / 0.322 |
| 16.9 ↔ 16.17 | 0.183 / 0.267 / 0.405 |
| 16.13 ↔ 16.17 | 0.196 / 0.289 / 0.445 |

- 同子网 RTT ~0.2–0.3ms，中间件跨机调用（PG/etcd/kafka）网络不是瓶颈。
- **ICMP 通 ≠ TCP 通**：腾讯云安全组对子网内 TCP 端口（PG 5432 / etcd 2379 / kafka 9092 / cs 37002 / gateway 8080）是否放行**尚未验证**，起服务后需逐端口实测（计划 checklist 已有此项）。

## 4. 软件基线（2026-09-07，开机 ~16 分钟时点）

**已具备（无需安装）**：envsubst、python3、curl、ss（iproute2）、vmstat/free（procps）、awk、tar、install、sysctl、chronyc/timedatectl、systemd 259。`tools/stress/collect.sh` 与 `deploy/systemd/` 渲染/安装脚本的依赖全齐。

**未安装**：docker、go（见 §5，go 本就不需要）。

**apt 源**：腾讯内网镜像 `mirrors.tencentyun.com`（resolute 主仓），装包走内网不耗公网带宽。可用候选：

| 包 | 候选版本 |
|---|---|
| postgresql（meta） | **18+290ubuntu1**（26.04 无 15） |
| docker.io | 29.1.3-0ubuntu4.1 |
| docker-compose-v2 | 2.40.3+ds1-0ubuntu1 |

**资源基线（压测前参照）**：FD 已分配 ~1100–1500/台，fs.file-max = 9223372036854775807（无需调）；nf_conntrack 模块**未加载**（干净基线，装 docker 后会变，见 §5）。

## 5. 与部署拓扑的对应（按需安装，不是三台全装）

部署形态 = 业务进程 systemd 直跑静态二进制（[deploy/systemd/README.md](../deploy/systemd/README.md)）。中间件按**拓扑**区分：

| 拓扑 | PG | etcd/kafka（docker） |
|---|---|---|
| 2+1'（S1/S2 连接轴） | 仅 node-1（原生） | **仅 node-3**（单点 etcd + 单 broker）；node-1/2 整停 docker 省内存 |
| 混部（S3–S5，DAU/消息轴） | 仅 node-1（原生） | 三台各一成员（etcd×3 + broker×3） |

即：**PG 永远只装 node-1**；docker 视阶段——只跑连接轴则只装 node-3，全程 campaign 则三台一次装齐。另两个装机后必做项：

1. 装完 docker 会因 iptables 拉起 nf_conntrack（默认表项 65536，30 万连接静默丢包）→ 按 [kernel-tuning.md](kernel-tuning.md) 方案 B `nf_conntrack_max=1048576` 并压测期盯 `/proc/net/nf_conntrack_count`；
2. README 的 `postgresql-15` / `/etc/postgresql/15/` 写法在 26.04 上应对应为 **postgresql-18** / `/etc/postgresql/18/main/`。

**目标机不需要 Go**：`build.sh` 以 `CGO_ENABLED=0` 静态交叉编译，运行时链进二进制；Go 工具链只在构建机（Mac，go1.23.5 darwin/arm64）需要。2026-09-07 起 build.sh 已同时打包 brave + stress 双二进制（`bin/brave`、`bin/stress`），`dist/brave-systemd-linux-amd64.tar.gz` 已重建（12MB）。

## 6. 装机与调优记录（2026-09-07，S1 连接轴先行口径）

按 §5 表执行（用户拍板 S1/S2 连接轴先行，混部前 node-1/2 不装 docker）：

| 节点 | 安装 | 结果 |
|---|---|---|
| node-1（16.9） | `postgresql`（meta → 18.6） | active；集群配置（shared_buffers/pg_hba/建库）留部署步骤 |
| node-2（16.13） | 无 | 干净（仅 brave 二进制将部署） |
| node-3（16.17） | `docker.io` 29.1.3 + `docker-compose-v2` 2.40.3 | docker info OK（overlayfs）；ubuntu 已入 docker 组（重登录生效） |

内核调优（`deploy/systemd/tuning/` 两档，参数逐项出自 kernel-tuning.md §3）已应用并验证：

- node-1/2：`ulimit -n` 800000（limits.d）+ tcp_rmem/wmem 512KB 档 + tcp_mem 943718/1887437/2516582 + somaxconn 65535；**nf_conntrack 保持未加载**（未装 docker，连接轴最干净状态）。
- node-3：`ulimit -n` 500000 + 256KB 缓冲档 + tcp_mem 629145/1258291/1887437；nf_conntrack 随 docker 加载，**max 已调 1048576**（实测表项 129）。
- vm.swappiness=1 / overcommit_memory=1 三台统一。

装机实测踩坑（已修入 deploy/systemd/README.md 坑清单）：

1. **云镜像 `/etc/sysctl.conf` 预置 `somaxconn=4096`**，而 `sysctl --system` 的 Debian 约定是 /etc/sysctl.conf 最后应用——任何 `99-*.conf` drop-in（含改名 99-zz-brave.conf）都会被它压掉，必须直接 sed 改该文件。
2. **pam_limits 在会话建立时读取**：同一条 ssh 里"装 limits.d + 验证 ulimit"会看到旧值 1024，新开会话才是真值——不是没生效。
3. conntrack 计数路径是 `/proc/sys/net/netfilter/nf_conntrack_count`（sysctl key），不是 `/proc/net/nf_conntrack_count`。

## 附录：采集命令（复核用）

```bash
# 系统全量信息（=XXX= 为分节标记）
ssh -i <key> ubuntu@<ip> 'hostname; systemd-detect-virt; lscpu | grep -E "^(Architecture|CPU\(s\)|Model name|...)"; free -h; swapon --show; lsblk -d -o NAME,SIZE,MODEL,ROTA; df -h /; ip -br addr; stat -fc %T /sys/fs/cgroup/; uname -r; cat /proc/cmdline; systemctl --version; timedatectl show -p NTPSynchronized; cat /proc/sys/fs/file-nr; lsmod | grep nf_conntrack'
# 内网 RTT
ping -c 3 -i 0.2 <内网IP>
# apt 候选
apt-cache policy postgresql docker.io docker-compose-v2
```
