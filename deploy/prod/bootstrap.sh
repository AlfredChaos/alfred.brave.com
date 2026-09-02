#!/usr/bin/env bash
# ============================================================
# bootstrap.sh — 生产三节点首次部署（§5 规格T18：写好不执行）
# 从运维机运行：./bootstrap.sh /path/to/repo
# 步骤：分发代码 → 装 docker → 构建镜像 → 起 node1（含迁移/init）→ 起 node2/3
# ⚠️ 从未在真实环境验证（服务器未购买）
# ============================================================
set -euo pipefail

REPO_PATH="${1:?usage: bootstrap.sh /path/to/repo}"
HERE="$(cd "$(dirname "$0")" && pwd)"

# 从 .env 读节点 IP（占位符检查：默认值未改则拒绝继续）
if [ ! -f "$HERE/.env" ]; then
    echo "ERROR: $HERE/.env not found. Copy .env.example to .env and fill real IPs." >&2
    exit 1
fi
# shellcheck disable=SC1091
source "$HERE/.env"
for v in NODE1_IP NODE2_IP NODE3_IP; do
    val="${!v}"
    if [[ "$val" == 10.0.0.* ]]; then
        echo "ERROR: $v is still the placeholder ($val). Fill real IPs first." >&2
        exit 1
    fi
done

NODES=(node1:$NODE1_IP node2:$NODE2_IP node3:$NODE3_IP)

echo "==> [0/5] preflight: ssh reachability"
for entry in "${NODES[@]}"; do
    ip="${entry#*:}"
    ssh -o ConnectTimeout=5 "root@${ip}" true
done

install_docker() {
    local ip="$1"
    ssh "root@${ip}" bash -s <<'REMOTE'
        if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
            echo "docker already installed"; exit 0
        fi
        curl -fsSL https://get.docker.com | sh
        systemctl enable --now docker
REMOTE
}

for entry in "${NODES[@]}"; do
    node="${entry%%:*}"; ip="${entry#*:}"
    echo "==> [1/5] $node ($ip): install docker"
    install_docker "$ip"

    echo "==> [2/5] $node: sync repo + prod config"
    ssh "root@${ip}" "mkdir -p /opt/brave"
    rsync -a --delete --exclude .git "$REPO_PATH/" "root@${ip}:/opt/brave/"
    rsync -a "$HERE/.env" "root@${ip}:/opt/brave/deploy/prod/.env"
done

# node1 先起（PG primary + 迁移 + topic init），等健康后再起 2/3（replica 需要 primary）
echo "==> [3/5] node1 up (primary + migrate + kafka-init)"
ssh "root@${NODE1_IP}" "cd /opt/brave && docker build -t brave:local . && \
    cd deploy/prod && NODE_ROLE=node1 docker compose -f docker-compose.prod.yml --profile node1 up -d"

echo "==> [4/5] wait for node1 primary readiness"
until ssh "root@${NODE1_IP}" "docker exec \$(docker ps -qf name=pg-primary) pg_isready -U postgres" >/dev/null 2>&1; do
    sleep 5; echo "    ... waiting for pg primary"
done

for entry in node2:$NODE2_IP node3:$NODE3_IP; do
    node="${entry%%:*}"; ip="${entry#*:}"
    echo "==> [5/5] $node ($ip) up"
    ssh "root@${ip}" "cd /opt/brave && docker build -t brave:local . && \
        cd deploy/prod && NODE_ROLE=$node docker compose -f docker-compose.prod.yml --profile $node up -d"
done

echo "bootstrap done. Run ./verify.sh next."
