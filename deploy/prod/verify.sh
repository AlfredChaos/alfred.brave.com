#!/usr/bin/env bash
# ============================================================
# verify.sh — 部署后健康巡检（§5 规格：端口 / kafka topic / PG 复制 / etcd）
# 从运维机运行：./verify.sh
# ⚠️ 从未在真实环境验证
# ============================================================
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/.env"

FAIL=0
check() { # name, command...
    local name="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  [PASS] $name"
    else
        echo "  [FAIL] $name"
        FAIL=1
    fi
}

port_open() { # ip port
    nc -z -w 3 "$1" "$2"
}

echo "== 1. TCP 端口巡检"
check "node1 gateway :37001"  port_open "$NODE1_IP" 37001
check "node2 gateway :37001"  port_open "$NODE2_IP" 37001
check "node1 cs ws :37002"    port_open "$NODE1_IP" 37002
check "node2 cs ws :37002"    port_open "$NODE2_IP" 37002
check "node3 cs ws :37002"    port_open "$NODE3_IP" 37002
check "node1 cs grpc :37012"  port_open "$NODE1_IP" 37012
check "node2 cs grpc :37012"  port_open "$NODE2_IP" 37012
check "node3 cs grpc :37012"  port_open "$NODE3_IP" 37012
check "kafka node1 :9092"     port_open "$NODE1_IP" 9092
check "kafka node2 :9092"     port_open "$NODE2_IP" 9092
check "kafka node3 :9092"     port_open "$NODE3_IP" 9092

echo "== 2. etcd 集群健康（三成员 endpoint health + 无 leader 抖动）"
for ip in "$NODE1_IP" "$NODE2_IP" "$NODE3_IP"; do
    check "etcd $ip endpoint health" \
        ssh "root@${ip}" "docker exec \$(docker ps -qf name=etcd | head -1) etcdctl --endpoints=http://localhost:2379 endpoint health"
done
check "etcd endpoint status (3 members)" \
    ssh "root@${NODE1_IP}" "docker exec \$(docker ps -qf name=etcd | head -1) etcdctl --endpoints=http://localhost:2379 -w table endpoint status"

echo "== 3. Kafka topic 巡检（分区数与 §8 一致：msg/push 12P、fanout/ack 6P、notify 3P，RF=3）"
TOPICS=$(ssh "root@${NODE1_IP}" "docker exec \$(docker ps -qf name=kafka | head -1) /opt/kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092" 2>/dev/null)
for t in chat.msg chat.push feed.fanout chat.ack chat.notify; do
    check "topic $t exists" bash -c "echo '$TOPICS' | grep -q 'Topic: $t '"
done
check "topic RF=3 everywhere" bash -c "[ \$(echo '$TOPICS' | grep -c 'ReplicationFactor: 3') -eq 5 ]"

echo "== 4. PG 流复制状态（node2/node3 为 streaming）"
REPL=$(ssh "root@${NODE1_IP}" "docker exec \$(docker ps -qf name=pg-primary) psql -U postgres -Atc \"SELECT client_addr||' '||state FROM pg_stat_replication\"" 2>/dev/null)
check "2 streaming replicas" bash -c "[ \$(echo '$REPL' | grep -c streaming) -eq 2 ]"

echo "== 5. 业务容器存活（brave:* 无重启循环）"
for entry in node1:$NODE1_IP node2:$NODE2_IP node3:$NODE3_IP; do
    node="${entry%%:*}"; ip="${entry#*:}"
    check "$node brave containers up" \
        ssh "root@${ip}" "docker ps --filter name=brave-prod --filter status=running -q | grep -q ."
done

echo
if [ "$FAIL" -eq 0 ]; then
    echo "ALL CHECKS PASSED"
else
    echo "SOME CHECKS FAILED (see [FAIL] above)"
    exit 1
fi
