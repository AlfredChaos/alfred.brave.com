#!/usr/bin/env bash
# collect.sh —— 被测节点资源采集（stress-plan.md §4.2）。
# 在每台被测节点上运行，默认 10s 周期追加 CSV；Ctrl-C 结束时写 collect-summary.json。
# 依赖：/proc、ss、free、vmstat（procps）；docker 可选（容器级 pprof）；curl 可选。
#
# 用法：./collect.sh [输出目录]        # 环境变量：COLLECT_INTERVAL=10 COLLECT_CS_CONTAINER=cs-1
set -euo pipefail

OUT="${1:-collect-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"
CSV="$OUT/collect.csv"
INTERVAL="${COLLECT_INTERVAL:-10}"
CS_CTR="${COLLECT_CS_CONTAINER:-}"

echo "collecting every ${INTERVAL}s -> $CSV (Ctrl-C to stop)"

cleanup() {
  {
    echo "{"
    echo "  \"stopped_at\": \"$(date -Iseconds)\","
    echo "  \"interval_s\": $INTERVAL,"
    echo "  \"samples\": $(( $(wc -l < "$CSV") - 1 )),"
    echo "  \"columns\": \"ts,cpu_idle_used_pct,mem_used_mb,mem_used_pct,swap_si_so_kb,fds_allocated,tcp_estab,tcp_timewait,retrans_segs_delta,cs_goroutines,relay_msg,relay_ack,relay_send_full,relay_not_found\","
    echo "  \"cs_container\": \"${CS_CTR:-none}\","
    echo "  \"note\": \"stress-plan.md §2.3 指标；cpu 列=100-idle（1s 采样窗）；retrans 为周期增量\""
    echo "}"
  } > "$OUT/collect-summary.json"
  echo "done: $CSV (+ collect-summary.json)"
}
trap cleanup EXIT

echo "ts,cpu_used_pct,mem_used_mb,mem_used_pct,swap_si_so_kb,fds_allocated,tcp_estab,tcp_timewait,retrans_delta,cs_goroutines,relay_msg,relay_ack,relay_send_full,relay_not_found" > "$CSV"

# 自动发现 cs 容器（prod: brave-prod-cs-1-1 / local: brave-local-cs-1-1）
if [ -z "$CS_CTR" ]; then
  CS_CTR=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E '(^|-)cs-1(-|$)' | head -1 || true)
fi
[ -n "$CS_CTR" ] && echo "cs container for goroutine sampling: $CS_CTR"

# goroutine 采样：优先直连本机 cs pprof（systemd 直跑形态，BRAVE_PPROF=1 → 127.0.0.1:6060）；
# 拿不到再走 docker exec（compose 形态）。COLLECT_CS_PPOF_URL 可覆盖（如 6061 第二实例）。
PPROF_URL="${COLLECT_CS_PPROF_URL:-http://127.0.0.1:6060}"

goroutines() {
  local n
  n=$(curl -s --max-time 2 "$PPROF_URL/debug/pprof/goroutine?debug=1" 2>/dev/null | head -1 | grep -oE '[0-9]+' | head -1)
  if [ -n "$n" ]; then echo "$n"; return 0; fi
  [ -z "$CS_CTR" ] && return 0
  docker exec "$CS_CTR" sh -c \
    'curl -s "http://127.0.0.1:6060/debug/pprof/goroutine?debug=1" 2>/dev/null | head -1' 2>/dev/null \
    | grep -oE '[0-9]+' | head -1 || true
}

prev_rt=""
while true; do
  ts=$(date -Iseconds)
  vm=$(vmstat 1 2 2>/dev/null | tail -1)
  cpu=$(echo "$vm" | awk '{print 100-$15}')
  swap=$(echo "$vm" | awk '{print $7+$8}')
  memu=$(free -m | awk '/^Mem:/{print $3}')
  memp=$(free -m | awk '/^Mem:/{printf "%.1f", $3/$2*100}')
  fds=$(awk '{print $1}' /proc/sys/fs/file-nr)
  estab=$(ss -Htan state established 2>/dev/null | wc -l)
  tw=$(ss -Htan state time-wait 2>/dev/null | wc -l)
  rt=$(awk '/^Tcp:/{
      if (!h++) { split($0,H," "); next }
      split($0,V," ")
      for (i=1;i<=length(H);i++) if (H[i]=="RetransSegs") { print V[i]; exit }
    }' /proc/net/snmp)
  [ -z "$rt" ] && rt=0
  if [ -n "$prev_rt" ]; then rtg=$(( rt - prev_rt )); else rtg=0; fi
  prev_rt=$rt
  gr=$(goroutines)
  # relay 计数快照（S3c）：增量列由分析端做差；无 /debug/relay 时记 0
  relay=$(curl -s --max-time 2 "$PPROF_URL/debug/relay" 2>/dev/null || true)
  r_msg=$(echo "$relay" | grep -oE '"message_enqueued":[0-9]+' | cut -d: -f2)
  r_ack=$(echo "$relay" | grep -oE '"ack_enqueued":[0-9]+' | cut -d: -f2)
  r_full=$(echo "$relay" | grep -oE '"send_full":[0-9]+' | cut -d: -f2)
  r_nf=$(echo "$relay" | grep -oE '"not_found":[0-9]+' | cut -d: -f2)
  echo "$ts,${cpu:-},${memu:-},${memp:-},${swap:-},${fds:-},${estab:-},${tw:-},${rtg:-0},${gr:-},${r_msg:-0},${r_ack:-0},${r_full:-0},${r_nf:-0}" >> "$CSV"
  sleep "$INTERVAL"
done
