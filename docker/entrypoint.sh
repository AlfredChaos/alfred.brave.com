#!/usr/bin/env bash
# 容器入口：用环境变量渲染 yaml 模板，再按角色(work|server)启动对应进程。
# - work: supervisord 托管 etcd + joker（joker 连本地 127.0.0.1:2379）
# - server: 单进程 brave start
set -euo pipefail

# 将 ${VAR} 形式的模板渲染成最终配置（envsubst 用当前环境变量替换）。
render() {
    local tmpl="$1"
    local out="$2"
    if [ -f "$tmpl" ]; then
        envsubst < "$tmpl" > "$out"
        echo "[entrypoint] rendered $out from $tmpl"
    else
        echo "[entrypoint] WARN: template $tmpl not found, skip" >&2
    fi
}

ROLE="${1:-work}"
export PROJECT_PATH="${PROJECT_PATH:-/app}"

case "$ROLE" in
    work)
        # joker 注册地址用容器名 advertise_host，etcd 连本地成员
        render "$PROJECT_PATH/etc/joker.yaml.tmpl" "$PROJECT_PATH/etc/joker.yaml"
        echo "[entrypoint] role=work, launching supervisord (etcd + joker)"
        exec /usr/bin/supervisord -c /etc/supervisord-work.conf
        ;;
    server)
        render "$PROJECT_PATH/etc/brave.yaml.tmpl" "$PROJECT_PATH/etc/brave.yaml"
        echo "[entrypoint] role=server, launching brave start"
        exec /app/brave start
        ;;
    *)
        echo "[entrypoint] unknown role: '$ROLE' (expect 'work' or 'server')" >&2
        exit 1
        ;;
esac
