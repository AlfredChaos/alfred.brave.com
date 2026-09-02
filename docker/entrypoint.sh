#!/usr/bin/env bash
# 容器入口：按角色渲染 yaml 模板并启动对应 brave 子命令。
# 角色：gateway(server) | joker | persist | deliver | ghost | fanout | push | feed | migrate
set -euo pipefail

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

ROLE="${1:-joker}"
export PROJECT_PATH="${PROJECT_PATH:-/app}"
ETC="$PROJECT_PATH/etc"

case "$ROLE" in
    gateway|server)
        render "$ETC/brave.yaml.tmpl" "$ETC/brave.yaml"
        echo "[entrypoint] role=$ROLE, launching brave start"
        exec /app/brave start
        ;;
    joker)
        render "$ETC/joker.yaml.tmpl" "$ETC/joker.yaml"
        echo "[entrypoint] role=joker, launching brave joker"
        exec /app/brave joker
        ;;
    persist|deliver|ghost|fanout|push)
        render "$ETC/worker.yaml.tmpl" "$ETC/$ROLE.yaml"
        echo "[entrypoint] role=$ROLE, launching brave $ROLE"
        exec /app/brave "$ROLE"
        ;;
    feed)
        render "$ETC/feed.yaml.tmpl" "$ETC/feed.yaml"
        echo "[entrypoint] role=feed, launching brave feed"
        exec /app/brave feed
        ;;
    migrate)
        # migration 子命令读 brave 配置（conf.InitConfig(ProjectName)），渲染对应模板
        render "$ETC/brave.yaml.tmpl" "$ETC/brave.yaml"
        echo "[entrypoint] role=migrate, running brave migration up"
        exec /app/brave migration up
        ;;
    work)
        # 旧拓扑（etcd+joker 同容器 supervisord），已被 deploy/local 取代，保留兼容
        render "$ETC/joker.yaml.tmpl" "$ETC/joker.yaml"
        echo "[entrypoint] role=work, launching supervisord (etcd + joker)"
        exec /usr/bin/supervisord -c /etc/supervisord-work.conf
        ;;
    *)
        echo "[entrypoint] unknown role: '$ROLE'" >&2
        exit 1
        ;;
esac
