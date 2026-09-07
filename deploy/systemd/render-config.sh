#!/usr/bin/env bash
# 在目标机上渲染配置：/etc/brave/env (KEY=VALUE) + etc/*.tmpl -> etc/*.yaml。
# 由 install.sh 调用，也可单独重跑（改 env 后 systemctl restart brave-*）。
set -euo pipefail
ROOT=${1:-/etc/brave}

[ -f "$ROOT/env" ] || { echo "ERROR: $ROOT/env missing (see env.brave.example)"; exit 1; }
set -a; source "$ROOT/env"; set +a

mkdir -p "$ROOT/etc"
for tmpl in "$ROOT"/etc/*.tmpl; do
  out="${tmpl%.tmpl}"
  if command -v envsubst >/dev/null; then
    envsubst < "$tmpl" > "$out"
  else
    # 无 gettext 的兜底：python 逐变量替换
    python3 - "$tmpl" "$out" << 'PY'
import sys, os
src, dst = sys.argv[1], sys.argv[2]
t = open(src).read()
for k, v in os.environ.items():
    t = t.replace("${%s}" % k, v)
open(dst, "w").write(t)
PY
  fi
  echo "rendered $(basename "$out")"
done

# worker 角色别名：persist/deliver/ghost/fanout/push 各自按服务名读 <role>.yaml
# （对齐 docker/entrypoint.sh 的 render worker.yaml.tmpl → $ROLE.yaml）
if [ -f "$ROOT/etc/worker.yaml" ]; then
  for role in persist deliver ghost fanout push; do
    cp "$ROOT/etc/worker.yaml" "$ROOT/etc/$role.yaml"
    echo "rendered $role.yaml"
  done
fi
