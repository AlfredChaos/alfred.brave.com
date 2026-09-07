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
