#!/usr/bin/env bash
# 目标机安装：解包（本目录即为解包后的 systemd/）→ 铺二进制/units → 渲染配置。
# 用法：./install.sh node1|node2|node3   （拓扑见 README 角色表）
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PKG=$(cd "$HERE/.." && pwd)   # tar 解包根：bin/ etc/ database/ systemd/
ROLE=${1:?usage: install.sh node1|node2|node3}

install -m 0755 "$PKG/bin/brave" /usr/local/bin/brave
install -d /etc/brave
cp -n "$HERE/env.brave.example" /etc/brave/env 2>/dev/null || true
cp -R "$PKG/etc/"*.tmpl /etc/brave/etc/ 2>/dev/null || { install -d /etc/brave/etc; cp "$PKG/etc/"*.tmpl /etc/brave/etc/; }

install -m 0644 "$HERE"/units/*.service /etc/systemd/system/
systemctl daemon-reload

case "$ROLE" in
  node1) UNITS="brave-migrate brave-gateway brave-cs brave-persist" ;;
  node2) UNITS="brave-gateway brave-cs brave-feed brave-fanout brave-persist" ;;
  node3) UNITS="brave-cs brave-feed brave-deliver brave-push brave-ghost" ;;
esac
echo "role $ROLE -> enable: $UNITS"
echo "NEXT: 1) edit /etc/brave/env (real IPs/secrets)  2) ./render-config.sh  3) systemctl start $UNITS"
