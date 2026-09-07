#!/usr/bin/env bash
# 交叉编译 + 打包 systemd 部署 tar（在仓库根或任意目录执行）。
# 产物 dist/brave-systemd-<os>-<arch>.tar.gz：brave 二进制 + etc 模板 + migration SQL
# + units + 中间件 compose + 安装脚本。scp 到三台后 ./install.sh <nodeN>。
set -euo pipefail
cd "$(dirname "$0")/../.."

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}   # 真机架构（默认 x86_64）
DIST=dist
TAG="$GOOS-$GOARCH"

mkdir -p "$DIST/pkg"
echo "building brave ($GOOS/$GOARCH, static)..."
CGO_ENABLED=0 GOOS=$GOOS GOARCH=$GOARCH go build -trimpath -ldflags="-s -w" -o "$DIST/pkg/bin/brave" ./cmd

# 打包运行资产（与 Dockerfile COPY 集对齐：etc 模板 / migration SQL / systemd 套件）
# 只拷 .tmpl：仓库里 committed 的渲染产物 yaml（含 127.0.0.1 残留）不进包
install -d "$DIST/pkg/etc" "$DIST/pkg/database" "$DIST/pkg/systemd"
cp etc/*.tmpl "$DIST/pkg/etc/"
cp -R database/migration "$DIST/pkg/database/migration"
cp -R deploy/systemd/units deploy/systemd/middleware deploy/systemd/*.sh deploy/systemd/env.brave.example deploy/systemd/README.md "$DIST/pkg/systemd/"

tar czf "$DIST/brave-systemd-$TAG.tar.gz" -C "$DIST/pkg" bin etc database systemd
rm -rf "$DIST/pkg"
echo "done: $DIST/brave-systemd-$TAG.tar.gz"
ls -lh "$DIST/brave-systemd-$TAG.tar.gz"
