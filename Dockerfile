# ---------- Stage 1: build brave binary ----------
# 锁定 >= go.mod 的 toolchain(go1.23.5)，并用 GOTOOLCHAIN=local 禁止构建期下载 toolchain。
FROM golang:1.23.5 AS builder
WORKDIR /src
ENV GOTOOLCHAIN=local \
    GOPROXY=https://goproxy.cn,direct \
    GOSUMDB=off \
    CGO_ENABLED=0
# 先拷依赖清单，利用层缓存
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/brave ./cmd

# ---------- Stage 2: runtime ----------
# 新栈（deploy/local）中 etcd/kafka/pg 均为独立容器，brave 镜像只需静态二进制 +
# envsubst 渲染配置 + CA/时区。alpine 足够（brave 为 CGO_ENABLED=0 静态链接）。
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata bash gettext \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# brave 二进制 + 配置目录(含 .tmpl 模板)
COPY --from=builder /out/brave /app/brave
COPY etc /app/etc
COPY template /app/template
COPY database /app/database
COPY web /app/web

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# PROJECT_PATH 必须指向含 etc/ 的目录，viper 据此加载 etc/<svc>.yaml
ENV PROJECT_PATH=/app
WORKDIR /app

# gateway:37001 · cs WS:37002 / gRPC:37012
EXPOSE 37001 37002 37012

# 角色由 compose 的 command 覆盖（gateway|joker|persist|deliver|ghost|migrate）
ENTRYPOINT ["/entrypoint.sh"]
CMD ["joker"]
