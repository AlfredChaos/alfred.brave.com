# syntax=docker/dockerfile:1

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

# ---------- Stage 2: borrow etcd binaries from official image ----------
# etcd 官方二进制基于 glibc，因此运行时用 debian(glibc)，而非 alpine(musl)。
FROM quay.io/coreos/etcd:v3.5.5 AS etcd-src

# ---------- Stage 3: runtime (brave + etcd + supervisord) ----------
FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      supervisor gettext-base ca-certificates wget tzdata bash \
    && rm -rf /var/lib/apt/lists/* \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# etcd / etcdctl 二进制
COPY --from=etcd-src /usr/local/bin/etcd /usr/local/bin/etcd
COPY --from=etcd-src /usr/local/bin/etcdctl /usr/local/bin/etcdctl

# brave 二进制 + 配置目录(含 .tmpl 模板) + HTML 模板(server 的 LoadHTMLGlob 需要)
COPY --from=builder /out/brave /app/brave
COPY etc /app/etc
COPY template /app/template

# 运行时辅助脚本
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/etcd-bootstrap.sh /etcd-bootstrap.sh
COPY docker/supervisord-work.conf /etc/supervisord-work.conf
RUN chmod +x /entrypoint.sh /etcd-bootstrap.sh

# PROJECT_PATH 必须指向含 etc/ 的目录，viper 据此加载 etc/<svc>.yaml
ENV PROJECT_PATH=/app
WORKDIR /app

# work: 2379(etcd client)/2380(etcd peer)/37002(joker WS)；server: 37001
EXPOSE 2379 2380 37001 37002

# 角色（work|server）由 compose 的 command 覆盖；默认 work。
ENTRYPOINT ["/entrypoint.sh"]
CMD ["work"]
