#!/usr/bin/env bash
# etcd 配置全部通过其标准环境变量(ETCD_*)注入，命令行不再重复传 flag。
# 原因：etcd 把 ETCD_<FLAG> 当作自身环境变量，若同时又用同名命令行 flag 传值，
# etcd 会因“环境变量被 flag 遮蔽”而 fatal 退出（防歧义）。
# 这里只做关键变量校验，再 exec etcd（etcd 自行读取 ETCD_* 环境变量）。
set -euo pipefail

: "${ETCD_NAME:?ETCD_NAME is required}"
: "${ETCD_INITIAL_CLUSTER:?ETCD_INITIAL_CLUSTER is required}"
: "${ETCD_INITIAL_ADVERTISE_PEER_URLS:?ETCD_INITIAL_ADVERTISE_PEER_URLS is required}"
: "${ETCD_ADVERTISE_CLIENT_URLS:?ETCD_ADVERTISE_CLIENT_URLS is required}"

exec etcd
