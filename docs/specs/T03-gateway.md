# T03 · 网关化（token 鉴权 + ws_addr 下发 + cloudware 删除）

## 1. 目标与范围

**做**：
1. `POST /v1/login` 返回 `{token, ws_addr}`：bcrypt 校验 → HMAC token 签发 → 经 etcd 服务表（`StartListenService` 每 5s 刷新）随机选 CS → 下发其 advertise 地址。服务表为空回 503。
2. token 鉴权中间件 `AuthRequired`（Bearer，注入 uid 到 gin.Context）；`/v1/user/:id`、`/v1/users`、`/v1/friends*` 挂保护；`/v1/register`、`/v1/login` 公开。
3. 好友 API（POST/GET/DELETE `/v1/friends`）与用户列表（GET `/v1/users`）——T19 Web 客户端选人/加好友的最小闭环。
4. cloudware 删除：包、子命令、常量、etc/cloudware.yaml（§1 决策表：服务发现已由 etcd+网关承担）。
5. 旧登录链路移除：网关不再调 joker HTTP /login、不再读写 etcd users/{uid}（joker 侧端点 T04 随 online kv 一起移除）。

**不做**：WS 连接鉴权（身份仍由 /ws/:id 提供，已声明的演示级限制）；feed 路由（T09）；会话/历史 API（T05/T06）。

## 2. 接口契约

- `POST /v1/login {user_name|email, password}` → 200：
  ```json
  {"uid": "...", "token": "<uid.exp.hmac>", "ws_addr": "cs-host:37002", "user_name": "...", "friends": [...]}
  ```
  失败：400 参数非法 / 401 账密错 / 503 无可用 CS。
- token 格式：`base64url(uid).base64url(expUnix).base64url(hmac-sha256(secret, payload))`，TTL 7d，secret 来自 `auth.secret`（网关与 feed-api 共享）。
- `Authorization: Bearer <token>`；无效/过期 → 401 `Unauthorized: invalid or expired token`。
- `POST /v1/friends {uid | user_name}`、`GET /v1/friends`、`DELETE /v1/friends/:uid`、`GET /v1/users`（保护）。

## 3. 架构符合性声明

- **D01**：网关只做鉴权/选点/下发 ws_addr，不接触消息数据面；WS 直连 CS 不变。
- **§3 0a-0d**：login → etcd 选 CS → 下发 ws_addr → WS 直连，与握手四步一致。
- **§1 决策表 cloudware 行**：删除（消除死代码）。
- token 为自签 HMAC 非 JWT：字段仅 uid+exp，零依赖；代价是不可吊销（练手可接受，声明在包注释）。

## 4. 测试计划

- `internal/token`：往返/过期/篡改/错 secret/垃圾输入（5 用例全绿）。
- `server/api` 中间件：无 header、非 Bearer、坏 token、过期 → 401；有效 → 200 且 uid 注入。
- 登录全流程含 DB/etcd，留 T08 compose 端到端验证。

## 5. 验收标准

```bash
gofmt -l . && go vet ./... && go build ./... && go test ./...   # 全绿
git grep cloudware -- '*.go'                                    # 无结果
```

## 6. 验收记录（实测粘贴）

- 四门禁全绿（gofmt 空 / vet 0 告警 / build ok / test 4 包 ok 0 失败）✅
- `git grep cloudware -- '*.go'` → 无输出 ✅
- token 单测 5/5、middleware 用例 4+1 全 PASS ✅

## 7. Review 记录

- D01 职责边界 ✅；无新增包级可变全局（Services 为既有变量沿用）✅
- 已知限制（诚实声明）：WS 升级不校验 token；joker /login HTTP 端点仍在（T04 删）；根 docker-compose.yml 仍旧拓扑（T17 重建）
