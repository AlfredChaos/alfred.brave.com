# 本地开发栈快捷方式（沿用命令行习惯；无外部依赖要求）
.PHONY: local-up local-down local-logs local-rebuild test integration fmt vet

# 起本地全栈（首次会构建镜像；PG/etcd/Kafka 就绪后自动拉起业务）
local-up:
	cd deploy/local && docker compose up -d --build

local-down:
	cd deploy/local && docker compose down

local-logs:
	cd deploy/local && docker compose logs -f --tail=100

# 全量重建（改代码后）
local-rebuild:
	cd deploy/local && docker compose up -d --build --force-recreate gateway cs-1 cs-2 persist deliver ghost

# 单元测试 + 静态检查
test:
	go test ./...

# 集成测试（前置：docker 起好 PG:55432 与 Kafka:9092）
integration:
	go test -tags=integration ./... -count=1 -timeout 600s

fmt:
	gofmt -l .

vet:
	go vet ./...
