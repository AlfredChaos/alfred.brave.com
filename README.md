# Alfred Brave

一个基于Go语言开发的分布式即时通讯系统，集成了服务注册发现、WebSocket通信、数据库迁移等功能。

## 项目架构

该项目采用微服务架构，主要包含以下几个核心服务：

- **Server**: Web服务器，提供HTTP API接口
- **Joker**: 即时通讯核心服务，处理WebSocket连接和消息分发
- **Cloudware**: 服务注册发现中心
- **Database**: 数据库服务和迁移工具

## 技术栈

- Go 1.14+
- Gin Web框架
- GORM数据库ORM
- WebSocket
- etcd服务发现
- CLI工具
- MariaDB/MySQL
·
## 主要功能

1. **即时通讯**
   - WebSocket长连接
   - 实时消息推送
   - 用户在线状态管理

2. **服务治理**
   - 服务注册与发现
   - 服务健康检查
   - 负载均衡

3. **用户系统**
   - 用户注册登录
   - 会话管理
   - 权限控制

4. **数据管理**
   - 数据库迁移工具
   - SQL和Go迁移脚本支持

## 快速开始

### 环境要求

- Go 1.14或更高版本
- MariaDB/MySQL
- etcd

### 配置

配置文件位于`etc/`目录下：
- `brave.yaml`: 主配置文件
- `cloudware.yaml`: 服务发现配置
- `joker.yaml`: 即时通讯服务配置

### 运行服务

1. **启动Web服务**
```bash
go run cmd/brave.go start
```

2. **启动即时通讯服务**
```bash
go run cmd/brave.go joker
```

3. **启动服务发现中心**
```bash
go run cmd/brave.go cloudware
```

### 数据库迁移

创建新的迁移脚本：
```bash
go run cmd/brave.go migration create --name <script_name> --type <sql|go>
```

执行迁移：
```bash
go run cmd/brave.go migration up
```

查看迁移状态：
```bash
go run cmd/brave.go migration status
```

## 项目结构

```
.
├── cmd/            # 命令行入口
├── commands/       # 命令实现
├── common/         # 公共代码
├── conf/          # 配置管理
├── database/      # 数据库相关
├── etc/           # 配置文件
├── event/         # 事件系统
├── internal/      # 内部包
├── joker/         # 即时通讯服务
├── server/        # Web服务
├── cloudware/     # 服务发现
└── template/      # 模板文件
```

## 开发说明

1. **添加新功能**
   - 遵循Go项目标准布局
   - 使用依赖注入管理服务
   - 编写单元测试

2. **代码规范**
   - 遵循Go代码规范
   - 使用gofmt格式化代码
   - 添加适当的注释

3. **错误处理**
   - 使用统一的日志系统
   - 实现优雅的错误处理
   - 保持良好的错误追踪

## 许可证

Private Project for Golang Learning

## 贡献指南

这是一个私有项目，主要用于Go语言学习。如需贡献代码，请联系项目维护者。
