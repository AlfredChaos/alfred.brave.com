package common

import "os"

// global constant
const (
	ProjectName = "brave"
	JokerName   = "joker"
)

// 中间件
const (
	MiddlewareDatabase = iota
	MiddlewareEtcd
)

// 日志等级
const (
	LogLevelDebug = "debug"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
	LogLevelInfo  = "info"
)

// 日志输出
const (
	LogOutputStderr = "stderr"
	LogOutputStdout = "stdout"
	LogOutputFile   = "file"
)

// 时间格式化常量
const (
	TimeFormat = "2006-01-02 15:04:05"
)

// ProjectPath 返回项目根目录（等价 os.Getenv("PROJECT_PATH")，集中一处取用）。
func ProjectPath() string {
	return os.Getenv("PROJECT_PATH")
}
