package common

// global constant
const (
	ProjectName   = "brave"
	JokerName     = "joker"
	CloudwareName = "cloudware"
)

// 中间间
const (
	MiddlewareMysql = iota
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
