package golog

const (
	LEVEL_INFO  = 0
	LEVEL_ERROR = 1
	LEVEL_DEBUG = 2
)

type Logger interface {
	Init() error
	Finish() error
	Log(level int, data ...interface{})
	Logw(level int, message string, data ...interface{})
	Info(data ...interface{})
	Infow(message string, data ...interface{})
	Error(data ...interface{})
	Errorw(message string, data ...interface{})
	Debug(data ...interface{})
	Debugw(message string, data ...interface{})
}
