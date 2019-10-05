package golog

import "go.uber.org/zap"

func StdOut(level int) Logger {
	return newBase(level)
}

type baseLogger struct {
	level int
	l     *zap.SugaredLogger
}

func newBase(level int) *baseLogger {
	b := &baseLogger{
		level: level,
	}

	pl, _ := zap.NewProduction()
	b.l = pl.Sugar()

	return b
}

func (b *baseLogger) Init() error {
	return nil
}

func (b *baseLogger) Finish() error {
	// nothing to do here
	return nil
}

func (b *baseLogger) Log(level int, data ...interface{}) {
	if b.level >= level {
		b.l.Info(data...)
	}
}

func (b *baseLogger) Logw(level int, message string, data ...interface{}) {
	if b.level >= level {
		b.l.Infow(message, data...)
	}
}

func (b *baseLogger) Info(data ...interface{}) {
	if b.level >= LEVEL_INFO {
		b.l.Info(data...)
	}
}

func (b *baseLogger) Infow(message string, data ...interface{}) {
	if b.level >= LEVEL_INFO {
		b.l.Infow(message, data...)
	}
}

func (b *baseLogger) Error(data ...interface{}) {
	if b.level >= LEVEL_ERROR {
		b.l.Info(data...)
	}
}

func (b *baseLogger) Errorw(message string, data ...interface{}) {
	if b.level >= LEVEL_ERROR {
		b.l.Infow(message, data...)
	}
}

func (b *baseLogger) Debug(data ...interface{}) {
	if b.level >= LEVEL_DEBUG {
		b.l.Info(data...)
	}
}

func (b *baseLogger) Debugw(message string, data ...interface{}) {
	if b.level >= LEVEL_DEBUG {
		b.l.Infow(message, data...)
	}
}
