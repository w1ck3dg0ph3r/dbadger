package dbadger

import (
	"io"
	"log"
	"strings"

	"github.com/hashicorp/go-hclog"
)

type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warningf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

type nullLogger struct{}

func (l nullLogger) Debugf(format string, args ...any)   {}
func (l nullLogger) Infof(format string, args ...any)    {}
func (l nullLogger) Warningf(format string, args ...any) {}
func (l nullLogger) Errorf(format string, args ...any)   {}
func (l nullLogger) Fatalf(format string, args ...any)   {}

type logFunc func(format string, args ...any)

type badgerLogAdapter struct {
	prefix string
	log    logFunc
}

func newBadgerLogAdapter(prefix string, logger logFunc) *badgerLogAdapter {
	return &badgerLogAdapter{
		prefix: prefix,
		log:    logger,
	}
}

func (l *badgerLogAdapter) Debugf(format string, args ...any) {
	l.log(l.prefix+strings.TrimRight(format, "\r\n"), args...)
}

func (l *badgerLogAdapter) Infof(format string, args ...any) {
	l.log(l.prefix+strings.TrimRight(format, "\r\n"), args...)
}

func (l *badgerLogAdapter) Warningf(format string, args ...any) {
	l.log(l.prefix+strings.TrimRight(format, "\r\n"), args...)
}

func (l *badgerLogAdapter) Errorf(format string, args ...any) {
	l.log(l.prefix+strings.TrimRight(format, "\r\n"), args...)
}

type raftLogAdapter struct {
	prefix string
	log    logFunc
}

func newRaftLogAdapter(prefix string, logger logFunc) *raftLogAdapter {
	return &raftLogAdapter{
		prefix: prefix,
		log:    logger,
	}
}

func (l *raftLogAdapter) Log(level hclog.Level, msg string, args ...any) {
}

func (l *raftLogAdapter) Trace(msg string, args ...any) {
	l.adapt(l.prefix+msg, args...)
}

func (l *raftLogAdapter) Debug(msg string, args ...any) {
	l.adapt(l.prefix+msg, args...)
}

func (l *raftLogAdapter) Info(msg string, args ...any) {
	l.adapt(l.prefix+msg, args...)
}

func (l *raftLogAdapter) Warn(msg string, args ...any) {
	l.adapt(l.prefix+msg, args...)
}

func (l *raftLogAdapter) Error(msg string, args ...any) {
	l.adapt(l.prefix+msg, args...)
}

func (l *raftLogAdapter) adapt(msg string, args ...any) {
	fmt := &strings.Builder{}
	fmtargs := make([]any, 0, len(args)/2)
	fmt.WriteString(msg)
	if len(args) > 0 {
		fmt.WriteString(" (")
		for i := 0; i < len(args); i += 2 {
			if i > 0 {
				fmt.WriteString(", ")
			}
			fmt.WriteString(args[i].(string)) //nolint:forcetypeassert // Per hclog interface
			fmt.WriteString(": ")
			switch arg := args[i+1].(type) {
			case hclog.Format:
				fmt.WriteString(arg[0].(string)) //nolint:forcetypeassert // Per hclog interface
				fmtargs = append(fmtargs, arg[1])
			default:
				fmt.WriteString("%v")
				fmtargs = append(fmtargs, arg)
			}
		}
		fmt.WriteString(")")
	}
	l.log(fmt.String(), fmtargs...)
}

func (l *raftLogAdapter) IsTrace() bool {
	return false
}

func (l *raftLogAdapter) IsDebug() bool {
	return true
}

func (l *raftLogAdapter) IsInfo() bool {
	return true
}

func (l *raftLogAdapter) IsWarn() bool {
	return true
}

func (l *raftLogAdapter) IsError() bool {
	return true
}

//nolint:ireturn,nolintlint // Required to satisfy hclog.Logger
func (l *raftLogAdapter) With(args ...any) hclog.Logger {
	return l
}

func (l *raftLogAdapter) ImpliedArgs() []any {
	return nil
}

//nolint:ireturn,nolintlint // Required to satisfy hclog.Logger
func (l *raftLogAdapter) Named(name string) hclog.Logger {
	return newRaftLogAdapter(name, l.log)
}

//nolint:ireturn,nolintlint // Required to satisfy hclog.Logger
func (l *raftLogAdapter) ResetNamed(name string) hclog.Logger {
	return newRaftLogAdapter(name, l.log)
}

func (l *raftLogAdapter) Name() string {
	return ""
}

func (l *raftLogAdapter) SetLevel(level hclog.Level) {
}

func (l *raftLogAdapter) GetLevel() hclog.Level {
	return hclog.Debug
}

func (l *raftLogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return nil
}

func (l *raftLogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return nil
}

type grpcLogAdapter struct {
	prefix string
	log    logFunc
}

func newGRPCLogAdapter(prefix string, logger logFunc) *grpcLogAdapter {
	return &grpcLogAdapter{
		prefix: prefix,
		log:    logger,
	}
}

func (l *grpcLogAdapter) Info(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Infoln(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Infof(format string, args ...any) {
	l.log(l.prefix+format, args)
}

func (l *grpcLogAdapter) Warning(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Warningln(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Warningf(format string, args ...any) {
	l.log(l.prefix+format, args)
}

func (l *grpcLogAdapter) Error(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Errorln(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Errorf(format string, args ...any) {
	l.log(l.prefix+format, args)
}

func (l *grpcLogAdapter) Fatal(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Fatalln(args ...any) {
	l.log(l.prefix+strings.Repeat("%v ", len(args)), args...)
}

func (l *grpcLogAdapter) Fatalf(format string, args ...any) {
	l.log(l.prefix+format, args)
}

func (l *grpcLogAdapter) V(int) bool {
	return true
}
