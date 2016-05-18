// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zap

import (
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// A Logger enables leveled, structured logging. All methods are safe for
// concurrent use.
type Logger interface {
	// Check if output at a specific level is enabled.
	Enabled(Level) bool
	// Check the minimum enabled log level.
	Level() Level
	// Change the level of this logger, as well as all its ancestors and
	// descendants. This makes makes it easy to change the log level at runtime
	// without restarting your application.
	SetLevel(Level)
	// Create a child logger, and optionally add some context to that logger.
	With(...Field) Logger
	// StubTime stops the logger from including the current time in each
	// message. Instead, it always reports the time as Unix epoch 0. (This is
	// useful in tests and examples.)
	StubTime()

	// Log a message at the given level. Messages include any context that's
	// accumulated on the logger, as well as any fields added at the log site.
	Debug(string, ...Field)
	Info(string, ...Field)
	Warn(string, ...Field)
	Error(string, ...Field)
	Panic(string, ...Field)
	Fatal(string, ...Field)
}

type jsonLogger struct {
	level       *int32 // atomic
	enc         encoder
	hooks       []hook
	errW        WriteSyncer
	w           WriteSyncer
	alwaysEpoch bool
	appendFunc  AppendFunc
}

type AppendFunc func(data []byte, t time.Time) []byte

func defaultAppendFunc(data []byte, ts time.Time) []byte {
	data = append(data, `,"ts":`...)
	return strconv.AppendInt(data, ts.UnixNano(), 10)
}

// NewJSON returns a logger that formats its output as JSON. Zap uses a
// customized JSON encoder to avoid reflection and minimize allocations.
//
// By default, the logger will write Info logs or higher to standard
// out. Any errors during logging will be written to standard error.
//
// Options can change the log level, the output location, or the initial
// fields that should be added as context.
func NewJSON(options ...Option) Logger {
	defaultLevel := int32(Info)
	jl := &jsonLogger{
		enc:        newJSONEncoder(),
		level:      &defaultLevel,
		errW:       os.Stderr,
		w:          os.Stdout,
		appendFunc: defaultAppendFunc,
	}

	for _, opt := range options {
		if err := opt.apply(jl); err != nil {
			jl.internalError(err.Error())
		}
	}

	return jl
}

func (jl *jsonLogger) Level() Level {
	return Level(atomic.LoadInt32(jl.level))
}

func (jl *jsonLogger) SetLevel(lvl Level) {
	atomic.StoreInt32(jl.level, int32(lvl))
}

func (jl *jsonLogger) Enabled(lvl Level) bool {
	return lvl >= jl.Level()
}

func (jl *jsonLogger) With(fields ...Field) Logger {
	if len(fields) == 0 {
		return jl
	}
	clone := &jsonLogger{
		level:       jl.level,
		enc:         jl.enc.Clone(),
		hooks:       jl.hooks,
		w:           jl.w,
		errW:        jl.errW,
		alwaysEpoch: jl.alwaysEpoch,
		appendFunc:  jl.appendFunc,
	}
	if err := clone.enc.AddFields(fields); err != nil {
		jl.internalError(err.Error())
	}
	return clone
}

func (jl *jsonLogger) StubTime() {
	jl.alwaysEpoch = true
}

func (jl *jsonLogger) Debug(msg string, fields ...Field) {
	jl.log(Debug, msg, fields)
}

func (jl *jsonLogger) Info(msg string, fields ...Field) {
	jl.log(Info, msg, fields)
}

func (jl *jsonLogger) Warn(msg string, fields ...Field) {
	jl.log(Warn, msg, fields)
}

func (jl *jsonLogger) Error(msg string, fields ...Field) {
	jl.log(Error, msg, fields)
}

func (jl *jsonLogger) Panic(msg string, fields ...Field) {
	jl.log(Panic, msg, fields)
	panic(msg)
}

func (jl *jsonLogger) Fatal(msg string, fields ...Field) {
	jl.log(Fatal, msg, fields)
	os.Exit(1)
}

func (jl *jsonLogger) log(lvl Level, msg string, fields []Field) {
	if !jl.Enabled(lvl) {
		return
	}

	temp := jl.enc.Clone()
	if err := temp.AddFields(fields); err != nil {
		jl.internalError(err.Error())
	}

	for _, hook := range jl.hooks {
		newMsg, err := hook(lvl, msg, temp)
		if err != nil {
			jl.internalError(err.Error())
		}
		msg = newMsg
	}

	now := time.Now().UTC()
	if jl.alwaysEpoch {
		now = time.Unix(0, 0)
	}
	if err := temp.WriteMessage(jl.w, lvl.String(), msg, now, jl.appendFunc); err != nil {
		jl.internalError(err.Error())
	}
	temp.Free()

	if lvl > Error {
		// Sync on Panic and Fatal, since they may crash the program.
		jl.w.Sync()
	}
}

func (jl *jsonLogger) internalError(msg string) {
	fmt.Fprintln(jl.errW, msg)
	jl.errW.Sync()
}
