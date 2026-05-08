package util

import (
	"fmt"
	"os"
	"strings"
)

// Logger is the minimal interface expected by the runtime.
type Logger interface {
	Info(msg string, ctx map[string]interface{})
	Warn(msg string, ctx map[string]interface{})
	Error(msg string, ctx map[string]interface{})
}

var enabled = func() bool {
	v := strings.ToLower(os.Getenv("QUEEN_STREAMS_LOG"))
	return v == "1" || v == "true" || v == "yes"
}()

type defaultLogger struct{}

func (defaultLogger) Info(msg string, ctx map[string]interface{}) {
	if !enabled {
		return
	}
	if ctx != nil {
		fmt.Printf("[queen-streams] %s %v\n", msg, ctx)
	} else {
		fmt.Printf("[queen-streams] %s\n", msg)
	}
}

func (defaultLogger) Warn(msg string, ctx map[string]interface{}) {
	if ctx != nil {
		fmt.Fprintf(os.Stderr, "[queen-streams] WARN %s %v\n", msg, ctx)
	} else {
		fmt.Fprintf(os.Stderr, "[queen-streams] WARN %s\n", msg)
	}
}

func (defaultLogger) Error(msg string, ctx map[string]interface{}) {
	if ctx != nil {
		fmt.Fprintf(os.Stderr, "[queen-streams] ERROR %s %v\n", msg, ctx)
	} else {
		fmt.Fprintf(os.Stderr, "[queen-streams] ERROR %s\n", msg)
	}
}

// MakeLogger returns the user-supplied logger if non-nil, otherwise the
// default one (which only prints info-level when QUEEN_STREAMS_LOG=1).
func MakeLogger(custom Logger) Logger {
	if custom != nil {
		return custom
	}
	return defaultLogger{}
}
