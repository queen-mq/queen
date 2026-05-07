// Package clierr defines the queenctl exit-code conventions used by every
// command. Shell scripts can rely on these to branch on outcome.
package clierr

import (
	"errors"
	"fmt"
	"os"
)

const (
	// CodeOK indicates successful execution.
	CodeOK = 0
	// CodeUser indicates a user-side error: bad flag, bad input, missing arg.
	CodeUser = 1
	// CodeServer indicates the server returned an error or was unreachable.
	CodeServer = 2
	// CodeAuth indicates an authentication or authorization failure.
	CodeAuth = 3
	// CodeEmpty indicates a successful no-op (empty pop, no DLQ messages, etc.)
	// so shell scripts can branch without parsing output.
	CodeEmpty = 4
)

// Error is a typed CLI error carrying an exit code.
type Error struct {
	Code int
	Err  error
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

// User wraps err as a user-side error (exit 1).
func User(err error) error {
	if err == nil {
		return nil
	}
	return &Error{Code: CodeUser, Err: err}
}

// Userf builds a user-side error from format args.
func Userf(format string, a ...any) error {
	return &Error{Code: CodeUser, Err: fmt.Errorf(format, a...)}
}

// Server wraps err as a server-side error (exit 2).
func Server(err error) error {
	if err == nil {
		return nil
	}
	return &Error{Code: CodeServer, Err: err}
}

// Auth wraps err as an authentication error (exit 3).
func Auth(err error) error {
	if err == nil {
		return nil
	}
	return &Error{Code: CodeAuth, Err: err}
}

// Empty marks a successful no-op (exit 4).
func Empty(msg string) error {
	return &Error{Code: CodeEmpty, Err: errors.New(msg)}
}

// CodeOf returns the exit code for an error, defaulting to CodeServer for
// unwrapped errors and CodeOK for nil.
func CodeOf(err error) int {
	if err == nil {
		return CodeOK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeServer
}

// Exit prints the error to stderr (when non-empty) and exits with the
// matching code. Should be called from a single place in main / cobra root.
func Exit(err error) {
	if err == nil {
		os.Exit(CodeOK)
	}
	code := CodeOf(err)
	// Empty is success-with-signal; don't print the message unless DEBUG.
	if code != CodeEmpty {
		if msg := err.Error(); msg != "" {
			fmt.Fprintln(os.Stderr, "queenctl: "+msg)
		}
	}
	os.Exit(code)
}
