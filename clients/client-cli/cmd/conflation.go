package cmd

import (
	"errors"
	"os"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
)

// Conflation support shared by `queenctl pop --conflation` and
// `queenctl tail --conflation` (PLAN_CONFLATION.md §4, the client-cli row).
//
// queenctl sends the flag through the Go SDK and does not re-implement any of
// the policy: the broker owns it (§3.3 — SQL is the authority, the request flag
// only ever registers a new group or reveals a disagreement). What queenctl
// owns is making the two things the SDK reports actually reach a human at a
// terminal, which is a different job from making them reach a program.

// showConflationWarnings lifts the SDK's log floor to WARN for this invocation.
//
// A queenctl invocation can perfectly well ask for one policy and get another:
// conflation belongs to the consumer GROUP, so the broker answers
// "conflationConflict":true, keeps its stored setting, and keeps serving. The
// SDK notes that exactly once per (queue, group) per process — but through its
// own logger, which is OFF unless QUEEN_CLIENT_LOG is set, and an operator who
// just typed --conflation has no reason to know that variable exists. Silently
// ignoring a flag someone typed is the failure mode §4 exists to prevent, so a
// --conflation invocation opts itself into the SDK's warnings.
//
// Scoped to --conflation deliberately: no other command changes, and an
// operator who set QUEEN_CLIENT_LOG themselves keeps the level they chose.
func showConflationWarnings() {
	if os.Getenv("QUEEN_CLIENT_LOG") != "" {
		return
	}
	if queen.GetLogLevel() < queen.LogLevelWarn {
		queen.SetLogLevel(queen.LogLevelWarn)
	}
}

// conflationErr classifies the SDK's degrade-loudly failure, or returns nil
// when err is something else.
//
// The SDK raises this when it asked for conflation and the response did not
// acknowledge it — i.e. the broker predates 1.1.0 and ignored the unknown query
// param. It is exit 1 rather than exit 2 for the same reason a proxy-blocked
// route is (see blockedErr): retrying cannot help, and the fix is to change the
// command or the broker. Exit 2 would tell a wrapping script to back off and
// try again forever.
func conflationErr(err error, verb string) error {
	if err == nil || !errors.Is(err, queen.ErrConflationUnsupported) {
		return nil
	}
	return clierr.Userf("%s: %v; drop --conflation or upgrade the broker to 1.1.0", verb, err)
}
