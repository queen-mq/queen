package cmd

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
)

func TestBlockedErrExplainsRouteBlocked(t *testing.T) {
	err := blockedErr(
		&queen.HTTPError{StatusCode: 404, Code: codeRouteBlocked, Body: `{"error":"not available","code":"route_blocked"}`},
		"GET /api/v1/status", "run 'queenctl queue list'")
	if got := clierr.CodeOf(err); got != clierr.CodeUser {
		t.Errorf("blocked route should exit %d, got %d", clierr.CodeUser, got)
	}
	for _, want := range []string{"GET /api/v1/status", "queen-proxy", "queenctl queue list"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q should mention %q", err.Error(), want)
		}
	}
}

func TestBlockedErrPassesOtherErrorsThrough(t *testing.T) {
	// A plain 404 from the broker (unknown queue, typo'd path) is not the
	// proxy's fail-closed verdict and must keep the server exit code.
	plain := &queen.HTTPError{StatusCode: 404, Body: `{"error":"not found"}`}
	if got := clierr.CodeOf(blockedErr(plain, "GET /api/v1/status", "alt")); got != clierr.CodeServer {
		t.Errorf("plain 404 should exit %d, got %d", clierr.CodeServer, got)
	}
	// 403 forbidden carries its own meaning even with a proxy code attached.
	forbidden := &queen.HTTPError{StatusCode: 403, Code: "forbidden"}
	if got := clierr.CodeOf(blockedErr(forbidden, "GET /api/v1/status", "alt")); got != clierr.CodeServer {
		t.Errorf("403 should exit %d, got %d", clierr.CodeServer, got)
	}
	if got := clierr.CodeOf(blockedErr(errors.New("dial tcp: refused"), "x", "")); got != clierr.CodeServer {
		t.Errorf("transport error should exit %d, got %d", clierr.CodeServer, got)
	}
}

// The SDK error is wrapped at some call sites (pop); the classification must
// survive that.
func TestBlockedErrUnwraps(t *testing.T) {
	wrapped := fmt.Errorf("pop: %w", &queen.HTTPError{StatusCode: 404, Code: codeRouteBlocked})
	if got := clierr.CodeOf(blockedErr(wrapped, "GET /api/v1/pop", "name a queue")); got != clierr.CodeUser {
		t.Errorf("wrapped blocked route should exit %d, got %d", clierr.CodeUser, got)
	}
}

func TestBlockedErrWithoutAlternative(t *testing.T) {
	err := blockedErr(&queen.HTTPError{StatusCode: 404, Code: codeRouteBlocked}, "GET /metrics", "")
	if strings.HasSuffix(err.Error(), "; ") {
		t.Errorf("no dangling separator when there is no alternative: %q", err.Error())
	}
}
