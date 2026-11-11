package runtime

import (
	"context"
	"testing"
)

type staticTokens string

func (s staticTokens) Token(context.Context, string) (string, error) { return string(s), nil }

// The runner's own calls (register, cycle, state) go through its HTTP client:
// it must carry the source the options gave, or they go out with no token.
func TestNewRunnerGivesItsClientTheTokenSource(t *testing.T) {
	src := staticTokens("token-0")
	r := NewRunner(&CompiledStream{}, RunOptions{QueryID: "q", URL: "http://127.0.0.1:1", TokenSource: src})
	if r.http.TokenSource != TokenSource(src) {
		t.Fatalf("runner client TokenSource = %v, want the options' source", r.http.TokenSource)
	}
}
