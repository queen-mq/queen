package cmd

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
)

// Conflation, queenctl side (PLAN_CONFLATION.md §4, the client-cli row; §7.2
// "the same three tests per SDK").
//
// queenctl is a thin shell over client-go, so these tests are deliberately
// driven through the REAL cobra commands against a fake broker rather than
// against a param builder. Three things have to hold, and each one fails
// differently:
//
//  1. THE FLAG MUST REACH THE WIRE FROM BOTH COMMANDS. `pop` and `tail` take
//     two different paths through the SDK — QueueBuilder.buildPopParams for the
//     one-shot pop, ConsumerManager.buildParams for the consume loop `tail`
//     runs. §4 calls that split out as the standing hazard; a test that only
//     drove `pop` would pass with a `tail` that silently dropped the option.
//
//  2. DEGRADING MUST BE LOUD. Against a broker older than 1.1.0 the unknown
//     query param is ignored and the pop answers with the whole backlog. The
//     only usable signal is the ECHO — a conflating broker emits
//     `"conflation":true` on every conflating pop, empty ones included — so its
//     absence has to be an ERROR on the FIRST response, before a single stale
//     message is printed (§4 blockquote). An operator piping `queenctl tail
//     --conflation` into a downstream job must not get 4M stale lines.
//
//  3. A CONFLICT IS NOT AN OLD BROKER. `conflationConflict` is a live 1.1.0
//     broker saying "the group is already registered the other way, my stored
//     value wins" (§3.3). It warns ONCE per (queue, group) per process and the
//     command keeps working, which is what makes a rolling deploy survivable.

// ---------------------------------------------------------------------------
// fake broker
// ---------------------------------------------------------------------------

type recordedRequest struct {
	Method string
	Path   string
	Query  url.Values
}

// fakeBroker answers pops from a scripted list of bodies (the last one repeats)
// and everything else — acks above all — with a bland success object. It
// records every request so a test can assert on the query string that actually
// left the process.
type fakeBroker struct {
	srv       *httptest.Server
	mu        sync.Mutex
	reqs      []recordedRequest
	popBodies []string
	popIdx    int
	routes    map[string]string
}

func newFakeBroker(t *testing.T, popBodies ...string) *fakeBroker {
	t.Helper()
	if len(popBodies) == 0 {
		popBodies = []string{`{"messages":[]}`}
	}
	fb := &fakeBroker{popBodies: popBodies, routes: map[string]string{}}
	fb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		fb.mu.Lock()
		fb.reqs = append(fb.reqs, recordedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.Query(),
		})
		body := `{"success":true}`
		if strings.HasPrefix(r.URL.Path, "/api/v1/pop") {
			i := fb.popIdx
			if i >= len(fb.popBodies) {
				i = len(fb.popBodies) - 1
			}
			fb.popIdx++
			body = fb.popBodies[i]
		} else if b, ok := fb.routes[r.URL.Path]; ok {
			body = b
		}
		fb.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(fb.srv.Close)
	return fb
}

func (fb *fakeBroker) url() string { return fb.srv.URL }

// route pins an exact path to a body, for the admin surfaces (`cg list`,
// `cg describe`) that are not pops.
func (fb *fakeBroker) route(path, body string) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	fb.routes[path] = body
}

// pops returns only the pop round trips, so a `tail` test can ignore the acks
// the consume loop interleaves.
func (fb *fakeBroker) pops() []recordedRequest {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	var out []recordedRequest
	for _, r := range fb.reqs {
		if strings.HasPrefix(r.Path, "/api/v1/pop") {
			out = append(out, r)
		}
	}
	return out
}

func (fb *fakeBroker) firstPop(t *testing.T) recordedRequest {
	t.Helper()
	pops := fb.pops()
	if len(pops) == 0 {
		t.Fatal("no pop request reached the broker")
	}
	return pops[0]
}

// popBody is a single message shaped the way the broker renders one, so the
// SDK's parser accepts it and the command has something to print.
func popBody(queue string, extraKeys string) string {
	msg := `{"transactionId":"11111111-1111-4111-8111-111111111111",` +
		`"partitionId":"22222222-2222-4222-8222-222222222222",` +
		`"queue":"` + queue + `","partition":"Default",` +
		`"data":{"n":1},"createdAt":"2026-08-21T10:00:00.000Z"}`
	return `{"messages":[` + msg + `]` + extraKeys + `}`
}

func emptyPopBody(extraKeys string) string {
	return `{"messages":[]` + extraKeys + `}`
}

// ---------------------------------------------------------------------------
// driving the real command tree
// ---------------------------------------------------------------------------

// resetConflationFlags puts every flag these tests touch back to its default.
// The cobra tree is a package singleton, so a --conflation left set by one test
// would silently green the next one.
func resetConflationFlags() {
	popGroup, popPartition, popNamespace, popTask = "", "", "", ""
	popLimit, popBatch, popMaxParts = 1, 0, 1
	popAutoAck, popWait = false, true
	popTimeout = 10 * time.Second
	popSubMode, popSubFrom = "", ""
	popConflation = false

	tailGroup, tailPartition = "", ""
	tailFollow, tailAutoAck = false, false
	tailLimit, tailBatch, tailIdleMillis, tailConcurrency = 0, 0, 0, 0
	tailMaxParts = 1
	tailFromMode, tailFromAt = "", ""
	tailTimeout = 0
	tailConflation = false

	gf = globalFlags{}
}

// runCLI executes the real root command in-process and returns the error the
// exit-code mapper would see.
func runCLI(t *testing.T, server string, args ...string) error {
	t.Helper()
	resetConflationFlags()
	t.Cleanup(resetConflationFlags)

	cfg := filepath.Join(t.TempDir(), "config.yaml") // never written: keeps the developer's real config out
	full := append([]string{"--server", server, "--config", cfg, "--no-color"}, args...)

	// cobra copies the root context onto a subcommand only when the subcommand
	// has none yet (ExecuteC: `if cmd.ctx == nil`). The real binary executes
	// once so it never notices; a test process that runs the tree repeatedly
	// would hand every run after the first a context its predecessor already
	// cancelled.
	clearCommandContexts(rootCmd)

	rootCmd.SetArgs(full)
	rootCmd.SetOut(io.Discard)
	rootCmd.SetErr(io.Discard)
	defer rootCmd.SetArgs(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return rootCmd.ExecuteContext(ctx)
}

func clearCommandContexts(c *cobra.Command) {
	c.SetContext(nil)
	for _, sub := range c.Commands() {
		clearCommandContexts(sub)
	}
}

// captureStdio swaps os.Stdout/os.Stderr for pipes. The commands write NDJSON
// straight to os.Stdout and the SDK's warnings straight to os.Stderr, so this
// is the only way to see either from in-process.
func captureStdio(t *testing.T) func() (string, string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = wOut, wErr

	var outBuf, errBuf bytes.Buffer
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(&outBuf, rOut); done <- struct{}{} }()
	go func() { _, _ = io.Copy(&errBuf, rErr); done <- struct{}{} }()

	var once sync.Once
	stop := func() (string, string) {
		once.Do(func() {
			_ = wOut.Close()
			_ = wErr.Close()
			<-done
			<-done
			os.Stdout, os.Stderr = oldOut, oldErr
			_ = rOut.Close()
			_ = rErr.Close()
		})
		return outBuf.String(), errBuf.String()
	}
	t.Cleanup(func() { stop() })
	return stop
}

// ---------------------------------------------------------------------------
// 1. the flag reaches the wire, from `pop` AND from `tail`
// ---------------------------------------------------------------------------

func TestPopSendsConflationOnTheWire(t *testing.T) {
	fb := newFakeBroker(t, popBody("cfl-pop", `,"conflation":true`))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "cfl-pop", "--cg", "workers", "--conflation",
		"--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop --conflation: %v", err)
	}

	req := fb.firstPop(t)
	if got := req.Query.Get("conflation"); got != "true" {
		t.Fatalf("pop query conflation = %q, want %q (raw: %v)", got, "true", req.Query.Encode())
	}
}

func TestPopOmitsConflationWhenNotRequested(t *testing.T) {
	// Default off (§8): a consumer that did not opt in must produce a
	// byte-identical request, never conflation=false.
	fb := newFakeBroker(t, popBody("cfl-pop-off", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "cfl-pop-off", "--cg", "workers",
		"--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("pop: %v", err)
	}

	req := fb.firstPop(t)
	if _, present := req.Query["conflation"]; present {
		t.Fatalf("conflation must not appear on the wire when off; raw: %v", req.Query.Encode())
	}
}

func TestTailSendsConflationOnTheWire(t *testing.T) {
	// `tail` runs the consume loop, whose params are built by a DIFFERENT
	// function than the one `pop` uses (§4). This is the half a pop-only test
	// would miss.
	fb := newFakeBroker(t, popBody("cfl-tail", `,"conflation":true`))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "cfl-tail", "--cg", "workers", "--conflation",
		"-n", "1", "--timeout", "2s")
	stdout, _ := stop()
	if err != nil {
		t.Fatalf("tail --conflation: %v", err)
	}
	if !strings.Contains(stdout, `"transactionId"`) {
		t.Fatalf("tail printed no message; stdout=%q", stdout)
	}

	req := fb.firstPop(t)
	if got := req.Query.Get("conflation"); got != "true" {
		t.Fatalf("tail query conflation = %q, want %q (raw: %v)", got, "true", req.Query.Encode())
	}
}

func TestTailOmitsConflationWhenNotRequested(t *testing.T) {
	fb := newFakeBroker(t, popBody("cfl-tail-off", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "cfl-tail-off", "--cg", "workers",
		"-n", "1", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("tail: %v", err)
	}

	req := fb.firstPop(t)
	if _, present := req.Query["conflation"]; present {
		t.Fatalf("conflation must not appear on the wire when off; raw: %v", req.Query.Encode())
	}
}

// ---------------------------------------------------------------------------
// 2. degrade loudly
// ---------------------------------------------------------------------------

func TestPopFailsLoudlyWhenTheBrokerDoesNotEchoConflation(t *testing.T) {
	// The old-broker case: the param was ignored, so no echo comes back and the
	// body is the whole backlog. A warning here is worthless — the message has
	// already been printed by the time anybody reads stderr — so this is an
	// error and nothing is emitted.
	fb := newFakeBroker(t, popBody("cfl-old-pop", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "cfl-old-pop", "--cg", "workers", "--conflation",
		"--wait=false", "--timeout", "2s")
	stdout, _ := stop()

	if err == nil {
		t.Fatal("pop --conflation against a broker that ignored it returned no error")
	}
	if !strings.Contains(err.Error(), "requires broker >= 1.1.0") {
		t.Fatalf("error does not name the version requirement: %v", err)
	}
	if code := clierr.CodeOf(err); code == clierr.CodeOK || code == clierr.CodeEmpty {
		t.Fatalf("degraded pop exited %d; must be a failure code", code)
	}
	if strings.Contains(stdout, `"transactionId"`) {
		t.Fatalf("a degraded pop must not print the backlog it was handed; stdout=%q", stdout)
	}
}

func TestPopIgnoresTheEchoWhenConflationWasNotRequested(t *testing.T) {
	// Old SDK / new broker and every non-conflating consumer: the echo is
	// absent by design and must never be an error (§8 compat matrix).
	fb := newFakeBroker(t, popBody("cfl-noflag", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "cfl-noflag", "--cg", "workers",
		"--wait=false", "--timeout", "2s")
	stop()
	if err != nil {
		t.Fatalf("a pop that never asked for conflation must not care about the echo: %v", err)
	}
}

func TestTailStopsLoudlyWhenTheBrokerDoesNotEchoConflation(t *testing.T) {
	// The echo rides empty pops too, so the loop dies on the FIRST round trip —
	// before one stale line reaches the pipe.
	fb := newFakeBroker(t, popBody("cfl-old-tail", ""))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "tail", "cfl-old-tail", "--cg", "workers", "--conflation",
		"--follow", "--timeout", "2s")
	stdout, _ := stop()

	if err == nil {
		t.Fatal("tail --conflation against a broker that ignored it ran on without an error")
	}
	if !strings.Contains(err.Error(), "requires broker >= 1.1.0") {
		t.Fatalf("error does not name the version requirement: %v", err)
	}
	if strings.Contains(stdout, `"transactionId"`) {
		t.Fatalf("a degraded tail must not emit messages; stdout=%q", stdout)
	}
	if n := len(fb.pops()); n != 1 {
		t.Fatalf("tail made %d pops, want exactly 1 (the loop must stop on the first)", n)
	}
}

// ---------------------------------------------------------------------------
// 3. the conflict warning: once per (queue, group), and NOT an error
// ---------------------------------------------------------------------------

// conflationWarnings counts the stderr lines that are about a conflation
// conflict. queenctl's contract is that an operator SEES this without having to
// know about QUEEN_CLIENT_LOG.
func conflationWarnings(stderr string) []string {
	var out []string
	for _, line := range strings.Split(stderr, "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, "conflation") || strings.Contains(l, "conflict") {
			out = append(out, line)
		}
	}
	return out
}

func TestConflationConflictWarnsExactlyOncePerQueueAndGroup(t *testing.T) {
	// Three pops of the same (queue, group) against a broker that keeps saying
	// "your declaration lost". The stored setting wins, every pop succeeds, and
	// the operator is told once — not once per round trip, which on a tail loop
	// is a stderr flood.
	body := emptyPopBody(`,"conflation":true,"conflationConflict":true`)
	fb := newFakeBroker(t, body, body, body)
	stop := captureStdio(t)

	for i := 0; i < 3; i++ {
		err := runCLI(t, fb.url(), "pop", "cfl-conflict", "--cg", "workers", "--conflation",
			"--wait=false", "--timeout", "2s")
		// An empty pop is exit 4, a success-with-signal, not a failure.
		if code := clierr.CodeOf(err); code != clierr.CodeEmpty {
			t.Fatalf("pop %d: a declaration conflict must not fail the pop (exit %d): %v", i, code, err)
		}
	}
	_, stderr := stop()

	if got := conflationWarnings(stderr); len(got) != 1 {
		t.Fatalf("emitted %d conflict warnings, want exactly 1:\n%s", len(got), strings.Join(got, "\n"))
	}
}

func TestConflationConflictIsNotAnOldBroker(t *testing.T) {
	// The conflict where the STORED value is false: the broker answers
	// conflationConflict WITHOUT "conflation":true because the effective policy
	// is off. That is a live 1.1.0 broker, not an old one — treating it as an
	// old one would take down exactly the half of a rolling fleet that is
	// already correct (§3.3, Q3).
	fb := newFakeBroker(t, emptyPopBody(`,"conflationConflict":true`))
	stop := captureStdio(t)

	err := runCLI(t, fb.url(), "pop", "cfl-conflict-stored-off", "--cg", "workers", "--conflation",
		"--wait=false", "--timeout", "2s")
	_, stderr := stop()

	if code := clierr.CodeOf(err); code != clierr.CodeEmpty {
		t.Fatalf("a declaration conflict must not fail the pop (exit %d): %v", code, err)
	}
	if got := conflationWarnings(stderr); len(got) != 1 {
		t.Fatalf("emitted %d conflict warnings, want exactly 1:\n%s", len(got), strings.Join(got, "\n"))
	}
}
