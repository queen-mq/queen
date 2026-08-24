// Conflation reaches the source the Runner pops from (PLAN_CONFLATION.md §4,
// the client-go row: "streams runner/adapter"). RunOptions is the only handle a
// streams query has on its pop, so an option that stops at RunOptions is an
// option that does not exist.
package runtime

import (
	"context"
	"testing"
)

// fakeSource records the builder calls popMessages makes on it. Every With-style
// method returns a NEW value the way the real QueueBuilder adapter does, so a
// Runner that built the source correctly but then popped from the UNCONFIGURED
// original would still fail this test.
type fakeSource struct {
	rec *sourceCalls
}

type sourceCalls struct {
	batch            int
	wait             bool
	timeoutMillis    int
	group            string
	partitions       int
	subscriptionMode string
	subscriptionFrom string
	conflation       *bool
	popped           bool
}

func (f *fakeSource) Name() string { return "fake" }
func (f *fakeSource) Batch(n int) Source {
	f.rec.batch = n
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) Wait(b bool) Source {
	f.rec.wait = b
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) TimeoutMillis(ms int) Source {
	f.rec.timeoutMillis = ms
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) Group(g string) Source {
	f.rec.group = g
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) Partitions(n int) Source {
	f.rec.partitions = n
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) SubscriptionMode(m string) Source {
	f.rec.subscriptionMode = m
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) SubscriptionFrom(ts string) Source {
	f.rec.subscriptionFrom = ts
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) Conflation(b bool) Source {
	v := b
	f.rec.conflation = &v
	return &fakeSource{rec: f.rec}
}
func (f *fakeSource) Pop(ctx context.Context) ([]Message, error) {
	f.rec.popped = true
	return nil, nil
}

func newConflationRunner(t *testing.T, opts RunOptions) (*Runner, *sourceCalls) {
	t.Helper()
	rec := &sourceCalls{}
	stream := &CompiledStream{Source: &fakeSource{rec: rec}}
	opts.URL = "http://127.0.0.1:1"
	if opts.QueryID == "" {
		opts.QueryID = "q-conflation"
	}
	return NewRunner(stream, opts), rec
}

func TestRunnerAppliesConflationToTheSource(t *testing.T) {
	r, rec := newConflationRunner(t, RunOptions{Conflation: true})

	if _, err := r.popMessages(context.Background()); err != nil {
		t.Fatalf("popMessages: %v", err)
	}
	if !rec.popped {
		t.Fatal("the runner never popped")
	}
	if rec.conflation == nil {
		t.Fatal("RunOptions.Conflation never reached the source")
	}
	if !*rec.conflation {
		t.Fatal("the source was configured with conflation=false")
	}
}

func TestRunnerLeavesConflationAloneWhenOff(t *testing.T) {
	// Same discipline as SubscriptionMode above it: an unset option touches
	// nothing, so a streams query that never heard of conflation makes
	// byte-identical pops.
	r, rec := newConflationRunner(t, RunOptions{})

	if _, err := r.popMessages(context.Background()); err != nil {
		t.Fatalf("popMessages: %v", err)
	}
	if rec.conflation != nil {
		t.Fatalf("Conflation(%v) was called although the option is off", *rec.conflation)
	}
}
