// Closing windows under a claim (broker 2.7, issue #153). Mirror of
// a_session_open_at_a_restart_closes_without_a_new_event and
// two_runners_close_each_session_once in clients/client-rust/tests/streams.rs.
package streams_integration

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

// sessionCount is a processing-time session window counting events into sink.
func sessionCount(src, sink string, gap float64, idleFlushMs int64) *streams.Stream {
	return streams.From(testClient.Queue(src).AsStreamSource()).
		WindowSession(gap, streams.WithIdleFlushMs(idleFlushMs)).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(interface{}) (float64, error) { return 1, nil },
		}, "count").
		To(testClient.Queue(sink))
}

// countsByPartition gives each sink partition's emitted counts, in order.
func countsByPartition(msgs []*queen.Message) map[string][]float64 {
	out := map[string][]float64{}
	for _, m := range msgs {
		c, ok := m.Data["count"].(float64)
		if !ok {
			c = -1
		}
		out[m.Partition] = append(out[m.Partition], c)
	}
	return out
}

func newDrainGroup() string {
	return "drain-" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

func dropQueues(names ...string) {
	for _, n := range names {
		_, _ = testClient.Queue(n).Delete().Execute(context.Background())
	}
}

// A session open when its runner stops closes on the next runner with no new
// event on its partition. Before claims, the idle flush only knew the
// partitions its own process had popped, so this window stayed open for good
// (and kept its partition from being cleaned up).
func TestSessionOpenAtARestartClosesWithoutANewEvent(t *testing.T) {
	src := mkName("claimRestart", "src")
	sink := mkName("claimRestart", "sink")
	queryID := mkName("claimRestart", "q")
	ctx := context.Background()
	for _, q := range []string{src, sink} {
		if _, err := testClient.Queue(q).Create().Execute(ctx); err != nil {
			t.Fatal(err)
		}
	}
	defer dropQueues(src, sink)
	opts := func(reset bool) streams.RunOptions {
		return streams.RunOptions{
			QueryID: queryID, URL: queenURL, MaxWaitMillis: 200, Reset: reset,
			SubscriptionMode: replayAll,
		}
	}

	// Two events of trace-a, then a stop while its 3 s session is open.
	first, err := sessionCount(src, sink, 3, 250).Run(ctx, opts(true))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := testClient.Queue(src).Partition("trace-a").Push(map[string]interface{}{"n": 1}).Execute(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(1200 * time.Millisecond)
	folded := first.Metrics().MessagesTotal
	first.Stop()
	if folded != 2 {
		t.Fatalf("runner 1 should have folded both events, folded %d", folded)
	}

	// Runner 2, same query. trace-b is the control: it gets an event now.
	second, err := sessionCount(src, sink, 3, 250).Run(ctx, opts(false))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	if _, err := testClient.Queue(src).Partition("trace-b").Push(map[string]interface{}{"n": 1}).Execute(ctx); err != nil {
		t.Fatal(err)
	}

	cg := newDrainGroup()
	out := drainGroupUntil(t, testClient.Queue(sink), cg, func(m []*queen.Message) bool {
		got := countsByPartition(m)
		return len(got["trace-a"]) > 0 && len(got["trace-b"]) > 0
	}, 15*time.Second)
	// Nothing more comes for either partition.
	time.Sleep(2 * time.Second)
	out = append(out, drainGroupUntil(t, testClient.Queue(sink), cg, nil, 500*time.Millisecond)...)

	got := countsByPartition(out)
	if !reflect.DeepEqual(got["trace-a"], []float64{2}) {
		t.Errorf("trace-a's session closes once, with both events: got %v", got["trace-a"])
	}
	if !reflect.DeepEqual(got["trace-b"], []float64{1}) {
		t.Errorf("trace-b: got %v", got["trace-b"])
	}
	if m := second.Metrics(); m.ErrorsTotal != 0 {
		t.Errorf("runner 2 errors: %d (last: %s)", m.ErrorsTotal, m.LastError)
	}
}

// Two runners on one query close every session exactly once: each close runs
// under its partition's lease, and the broker refuses a close whose lease is
// gone.
func TestTwoRunnersCloseEachSessionOnce(t *testing.T) {
	src := mkName("claimTwoRunners", "src")
	sink := mkName("claimTwoRunners", "sink")
	queryID := mkName("claimTwoRunners", "q")
	ctx := context.Background()
	for _, q := range []string{src, sink} {
		if _, err := testClient.Queue(q).Create().Execute(ctx); err != nil {
			t.Fatal(err)
		}
	}
	defer dropQueues(src, sink)
	opts := func(reset bool) streams.RunOptions {
		return streams.RunOptions{
			QueryID: queryID, URL: queenURL, MaxWaitMillis: 100, MaxPartitions: 2,
			Reset: reset, SubscriptionMode: replayAll,
		}
	}
	a, err := sessionCount(src, sink, 2, 100).Run(ctx, opts(true))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Stop()
	b, err := sessionCount(src, sink, 2, 100).Run(ctx, opts(false))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	const partitions = 20
	for round := 0; round < 2; round++ {
		for p := 0; p < partitions; p++ {
			_, err := testClient.Queue(src).Partition(fmt.Sprintf("s-%d", p)).
				Push(map[string]interface{}{"round": round}).Execute(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	cg := newDrainGroup()
	out := drainGroupUntil(t, testClient.Queue(sink), cg, untilCount(partitions), 20*time.Second)
	// Anything emitted twice would arrive within a few flush ticks.
	time.Sleep(2 * time.Second)
	out = append(out, drainGroupUntil(t, testClient.Queue(sink), cg, nil, 500*time.Millisecond)...)

	got := countsByPartition(out)
	if len(got) != partitions {
		t.Errorf("expected %d partitions emitted, got %d: %v", partitions, len(got), got)
	}
	names := make([]string, 0, len(got))
	for p := range got {
		names = append(names, p)
	}
	sort.Strings(names)
	for _, p := range names {
		if !reflect.DeepEqual(got[p], []float64{2}) {
			t.Errorf("partition %s: one session of two events, got %v", p, got[p])
		}
	}
	t.Logf("runner a: %+v", a.Metrics())
	t.Logf("runner b: %+v", b.Metrics())
}
