package runtime

// Auto-link on a queue source (PLAN_ROUTING.md §7), against a fake broker: a
// cycle whose output is its input message unchanged names the input as the
// push item's link, {offset, transactionId} in the cycle's own partition. The
// payload goes beside it, for a broker older than links (which would store a
// push item without one as {}). A refused link sends the cycle again pushed,
// and the runner pushes from then on.

import (
	"context"
	"reflect"
	"testing"

	"github.com/smartpricing/queen/clients/client-go/v2/streams/operators"
)

var keepAll = operators.FilterOperator{Fn: func(interface{}, operators.EmitCtx) (bool, error) { return true, nil }}

func linkRunner(url string, sink operators.SinkOperator, pre ...StatelessOp) *Runner {
	r := NewRunner(&CompiledStream{Stages: Stages{Pre: pre, Sink: sink}}, RunOptions{QueryID: "q-links", URL: url})
	r.serverQueryID = "srv-q-links"
	return r
}

// cycleGroup is partition 7 ("p1") holding an object message at offset 41 and
// one without a data object (a bare value, which a pop leaves out) at 42.
func cycleGroup() partitionGroup {
	return partitionGroup{PartitionID: "7", PartitionName: "p1", LeaseID: "l1", ConsumerGroup: "streams.q-links", Messages: []Message{
		{ID: "t1", TransactionID: "t1", PartitionID: "7", Partition: "p1", Offset: 41, Data: map[string]interface{}{"n": 1.0}},
		{ID: "t2", TransactionID: "t2", PartitionID: "7", Partition: "p1", Offset: 42},
	}}
}

func pushItems(t *testing.T, cycle map[string]interface{}) []interface{} {
	t.Helper()
	items, ok := cycle["push_items"].([]interface{})
	if !ok {
		t.Fatalf("cycle without push_items: %v", cycle)
	}
	return items
}

func ok200(string, map[string]interface{}) (int, interface{}) {
	return 200, map[string]interface{}{"success": true}
}

func TestCycleLinksAnUnchangedInput(t *testing.T) {
	f, url := newFakeBroker(t, ok200)
	r := linkRunner(url, operators.NewSinkOperator("out", nil), keepAll)
	if err := r.processCycle(context.Background(), cycleGroup()); err != nil {
		t.Fatal(err)
	}
	cycles := f.sent("/streams/v1/cycle")
	if len(cycles) != 1 {
		t.Fatalf("%d cycles", len(cycles))
	}
	items := pushItems(t, cycles[0])
	want := []interface{}{
		map[string]interface{}{"queue": "out", "partition": "p1", "payload": map[string]interface{}{"n": 1.0},
			"link": map[string]interface{}{"offset": 41.0, "transactionId": "t1"}},
		// No data object: pushed as before (a link would hand the sink the
		// source's payload, which the push does not carry).
		map[string]interface{}{"queue": "out", "partition": "p1", "payload": nil},
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("push items\n got  %v\n want %v", items, want)
	}
	if cycles[0]["partition_id"] != "7" {
		t.Fatalf("the link's partition is the cycle's: %v", cycles[0]["partition_id"])
	}
}

func TestCyclePushesWhatTheChainComputesOrWhenLinkIsOff(t *testing.T) {
	f, url := newFakeBroker(t, ok200)
	off := false
	noLink := operators.NewSinkOperator("out", nil)
	noLink.Link = &off
	double := operators.MapOperator{Fn: func(msg interface{}, _ operators.EmitCtx) (interface{}, error) {
		return map[string]interface{}{"n": 2.0}, nil
	}}
	for _, r := range []*Runner{
		linkRunner(url, noLink, keepAll),
		linkRunner(url, operators.NewSinkOperator("out", nil), double),
	} {
		if r.linking {
			t.Fatal("a runner that must push links")
		}
		if err := r.processCycle(context.Background(), cycleGroup()); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range f.sent("/streams/v1/cycle") {
		for _, it := range pushItems(t, c) {
			if _, has := it.(map[string]interface{})["link"]; has {
				t.Fatalf("cycle %d linked: %v", i, it)
			}
		}
	}
}

// A refused link -- the cycle answers success false with source_gone, or a
// cluster below catalogue version 8 answers 4xx cluster_version -- sends the
// same cycle again, pushed, and the runner pushes from then on.
func TestCycleFallsBackToPushesWhenALinkIsRefused(t *testing.T) {
	for _, refusal := range []struct {
		status int
		body   map[string]interface{}
	}{
		{200, map[string]interface{}{"success": false, "reason": "source_gone", "error": "push item at index 0: source_gone"}},
		{409, map[string]interface{}{"error": "upgrade the cluster first", "code": "cluster_version"}},
	} {
		calls := 0
		f, url := newFakeBroker(t, func(path string, body map[string]interface{}) (int, interface{}) {
			if path == "/streams/v1/cycle" {
				calls++
				if calls == 1 {
					return refusal.status, refusal.body
				}
			}
			return ok200(path, body)
		})
		r := linkRunner(url, operators.NewSinkOperator("out", nil), keepAll)
		if err := r.processCycle(context.Background(), cycleGroup()); err != nil {
			t.Fatalf("%v: %v", refusal.body, err)
		}
		if err := r.processCycle(context.Background(), cycleGroup()); err != nil {
			t.Fatal(err)
		}
		cycles := f.sent("/streams/v1/cycle")
		if len(cycles) != 3 || !r.noLinks.Load() {
			t.Fatalf("%v: %d cycles, noLinks %v", refusal.body, len(cycles), r.noLinks.Load())
		}
		if _, linked := pushItems(t, cycles[0])[0].(map[string]interface{})["link"]; !linked {
			t.Fatal("the first cycle did not link")
		}
		for _, c := range cycles[1:] {
			item := pushItems(t, c)[0].(map[string]interface{})
			if _, linked := item["link"]; linked || !reflect.DeepEqual(item["payload"], map[string]interface{}{"n": 1.0}) {
				t.Fatalf("a cycle after the refusal: %v", item)
			}
		}
	}
}

// A refusal that is not the links' is the caller's, and nothing is sent again.
func TestCycleDoesNotRetryAnotherRefusal(t *testing.T) {
	f, url := newFakeBroker(t, func(path string, body map[string]interface{}) (int, interface{}) {
		if path == "/streams/v1/cycle" {
			return 200, map[string]interface{}{"success": false, "reason": "reading_queue", "error": "out reads other queues"}
		}
		return ok200(path, body)
	})
	r := linkRunner(url, operators.NewSinkOperator("out", nil), keepAll)
	if err := r.processCycle(context.Background(), cycleGroup()); err == nil {
		t.Fatal("a refused cycle passed")
	}
	if n := len(f.sent("/streams/v1/cycle")); n != 1 || r.noLinks.Load() {
		t.Fatalf("%d cycles, noLinks %v", n, r.noLinks.Load())
	}
}
