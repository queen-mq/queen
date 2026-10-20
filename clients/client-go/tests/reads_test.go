package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
)

// Reads by offset (Fetch) and partition discovery (ChangedSince) against a
// live broker.

func pushEach(ctx context.Context, t *testing.T, client *queen.Queen, queue string, partitions []string, txn func(string) string, data func(string) interface{}) {
	t.Helper()
	for i := 0; i < len(partitions); i += 200 {
		var wg sync.WaitGroup
		errs := make(chan error, 200)
		for _, p := range partitions[i:min(i+200, len(partitions))] {
			wg.Add(1)
			go func(p string) {
				defer wg.Done()
				res, err := client.Queue(queue).Partition(p).Push(data(p)).TransactionID(txn(p)).Execute(ctx)
				if err == nil && (len(res) != 1 || res[0].Status != "queued") {
					err = fmt.Errorf("push %s: %+v", p, res)
				}
				if err != nil {
					errs <- err
				}
			}(p)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
}

func TestFetchReadsEveryRecordByOffset(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	queue := generateQueueName("reads-fetch")

	// 1,200 partitions: more than one fetch call (1000 entries each).
	partitions := make([]string, 1200)
	for i := range partitions {
		partitions[i] = fmt.Sprintf("entity-%d", i)
	}
	const perPartition = 3
	for n := 0; n < perPartition; n++ {
		pushEach(ctx, t, client, queue, partitions,
			func(p string) string { return fmt.Sprintf("%s-%d", p, n) },
			func(p string) interface{} { return map[string]interface{}{"p": p, "n": n} })
	}

	read, err := client.Queue(queue).Fetch(ctx, partitions)
	if err != nil {
		t.Fatal(err)
	}
	if len(read) != len(partitions) {
		t.Fatalf("fetch answered %d partitions for %d", len(read), len(partitions))
	}
	for _, p := range partitions {
		part := read[p]
		if len(part.Records) != perPartition || part.HighWatermark != perPartition || part.NextOffset != perPartition || part.LogStartOffset != 0 {
			t.Fatalf("%s: %+v", p, part)
		}
		for n, r := range part.Records {
			var payload struct {
				P string `json:"p"`
				N int    `json:"n"`
			}
			if err := json.Unmarshal(r.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if r.Offset != int64(n) || payload.P != p || payload.N != n || r.TransactionID != fmt.Sprintf("%s-%d", p, n) || r.Ts.IsZero() {
				t.Fatalf("%s record %d: %+v %s", p, n, r, r.Payload)
			}
		}
	}

	// A later read from NextOffset answers only what came after.
	if _, err := client.Queue(queue).Partition("entity-7").Push(map[string]bool{"late": true}).Execute(ctx); err != nil {
		t.Fatal(err)
	}
	later, err := client.Queue(queue).Fetch(ctx, []string{"entity-7", "entity-8"}, queen.FetchOptions{
		Offsets: map[string]int64{"entity-7": read["entity-7"].NextOffset, "entity-8": read["entity-8"].NextOffset},
	})
	if err != nil {
		t.Fatal(err)
	}
	if late := later["entity-7"].Records; len(late) != 1 || late[0].Offset != perPartition || string(late[0].Payload) != `{"late":true}` {
		t.Fatalf("read from NextOffset: %+v", late)
	}
	if len(later["entity-8"].Records) != 0 {
		t.Fatalf("a partition with nothing new answered %+v", later["entity-8"])
	}

	// The reads took no lease and moved no cursor: a pop still gets the messages.
	popped, err := client.Queue(queue).Partition("entity-9").Batch(10).Wait(false).Pop(ctx)
	if err != nil || len(popped) != perPartition {
		t.Fatalf("pop after fetch: %v, %d messages", err, len(popped))
	}

	missing, err := client.Queue(generateQueueName("reads-missing")).Fetch(ctx, []string{"x"})
	if err != nil || len(missing["x"].Records) != 0 {
		t.Fatalf("missing queue: %v %+v", err, missing)
	}
}

func TestChangedSinceListsTheWrittenPartitions(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	queue := generateQueueName("reads-changed")
	before := time.Now().Add(-time.Minute)

	names := make([]string, 1100)
	for i := range names {
		names[i] = fmt.Sprintf("c-%d", i)
	}
	pushEach(ctx, t, client, queue, names,
		func(p string) string { return p + "-0" },
		func(p string) interface{} { return map[string]string{"p": p} })

	// 1,100 partitions: two pages.
	first, err := client.Queue(queue).ChangedSince(ctx, before)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]queen.ChangedPartition{}
	for _, p := range first.Partitions {
		listed[p.Name] = p
	}
	if len(first.Partitions) != len(names) || len(listed) != len(names) || first.SafeTime.IsZero() {
		t.Fatalf("listed %d partitions (%d distinct), safeTime %v", len(first.Partitions), len(listed), first.SafeTime)
	}
	if p := listed["c-5"]; p.LastOffset != 0 || p.LogStart != 0 || p.LastWriteAt.IsZero() || p.ID == "" {
		t.Fatalf("partition fields: %+v", p)
	}

	// From the SafeTime of that pass, a write to one partition lists it, and
	// nothing written before that SafeTime comes back.
	if _, err := client.Queue(queue).Partition("c-42").Push(map[string]bool{"again": true}).Execute(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := client.Queue(queue).ChangedSince(ctx, first.SafeTime)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range next.Partitions {
		if p.Name == "c-42" && p.LastOffset == 1 {
			found = true
		}
		if p.LastWriteAt.Before(first.SafeTime) {
			t.Fatalf("%s was written before safeTime and listed", p.Name)
		}
	}
	if !found {
		t.Fatalf("c-42 not listed after its write: %+v", next.Partitions)
	}

	all, err := client.Queue(queue).ChangedSince(ctx, time.Time{})
	if err != nil || len(all.Partitions) != len(names) {
		t.Fatalf("ChangedSince with a zero since: %v, %d partitions", err, len(all.Partitions))
	}
	none, err := client.Queue(generateQueueName("reads-missing")).ChangedSince(ctx, before)
	if err != nil || len(none.Partitions) != 0 {
		t.Fatalf("missing queue: %v %+v", err, none)
	}
}
