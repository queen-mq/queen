package compat

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CHECK 10. Two producers and a consumer against one facade for ten seconds:
// the dispatch is serial per connection, so what this looks for is the failure
// modes of three of them at once — a panic, a desynchronised response, a
// record that never arrives. Counts have to add up exactly, per partition.
func TestConcurrentProducersAndConsumer(t *testing.T) {
	setup := newClient(t)
	topic := newTopic(t)
	ensureTopic(t, setup, topic)

	const runFor = 10 * time.Second
	const batch = 20
	width := topicWidth(t)
	half := width / 2
	if half < 1 {
		half = 1
	}

	var produced int64
	var wg sync.WaitGroup
	stop := time.Now().Add(runFor)
	errCh := make(chan error, 8)

	// Each producer owns its own half of the partitions, so a missing record
	// names the producer that wrote it.
	producer := func(name string, first, last int32) {
		defer wg.Done()
		cl := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
		seq := 0
		for time.Now().Before(stop) {
			recs := make([]*kgo.Record, 0, batch)
			for i := 0; i < batch; i++ {
				p := first + int32(seq)%(last-first+1)
				recs = append(recs, &kgo.Record{
					Topic:     topic,
					Partition: p,
					Key:       []byte(fmt.Sprintf("%s-%06d", name, seq)),
					Value:     []byte(fmt.Sprintf(`{"producer":"%s","seq":%d}`, name, seq)),
					Timestamp: time.Now(),
				})
				seq++
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			res := cl.ProduceSync(ctx, recs...)
			cancel()
			if err := res.FirstErr(); err != nil {
				errCh <- fmt.Errorf("producer %s: %w", name, err)
				return
			}
			atomic.AddInt64(&produced, int64(len(recs)))
		}
	}

	wg.Add(2)
	go producer("alpha", 0, half-1)
	go producer("beta", half, width-1)
	producersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(producersDone)
	}()

	// The consumer runs alongside them, from the beginning of the log, so it is
	// fetching against partitions that are being written the whole time. It
	// stops only once the producers are finished AND it has caught up with
	// their count.
	consumer := newClient(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(2*time.Second),
	)
	seen := make(map[string]bool)
	perPartition := make(map[int32][]int64)
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		ctx, cancel := context.WithTimeout(context.Background(), runFor+90*time.Second)
		defer cancel()
		for {
			fs := consumer.PollRecords(ctx, 500)
			if ctx.Err() != nil {
				errCh <- fmt.Errorf("consumer: gave up with %d of %d records", len(seen), atomic.LoadInt64(&produced))
				return
			}
			if errs := fs.Errors(); len(errs) > 0 {
				errCh <- fmt.Errorf("consumer: %v", errs)
				return
			}
			fs.EachRecord(func(r *kgo.Record) {
				if seen[string(r.Key)] {
					errCh <- fmt.Errorf("consumer: duplicate delivery of %s", r.Key)
				}
				seen[string(r.Key)] = true
				perPartition[r.Partition] = append(perPartition[r.Partition], r.Offset)
			})
			select {
			case <-producersDone:
				if int64(len(seen)) >= atomic.LoadInt64(&produced) {
					return
				}
			default:
			}
		}
	}()

	<-producersDone
	select {
	case <-consumerDone:
	case <-time.After(90 * time.Second):
		t.Fatalf("the consumer never caught up")
	}
	close(errCh)
	for err := range errCh {
		t.Errorf("%v", err)
	}

	total := atomic.LoadInt64(&produced)
	t.Logf("%d records produced by 2 producers in %s, %d consumed", total, runFor, len(seen))
	if total == 0 {
		t.Fatalf("no records were produced at all")
	}
	if int64(len(seen)) != total {
		t.Errorf("consumed %d distinct records, produced %d", len(seen), total)
	}
	// Per partition the log is still a log: contiguous from 0, in order.
	for p, offs := range perPartition {
		for i, off := range offs {
			if off != int64(i) {
				t.Errorf("partition %d: record %d has offset %d, want %d", p, i, off, i)
				break
			}
		}
	}

	// Still alive and still speaking Kafka after all that.
	ensureTopic(t, setup, topic)
}
