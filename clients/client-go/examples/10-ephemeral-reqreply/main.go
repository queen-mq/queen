// Example 10 — Request/reply over ephemeral inbox queues. 1:1 port of
// 36-ephemeral-reqreply.js, with the buffered-push coda of §4.1 at the end.
//
// This example demonstrates:
//   - implicit queues: an inbox created by the first message that names it and
//     garbage-collected when it goes quiet — no Configure, no PG row, no cleanup
//   - a long-poll pop parked on a RAM gate as the reply leg
//   - the correlation pattern: the requester names its own inbox, the responder
//     pushes the answer straight into it
//   - a buffered push, which reuses the durable buffer machinery through its
//     FlushFunc seam and drains to the ephemeral route
//
// WHY THIS IS THE FLAGSHIP CASE. Request/reply is where the durable engine is
// least at home: every inbox is a partition, every reply is a serial claim
// transaction, and the shape (thousands of short-lived queues, tiny payloads,
// immediate consumption, worthless history) is exactly what costs the most on a
// log built for durability. On the ephemeral engine the reply leg is a memory
// write, a wake, and a socket — there is no database in the path at all, and no
// polling interval anywhere: the pop is parked on a gate the push rings.
//
// The trade is stated once and never hidden: a reply in flight when the broker
// restarts is GONE. Request/reply already has a timeout for exactly that class
// of failure, which is what makes this workload the right first tenant of a
// storage class that survives nothing.
//
// Run:
//
//	QUEEN_URL=http://localhost:6632 go run ./examples/10-ephemeral-reqreply
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// One well-known queue for the requests. The inboxes are per-request and never
// declared: naming one in a push or a pop is what creates it.
const requestsQueue = "rpc-requests"

const requestCount = 5

type rpcRequest struct {
	N       int    `json:"n"`
	ReplyTo string `json:"replyTo"`
}

type rpcReply struct {
	N        int `json:"n"`
	Squared  int `json:"squared"`
	ServedBy int `json:"servedBy"`
}

func main() {
	url := os.Getenv("QUEEN_URL")
	if url == "" {
		url = "http://localhost:6632"
	}

	q, err := queen.New(url)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	eph := q.Ephemeral()

	// The responder runs until its context is cancelled.
	responderCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var served int
	wg.Add(1)
	go func() {
		defer wg.Done()
		served = responder(responderCtx, eph)
	}()

	fmt.Printf("Sending %d requests, each with its own throw-away inbox...\n\n", requestCount)

	latencies := make([]time.Duration, 0, requestCount)
	for n := 1; n <= requestCount; n++ {
		started := time.Now()
		reply, err := request(ctx, eph, n, 5*time.Second)
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(started)
		latencies = append(latencies, elapsed)
		fmt.Printf("  %d² = %d  (%s round trip, served by pid %d)\n", n, reply.Squared, elapsed, reply.ServedBy)
	}

	stop()
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	fmt.Printf("\nServed %d request(s).\n", served)
	fmt.Printf("Round trip: min %s, median %s, max %s\n",
		latencies[0], latencies[len(latencies)/2], latencies[len(latencies)-1])
	fmt.Printf("%d inboxes were created and abandoned; none of them was ever declared,\n", requestCount)
	fmt.Println("and none of them left anything behind in PostgreSQL.")

	bufferedCoda(ctx, q, eph)

	// The request queue is implicit too — left empty, it is collected on its
	// own. Deleting it is a courtesy to whoever reads the dashboard next.
	//
	// Deleted, not err, is the answer: a queue that was not there is a 200 with
	// Deleted:false, which on this class is a perfectly ordinary outcome — the
	// idle collector may have got there first.
	deleted, err := eph.Delete(ctx, requestsQueue)
	switch {
	case errors.Is(err, queen.ErrEphemeralUnsupported):
		// A broker older than 1.1 answers 404 on the whole family. Not worth
		// failing an example over.
		fmt.Println("\n(this broker does not support ephemeral queues; nothing to clean up)")
	case err != nil:
		panic(err)
	default:
		fmt.Printf("\nrpc-requests deleted=%v (declared=%v)\n", deleted.Deleted, deleted.Declared)
	}

	// Close flushes every buffer — durable and ephemeral alike, they live in
	// one manager — before releasing the connections.
	if err := q.Close(ctx); err != nil {
		panic(err)
	}
}

// responder pops requests, does the work, and pushes the answer to the inbox the
// requester named. It never learns about an inbox before a message mentions one.
func responder(ctx context.Context, eph *queen.Ephemeral) int {
	served := 0

	for ctx.Err() == nil {
		batch, err := eph.Pop(ctx, requestsQueue, queen.EphemeralPopOptions{
			// Competing consumers: run this process N times and they share the
			// load, exactly as on a durable queue. Consumption semantics come
			// from the group, and there is no queue-level mode to choose.
			Group: "workers",
			Batch: 10,
			Wait:  true,
			// Short, so the loop notices cancellation promptly.
			TimeoutMillis: 1000,
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Fprintf(os.Stderr, "responder: %v\n", err)
			continue
		}
		if len(batch.Messages) == 0 {
			continue
		}

		for _, message := range batch.Messages {
			var req rpcRequest
			if err := json.Unmarshal(message.Payload, &req); err != nil {
				fmt.Fprintf(os.Stderr, "responder: undecodable request: %v\n", err)
				continue
			}
			// The reply leg. ReplyTo is a queue that did not exist a moment ago.
			if _, err := eph.Push(ctx, req.ReplyTo, rpcReply{
				N: req.N, Squared: req.N * req.N, ServedBy: os.Getpid(),
			}); err != nil {
				fmt.Fprintf(os.Stderr, "responder: reply to %s: %v\n", req.ReplyTo, err)
				continue
			}
			served++
		}

		// Ack with the same group that popped — cursors are per group. Unacked
		// messages redeliver when the lease expires, with Attempts incremented:
		// at-least-once for as long as the owning broker incarnation lives.
		if _, err := eph.Ack(ctx, requestsQueue, batch.Messages, queen.EphemeralAckOptions{Group: "workers"}); err != nil {
			fmt.Fprintf(os.Stderr, "responder: ack: %v\n", err)
		}
	}

	return served
}

// request mints an inbox name, pushes the request carrying it, and parks on the
// inbox until the answer arrives.
func request(ctx context.Context, eph *queen.Ephemeral, n int, timeout time.Duration) (rpcReply, error) {
	inbox := "rpc-inbox-" + queen.GenerateUUID()

	if _, err := eph.Push(ctx, requestsQueue, rpcRequest{N: n, ReplyTo: inbox}); err != nil {
		return rpcReply{}, err
	}

	batch, err := eph.Pop(ctx, inbox, queen.EphemeralPopOptions{
		Batch:         1,
		Wait:          true,
		TimeoutMillis: int(timeout / time.Millisecond),
		// At-most-once, and exactly right here: the reply is consumed once, by
		// the one caller waiting for it, and there is nothing to ack afterwards.
		AutoAck: true,
	})
	if err != nil {
		return rpcReply{}, err
	}
	if len(batch.Messages) == 0 {
		// The inbox is empty and idle now, so the broker collects it on its own.
		return rpcReply{}, fmt.Errorf("request %d: no reply within %s", n, timeout)
	}

	var reply rpcReply
	if err := json.Unmarshal(batch.Messages[0].Payload, &reply); err != nil {
		return rpcReply{}, err
	}
	return reply, nil
}

// bufferedCoda is the OTHER shape this class is for: presence and telemetry
// fan-out, where the individual message is worth nothing and the batch is worth
// a round trip.
//
// It goes through the same buffer machinery a durable push uses (§4.1) —
// blocking backpressure at MaxSize, a failed batch put back at the FRONT and
// retried, Queen.Close draining what is left — under an `eph:` address, so an
// ephemeral queue and a durable queue of the same name never share a buffer.
//
// Buffering is a client-side latency/efficiency trade, not a durability change:
// a buffered message that has not flushed dies with the process. That is already
// inside this class's contract, which is why buffering is a reasonable default
// here and a considered decision on a durable queue.
func bufferedCoda(ctx context.Context, q *queen.Queen, eph *queen.Ephemeral) {
	const presence = "presence"
	buffer := &queen.BufferConfig{MessageCount: 10, TimeMillis: 200}

	fmt.Println("\nBuffered fan-out: 25 presence beats through one client-side buffer...")
	for i := 0; i < 25; i++ {
		if _, err := eph.Push(ctx, presence, map[string]interface{}{
			"user": fmt.Sprintf("user-%d", i%5), "typing": i%2 == 0,
		}, queen.EphemeralPushOptions{Partition: "room-7", Buffered: buffer}); err != nil {
			panic(err)
		}
	}
	// The last partial batch is still in memory here; Flush is what puts it on
	// the wire without waiting out the linger.
	if err := eph.Flush(ctx, presence, queen.EphemeralPushOptions{Partition: "room-7"}); err != nil {
		panic(err)
	}

	stats := q.GetBufferStats()
	fmt.Printf("  %d flush(es), %d message(s) still buffered\n", stats.FlushesPerformed, stats.TotalBufferedMessages)

	batch, err := eph.Pop(ctx, presence, queen.EphemeralPopOptions{
		Partition: "room-7", Batch: 25, AutoAck: true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("  popped %d beat(s) back off the ring\n", len(batch.Messages))
}
