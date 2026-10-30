// The little book of QueenMQ: the use cases of chapter 3 ("Real applications
// examples"), in Go.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each use case does what the book
// says it does.
//
// Run it against a node:
//
//	docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.7.0
//	GOWORK=off go run ./book-use-cases
//
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os/signal"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/helpers"
)

const url = "http://localhost:6632"

func check(ok bool, what string) {
	if !ok {
		log.Fatalf("FAIL: %s", what)
	}
}

// stopWhen calls stop once done reports true: test bookkeeping that plays the
// part of the SIGTERM a deploy would send.
func stopWhen(done func() bool, stop func()) {
	for !done() {
		time.Sleep(50 * time.Millisecond)
	}
	stop()
}

// The day's report, built by whichever worker gets the lock.
func nightlyWorker(ctx context.Context, client *queen.Queen) (string, error) {
	// guide:start(uc-singleton)
	lock := client.Lock("nightly-report", 30*time.Second)
	wait, cancel := context.WithTimeout(ctx, time.Second) // wait at most 1 s for it
	defer cancel()
	if err := lock.Acquire(wait); errors.Is(err, context.DeadlineExceeded) {
		return "skipped", nil // another worker has it
	} else if err != nil {
		return "", err
	}
	defer func() { _, _ = lock.Release(ctx) }() // if this fails, the lock expires with its ttl

	day := "2026-10-24"
	report := map[string]interface{}{"day": day, "bookings": 412}
	result, err := client.Transaction().
		Guard(lock). // commits only while the lock is ours
		Queue("daily-reports").
		Push(queen.PushItem{TransactionID: "report-" + day, Payload: report}).
		Commit(ctx)
	if err != nil {
		return "", err
	}
	if result.IsKVPrecondition() {
		return "lost the lock", nil
	}
	time.Sleep(1500 * time.Millisecond) // guide:hide
	return "done", nil
	// guide:end
}

func main() {
	ctx := context.Background()
	client, err := queen.New(url)
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{"thumbnails", "order-placed", "chat", "order-events", "shipping",
		"trace-events", "traces", "sms-out", "availability", "review-reminders", "webhooks",
		"daily-reports"} {
		_, _ = client.Queue(q).Delete().Execute(ctx)
	}
	_, _ = client.KV().Delete(ctx, "orders", "order-2001")
	_, _ = client.Ephemeral().Delete(ctx, "quotes")

	// --- Work queue: competing consumers ---------------------------------------
	var doneMu sync.Mutex
	type job struct{ photo, worker string }
	var done []job
	resize := func(photo, worker string) error {
		doneMu.Lock()
		done = append(done, job{photo, worker})
		doneMu.Unlock()
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	// guide:start(uc-work)
	// Twelve photos to resize, from four users.
	for i := 1; i <= 12; i++ {
		_, err := client.Queue("thumbnails").Partition(fmt.Sprintf("user-%d", i%4)).
			Push(map[string]interface{}{"photo": fmt.Sprintf("p%d.jpg", i)}).
			Execute(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Three workers compete for the jobs: each job goes to exactly one of them.
	worker := func(name string) error {
		return client.Queue("thumbnails").
			Partitions(1). // one user's photos at a time
			Each().
			IdleMillis(1500). // stop when there is nothing left
			Consume(ctx, func(ctx context.Context, job *queen.Message) error {
				return resize(job.Data["photo"].(string), name)
			}).
			Execute(ctx)
	}

	var workers sync.WaitGroup
	for _, name := range []string{"w1", "w2", "w3"} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := worker(name); err != nil {
				log.Fatal(err)
			}
		}()
	}
	workers.Wait()
	// guide:end
	{
		photos := map[string]bool{}
		shares := map[string]int{}
		for _, d := range done {
			photos[d.photo] = true
			shares[d.worker]++
		}
		check(len(done) == 12, fmt.Sprintf("twelve jobs handled (%d)", len(done)))
		check(len(photos) == 12, "each job once")
		check(len(shares) >= 2, fmt.Sprint("the jobs were shared: ", shares))
		share, _ := json.Marshal(shares)
		fmt.Println("ok: uc-work", string(share))
	}

	// --- Publish/subscribe and fan-out: consumer groups ------------------------
	var seenMu sync.Mutex
	seen := map[string][]int{}
	record := func(group string, orderID interface{}) {
		seenMu.Lock()
		seen[group] = append(seen[group], int(orderID.(float64)))
		seenMu.Unlock()
	}
	// guide:start(uc-pubsub)
	subscribe := func(group string) error {
		return client.Queue("order-placed").
			Group(group). // each group gets every message
			SubscriptionMode(queen.SubscriptionModeAll).
			Each().
			IdleMillis(1000). // guide:hide
			Consume(ctx, func(ctx context.Context, event *queen.Message) error {
				record(group, event.Data["orderId"]) // guide:hide
				fmt.Println(group, event.Data["orderId"])
				return nil
			}).
			Execute(ctx)
	}

	for _, orderID := range []int{3001, 3002, 3003} {
		_, err := client.Queue("order-placed").Partition(fmt.Sprintf("order-%d", orderID)).
			Push(map[string]interface{}{"orderId": orderID}).
			Execute(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
	var subscribers sync.WaitGroup
	for _, group := range []string{"billing", "shipping"} {
		subscribers.Add(1)
		go func() {
			defer subscribers.Done()
			if err := subscribe(group); err != nil {
				log.Fatal(err)
			}
		}()
	}
	subscribers.Wait()

	// Months later, analytics subscribes too, and reads every order ever placed.
	if err := subscribe("analytics"); err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		for _, g := range []string{"billing", "shipping", "analytics"} {
			got := append([]int(nil), seen[g]...)
			sort.Ints(got)
			check(reflect.DeepEqual(got, []int{3001, 3002, 3003}), fmt.Sprint(g, " got every order: ", got))
		}
		// A group that does not ask for 'all' starts at the end of the queue.
		late, err := client.Queue("order-placed").Group("late").Wait(false).Batch(10).Pop(ctx)
		check(err == nil && len(late) == 0, "a new group without subscriptionMode all skips the history")
		fmt.Println("ok: uc-pubsub")
	}

	// --- 1. Chat processing: a burst is one batch (windowBuffer) ---------------
	var t0 time.Time
	calls := 0
	asked := ""
	// guide:start(uc-chat)
	// A burst waits 2 seconds, then goes to the handler as one batch.
	_, err = client.Queue("chat").Config(queen.QueueConfig{WindowBuffer: 2}).
		Create().Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	t0 = time.Now() // guide:hide

	// The guest sends three messages in a row.
	for _, text := range []string{"Hi!", "Is breakfast included?", "And is there parking?"} {
		_, err := client.Queue("chat").Partition("conv-42").
			Push(map[string]interface{}{"text": text}).
			Execute(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	// The assistant answers the whole burst once.
	err = client.Queue("chat").
		Group("assistant").
		SubscriptionMode(queen.SubscriptionModeAll).
		Batch(10). // the handler gets every message of the burst
		Limit(3).  // guide:hide
		ConsumeBatch(ctx, func(ctx context.Context, messages []*queen.Message) error {
			calls++ // guide:hide
			texts := make([]string, len(messages))
			for i, m := range messages {
				texts[i] = m.Data["text"].(string)
			}
			question := strings.Join(texts, " ")
			asked = question      // guide:hide
			fmt.Println(question) // Hi! Is breakfast included? And is there parking?
			return nil
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(calls == 1, fmt.Sprintf("the three messages reached the handler in one call (%d)", calls))
		check(asked == "Hi! Is breakfast included? And is there parking?", "the burst in order: "+asked)
		waited := time.Since(t0).Milliseconds()
		check(waited >= 1500, fmt.Sprintf("the burst was held for the window (%d ms)", waited))
		fmt.Printf("ok: uc-chat (one call, after %d ms)\n", waited)
	}

	// --- 2. Order workflow: transitions that cannot race (expect) --------------
	_, err = client.KV().Put(ctx, "orders", "order-2001", map[string]interface{}{"status": "paid"},
		queen.TTL(30*24*time.Hour))
	check(err == nil, fmt.Sprint("kv put: ", err))
	_, err = client.Queue("order-events").Partition("order-2001").
		Push(map[string]interface{}{"orderId": "order-2001"}).TransactionID("order-2001-paid").Execute(ctx)
	check(err == nil, fmt.Sprint("push order event: ", err))
	var verdicts []bool
	// guide:start(uc-order)
	err = client.Queue("order-events").
		Group("workflow").
		SubscriptionMode(queen.SubscriptionModeAll).
		AutoAck(false).
		Each().
		Limit(1). // guide:hide
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			opts := queen.AckOptions{ConsumerGroup: "workflow"}
			orderID := msg.Data["orderId"].(string)
			order, err := client.KV().Get(ctx, "orders", orderID) // Value {"status":"paid"}
			if err != nil {
				return err
			}
			var state struct{ Status string }
			if err := json.Unmarshal(order.Value, &state); err != nil {
				return err
			}
			if state.Status != "paid" { // already moved: just ack
				_, err := client.Ack(ctx, msg, true, opts)
				return err
			}

			// Do the work of this step here: charge the card, book the courier, ...

			next := map[string]interface{}{"status": "shipping"}
			result, err := client.Transaction().
				Ack(msg, queen.AckStatusCompleted, opts).
				KV(queen.KVPutOp("orders", orderID, next, queen.TTL(30*24*time.Hour),
					queen.KVWriteOptions{
						Expect:   queen.Expect(order.Version), // only from what we read
						Required: true,
					})).
				Queue("shipping").Partition(orderID).
				Push(queen.PushItem{TransactionID: "ship-" + orderID,
					Payload: map[string]interface{}{"orderId": orderID}}).
				Commit(ctx)
			if err != nil {
				return err
			}
			verdicts = append(verdicts, result.Success) // guide:hide
			if result.IsKVPrecondition() {
				fmt.Println(orderID, "moved by someone else: nothing written")
			}
			return nil
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(reflect.DeepEqual(verdicts, []bool{true}), fmt.Sprint("the step committed: ", verdicts))
		order, err := client.KV().Get(ctx, "orders", "order-2001")
		check(err == nil && string(order.Value) == `{"status":"shipping"}`, "order-2001 is shipping: "+string(order.Value))
		// The same transition from the old version is refused, and writes nothing.
		stale, err := client.Transaction().
			KV(queen.KVPutOp("orders", "order-2001", map[string]interface{}{"status": "shipping"},
				queen.TTL(30*24*time.Hour), queen.KVWriteOptions{Expect: queen.Expect(order.Version - 1), Required: true})).
			Queue("shipping").Partition("order-2001").
			Push(queen.PushItem{TransactionID: "ship-again", Payload: map[string]interface{}{}}).
			Commit(ctx)
		check(err == nil && stale != nil && !stale.Success && stale.IsKVPrecondition(),
			fmt.Sprint("a transition from a stale version rolls back, returned not raised: ", err))
		read, err := client.Queue("shipping").Fetch(ctx, []string{"order-2001"})
		check(err == nil && len(read["order-2001"].Records) == 1, "one shipment, not two")
		fmt.Println("ok: uc-order")
	}

	// --- 3. Trace management: events merged per trace (session window) ---------
	// guide:start(uc-trace)
	// A trace is complete when it has been quiet for 2 seconds.
	merger, err := streams.
		From(client.Queue("trace-events").AsStreamSource()).
		WindowSession(2, streams.WithIdleFlushMs(500)).
		Reduce(func(acc, value interface{}) (interface{}, error) {
			t, e := acc.(map[string]interface{}), value.(map[string]interface{})
			return map[string]interface{}{
				"spans": t["spans"].(float64) + 1,
				"ms":    t["ms"].(float64) + e["ms"].(float64),
			}, nil
		}, map[string]interface{}{"spans": 0.0, "ms": 0.0}).
		To(client.Queue("traces")). // one message per finished trace
		Run(ctx, streams.RunOptions{QueryID: "trace-merger", URL: url,
			SubscriptionMode: queen.SubscriptionModeAll})
	if err != nil {
		log.Fatal(err)
	}

	// Every SDK call is one event, in the partition of its trace.
	// (Go's push sets the transactionId of its first message only: one push per event.)
	event := func(span string, ms int) error {
		_, err := client.Queue("trace-events").Partition("trace-7f3a").
			Push(map[string]interface{}{"span": span, "ms": ms}).
			TransactionID("trace-7f3a:" + span).
			Execute(ctx)
		return err
	}
	if err := errors.Join(event("http", 120), event("db", 45), event("cache", 3)); err != nil {
		log.Fatal(err)
	}
	if err := event("db", 45); err != nil { // a retried call: stored once
		log.Fatal(err)
	}
	// guide:end
	{
		var merged map[string]interface{}
		var raw json.RawMessage
		for i := 0; i < 40 && raw == nil; i++ {
			time.Sleep(250 * time.Millisecond)
			read, err := client.Queue("traces").Fetch(ctx, []string{"trace-7f3a"})
			check(err == nil, fmt.Sprint("fetch traces: ", err))
			if records := read["trace-7f3a"].Records; len(records) > 0 {
				raw = records[0].Payload
			}
		}
		merger.Stop()
		check(raw != nil, "the finished trace reached the traces queue")
		check(json.Unmarshal(raw, &merged) == nil, "the trace decodes")
		value := merged
		if v, ok := merged["value"].(map[string]interface{}); ok {
			value = v
		}
		check(value["spans"] == float64(3), fmt.Sprint("three spans: the retried event was stored once: ", value))
		check(value["ms"] == float64(168), fmt.Sprint("168 ms: ", value))
		fmt.Println("ok: uc-trace", string(raw))
	}

	// --- 4. Rate limiting: a token bucket gate per customer -------------------
	type sms struct {
		n  int
		at time.Duration
	}
	var sentMu sync.Mutex
	var sent []sms
	t4 := time.Now()
	sendSms := func(message interface{}) error {
		n := int(message.(map[string]interface{})["n"].(float64))
		sentMu.Lock()
		sent = append(sent, sms{n, time.Since(t4)})
		sentMu.Unlock()
		return nil
	}
	sentCount := func() int {
		sentMu.Lock()
		defer sentMu.Unlock()
		return len(sent)
	}
	// guide:start(uc-rate)
	// The SMS provider accepts 5 messages a second for each customer.
	// A refused message comes back when its lease ends: here after 1 second.
	_, err = client.Queue("sms-out").Config(queen.QueueConfig{LeaseTime: 1}).
		Create().Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	limiter, err := streams.
		From(client.Queue("sms-out").AsStreamSource()). // one partition per customer
		Gate(helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
			Capacity: 5, RefillPerSec: 5, // 5 at once, then 5 a second
		})).
		Foreach(func(sms interface{}, _ streams.EmitCtx) error {
			return sendSms(sms) // only what the gate lets through
		}).
		Run(ctx, streams.RunOptions{QueryID: "sms-gate", URL: url,
			SubscriptionMode: queen.SubscriptionModeAll})
	if err != nil {
		log.Fatal(err)
	}

	// Twelve messages for one customer, all at once.
	burst := make([]interface{}, 12)
	for i := range burst {
		burst[i] = map[string]interface{}{"n": i + 1, "text": fmt.Sprintf("#%d", i+1)}
	}
	_, err = client.Queue("sms-out").Partition("customer-42").Push(burst).Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		for i := 0; i < 80 && sentCount() < 12; i++ {
			time.Sleep(100 * time.Millisecond)
		}
		limiter.Stop()
		sentMu.Lock()
		order := make([]int, len(sent))
		offsets := make([]int64, len(sent))
		for i, s := range sent {
			order[i] = s.n
			offsets[i] = (s.at - sent[0].at).Milliseconds()
		}
		sentMu.Unlock()
		check(reflect.DeepEqual(order, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}),
			fmt.Sprint("all twelve were sent, in order: ", order))
		first := 0
		for _, at := range offsets {
			if at < 800 {
				first++
			}
		}
		check(first <= 5, fmt.Sprintf("at most 5 in the first burst (%d)", first))
		check(offsets[11] >= 1000, fmt.Sprintf("the rest waited (%d ms)", offsets[11]))
		spread, _ := json.Marshal(offsets)
		fmt.Println("ok: uc-rate", string(spread))
	}

	// --- 5. Only the newest value (conflation) ----------------------------------
	// guide:start(uc-latest)
	// The availability of a hotel changes often; the screen needs only the last value.
	for _, rooms := range []int{12, 11, 9} {
		_, err := client.Queue("availability").Partition("hotel-7").
			Push(map[string]interface{}{"hotel": 7, "rooms": rooms}).
			Execute(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	newest, err := client.Queue("availability").
		Group("screen").
		SubscriptionMode(queen.SubscriptionModeAll).
		Conflation(true). // the newest message of each hotel, the rest skipped
		Wait(false).
		Pop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	latest := newest[0]
	fmt.Println(latest.Data) // map[hotel:7 rooms:9]
	_, err = client.Ack(ctx, latest, true, queen.AckOptions{ConsumerGroup: "screen"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(len(newest) == 1 && reflect.DeepEqual(latest.Data, map[string]interface{}{"hotel": float64(7), "rooms": float64(9)}),
			fmt.Sprint("the screen got the newest value: ", latest.Data))
		again, err := client.Queue("availability").Group("screen").Wait(false).Pop(ctx)
		check(err == nil && len(again) == 0, "the older values were skipped, not delivered later")
		fmt.Println("ok: uc-latest")
	}

	// --- 6. Reminders and delayed jobs (timers) ---------------------------------
	var statuses []string
	// guide:start(uc-reminder)
	// When an order is delivered, ask for a review in 24 hours.
	r, err := client.Timers().Schedule(ctx, queen.TimerSchedule{
		Queue: "review-reminders", TimerKey: "review-2001", Delay: 24 * time.Hour,
		Payload: map[string]interface{}{"orderId": 2001},
	})
	statuses = append(statuses, r.Status) // guide:hide
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r.Status) // scheduled

	// The delivery was late: schedule the same key again, and the reminder moves.
	r, err = client.Timers().Schedule(ctx, queen.TimerSchedule{
		Queue: "review-reminders", TimerKey: "review-2001", Delay: 48 * time.Hour,
		Payload: map[string]interface{}{"orderId": 2001},
	})
	statuses = append(statuses, r.Status) // guide:hide
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r.Status) // rescheduled

	// The guest wrote a review already: no reminder at all.
	_, err = client.Timers().Cancel(ctx, "review-reminders", "review-2001")
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(reflect.DeepEqual(statuses, []string{"scheduled", "rescheduled"}), fmt.Sprint("statuses: ", statuses))
		peek, err := client.Timers().Peek(ctx, "review-reminders", "review-2001")
		check(err == nil && !peek.Found, "the cancelled reminder is gone")
		// A timer fires as a real message of its queue.
		_, err = client.Timers().Schedule(ctx, queen.TimerSchedule{Queue: "review-reminders",
			TimerKey: "review-2002", Delay: 200 * time.Millisecond, Payload: map[string]interface{}{"orderId": 2002}})
		check(err == nil, fmt.Sprint("schedule review-2002: ", err))
		var fired map[string]interface{}
		err = client.Queue("review-reminders").Group("mailer").SubscriptionMode(queen.SubscriptionModeAll).
			Each().Limit(1).
			Consume(ctx, func(ctx context.Context, m *queen.Message) error {
				fired = m.Data
				return nil
			}).Execute(ctx)
		check(err == nil && reflect.DeepEqual(fired, map[string]interface{}{"orderId": float64(2002)}),
			fmt.Sprint("the timer arrived as a message: ", fired, err))
		fmt.Println("ok: uc-reminder")
	}

	// --- 7. Request and reply between services (ephemeral queues) --------------
	// guide:start(uc-reply)
	// The pricing service answers quote requests, in memory.
	pricing := func() error {
		batch, err := client.Ephemeral().Pop(ctx, "quotes",
			queen.EphemeralPopOptions{Group: "pricing", Wait: true})
		if err != nil {
			return err
		}
		for _, m := range batch.Messages {
			var req struct{ Room, ReplyTo string }
			if err := json.Unmarshal(m.Payload, &req); err != nil {
				return err
			}
			_, err := client.Ephemeral().Push(ctx, req.ReplyTo,
				[]map[string]interface{}{{"room": req.Room, "price": 129}})
			if err != nil {
				return err
			}
		}
		_, err = client.Ephemeral().Ack(ctx, "quotes", batch.Messages,
			queen.EphemeralAckOptions{Group: "pricing"})
		return err
	}

	// The booking page asks, and waits at most half a second for the answer.
	quote := func(room string) (map[string]interface{}, error) {
		inbox := "inbox-" + queen.GenerateUUID()
		_, err := client.Ephemeral().Push(ctx, "quotes",
			[]map[string]interface{}{{"room": room, "replyTo": inbox}})
		if err != nil {
			return nil, err
		}
		reply, err := client.Ephemeral().Pop(ctx, inbox,
			queen.EphemeralPopOptions{Wait: true, TimeoutMillis: 500, CommitOnDelivery: true})
		if err != nil {
			return nil, err
		}
		answer := map[string]interface{}{"room": room, "price": nil} // too slow: show "ask us"
		if len(reply.Messages) > 0 {
			err = json.Unmarshal(reply.Messages[0].Payload, &answer)
		}
		return answer, err
	}

	answered := make(chan error, 1)
	go func() { answered <- pricing() }()
	deluxe, err := quote("deluxe")
	if err := errors.Join(err, <-answered); err != nil {
		log.Fatal(err)
	}
	fmt.Println(deluxe) // map[price:129 room:deluxe]
	// guide:end
	{
		check(reflect.DeepEqual(deluxe, map[string]interface{}{"room": "deluxe", "price": float64(129)}),
			fmt.Sprint("the answer came back in time: ", deluxe))
		t := time.Now()
		suite, err := quote("suite") // nobody answers now
		check(err == nil && reflect.DeepEqual(suite, map[string]interface{}{"room": "suite", "price": nil}),
			fmt.Sprint("no answer: price nil: ", suite, err))
		check(time.Since(t) < 3*time.Second, "the caller gave up after its timeout")
		left, err := client.Ephemeral().Pop(ctx, "quotes", queen.EphemeralPopOptions{Group: "pricing"})
		check(err == nil, fmt.Sprint("drain quotes: ", err))
		_, err = client.Ephemeral().Ack(ctx, "quotes", left.Messages, queen.EphemeralAckOptions{Group: "pricing"})
		check(err == nil, fmt.Sprint("ack quotes: ", err))
		fmt.Println("ok: uc-reply")
	}

	// --- 8. Calling a webhook (retries, dead letters, idempotency key) ----------
	// A receiver that fails the first call, and keeps each idempotency key once.
	var hookMu sync.Mutex
	received := map[string]string{}
	callsSeen := 0
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hookMu.Lock()
		defer hookMu.Unlock()
		callsSeen++
		if callsSeen == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received[r.Header.Get("Idempotency-Key")] = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	hookURL := receiver.URL + "/hooks"
	_, err = client.Queue("webhooks").Config(queen.QueueConfig{RetryLimit: 5, RetryDelayMillis: 1000}).
		Create().Execute(ctx)
	check(err == nil, fmt.Sprint("configure webhooks: ", err))
	_, err = client.Queue("webhooks").Partition("customer-42").
		Push(map[string]interface{}{"url": hookURL,
			"event": map[string]interface{}{"type": "booking.confirmed", "id": 9001}}).
		TransactionID("evt-9001").Execute(ctx)
	check(err == nil, fmt.Sprint("push webhook: ", err))
	gotOne := func() bool {
		hookMu.Lock()
		defer hookMu.Unlock()
		return len(received) > 0
	}
	{
		// guide:start(uc-webhook)
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM) // a deploy stops it cleanly
		defer stop()
		go stopWhen(gotOne, stop) // guide:hide

		err := client.Queue("webhooks").
			Group("sender").
			SubscriptionMode(queen.SubscriptionModeAll).
			Each().
			Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
				event, err := json.Marshal(msg.Data["event"])
				if err != nil {
					return err
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost,
					msg.Data["url"].(string), bytes.NewReader(event))
				if err != nil {
					return err
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", msg.TransactionID)
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					return err
				}
				res.Body.Close()
				if res.StatusCode >= 300 {
					return fmt.Errorf("HTTP %d", res.StatusCode) // retried, then dead-lettered
				}
				return nil
			}).
			Execute(ctx) // returns once ctx is cancelled
		if err != nil {
			log.Fatal(err)
		}
		// guide:end
	}
	{
		receiver.Close()
		hookMu.Lock()
		check(callsSeen == 2, fmt.Sprintf("the failed call was retried once (%d calls)", callsSeen))
		_, ok := received["evt-9001"]
		check(ok && len(received) == 1, fmt.Sprint("the receiver got the event under its key: ", received))
		hookMu.Unlock()
		fmt.Println("ok: uc-webhook")
	}

	// --- 9. A job only one worker runs (lock + guard) ---------------------------
	{
		outcomes := make([]string, 3)
		var nightly sync.WaitGroup
		for i := range outcomes {
			nightly.Add(1)
			go func() {
				defer nightly.Done()
				outcome, err := nightlyWorker(ctx, client)
				check(err == nil, fmt.Sprint("nightly worker: ", err))
				outcomes[i] = outcome
			}()
		}
		nightly.Wait()
		ran := 0
		for _, o := range outcomes {
			if o == "done" {
				ran++
			}
		}
		check(ran == 1, fmt.Sprint("one worker ran it: ", outcomes))
		read, err := client.Queue("daily-reports").Fetch(ctx, []string{"Default"})
		check(err == nil && len(read["Default"].Records) == 1, "one report")
		list, _ := json.Marshal(outcomes)
		fmt.Println("ok: uc-singleton", string(list))
	}

	_ = client.Close(ctx)
	fmt.Println("all use cases checked")
}
