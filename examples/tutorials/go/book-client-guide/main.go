// The little book of QueenMQ: the examples of chapter 2 ("Writing applications")
// that are not already in the five-minute guide (five-minutes/main.go), in Go.
//
// The book shows the regions between `guide:start(<id>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each example does what the book
// says it does.
//
// Run it against the node that the book's first section starts:
//
//	docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
//	GOWORK=off go run ./book-client-guide
//
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.
package main

// guide:start(stream-gate)
import "github.com/smartpricing/queen/clients/client-go/v2/streams/helpers"

// guide:end

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
	"net/url"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
)

func check(ok bool, what string) {
	if !ok {
		log.Fatalf("FAIL: %s", what)
	}
}

// jsonEqual reports whether a JSON value the broker answered equals want.
func jsonEqual(got json.RawMessage, want string) bool {
	var a, b interface{}
	return json.Unmarshal(got, &a) == nil && json.Unmarshal([]byte(want), &b) == nil &&
		reflect.DeepEqual(a, b)
}

// stopWhen calls stop once done reports true: test bookkeeping that plays the
// part of the SIGTERM a deploy would send.
func stopWhen(done func() bool, stop func()) {
	for !done() {
		time.Sleep(20 * time.Millisecond)
	}
	stop()
}

// The helpers the examples call. In a real program they are yours.
var errPermanent = errors.New("permanent failure")

func sendEmail(data map[string]interface{}) error { return nil }

// One request the wire-forms client sent, and the answer's content type.
type wireCall struct {
	url, sent, got string
	body           []byte
}

// recordWire starts a forward proxy that records every request through it.
func recordWire(calls *[]wireCall, mu *sync.Mutex) *httptest.Server {
	plain := &http.Transport{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		out, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		out.Header = r.Header.Clone()
		res, err := plain.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer res.Body.Close()
		mu.Lock()
		*calls = append(*calls, wireCall{url: r.URL.String(), sent: r.Header.Get("Content-Type"),
			got: res.Header.Get("Content-Type"), body: body})
		mu.Unlock()
		for k, v := range res.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(res.StatusCode)
		_, _ = io.Copy(w, res.Body)
	}))
}

func main() {
	ctx := context.Background()
	client, err := queen.New("http://localhost:6632")
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{"jobs", "emails", "events", "reviews", "prices", "metrics", "tasks",
		"payments", "logins", "login-rate", "clicks", "sales", "sales-per-hour", "outgoing-sms",
		"invoices"} {
		_, _ = client.Queue(q).Delete().Execute(ctx)
	}

	// --- Configure ------------------------------------------------------------
	// guide:start(configure-jobs)
	_, err = client.Queue("jobs").
		Config(queen.QueueConfig{
			LeaseTime:                 120,    // 2 minutes per job, then another worker
			RetryLimit:                5,      // after 5 failures, the dead-letter queue
			RetryDelayMillis:          10000,  // a failed job is back after 10 s, not at once
			DelayedProcessing:         30,     // a new job waits 30 s before anyone takes it
			WindowBuffer:              5,      // a burst waits 5 s, then is handed over at once
			DedupWindowSeconds:        86400,  // a transactionId is remembered for a day
			RetentionEnabled:          true,   // retention runs only when this is on:
			CompletedRetentionSeconds: 3600,   //   jobs all groups finished go after an hour
			MaxWaitTimeSeconds:        604800, // anything over a week old goes, finished or not
		}).
		Create().
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		info, err := client.Admin().GetQueue(ctx, "jobs")
		check(err == nil, fmt.Sprint("getQueue: ", err))
		options, _ := info["options"].(map[string]interface{})
		check(options["retryDelay"] == float64(10000), "retryDelay")
		check(options["delayedProcessing"] == float64(30), "delayedProcessing")
		check(options["windowBuffer"] == float64(5), "windowBuffer")
		check(options["retentionEnabled"] == true, "retentionEnabled")
		check(options["completedRetentionSeconds"] == float64(3600), "completedRetentionSeconds")
		check(options["maxWaitTimeSeconds"] == float64(604800), "maxWaitTimeSeconds")
		check(options["leaseTime"] == float64(120), "leaseTime")
		check(options["retryLimit"] == float64(5), "retryLimit")
		check(options["dedupWindowSeconds"] == float64(86400), "dedupWindowSeconds")
		fmt.Println("ok: configure-jobs")
	}

	// --- Push -----------------------------------------------------------------
	// guide:start(push-callbacks)
	results, err := client.Queue("emails").
		Partition("customer-42").
		Push(map[string]interface{}{"to": "ada@example.com"}).
		TransactionID("welcome-42").
		Execute(ctx)
	if err != nil {
		log.Fatal(err) // the request failed: no message got an answer
	}
	for _, r := range results { // one answer per message; a refusal is not an error here
		switch r.Status {
		case "queued":
			fmt.Println("stored", r.TransactionID)
		case "duplicate":
			fmt.Println("already stored", r.TransactionID)
		default:
			fmt.Println("refused", r.Error)
		}
	}
	// guide:end
	{
		check(len(results) == 1 && results[0].Status == "queued", fmt.Sprint("first push: ", results))
		again, err := client.Queue("emails").Partition("customer-42").
			Push(map[string]interface{}{"to": "ada@example.com"}).TransactionID("welcome-42").Execute(ctx)
		check(err == nil && len(again) == 1 && again[0].Status == "duplicate",
			fmt.Sprint("the second push of the same id answered duplicate: ", again, err))
		fmt.Println("ok: push-callbacks")
	}

	// guide:start(push-buffer)
	for i := 0; i < 1000; i++ {
		_, err := client.Queue("events").
			Partition(fmt.Sprintf("device-%d", i%10)).
			Buffer(queen.BufferConfig{
				MessageCount: 100,  // 100 at a time,
				TimeMillis:   1000, // or every second
			}).
			Push(map[string]interface{}{"device": i % 10, "value": i}).
			TransactionID(fmt.Sprintf("reading-%d", i)).
			Execute(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := client.FlushAllBuffers(ctx); err != nil { // before exiting, send what is left
		log.Fatal(err)
	}
	// guide:end
	{
		names := make([]string, 10)
		for i := range names {
			names[i] = fmt.Sprintf("device-%d", i)
		}
		read, err := client.Queue("events").Fetch(ctx, names)
		check(err == nil, fmt.Sprint("fetch: ", err))
		total := 0
		for _, p := range read {
			total += len(p.Records)
		}
		check(total == 1000, fmt.Sprintf("every buffered reading was stored (%d)", total))
		fmt.Println("ok: push-buffer")
	}

	// Record what goes over the wire, to check the binary forms below: the
	// client made in the region clones http.DefaultTransport, proxy included.
	var wire []wireCall
	var wireMu sync.Mutex
	recorder := recordWire(&wire, &wireMu)
	defaultTransport := http.DefaultTransport.(*http.Transport)
	proxyBefore := defaultTransport.Proxy
	recorderURL, _ := url.Parse(recorder.URL)
	defaultTransport.Proxy = http.ProxyURL(recorderURL)
	var wireData map[string]interface{}
	// guide:start(wire-forms)
	fast, err := queen.New(queen.ClientConfig{
		URL:             "http://localhost:6632",
		BinaryPush:      true, // a push travels as one binary body, not JSON
		PushCompression: true, // with BinaryPush: a partition's batch is zstd-compressed here
		FramedPop:       true, // a pop gets the records as stored, and unpacks them here
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = fast.Queue("invoices").Partition("customer-42").
		Push(map[string]interface{}{"orderId": 2001, "total": 12.5}).
		TransactionID("invoice-2001").
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = fast.Queue("invoices").Each().Limit(1).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			wireData = msg.Data   // guide:hide
			fmt.Println(msg.Data) // map[orderId:2001 total:12.5], as with JSON
			return nil
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		defaultTransport.Proxy = proxyBefore
		_ = fast.Close(ctx)
		recorder.Close()
		var push, pop *wireCall
		for i := range wire {
			w := &wire[i]
			if push == nil && strings.Contains(w.url, "/api/v1/push") {
				push = w
			}
			if pop == nil && strings.Contains(w.url, "/api/v1/pop/") && strings.Contains(w.url, "frames=true") {
				pop = w
			}
		}
		check(push != nil && push.sent == "application/x-queen-push", "the push went as a binary body")
		check(bytes.Contains(push.body, []byte{0x28, 0xb5, 0x2f, 0xfd}), "the binary push carried a zstd frame")
		check(pop != nil && pop.got == "application/x-queen-pop", "the broker answered the pop with frames")
		check(reflect.DeepEqual(wireData, map[string]interface{}{"orderId": float64(2001), "total": 12.5}),
			fmt.Sprint("the framed pop unpacked the payload: ", wireData))
		fmt.Println("ok: wire-forms")
	}

	// --- Consume --------------------------------------------------------------
	archived := 0
	// guide:start(consume-batch)
	err = client.Queue("events").
		Group("archiver").
		SubscriptionMode(queen.SubscriptionModeAll).
		Batch(100). // up to 100 messages per call
		Limit(1000).
		// ConsumeBatch, not Consume: the handler gets the whole batch
		ConsumeBatch(ctx, func(ctx context.Context, messages []*queen.Message) error {
			archived += len(messages) // guide:hide
			fmt.Println("archiving", len(messages))
			return nil // all acked if it returns nil, all nacked if it returns an error
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	check(archived == 1000, fmt.Sprintf("the batch handler saw every reading once (%d)", archived))
	fmt.Println("ok: consume-batch")

	// guide:start(pop-ack)
	messages, err := client.Queue("events").
		Group("auditor").
		SubscriptionMode(queen.SubscriptionModeAll).
		Batch(50).
		Wait(true). // long poll: wait for messages, never return empty
		Pop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("auditing", len(messages))
	_, err = client.Ack(ctx, messages, true,
		queen.AckOptions{ConsumerGroup: "auditor"}) // the whole batch at once
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	check(len(messages) > 0 && len(messages) <= 50, "pop returned a batch")
	fmt.Println("ok: pop-ack")

	for i, stars := range []int{5, 1} {
		_, err := client.Queue("reviews").Partition("shop-3").Push(map[string]interface{}{"stars": stars}).
			TransactionID(fmt.Sprintf("review-%d", i+1)).Execute(ctx)
		check(err == nil, fmt.Sprint("push review: ", err))
	}
	pair, err := client.Queue("reviews").Partition("shop-3").Group("moderation").
		SubscriptionMode(queen.SubscriptionModeAll).Batch(2).Wait(true).Pop(ctx)
	check(err == nil && len(pair) == 2, fmt.Sprint("popped both reviews: ", len(pair), err))
	first, second := pair[0], pair[1]
	// guide:start(ack-each-status)
	// Go's Ack gives one status to all the messages it is passed: one call per status.
	_, err = client.Ack(ctx, first, true, queen.AckOptions{ConsumerGroup: "moderation"})
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Ack(ctx, second, false,
		queen.AckOptions{ConsumerGroup: "moderation", Error: "timeout"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		// A long poll already waiting is not woken when retryDelay passes (a broker
		// gap, reported); a new pop after the delay gets the message.
		var back []*queen.Message
		for i := 0; i < 5 && len(back) == 0; i++ {
			back, err = client.Queue("reviews").Partition("shop-3").Group("moderation").
				Batch(2).Wait(true).TimeoutMillis(1500).Pop(ctx)
			check(err == nil, fmt.Sprint("pop: ", err))
		}
		check(len(back) == 1 && back[0].TransactionID == "review-2", "only the failed review came back")
		_, err = client.Ack(ctx, back, true, queen.AckOptions{ConsumerGroup: "moderation"})
		check(err == nil, fmt.Sprint("ack: ", err))
		fmt.Println("ok: ack-each-status")
	}

	// guide:start(consume-callbacks)
	err = client.Queue("emails").
		Group("mailer").
		SubscriptionMode(queen.SubscriptionModeAll).
		AutoAck(false). // the client stops acking: the handler gives every answer
		Each().
		Limit(1).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			opts := queen.AckOptions{ConsumerGroup: "mailer"}
			err := sendEmail(msg.Data)
			switch {
			case err == nil:
				_, err = client.Ack(ctx, msg, true, opts)
			case errors.Is(err, errPermanent): // trying again cannot help
				// Ack takes no dlq status: a transaction carries it, without the error text.
				_, err = client.Transaction().Ack(msg, "dlq", opts).Commit(ctx)
			default:
				opts.Error = err.Error()
				_, err = client.Ack(ctx, msg, false, opts) // failed: back after retryDelay
			}
			return err
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		left, err := client.Queue("emails").Group("mailer").Wait(false).Pop(ctx)
		check(err == nil && len(left) == 0, "the handler acked the email")
		fmt.Println("ok: consume-callbacks")
	}

	for _, price := range []int{10, 11, 12} {
		_, err := client.Queue("prices").Partition("sku-A-1").
			Push(map[string]interface{}{"sku": "A-1", "price": price}).
			TransactionID(fmt.Sprintf("price-A-1-%d", price)).Execute(ctx)
		check(err == nil, fmt.Sprint("push price: ", err))
	}
	// guide:start(conflation)
	newest, err := client.Queue("prices").
		Group("repricer").
		SubscriptionMode(queen.SubscriptionModeAll).
		Conflation(true). // only the newest message of each partition
		Wait(false).
		Pop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	latest := newest[0]
	fmt.Println(latest.Data) // map[price:12 sku:A-1]: 10 and 11 are skipped
	_, err = client.Ack(ctx, latest, true, queen.AckOptions{ConsumerGroup: "repricer"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(len(newest) == 1 && latest.Data["price"] == float64(12), "conflation delivered the newest price")
		again, err := client.Queue("prices").Group("repricer").Conflation(true).Wait(false).Pop(ctx)
		check(err == nil && len(again) == 0, "and moved past the older ones")
		fmt.Println("ok: conflation")
	}

	for i := 0; i < 5; i++ {
		_, err := client.Queue("metrics").Partition("cpu").Push(map[string]interface{}{"load": i}).
			TransactionID(fmt.Sprintf("cpu-%d", i)).Execute(ctx)
		check(err == nil, fmt.Sprint("push metric: ", err))
	}
	// guide:start(commit-on-delivery)
	readings, err := client.Queue("metrics").
		Group("live-chart").
		SubscriptionMode(queen.SubscriptionModeAll).
		CommitOnDelivery(true). // the cursor moves as they are handed out: no ack
		Batch(100).
		Wait(false).
		Pop(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		check(len(readings) == 5, fmt.Sprintf("commitOnDelivery handed out every reading (%d)", len(readings)))
		left, err := client.Queue("metrics").Group("live-chart").Wait(false).Pop(ctx)
		check(err == nil && len(left) == 0, "and nothing is left to ack or redeliver")
		fmt.Println("ok: commit-on-delivery")
	}

	// guide:start(seek)
	_, err = client.Admin().SeekConsumerGroup(ctx, "archiver", "events",
		queen.SeekConsumerGroupOptions{Timestamp: "2026-10-22T00:00:00Z"})
	if err != nil {
		log.Fatal(err)
	}
	// the archiver group now reads again everything pushed since that time

	_, err = client.Admin().SeekConsumerGroup(ctx, "archiver", "events",
		queen.SeekConsumerGroupOptions{ToEnd: true})
	if err != nil {
		log.Fatal(err)
	}
	// and now it skips everything it has not read yet
	// guide:end
	{
		left, err := client.Queue("events").Group("archiver").Wait(false).Pop(ctx)
		check(err == nil && len(left) == 0, "after a seek to the end the group has nothing left")
		fmt.Println("ok: seek")
	}

	for i := 0; i < 40; i++ {
		_, err := client.Queue("tasks").Partition(fmt.Sprintf("account-%d", i%8)).
			Push(map[string]interface{}{"n": i}).TransactionID(fmt.Sprintf("task-%d", i)).Execute(ctx)
		check(err == nil, fmt.Sprint("push task: ", err))
	}
	var ran atomic.Int64
	runTask := func(data map[string]interface{}) error {
		ran.Add(1)
		time.Sleep(5 * time.Millisecond)
		return nil
	}
	{
		// guide:start(consume-run)
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM) // a deploy stops it cleanly
		defer stop()
		go stopWhen(func() bool { return ran.Load() >= 40 }, stop) // guide:hide

		err := client.Queue("tasks").
			Group("workers").
			SubscriptionMode(queen.SubscriptionModeAll).
			Concurrency(4).          // four handlers at once, on four partitions
			RenewLease(true, 10000). // long tasks keep their lease alive
			IdleMillis(60000).       // or stop after a minute with nothing to do
			Each().
			Consume(ctx, func(ctx context.Context, task *queen.Message) error {
				return runTask(task.Data)
			}).
			Execute(ctx) // returns once ctx is cancelled
		if err != nil {
			log.Fatal(err)
		}
		// guide:end
	}
	check(ran.Load() == 40, fmt.Sprintf("four workers ran every task, then the signal stopped them (%d)", ran.Load()))
	fmt.Println("ok: consume-run")

	// --- Retries and the dead-letter queue --------------------------------------
	_, err = client.Queue("payments").Config(queen.QueueConfig{RetryLimit: 1, RetryDelayMillis: 100}).
		Create().Execute(ctx)
	check(err == nil, fmt.Sprint("configure payments: ", err))
	_, err = client.Queue("payments").Partition("order-7").Push(map[string]interface{}{"amount": 25}).
		TransactionID("charge-7").Execute(ctx)
	check(err == nil, fmt.Sprint("push charge-7: ", err))
	err = client.Queue("payments").Group("charger").SubscriptionMode(queen.SubscriptionModeAll).
		Each().Limit(2).IdleMillis(5000).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			return errors.New("card declined")
		}).Execute(ctx)
	check(err == nil, fmt.Sprint("failing consumer: ", err))
	time.Sleep(300 * time.Millisecond)
	// guide:start(dlq-read)
	dead, err := client.Queue("payments").DLQ("charger").Limit(20).Get(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, letter := range dead.Messages {
		fmt.Println(letter.TransactionID, letter.ErrorMessage, letter.RetryCount)
	}
	// guide:end
	check(len(dead.Messages) == 1, fmt.Sprintf("the failing payment is in the dead-letter queue (%d)", len(dead.Messages)))
	check(dead.Messages[0].ErrorMessage == "card declined", "with its error: "+dead.Messages[0].ErrorMessage)
	fmt.Println("ok: dlq-read")

	// guide:start(dlq-replay)
	for _, letter := range dead.Messages {
		// back into its partition, and out of the dead-letter queue
		_, err := client.Admin().RetryMessage(ctx, letter.PartitionID, letter.TransactionID)
		if err != nil {
			log.Fatal(err)
		}
	}
	// guide:end
	{
		again, err := client.Queue("payments").Group("charger").Batch(10).Wait(true).TimeoutMillis(3000).Pop(ctx)
		check(err == nil && len(again) == 1, fmt.Sprintf("the replayed payment is delivered again (%d)", len(again)))
		_, err = client.Ack(ctx, again, true, queen.AckOptions{ConsumerGroup: "charger"})
		check(err == nil, fmt.Sprint("ack: ", err))
		left, err := client.Queue("payments").DLQ("charger").Get(ctx)
		check(err == nil && len(left.Messages) == 0, "and it left the dead-letter queue")
		fmt.Println("ok: dlq-replay")
	}

	{
		// The four ack statuses the book's table describes, checked here. 'retry'
		// gives a message back at once without spending a retry; 'dlq' files it
		// now. Go's Ack sends completed or failed only, so the two travel as
		// transaction acks, which carry no error text: the dead letter keeps the
		// broker's own reason, not 'bad payload'.
		for _, n := range []int{8, 9} {
			_, err := client.Queue("payments").Partition(fmt.Sprintf("order-%d", n)).
				Push(map[string]interface{}{"amount": n}).TransactionID(fmt.Sprintf("charge-%d", n)).Execute(ctx)
			check(err == nil, fmt.Sprint("push: ", err))
		}
		r1, err := client.Queue("payments").Partition("order-8").Group("statuses").
			SubscriptionMode(queen.SubscriptionModeAll).Batch(1).Wait(true).Pop(ctx)
		check(err == nil && len(r1) == 1, "popped charge-8")
		_, err = client.Transaction().Ack(r1[0], "retry", queen.AckOptions{ConsumerGroup: "statuses"}).Commit(ctx)
		check(err == nil, fmt.Sprint("retry ack: ", err))
		r2, err := client.Queue("payments").Partition("order-8").Group("statuses").Batch(1).Wait(true).Pop(ctx)
		check(err == nil && len(r2) == 1 && r2[0].TransactionID == r1[0].TransactionID,
			"'retry' redelivered the same message at once")
		check(r2[0].RetryCount == r1[0].RetryCount, "'retry' spent no retry")
		_, err = client.Transaction().Ack(r2[0], "dlq", queen.AckOptions{ConsumerGroup: "statuses"}).Commit(ctx)
		check(err == nil, fmt.Sprint("dlq ack: ", err))
		filed, err := client.Queue("payments").DLQ("statuses").Get(ctx)
		check(err == nil && len(filed.Messages) == 1, "'dlq' filed the message in the dead-letter queue")
		check(filed.Messages[0].TransactionID == "charge-8", "the dead letter is charge-8")
		fmt.Println("ok: ack statuses")
	}

	// --- KV -------------------------------------------------------------------
	_, err = client.KV().Delete(ctx, "carts", "customer-42")
	check(err == nil, fmt.Sprint("delete the cart: ", err))
	_, err = client.KV().Delete(ctx, "limits", "api:customer-42")
	check(err == nil, fmt.Sprint("delete the counter: ", err))
	// guide:start(kv)
	// Every write names its lifetime: a ttl, or forever.
	_, err = client.KV().Put(ctx, "carts", "customer-42", map[string]int{"items": 3},
		queen.TTL(7*24*time.Hour))
	if err != nil {
		log.Fatal(err)
	}
	cart, err := client.KV().Get(ctx, "carts", "customer-42") // Found, Value, Version, ...
	if err != nil {
		log.Fatal(err)
	}

	// Write only if nobody wrote since we read it: the version is the condition.
	saved, err := client.KV().Put(ctx, "carts", "customer-42", map[string]int{"items": 4},
		queen.TTL(7*24*time.Hour), queen.KVWriteOptions{Expect: queen.Expect(cart.Version)})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(saved.Applied) // false if the version moved

	// A counter with a ceiling: past max, the increment does not apply.
	hit, err := client.KV().Incr(ctx, "limits", "api:customer-42", 1, queen.TTL(time.Minute),
		queen.KVIncrOptions{Max: queen.Int64(100)})
	if err != nil {
		log.Fatal(err)
	}
	if !hit.Applied {
		fmt.Println("over the limit") // reason: "limit"
	}
	// guide:end
	{
		check(cart.Found, "the cart was read back")
		check(jsonEqual(cart.Value, `{"items":3}`), "the cart holds 3 items: "+string(cart.Value))
		check(saved.Applied, "the conditional write applied: "+saved.Reason)
		stale, err := client.KV().Put(ctx, "carts", "customer-42", map[string]int{"items": 9},
			queen.TTL(7*24*time.Hour), queen.KVWriteOptions{Expect: queen.Expect(cart.Version)})
		check(err == nil, fmt.Sprint("stale put: ", err))
		check(!stale.Applied, "a write from the old version is refused")
		check(stale.Reason == queen.KVReasonVersion, "with reason version: "+stale.Reason)
		check(jsonEqual(stale.Value, `{"items":4}`), "and answers the current value: "+string(stale.Value))
		check(hit.Applied, "the first increment applied: "+hit.Reason)
		capped, err := client.KV().Incr(ctx, "limits", "api:customer-42", 100, queen.TTL(time.Minute),
			queen.KVIncrOptions{Max: queen.Int64(100)})
		check(err == nil, fmt.Sprint("capped incr: ", err))
		check(!capped.Applied, "an increment past max does not apply")
		check(capped.Reason == queen.KVReasonLimit, "with reason limit: "+capped.Reason)
		check(capped.Value == 1, fmt.Sprintf("and the counter keeps its value (%d)", capped.Value))
		fmt.Println("ok: kv")
	}

	// --- Streams ----------------------------------------------------------------
	// guide:start(stream-sliding)
	loginRate, err := streams.
		From(client.Queue("logins").AsStreamSource()). // one partition per user
		WindowSliding(600, 60).                        // the last 10 minutes, every minute
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(login interface{}) (float64, error) { return 1, nil },
		}, "count").
		To(client.Queue("login-rate")).
		Run(ctx, streams.RunOptions{QueryID: "login-rate", URL: "http://localhost:6632"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	time.Sleep(1200 * time.Millisecond)
	check(loginRate.Metrics().ErrorsTotal == 0, "login-rate ran without errors: "+loginRate.Metrics().LastError)
	loginRate.Stop()
	fmt.Println("ok: stream-sliding")

	// guide:start(stream-session)
	visits, err := streams.
		From(client.Queue("clicks").AsStreamSource()). // one partition per visitor
		WindowSession(1800).                           // a visit ends after 30 minutes of quiet
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(click interface{}) (float64, error) { return 1, nil },
		}, "count").
		Foreach(func(visit interface{}, _ streams.EmitCtx) error {
			fmt.Println("visit ended", visit)
			return nil // acked after this returns
		}).
		Run(ctx, streams.RunOptions{QueryID: "visits", URL: "http://localhost:6632"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	time.Sleep(1200 * time.Millisecond)
	check(visits.Metrics().ErrorsTotal == 0, "visits ran without errors: "+visits.Metrics().LastError)
	visits.Stop()
	fmt.Println("ok: stream-session")

	// guide:start(stream-hourly)
	hourly, err := streams.
		From(client.Queue("sales").AsStreamSource()).
		WindowCron("hour", // on the clock: 10:00 to 11:00, 11:00 to 12:00
			streams.WithEventTime(func(msg interface{}) (int64, error) {
				// when the sale happened, not when it arrived
				// (eventTime gets the whole message: the sale is its data)
				sale := msg.(map[string]interface{})["data"].(map[string]interface{})
				at, err := time.Parse(time.RFC3339, sale["soldAt"].(string))
				return at.UnixMilli(), err
			}),
			streams.WithAllowedLateness(120), // a sale up to 2 minutes late still counts
		).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(sale interface{}) (float64, error) { return 1, nil },
			"sum": func(sale interface{}) (float64, error) {
				return sale.(map[string]interface{})["amount"].(float64), nil
			},
			"max": func(sale interface{}) (float64, error) {
				return sale.(map[string]interface{})["amount"].(float64), nil
			},
		}, "count", "sum", "max").
		To(client.Queue("sales-per-hour")).
		Run(ctx, streams.RunOptions{QueryID: "sales-per-hour", URL: "http://localhost:6632",
			SubscriptionMode: queen.SubscriptionModeAll})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	{
		// One sale, to run the eventTime function: its hour stays open, so
		// nothing is emitted, but a time it cannot read fails the cycle.
		_, err := client.Queue("sales").Partition("shop-1").
			Push(map[string]interface{}{"soldAt": time.Now().UTC().Format(time.RFC3339), "amount": 30}).
			TransactionID(fmt.Sprintf("sale-%d", time.Now().UnixNano())).Execute(ctx)
		check(err == nil, fmt.Sprint("push sale: ", err))
		for i := 0; i < 50 && hourly.Metrics().MessagesTotal == 0 && hourly.Metrics().ErrorsTotal == 0; i++ {
			time.Sleep(100 * time.Millisecond)
		}
		m := hourly.Metrics()
		check(m.ErrorsTotal == 0 && m.MessagesTotal == 1,
			fmt.Sprintf("the hourly window took the sale by its own time (%d, %s)", m.MessagesTotal, m.LastError))
	}
	hourly.Stop()
	fmt.Println("ok: stream-hourly")

	var sentMu sync.Mutex
	var sent []interface{}
	sendSms := func(sms interface{}) error {
		sentMu.Lock()
		defer sentMu.Unlock()
		sent = append(sent, sms)
		return nil
	}
	sentCount := func() int {
		sentMu.Lock()
		defer sentMu.Unlock()
		return len(sent)
	}
	// guide:start(stream-gate)
	limiter, err := streams.
		From(client.Queue("outgoing-sms").AsStreamSource()). // one partition per customer
		Gate(helpers.TokenBucketGate(helpers.TokenBucketGateOptions{
			Capacity: 10, RefillPerSec: 1, // 10 at once, then 1 a second
		})).
		Foreach(func(sms interface{}, _ streams.EmitCtx) error { return sendSms(sms) }).
		Run(ctx, streams.RunOptions{QueryID: "sms-limiter", URL: "http://localhost:6632"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	time.Sleep(time.Second)
	for i := 0; i < 3; i++ {
		_, err := client.Queue("outgoing-sms").Partition("customer-9").
			Push(map[string]interface{}{"to": "+390000000", "text": fmt.Sprintf("hello %d", i)}).
			TransactionID(fmt.Sprintf("sms-%d-%d", time.Now().UnixNano(), i)).Execute(ctx)
		check(err == nil, fmt.Sprint("push sms: ", err))
	}
	for i := 0; i < 100 && sentCount() < 3; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	limiter.Stop()
	check(sentCount() == 3, fmt.Sprintf("the gate let the first three messages through (%d)", sentCount()))
	fmt.Println("ok: stream-gate")

	_ = client.Close(ctx)
	fmt.Println("\nPASS")
}
