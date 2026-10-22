// Queen in five minutes: the program behind the homepage guide, in Go.
//
// The homepage shows the regions between `guide:start(<step>)` and `guide:end`,
// verbatim, minus lines ending in `guide:hide`. Everything outside them gives
// the program a clean start and checks that each step did what the page says.
//
// Run it against the node that step 1 of the guide starts:
//
//	docker run -p 6632:6632 ghcr.io/queen-mq/queen:2.6.0
//	GOWORK=off go run ./five-minutes
//
// run.sh runs a copy with localhost:6632 replaced by $QUEEN_URL.
package main

// guide:start(configure)
import queen "github.com/smartpricing/queen/clients/client-go/v2"

// guide:end

// guide:start(stream)
import "github.com/smartpricing/queen/clients/client-go/v2/streams"

// guide:end

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func check(ok bool, what string) {
	if !ok {
		log.Fatalf("FAIL: %s", what)
	}
}

func main() {
	admin, err := queen.New("http://localhost:6632")
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range []string{"orders", "shipments", "reminders", "reports", "revenue-per-minute"} {
		_, _ = admin.Queue(q).Delete().Execute(context.Background())
	}

	// guide:start(configure)
	ctx := context.Background()
	client, err := queen.New("http://localhost:6632")
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Queue("orders").
		Namespace("shop"). // groups queues in the dashboard
		Config(queen.QueueConfig{
			LeaseTime:        30,    // seconds a worker holds a message before it is redelivered
			RetryLimit:       3,     // failures before the message goes to the dead-letter queue
			RetentionSeconds: 86400, // keep messages for a day
		}).
		Create().
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	fmt.Println("ok: configure")

	// guide:start(push)
	_, err = client.Queue("orders").
		Partition("customer-42"). // one ordered partition per customer, created by this push
		Push(map[string]interface{}{"orderId": 1001, "customer": 42, "total": 59.9}).
		TransactionID("order-1001"). // the dedup key: pushing the same id again is a no-op
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	fmt.Println("ok: push")

	// guide:start(consume)
	err = client.Queue("orders").
		Each().   // the handler gets one message at a time
		Limit(1). // stop after one message; a worker leaves this out
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			fmt.Println(msg.Data) // acked when this returns nil, retried on an error
			return nil
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	left, err := client.Queue("orders").Wait(false).Pop(ctx)
	check(err == nil && len(left) == 0, "queue mode acked the order")
	fmt.Println("ok: consume")

	var billed []interface{}
	// guide:start(consume-group)
	err = client.Queue("orders").
		Group("billing").                            // every group gets every message
		SubscriptionMode(queen.SubscriptionModeAll). // a new group starts at the tail unless told otherwise
		Batch(100).                                  // up to 100 messages per pop...
		Partitions(10).                              // ...from up to 10 partitions
		RenewLease(true, 2000).                      // heartbeat every 2 s while the handler runs
		AutoAck(false).                              // the handler decides the outcome
		Each().
		Limit(1).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			ok := msg.Data["total"].(float64) > 0
			_, err := client.Ack(ctx, msg, ok, queen.AckOptions{ConsumerGroup: "billing"})
			billed = append(billed, msg.Data["orderId"]) // guide:hide
			return err
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	check(len(billed) == 1 && billed[0] == float64(1001), "the billing group read the order pushed before it existed")
	fmt.Println("ok: consume-group")

	// guide:start(transaction)
	err = client.Queue("orders").
		Group("fulfillment").
		SubscriptionMode(queen.SubscriptionModeAll).
		AutoAck(false).
		Each().
		Limit(1).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			orderID := fmt.Sprint(msg.Data["orderId"])
			_, err := client.Transaction().
				Ack(msg, queen.AckStatusCompleted, queen.AckOptions{ConsumerGroup: "fulfillment"}). // the input is done
				KV(queen.KVPutOp("orders", "order-"+orderID,                                        // the order's state
										map[string]interface{}{"status": "paid"}, queen.TTL(7*24*time.Hour))).
				Queue("shipments").Partition(msg.Partition). // the next event
				Push(queen.PushItem{TransactionID: "ship-" + orderID, Payload: map[string]interface{}{"orderId": orderID}}).
				Timers(queen.ScheduleTimerOp(queen.TimerSchedule{ // a message tomorrow
					Queue: "reminders", TimerKey: "review-" + orderID, Delay: 24 * time.Hour,
					Payload: map[string]interface{}{"orderId": orderID, "customer": msg.Data["customer"]},
				})).
				Commit(ctx)
			return err
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	state, err := client.KV().Get(ctx, "orders", "order-1001")
	check(err == nil && state.Found && string(state.Value) == `{"status":"paid"}`, "the KV write committed")
	shipped, err := client.Queue("shipments").Wait(false).Pop(ctx)
	check(err == nil && len(shipped) == 1, "the shipment was pushed")
	check(shipped[0].Partition == "customer-42", "into the same customer partition")
	fmt.Println("ok: transaction")

	var replayed []interface{}
	// guide:start(replay)
	err = client.Queue("orders").
		Group("audit").                           // a new group, with its own cursor
		SubscriptionFrom("2026-10-22T00:00:00Z"). // everything pushed from this time on
		Each().
		Limit(1).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			fmt.Println("again:", msg.Data)
			replayed = append(replayed, msg.Data["orderId"]) // guide:hide
			return nil
		}).
		Execute(ctx)
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	check(len(replayed) == 1 && replayed[0] == float64(1001), "the audit group read the order again")
	fmt.Println("ok: replay")

	// guide:start(fetch)
	read, err := client.Queue("orders").Fetch(ctx, []string{"customer-42"}) // no group, lease or ack
	if err != nil {
		log.Fatal(err)
	}
	for _, record := range read["customer-42"].Records {
		fmt.Println(record.Offset, string(record.Payload))
	}
	// guide:end
	check(len(read["customer-42"].Records) >= 1, "fetch read the partition")
	fmt.Println("ok: fetch")

	// guide:start(lock)
	lock := client.Lock("daily-report", 30*time.Second) // a lease with a fencing token
	held, err := lock.TryAcquire(ctx)                   // false: another worker has it
	if err != nil {
		log.Fatal(err)
	}
	if held {
		_, err = client.Transaction().
			Guard(lock). // commits only while the lock is ours
			Queue("reports").
			Push(queen.PushItem{TransactionID: "report-2026-10-22", Payload: map[string]interface{}{"day": "2026-10-22"}}).
			Commit(ctx)
		_, _ = lock.Release(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
	// guide:end
	reports, err := client.Queue("reports").Wait(false).Pop(ctx)
	check(err == nil && len(reports) == 1, "the guarded report was pushed")
	fmt.Println("ok: lock")

	// guide:start(stream)
	revenue, err := streams.
		From(client.Queue("orders").AsStreamSource()). // one window per partition: per customer
		WindowTumbling(60).
		Aggregate(map[string]streams.ExtractorFn{
			"count": func(order interface{}) (float64, error) { return 1, nil },
			"sum": func(order interface{}) (float64, error) {
				return order.(map[string]interface{})["total"].(float64), nil
			},
		}, "count", "sum").
		To(client.Queue("revenue-per-minute")). // one result per customer per minute
		Run(ctx, streams.RunOptions{QueryID: "revenue-per-minute", URL: "http://localhost:6632"})
	if err != nil {
		log.Fatal(err)
	}
	// guide:end
	time.Sleep(time.Second) // let the stream's group register at the tail
	_, err = client.Queue("orders").Partition("customer-42").
		Push(map[string]interface{}{"orderId": 1002, "customer": 42, "total": 40.1}).
		TransactionID(fmt.Sprintf("order-1002-%d", time.Now().UnixNano())).Execute(ctx)
	check(err == nil, "second order pushed")
	var windows []*queen.Message
	for i := 0; i < 100 && len(windows) == 0; i++ {
		windows, _ = client.Queue("revenue-per-minute").Wait(true).TimeoutMillis(1000).Pop(ctx)
	}
	revenue.Stop()
	check(len(windows) == 1, "the window closed and its result was pushed")
	check(windows[0].Data["count"] == float64(1) && windows[0].Data["sum"] == 40.1, fmt.Sprint("window totals: ", windows[0].Data))
	fmt.Println("ok: stream", windows[0].Data)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// guide:start(worker)
		batch, err := client.Ephemeral().Pop(ctx, "price-requests",
			queen.EphemeralPopOptions{Group: "pricers", Wait: true})
		if err != nil {
			log.Fatal(err)
		}
		for _, m := range batch.Messages {
			var req struct{ Sku, ReplyTo string }
			_ = json.Unmarshal(m.Payload, &req)
			_, _ = client.Ephemeral().Push(ctx, req.ReplyTo,
				[]map[string]interface{}{{"sku": req.Sku, "price": 59.9}})
		}
		_, _ = client.Ephemeral().Ack(ctx, "price-requests", batch.Messages,
			queen.EphemeralAckOptions{Group: "pricers"})
		// guide:end
	}()
	time.Sleep(300 * time.Millisecond) // the worker is already waiting when a caller asks

	// guide:start(caller)
	inbox := "inbox-" + queen.GenerateUUID() // a private reply queue
	_, err = client.Ephemeral().Push(ctx, "price-requests",
		[]map[string]interface{}{{"sku": "A-1", "replyTo": inbox}})
	if err != nil {
		log.Fatal(err)
	}
	reply, err := client.Ephemeral().Pop(ctx, inbox,
		queen.EphemeralPopOptions{Wait: true, TimeoutMillis: 5000, CommitOnDelivery: true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(reply.Messages[0].Payload)) // {"price":59.9,"sku":"A-1"}
	// guide:end
	<-done
	var got map[string]interface{}
	_ = json.Unmarshal(reply.Messages[0].Payload, &got)
	check(got["price"] == 59.9, "the reply came back")
	fmt.Println("ok: ephemeral")

	client.Close(ctx)
	admin.Close(ctx)
	fmt.Println("\nPASS")
}
