// Timer integration tests (PLAN_KV_TIMERS.md §4, §8.1, §15).
//
// These need a broker, and the fire test also needs its sweeper running (it is,
// unless QUEEN_SWEEPER says otherwise). They used to probe the surface and SKIP
// on a 404, because QUEEN_TIMERS_ENABLED was false by default and left the
// routes unregistered. The flag is gone (Alice, 2026-08-18): timers are part of
// the engine, so these tests run against any broker.
//
// Queues and timer keys are all under the prefixes cleanupTestData purges: a
// pending timer that outlived its test would fire into a queue that no longer
// belongs to anybody.

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

func timerKey(prefix string) string {
	return fmt.Sprintf("test-go-timer-%s-%d", prefix, time.Now().UnixNano())
}

func TestTimerSchedulePeekListCancel(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	timers := client.Timers()
	queueName := generateQueueName("timers")
	key := timerKey("peek")

	sched, err := timers.Schedule(ctx, queen.TimerSchedule{
		Queue:    queueName,
		TimerKey: key,
		Delay:    5 * time.Minute,
		Payload:  map[string]interface{}{"orderId": 7},
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if !sched.OK || sched.Status != queen.TimerStatusScheduled {
		t.Fatalf("schedule = %+v (is cleanupTestData purging queen.log_timers?)", sched)
	}
	// The message id is promised AT SCHEDULE: a client that knows it can
	// correlate the delivered frame without a second API.
	if sched.MessageID == "" || sched.TransactionID == "" {
		t.Fatalf("schedule did not promise an identity: %+v", sched)
	}
	if sched.DeliverAt.Before(time.Now().Add(4 * time.Minute)) {
		t.Errorf("deliverAt = %s, want ~5 minutes out", sched.DeliverAt)
	}

	info, err := timers.Peek(ctx, queueName, key)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if !info.Found {
		t.Fatal("peek did not find a timer that was just scheduled")
	}
	if info.TransactionID != sched.TransactionID || info.MessageID != sched.MessageID {
		t.Errorf("peek identity %s/%s != schedule %s/%s", info.TransactionID, info.MessageID, sched.TransactionID, sched.MessageID)
	}
	var payload struct {
		OrderID int `json:"orderId"`
	}
	if err := json.Unmarshal(info.Payload, &payload); err != nil || payload.OrderID != 7 {
		t.Fatalf("peek payload = %s (err %v)", string(info.Payload), err)
	}
	// A pending timer that nobody has claimed reads claimed:false.
	if info.Claimed {
		t.Error("a timer five minutes out should not be claimed")
	}

	page, err := timers.List(ctx, queueName)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page.Rows) != 1 || page.Rows[0].TimerKey != key {
		t.Fatalf("list = %+v", page)
	}
	// The list never carries payloads: that is what peek is for.
	if page.Rows[0].Payload != nil {
		t.Error("list carried a payload")
	}

	cancelled, err := timers.Cancel(ctx, queueName, key)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancelled.OK || cancelled.Status != queen.TimerStatusCancelled {
		t.Fatalf("cancel = %+v", cancelled)
	}

	// §4.4, the contract where a user gets hurt: there is no tombstone. A second
	// cancel answers `absent` with ok:FALSE, and absent does NOT mean "never
	// fired" -- it means "no longer pending".
	absent, err := timers.Cancel(ctx, queueName, key, queen.TimerCancelOptions{ExpectedTransactionID: sched.TransactionID})
	if err != nil {
		t.Fatalf("second cancel: %v", err)
	}
	if absent.OK {
		t.Fatal("absent must carry ok:false")
	}
	if absent.Status != queen.TimerStatusAbsent {
		t.Errorf("status = %q, want %q", absent.Status, queen.TimerStatusAbsent)
	}
	// The txn to look for in the destination queue comes back, so the "was it
	// already delivered?" check needs no second API.
	if absent.TransactionID != sched.TransactionID {
		t.Errorf("absent echoed txn %q, want %q", absent.TransactionID, sched.TransactionID)
	}
}

func TestTimerRescheduleIsTheSameUpsert(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	timers := client.Timers()
	queueName := generateQueueName("timers")
	key := timerKey("resched")

	first, err := timers.Schedule(ctx, queen.TimerSchedule{
		Queue: queueName, TimerKey: key, Delay: 5 * time.Minute, Payload: map[string]interface{}{"v": 1},
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}

	second, err := timers.Reschedule(ctx, queen.TimerSchedule{
		Queue: queueName, TimerKey: key, Delay: 10 * time.Minute, Payload: map[string]interface{}{"v": 2},
	})
	if err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	if second.Status != queen.TimerStatusRescheduled {
		t.Fatalf("status = %q, want %q", second.Status, queen.TimerStatusRescheduled)
	}
	// §20.2, ratified: every reschedule mints a NEW txn. A rescheduled timer is
	// a new message, so "this timer, rescheduled, delivered this message" is
	// answerable without ambiguity -- and a replaced payload never shares an
	// identifier with the one it replaced.
	if second.TransactionID == first.TransactionID {
		t.Error("a reschedule must mint a new txn")
	}

	info, err := timers.Peek(ctx, queueName, key)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	var payload struct {
		V int `json:"v"`
	}
	if err := json.Unmarshal(info.Payload, &payload); err != nil || payload.V != 2 {
		t.Fatalf("payload = %s, want the rescheduled one", string(info.Payload))
	}
	// A rescheduled timer is a new timer under an old name: the attempt budget
	// goes back to zero.
	if info.Attempts != 0 {
		t.Errorf("attempts = %d, want 0", info.Attempts)
	}

	if _, err := timers.Cancel(ctx, queueName, key); err != nil {
		t.Fatalf("cleanup cancel: %v", err)
	}
}

func TestTimerFiresIntoItsQueue(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	timers := client.Timers()
	queueName := generateQueueName("timerfire")
	key := timerKey("fire")

	if _, err := client.Queue(queueName).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	sched, err := timers.Schedule(ctx, queen.TimerSchedule{
		Queue:    queueName,
		TimerKey: key,
		Delay:    500 * time.Millisecond,
		Payload:  map[string]interface{}{"orderId": 7},
	})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}

	// deliverAt is "NO EARLIER THAN", never "exactly at": one hop plus one
	// sweeper cycle.
	//
	// THE WAIT IS 45 SECONDS AND THAT IS NOT PADDING. On an otherwise idle
	// broker the sweeper backs off to QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS (30 s by
	// default) because the timer table was empty when it last looked, and the
	// in-process wake that would cut that short is still a declared seam (§7.4:
	// the hint goes in after the commit, and nothing breaks without it because
	// deliverAt is "no earlier than"). So on a busy cell this test finishes in
	// under a second, and on a cold one it can take half a minute. Measured
	// here: 24 s from deliverAt to the frame's createdAt, on a broker that had
	// been idle. Shortening this wait would make the test flaky, not faster.
	msgs, err := waitForMessages(ctx, client, queueName, 1, 45*time.Second)
	if err != nil {
		t.Fatalf("the timer never fired: %v", err)
	}
	// The txn promised at schedule time is the txn of the delivered frame: that
	// is what makes "was it sent?" answerable by looking in the queue.
	if msgs[0].TransactionID != sched.TransactionID {
		t.Errorf("delivered txn %q, want the promised %q", msgs[0].TransactionID, sched.TransactionID)
	}
	if got := msgs[0].Data["orderId"]; fmt.Sprint(got) != "7" {
		t.Errorf("payload = %v", msgs[0].Data)
	}

	// And it is gone from the pending set: the row is DELETEd on fire, there is
	// no tombstone.
	info, err := timers.Peek(ctx, queueName, key)
	if err != nil {
		t.Fatalf("peek after fire: %v", err)
	}
	if info.Found {
		t.Error("a fired timer is still pending")
	}
}

// TestTimerInTransaction is the saga shape: schedule the compensation timer in
// the same commit that starts the work, cancel it in the same commit that acks
// the completion.
func TestTimerInTransaction(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	timers := client.Timers()
	workQueue := generateQueueName("sagawork")
	timerQueue := generateQueueName("sagacomp")
	key := timerKey("saga")

	if _, err := client.Queue(workQueue).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	open, err := client.Transaction().
		Queue(workQueue).Push(map[string]interface{}{"saga": 1}).
		Timers(queen.ScheduleTimerOp(queen.TimerSchedule{
			Queue: timerQueue, TimerKey: key, Delay: 5 * time.Minute,
			Payload: map[string]interface{}{"compensate": 1},
		})).
		Commit(ctx)
	if err != nil {
		t.Fatalf("open commit: %v", err)
	}
	if !open.Success {
		t.Fatalf("open commit failed: %+v", open)
	}
	if len(open.Timers) != 1 || open.Timers[0].Status != queen.TimerStatusScheduled {
		t.Fatalf("timer rider results = %+v", open.Timers)
	}

	info, err := timers.Peek(ctx, timerQueue, key)
	if err != nil || !info.Found {
		t.Fatalf("the timer did not commit with the bundle: %+v err=%v", info, err)
	}

	// Close the saga: the cancel travels in the transaction that acks the work.
	msgs, err := client.Queue(workQueue).Batch(1).Pop(ctx)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("pop: %v (%d messages)", err, len(msgs))
	}
	closed, err := client.Transaction().
		Ack(msgs[0], queen.AckStatusCompleted, queen.AckOptions{}).
		Timers(queen.CancelTimerOp(timerQueue, key)).
		Commit(ctx)
	if err != nil {
		t.Fatalf("close commit: %v", err)
	}
	if !closed.Success {
		t.Fatalf("close commit failed: %+v", closed)
	}
	if len(closed.Timers) != 1 || closed.Timers[0].Status != queen.TimerStatusCancelled {
		t.Fatalf("cancel rider results = %+v", closed.Timers)
	}

	gone, err := timers.Peek(ctx, timerQueue, key)
	if err != nil {
		t.Fatalf("peek after cancel: %v", err)
	}
	if gone.Found {
		t.Error("the compensation timer is still pending after the close bundle")
	}
}
