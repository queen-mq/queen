// docs:start(app-go-saga)
//
// A booking saga: a room is held, paid for, and released by a timer when the
// payment never comes.
//
// The release is the part that usually breaks. Kept as a time.AfterFunc in a
// worker, it dies with the worker, and a deploy in the middle of a hold leaves
// the room held forever. Here the release is a timer in the broker, scheduled
// in the same transaction that holds the room: the saga's state, the timer,
// the payment request and the ack of the booking commit as one log entry. If
// the room is held, its release exists; if anything failed, none of it
// happened.
//
//	bookings
//	  `-- group "reserver"     ONE transaction: state + timer + push + ack
//	        |-- payments (one partition per booking)
//	        |     `-- group "payer"        confirm + cancel the timer + ack
//	        `-- expiries (the timer delivers here when the hold runs out)
//	              `-- group "compensator"  reads the state before it releases
//
// Run it:
//
//	QUEEN_URL=http://localhost:6632 GOWORK=off go run ./saga
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// Fresh queues and a fresh KV namespace per run. Saga entries outlive the queues
// (they expire with their TTL), so a second run in the same namespace would
// find every booking already held.
var runID = strconv.FormatInt(time.Now().UnixMilli(), 36)

var (
	bookingsQueue = "app-go-saga-bookings-" + runID
	paymentsQueue = "app-go-saga-payments-" + runID
	expiriesQueue = "app-go-saga-expiries-" + runID
	ns            = "app-go-saga-" + runID
)

const (
	reserverGroup    = "reserver"
	payerGroup       = "payer"
	compensatorGroup = "compensator"

	// How long a room stays held before it is released. In production this is
	// minutes; it is the only number that changes. It has to outlast the
	// reserving and paying phases below, or a release would fire before the
	// payment that cancels it and the run would measure a race.
	hold = 10 * time.Second

	// Each phase ends on a count of messages, with this deadline behind it so a
	// stall fails the run instead of hanging it.
	phaseMillis = 20000

	// Timers fire on the leader's next tick after they are due (every 50 ms),
	// so the compensation phase only needs the hold plus a margin.
	timerDeadlineMillis = int((hold + 20*time.Second) / time.Millisecond)

	// B-3's card is declined, so its saga never reaches "confirmed" and the
	// timer is what gives the room back.
	declined = "B-3"

	// B-4 pays, but its cancel is skipped on purpose. That is the cancel that
	// comes too late, made reproducible: the release is delivered for a booking
	// that is already confirmed, and the compensator has to refuse it.
	cancelSkipped = "B-4"
)

// Four bookings, five submissions: B-2 is submitted twice, which is what a
// redelivery looks like from the reserver's side.
type booking struct {
	BookingID string `json:"bookingId"`
	Room      string `json:"room"`
	Cents     int    `json:"cents"`
}

var bookingsIn = []booking{
	{BookingID: "B-1", Room: "101", Cents: 24000},
	{BookingID: "B-2", Room: "102", Cents: 31000},
	{BookingID: "B-2", Room: "102", Cents: 31000},
	{BookingID: "B-3", Room: "103", Cents: 18000},
	{BookingID: "B-4", Room: "104", Cents: 27000},
}

var bookingIDs = []string{"B-1", "B-2", "B-3", "B-4"}

// sagaState is what the saga's KV entry holds. It is a struct because the value
// comes back as raw JSON and this program reads a field of it on every hop:
// with a map, a typo in a key would read as "the saga is not held" and the run
// would pass for the wrong reason.
type sagaState struct {
	Step  string `json:"step"`
	Room  string `json:"room"`
	Cents int    `json:"cents"`
}

// The saga's state key derives from the booking id, which is also the
// partition key of the payments queue. That is what makes the payer's
// read-then-write safe below.
func sagaKey(bookingID string) string { return "saga:" + bookingID }

var checks int

// assert is the whole test framework here. Go has no exceptions, so a failed
// check is an error that unwinds run() and is printed once, at the bottom.
func assert(condition bool, description string) error {
	if !condition {
		return fmt.Errorf("%s", description)
	}
	checks++
	fmt.Printf("  ok: %s\n", description)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nFAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nPASS: %d checks\n", checks)
}

func run() error {
	brokerURL := os.Getenv("QUEEN_URL")
	if brokerURL == "" {
		brokerURL = "http://localhost:6632"
	}

	// Every call in the Go client takes a context, and it is the only deadline
	// there is. This one bounds the whole program, so a broker that stops
	// answering fails the run when it expires.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	client, err := queen.New(brokerURL)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer client.Close(context.Background())

	// Clean up in every case, which is why this is deferred. Saga entries live
	// in KV, and a pending timer is stored by its queue and key: deleting the
	// queues removes neither, and a timer that fires into a deleted queue
	// creates the queue again. It is best effort, on a context of its own, so a
	// cleanup problem is reported without replacing the verdict.
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		for _, bookingID := range bookingIDs {
			if _, err := client.Timers().Cancel(cleanupCtx, expiriesQueue, bookingID); err != nil {
				fmt.Fprintf(os.Stderr, "  (cleanup incomplete: cancel %s: %v)\n", bookingID, err)
			}
			if _, err := client.KV().Delete(cleanupCtx, ns, sagaKey(bookingID)); err != nil {
				fmt.Fprintf(os.Stderr, "  (cleanup incomplete: delete %s: %v)\n", sagaKey(bookingID), err)
			}
		}
		for _, q := range []string{bookingsQueue, paymentsQueue, expiriesQueue} {
			if _, err := client.Queue(q).Delete().Execute(cleanupCtx); err != nil {
				fmt.Fprintf(os.Stderr, "  (cleanup incomplete: delete %s: %v)\n", q, err)
			}
		}
	}()

	// A consumer for one phase: acks ride the transactions, so AutoAck is off,
	// and the phase ends after limit messages or after idleMillis with none.
	// Concurrency stays at the default of one, so each handler below runs on a
	// single goroutine, its bookkeeping needs no lock, and Limit (which this
	// client counts per worker) is the count for the whole phase.
	phase := func(queue, group string, limit, idleMillis int) *queen.QueueBuilder {
		return client.Queue(queue).
			Group(group).
			SubscriptionMode(queen.SubscriptionModeAll).
			AutoAck(false).
			Each().
			Limit(limit).
			TimeoutMillis(1000).
			IdleMillis(idleMillis)
	}

	fmt.Printf("broker %s\n", brokerURL)

	for _, q := range []string{bookingsQueue, paymentsQueue, expiriesQueue} {
		if _, err := client.Queue(q).
			Config(queen.QueueConfig{LeaseTime: 30, RetryLimit: 3}).
			Create().Execute(ctx); err != nil {
			return fmt.Errorf("create %s: %w", q, err)
		}
	}

	fmt.Println("\nsubmitting bookings")
	for i, b := range bookingsIn {
		if _, err := client.Queue(bookingsQueue).
			Push(b).
			// One id per submission, so the duplicate of B-2 is stored and
			// reaches the reserver, where the gate has to catch it.
			TransactionID(fmt.Sprintf("submit-%d-%s", i, b.BookingID)).
			Execute(ctx); err != nil {
			return fmt.Errorf("submit %s: %w", b.BookingID, err)
		}
	}
	fmt.Printf("  %d submissions for %d bookings\n", len(bookingsIn), len(bookingIDs))

	// -------------------------------------------------------------- reserving
	//
	// The transaction is the whole point of the example: four things commit
	// together, so there is no order between them to get wrong. Written as four
	// calls, a crash between the timer and the push leaves a release for a
	// payment that was never asked for, and a crash the other way round leaves
	// a hold with no release.
	fmt.Println("\nreserving")
	var reserveDecisions []string
	gatesLost := 0

	err = phase(bookingsQueue, reserverGroup, len(bookingsIn), phaseMillis).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			bookingID, _ := msg.Data["bookingId"].(string)
			room, _ := msg.Data["room"].(string)
			cents, ok := msg.Data["cents"].(float64)
			if !ok {
				return fmt.Errorf("submission %s carries no numeric cents", msg.TransactionID)
			}

			res, err := client.Transaction().
				// 1. The gate and the first state, in one KV entry. Required
				//    makes the putIfAbsent a gate: if the booking is already
				//    held, the whole transaction rolls back and nothing below
				//    happens.
				KV(queen.KVPutIfAbsentOp(
					ns,
					sagaKey(bookingID),
					sagaState{Step: "held", Room: room, Cents: int(cents)},
					// Every KV write carries an expiry, and the zero value of
					// queen.Expiry is refused. queen.Forever() exists, but an
					// example that runs in CI must not be able to leave an
					// entry behind for good.
					queen.TTL(time.Hour),
					queen.KVWriteOptions{Required: true},
				)).
				// 2. The release. From this commit on it is a record in the
				//    broker's replicated state, independent of this process.
				//    The key is ours, which is what lets the payer cancel it by
				//    name.
				Timers(queen.ScheduleTimerOp(queen.TimerSchedule{
					Queue:    expiriesQueue,
					TimerKey: bookingID,
					Delay:    hold,
					Payload:  map[string]interface{}{"bookingId": bookingID, "room": room},
				})).
				// 3. The payment request, in the booking's own partition. It
				//    needs no transaction id of its own (the client mints one):
				//    it can only commit together with the saga entry, so the gate
				//    above is its idempotency key.
				Queue(paymentsQueue).
				Partition(bookingID).
				Push(map[string]interface{}{"bookingId": bookingID, "cents": int(cents)}).
				// 4. The ack, with this delivery's lease. If the lease ran out,
				//    the ack is refused and the other three are refused with it.
				Ack(msg, "completed", queen.AckOptions{ConsumerGroup: reserverGroup}).
				Commit(ctx)

			reserveDecisions = append(reserveDecisions, bookingID)

			// A lost gate comes back as a value (err is nil, Success is false,
			// Reason is "kv_precondition"), because a duplicate is a normal
			// outcome and not an error to retry. Every other failed commit is
			// an error. Nothing was written, so the message is acked on its
			// own.
			if res.IsKVPrecondition() {
				gatesLost++
				if _, err := client.Ack(ctx, msg, true, queen.AckOptions{ConsumerGroup: reserverGroup}); err != nil {
					return fmt.Errorf("ack the duplicate submission: %w", err)
				}
				fmt.Printf("  %s: already held, nothing written (%s)\n", bookingID, res.KVReason)
				return nil
			}
			if err != nil {
				return fmt.Errorf("reserve %s: %w", bookingID, err)
			}

			fmt.Printf("  %s: room %s held, release armed for %s\n", bookingID, room, hold)
			return nil
		}).
		Execute(ctx)
	if err != nil {
		return fmt.Errorf("reserving: %w", err)
	}

	if err := assert(
		len(reserveDecisions) == len(bookingsIn),
		fmt.Sprintf("the reserver decided every submission (%d, got %d)", len(bookingsIn), len(reserveDecisions)),
	); err != nil {
		return err
	}
	if err := assert(gatesLost == 1, "the duplicate submission of B-2 lost the gate, once"); err != nil {
		return err
	}

	// Pending timers can be listed, because each release is a record in the
	// broker.
	armed, err := client.Timers().List(ctx, expiriesQueue, queen.TimerListOptions{Limit: 50})
	if err != nil {
		return fmt.Errorf("list the armed releases: %w", err)
	}
	armedKeys := make([]string, 0, len(armed.Rows))
	for _, row := range armed.Rows {
		armedKeys = append(armedKeys, row.TimerKey)
	}
	sort.Strings(armedKeys)
	fmt.Printf("  timers armed: %s\n", strings.Join(armedKeys, ", "))
	if err := assert(
		len(armedKeys) == len(bookingIDs),
		fmt.Sprintf("one release per booking, none for the duplicate (%d, got %d)", len(bookingIDs), len(armedKeys)),
	); err != nil {
		return err
	}

	// ----------------------------------------------------------------- paying
	//
	// A settled payment confirms the saga and cancels the release in one
	// commit. A declined card leaves the state alone and lets the timer do its
	// work.
	fmt.Println("\npaying")
	var paymentsRequested []string

	err = phase(paymentsQueue, payerGroup, len(bookingIDs), phaseMillis).
		Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
			bookingID, _ := msg.Data["bookingId"].(string)
			paymentsRequested = append(paymentsRequested, bookingID)

			// A read now and a write in the transaction below. That is safe
			// here because the key derives from the partition key: every
			// message about this booking is in one partition, and a partition
			// is held by one worker of the group at a time.
			state, version, err := readSaga(ctx, client, bookingID)
			if err != nil {
				return err
			}

			if bookingID == declined {
				// A declined card is a business outcome, not a failed delivery:
				// the message is done. The room stays held, and nothing in this
				// process is responsible for giving it back.
				if _, err := client.Ack(ctx, msg, true, queen.AckOptions{ConsumerGroup: payerGroup}); err != nil {
					return fmt.Errorf("ack the declined payment: %w", err)
				}
				fmt.Printf("  %s: card declined, hold left to expire\n", bookingID)
				return nil
			}

			state.Step = "confirmed"
			tx := client.Transaction().
				// Expect makes the "one worker per booking" assumption
				// checkable: if it ever fails, two consumers were serving one
				// partition.
				KV(queen.KVPutOp(ns, sagaKey(bookingID), state, queen.TTL(time.Hour), queen.KVWriteOptions{
					Expect:   queen.Expect(version),
					Required: true,
				}))
			if bookingID != cancelSkipped {
				// The cancel rides the transaction: the booking is confirmed
				// and its release cancelled, or neither happens.
				tx = tx.Timers(queen.CancelTimerOp(expiriesQueue, bookingID))
			}

			res, err := tx.Ack(msg, "completed", queen.AckOptions{ConsumerGroup: payerGroup}).Commit(ctx)
			if res.IsKVPrecondition() {
				return fmt.Errorf("%s: confirmation lost its fence (%s)", bookingID, res.KVReason)
			}
			if err != nil {
				return fmt.Errorf("confirm %s: %w", bookingID, err)
			}

			tail := "release cancelled"
			if bookingID == cancelSkipped {
				tail = "release deliberately NOT cancelled"
			}
			fmt.Printf("  %s: paid and confirmed, %s\n", bookingID, tail)
			return nil
		}).
		Execute(ctx)
	if err != nil {
		return fmt.Errorf("paying: %w", err)
	}

	if err := assert(
		len(paymentsRequested) == len(bookingIDs),
		fmt.Sprintf("every booking was asked to pay once, B-2 included (%s)", orNone(strings.Join(paymentsRequested, ", "))),
	); err != nil {
		return err
	}
	if err := assert(distinct(paymentsRequested), "no booking was asked to pay twice"); err != nil {
		return err
	}

	// A cancelled timer is gone before it fires. Peek answers Found: false
	// with HTTP 200 for a timer that does not exist.
	peeked := map[string]bool{}
	for _, bookingID := range bookingIDs {
		info, err := client.Timers().Peek(ctx, expiriesQueue, bookingID)
		if err != nil {
			return fmt.Errorf("peek %s: %w", bookingID, err)
		}
		peeked[bookingID] = info.Found
	}
	if err := assert(!peeked["B-1"], "the release cancelled with the confirmation is gone"); err != nil {
		return err
	}
	if err := assert(peeked[declined], declined+" was never confirmed, so its release is still armed"); err != nil {
		return err
	}
	if err := assert(peeked[cancelSkipped], cancelSkipped+" is confirmed and its release is still armed, on purpose"); err != nil {
		return err
	}

	// ------------------------------------------------------------ compensating
	//
	// What the timers deliver, and the consumer that must not trust them. A
	// release message asks a question: is this booking still only held? A
	// fired timer leaves nothing behind, so a cancel that arrives a moment too
	// late answers "absent" and the release is delivered anyway. The saga's
	// state decides.
	//
	// This message arrives on another queue, in a partition unrelated to the
	// payments partition, so nothing serialises the compensator with the
	// payer. Here Expect is what stops a release computed from a stale read
	// from overwriting a confirmation that landed in between.
	fmt.Println("\ncompensating")
	var compensationsDelivered []string
	var roomsReleased []string
	var compensationsRefused []string

	compensate := func(limit, idleMillis int) error {
		return phase(expiriesQueue, compensatorGroup, limit, idleMillis).
			Consume(ctx, func(ctx context.Context, msg *queen.Message) error {
				bookingID, _ := msg.Data["bookingId"].(string)
				room, _ := msg.Data["room"].(string)
				compensationsDelivered = append(compensationsDelivered, bookingID)

				state, version, err := readSaga(ctx, client, bookingID)
				if err != nil {
					return err
				}

				if state.Step != "held" {
					compensationsRefused = append(compensationsRefused, bookingID)
					if _, err := client.Ack(ctx, msg, true, queen.AckOptions{ConsumerGroup: compensatorGroup}); err != nil {
						return fmt.Errorf("ack the refused release: %w", err)
					}
					step := state.Step
					if step == "" {
						step = "gone"
					}
					fmt.Printf("  %s: state is %s, release refused\n", bookingID, step)
					return nil
				}

				state.Step = "expired"
				res, err := client.Transaction().
					KV(queen.KVPutOp(ns, sagaKey(bookingID), state, queen.TTL(time.Hour), queen.KVWriteOptions{
						Expect:   queen.Expect(version),
						Required: true,
					})).
					Ack(msg, "completed", queen.AckOptions{ConsumerGroup: compensatorGroup}).
					Commit(ctx)
				if res.IsKVPrecondition() {
					// Confirmed between the read and the commit: nothing was
					// written.
					compensationsRefused = append(compensationsRefused, bookingID)
					if _, err := client.Ack(ctx, msg, true, queen.AckOptions{ConsumerGroup: compensatorGroup}); err != nil {
						return fmt.Errorf("ack the fenced release: %w", err)
					}
					fmt.Printf("  %s: confirmed in the meantime, release refused\n", bookingID)
					return nil
				}
				if err != nil {
					return fmt.Errorf("release %s: %w", bookingID, err)
				}

				roomsReleased = append(roomsReleased, room)
				fmt.Printf("  %s: hold expired, room %s released\n", bookingID, room)
				return nil
			}).
			Execute(ctx)
	}

	// Two releases were left armed, so two messages have to arrive.
	if err := compensate(2, timerDeadlineMillis); err != nil {
		return fmt.Errorf("compensating: %w", err)
	}
	if err := assert(
		len(compensationsDelivered) == 2,
		fmt.Sprintf("both armed releases were delivered (got %s)", orNone(strings.Join(compensationsDelivered, ", "))),
	); err != nil {
		return err
	}

	// A second, short pass with room for more: the only way to show that the
	// cancelled releases never arrive is to wait for them and see nothing.
	if err := compensate(2, 4000); err != nil {
		return fmt.Errorf("second compensation pass: %w", err)
	}

	// --------------------------------------------------------------- checking
	fmt.Println("\nchecking")

	if err := assert(
		len(compensationsDelivered) == 2,
		fmt.Sprintf("nothing arrived on the second pass, still 2 releases (got %d)", len(compensationsDelivered)),
	); err != nil {
		return err
	}
	if err := assert(
		!contains(compensationsDelivered, "B-1") && !contains(compensationsDelivered, "B-2"),
		"no cancelled release was ever delivered",
	); err != nil {
		return err
	}
	if err := assert(
		len(roomsReleased) == 1 && roomsReleased[0] == "103",
		fmt.Sprintf("exactly one room went back on sale, the declined one (got %s)",
			orNone(strings.Join(roomsReleased, ", "))),
	); err != nil {
		return err
	}
	if err := assert(
		len(compensationsRefused) == 1 && compensationsRefused[0] == cancelSkipped,
		"the late release for "+cancelSkipped+" was refused by the compensator",
	); err != nil {
		return err
	}

	keys := make([]string, 0, len(bookingIDs))
	for _, bookingID := range bookingIDs {
		keys = append(keys, sagaKey(bookingID))
	}
	states, err := client.KV().GetMany(ctx, ns, keys)
	if err != nil {
		return fmt.Errorf("read the saga entries: %w", err)
	}
	if err := assert(
		len(states.Rows) == len(bookingIDs) && len(states.Missing) == 0,
		fmt.Sprintf("every booking has exactly one saga entry (%d)", len(states.Rows)),
	); err != nil {
		return err
	}

	step := map[string]string{}
	for _, row := range states.Rows {
		var st sagaState
		if err := json.Unmarshal(row.Value, &st); err != nil {
			return fmt.Errorf("decode saga entry %s: %w", row.Key, err)
		}
		step[strings.TrimPrefix(row.Key, "saga:")] = st.Step
	}

	if err := assert(
		step["B-1"] == "confirmed" && step["B-2"] == "confirmed",
		"B-1 and B-2 ended confirmed",
	); err != nil {
		return err
	}
	if err := assert(
		step[cancelSkipped] == "confirmed",
		cancelSkipped+" ended confirmed, although its release was delivered",
	); err != nil {
		return err
	}
	if err := assert(
		step[declined] == "expired",
		declined+" was released by its timer, with no process waiting for it",
	); err != nil {
		return err
	}

	final := make([]string, 0, len(step))
	for k, v := range step {
		final = append(final, k+"="+v)
	}
	sort.Strings(final)
	fmt.Printf("\n  final: %s\n", strings.Join(final, ", "))

	return nil
}

// readSaga reads one saga entry and its version. The version is what a later
// write passes back as Expect, so the two always travel together.
//
// An entry past its expiry reads as absent, and an absent entry comes back as
// the zero sagaState, whose Step is the empty string and so never "held".
func readSaga(ctx context.Context, client *queen.Queen, bookingID string) (sagaState, int64, error) {
	var state sagaState
	entry, err := client.KV().Get(ctx, ns, sagaKey(bookingID))
	if err != nil {
		return state, 0, fmt.Errorf("read the saga state of %s: %w", bookingID, err)
	}
	if !entry.Found {
		return state, 0, nil
	}
	if err := json.Unmarshal(entry.Value, &state); err != nil {
		return state, 0, fmt.Errorf("decode the saga state of %s: %w", bookingID, err)
	}
	return state, entry.Version, nil
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func distinct(values []string) bool {
	seen := map[string]bool{}
	for _, v := range values {
		if seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// docs:end
