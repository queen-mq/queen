package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
)

// Locks, semaphores, `check` and guarded transactions against a live broker.
//
// Every lock here carries a per-run name and a lifetime, so a run that goes
// wrong leaves nothing that does not expire by itself, and a rerun against the
// same broker does not meet the first run's permits.
//
// What only a real broker can show: one holder; a call sent again by its owner
// is the same permit; an expired lock goes to the next handle with a higher
// token and the old handle's guarded step pushes nothing; a guarded step
// survives the lock's own renewal; a semaphore never grants more than its
// limit.

func lockName(base string) string {
	return fmt.Sprintf("test-go-%s-%d", base, time.Now().UnixNano())
}

// checksAreServed waits for the broker to serve the `check` op: a broker alone
// raises its cluster version to 5 on its first tick.
func checksAreServed(t *testing.T, client *queen.Queen) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, err := client.KV().Check(ctx, "test-go-probe", "k", 0)
		if err == nil {
			return
		}
		var se *queen.SurfaceError
		if !errors.As(err, &se) || se.StatusCode != 503 {
			t.Fatalf("check: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("the broker never served a check (cluster version below 5?)")
}

func drainLockQueue(t *testing.T, client *queen.Queen, queue string) []string {
	t.Helper()
	ctx := context.Background()
	var seen []string
	for {
		msgs, err := client.Queue(queue).Batch(50).Wait(false).Pop(ctx)
		if err != nil {
			t.Fatalf("pop: %v", err)
		}
		if len(msgs) == 0 {
			return seen
		}
		for _, m := range msgs {
			b, _ := json.Marshal(m.Data)
			seen = append(seen, string(b))
		}
		if _, err := client.Ack(ctx, msgs, true, queen.AckOptions{}); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
}

func TestLockHasOneHolderAndItsGuardedStepCommits(t *testing.T) {
	client := requireClient(t)
	checksAreServed(t, client)
	ctx := context.Background()
	name := lockName("one-holder")
	a := client.Lock(name, 30*time.Second)
	b := client.Lock(name, 30*time.Second)
	defer a.Release(ctx)
	defer b.Release(ctx)

	if ok, err := a.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("the first handle: %v %v", ok, err)
	}
	if ok, err := b.TryAcquire(ctx); err != nil || ok {
		t.Fatalf("a second handle acquired a held lock: %v %v", ok, err)
	}
	token := a.Token()

	who, err := client.Locks().Get(ctx, name)
	if err != nil || !who.Held || who.Holders[0].Owner != a.Owner() || who.Holders[0].Token != token {
		t.Fatalf("get: %+v %v", who, err)
	}
	if who.Holders[0].ExpiresAt.IsZero() {
		t.Fatal("a holder carries its expiry")
	}
	// The permit is a KV row and nothing else.
	row, err := client.KV().Get(ctx, queen.LockNamespace, name+"#0")
	if err != nil || !row.Found || row.Version != token {
		t.Fatalf("the permit's row: %+v %v", row, err)
	}

	queue := generateQueueName("lock-one-holder")
	resp, err := client.Transaction().Guard(a).Queue(queue).Push(map[string]int{"step": 1}).Commit(ctx)
	if err != nil || !resp.Success {
		t.Fatalf("the holder's guarded step: %+v %v", resp, err)
	}
	if got := drainLockQueue(t, client, queue); len(got) != 1 || got[0] != `{"step":1}` {
		t.Fatalf("the one guarded message: %v", got)
	}

	if ok, err := a.Release(ctx); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	if ok, err := b.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("free after its release: %v %v", ok, err)
	}
	if b.Token() <= token {
		t.Fatalf("a later holder has a higher token: %d after %d", b.Token(), token)
	}
}

func TestLockCallSentAgainByItsOwnerIsTheSamePermit(t *testing.T) {
	client := requireClient(t)
	ctx := context.Background()
	name := lockName("retry-owner")
	locks := client.Locks()
	ttl := 30 * time.Second

	first, err := locks.Send(ctx, queen.LockAcquireOp(name, ttl).WithOwner("me"))
	if err != nil || !first.Acquired {
		t.Fatalf("%+v %v", first, err)
	}
	again, err := locks.Send(ctx, queen.LockAcquireOp(name, ttl).WithOwner("me"))
	if err != nil || !again.Acquired || !again.Already || again.Token != first.Token {
		t.Fatalf("the retry answers the SAME permit: %+v %v", again, err)
	}
	renewed, err := locks.Send(ctx, queen.LockRenewOp(name, first.Token, ttl).WithOwner("me"))
	if err != nil || !renewed.Renewed {
		t.Fatalf("%+v %v", renewed, err)
	}
	// The renew's answer is lost; the old token is sent again.
	resent, err := locks.Send(ctx, queen.LockRenewOp(name, first.Token, ttl).WithOwner("me"))
	if err != nil || !resent.Renewed || resent.Token <= renewed.Token {
		t.Fatalf("carried through: %+v %v", resent, err)
	}
	stranger, err := locks.Send(ctx, queen.LockRenewOp(name, first.Token, ttl).WithOwner("somebody-else"))
	if err != nil || stranger.Renewed || stranger.Reason != queen.LockReasonLost || stranger.Holders[0].Owner != "me" {
		t.Fatalf("a stranger's stale token is lost: %+v %v", stranger, err)
	}
	if d, err := locks.Send(ctx, queen.LockReleaseOp(name, resent.Token)); err != nil || !d.Released {
		t.Fatalf("%+v %v", d, err)
	}
}

func TestExpiredLockIsTakenOverAndTheOldHolderCommitsNothing(t *testing.T) {
	client := requireClient(t)
	checksAreServed(t, client)
	ctx := context.Background()
	name := lockName("expiry")
	queue := generateQueueName("lock-expiry")
	// The old holder does not renew: it is "paused" for longer than its lease.
	old := client.Lock(name, time.Second, queen.LockOptions{ManualRenew: true})
	next := client.Lock(name, 30*time.Second)
	defer next.Release(ctx)

	if ok, err := old.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	oldToken := old.Token()
	staleGuard, err := old.Guard()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1300 * time.Millisecond)
	if old.Held() {
		t.Fatal("past its lifetime a handle does not claim to hold")
	}
	if ok, err := next.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("an expired lock is free: %v %v", ok, err)
	}
	if next.Token() <= oldToken {
		t.Fatalf("token %d is not above %d", next.Token(), oldToken)
	}

	// The old holder wakes up and sends the step it was about to send.
	stale, err := client.Transaction().KV(staleGuard).Queue(queue).Push(map[string]string{"from": "old"}).Commit(ctx)
	if err != nil || !stale.IsKVPrecondition() {
		t.Fatalf("the old holder's step must roll back: %+v %v", stale, err)
	}
	ok, err := client.Transaction().Guard(next).Queue(queue).Push(map[string]string{"from": "next"}).Commit(ctx)
	if err != nil || !ok.Success {
		t.Fatalf("%+v %v", ok, err)
	}
	if got := drainLockQueue(t, client, queue); len(got) != 1 || got[0] != `{"from":"next"}` {
		t.Fatalf("only the new holder's message exists: %v", got)
	}
}

func TestGuardedStepSurvivesTheLocksOwnRenewal(t *testing.T) {
	client := requireClient(t)
	checksAreServed(t, client)
	ctx := context.Background()
	name := lockName("renewal")
	queue := generateQueueName("lock-renewal")
	lock := client.Lock(name, 2*time.Second, queen.LockOptions{RenewEvery: 100 * time.Millisecond})
	defer lock.Release(ctx)
	if ok, err := lock.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	first := lock.Token()
	committed := 0
	for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); {
		resp, err := client.Transaction().Guard(lock).Queue(queue).Push(map[string]int{"n": committed}).Commit(ctx)
		if err != nil || !resp.Success {
			t.Fatalf("step %d while the lock was held: %+v %v", committed, resp, err)
		}
		committed++
	}
	if lock.Token() <= first {
		t.Fatal("the lock never renewed during the run; the test proved nothing")
	}
	if !lock.Held() {
		t.Fatal("the lock was lost under its own renewal")
	}
	if got := drainLockQueue(t, client, queue); len(got) != committed {
		t.Fatalf("%d steps committed, %d messages exist", committed, len(got))
	}
}

func TestSemaphoreNeverGrantsMoreThanItsLimit(t *testing.T) {
	client := requireClient(t)
	ctx := context.Background()
	name := lockName("crowd")
	const limit = 3
	permits := make([]*queen.Lock, 12)
	for i := range permits {
		permits[i] = client.Semaphore(name, limit, 30*time.Second)
	}
	defer func() {
		for _, p := range permits {
			p.Release(ctx)
		}
	}()

	var wg sync.WaitGroup
	got := make([]bool, len(permits))
	for i, p := range permits {
		wg.Add(1)
		go func(i int, p *queen.Lock) {
			defer wg.Done()
			ok, err := p.TryAcquire(ctx)
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
			got[i] = ok
		}(i, p)
	}
	wg.Wait()
	var holders []*queen.Lock
	for i, p := range permits {
		if got[i] {
			holders = append(holders, p)
		}
	}
	if len(holders) < 1 || len(holders) > limit {
		t.Fatalf("%d permits granted of %d", len(holders), limit)
	}
	// A crowd can leave a permit free; one at a time, the semaphore fills.
	for _, p := range permits {
		if len(holders) == limit {
			break
		}
		if !p.Held() {
			if ok, _ := p.TryAcquire(ctx); ok {
				holders = append(holders, p)
			}
		}
	}
	if len(holders) != limit {
		t.Fatalf("the semaphore did not fill: %d of %d", len(holders), limit)
	}
	slots := make([]int, 0, limit)
	for _, p := range holders {
		slots = append(slots, p.Slot())
	}
	sort.Ints(slots)
	if fmt.Sprint(slots) != "[0 1 2]" {
		t.Fatalf("one holder per slot: %v", slots)
	}

	extra := client.Semaphore(name, limit, 30*time.Second)
	defer extra.Release(ctx)
	if ok, _ := extra.TryAcquire(ctx); ok {
		t.Fatal("a permit past the limit was granted")
	}
	if who, err := client.Locks().Get(ctx, name); err != nil || len(who.Holders) != limit {
		t.Fatalf("get: %+v %v", who, err)
	}

	// One leaves; the waiter gets exactly that slot.
	freed := holders[0].Slot()
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- extra.Acquire(waitCtx) }()
	time.Sleep(200 * time.Millisecond)
	if _, err := holders[0].Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("a waiter did not get the freed permit: %v", err)
	}
	if extra.Slot() != freed {
		t.Fatalf("the waiter got slot %d, not the freed %d", extra.Slot(), freed)
	}
}
