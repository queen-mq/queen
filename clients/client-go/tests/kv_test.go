// KV integration tests (PLAN_KV_TIMERS.md §5, §8.1, §15).
//
// These need a broker, and nothing else. They used to probe the surface first
// and SKIP when it answered 404, because QUEEN_KV_ENABLED was false by default
// and a cell without the flag did not register the routes at all. The flag is
// gone (Alice, 2026-08-18): KV is part of the engine, every broker that answers
// has it, and so these tests now simply run. A test that skips says nothing, and
// that was tolerable only while the 404 was a legitimate configuration.
//
// Every key written here lives under a namespace this package's cleanupTestData
// purges. That purge is not cosmetic (§10.4): without it a putIfAbsent test is
// green on the first run and red forever after, and an incr accumulates between
// runs.
//
// And no test here uses Forever: an example that fails would leave immortal
// state in a shared test database.

package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go"
)

// kvNamespace mints a namespace matching the server's charset
// (^[a-z0-9][a-z0-9._-]{0,63}$) under the prefix cleanupTestData purges.
func kvNamespace(prefix string) string {
	return fmt.Sprintf("test-go-kv-%s-%d", prefix, time.Now().UnixNano())
}

func TestKVPutGetDelete(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("crud")

	write, err := kv.Put(ctx, ns, "order/9f1/items", map[string]interface{}{"count": 2}, queen.TTLSeconds(120))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	// The version comes from a GLOBAL sequence, not a per-key counter: a key
	// that expired, was pruned and was recreated must not be able to re-issue a
	// version an old holder still carries.
	//
	// What is guaranteed is UNIQUENESS, not order. The sequence is declared
	// CACHE 1000, so each backend connection hands out its own block and two
	// writes of the same key through different pooled connections can come back
	// 2022 then 21 (observed here). A client that compared versions with `<`
	// would be wrong on any pooled broker, which is every broker.
	if !write.Applied || write.Version <= 0 {
		t.Fatalf("put = %+v, want applied with a version", write)
	}
	rewrite, err := kv.Put(ctx, ns, "order/9f1/items", map[string]interface{}{"count": 3}, queen.TTLSeconds(120))
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if rewrite.Version == write.Version {
		t.Fatalf("a rewrite must mint a new version, got %d twice", write.Version)
	}
	if _, err := kv.Put(ctx, ns, "order/9f1/items", map[string]interface{}{"count": 2}, queen.TTLSeconds(120)); err != nil {
		t.Fatalf("put back: %v", err)
	}

	entry, err := kv.Get(ctx, ns, "order/9f1/items")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !entry.Found {
		t.Fatalf("get = %+v", entry)
	}
	// The value comes back as it is stored, i.e. canonical JSONB text: compare
	// the decoded datum, never the bytes.
	var stored struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(entry.Value, &stored); err != nil || stored.Count != 2 {
		t.Fatalf("value = %s (err %v)", string(entry.Value), err)
	}
	if entry.ExpiresAt.IsZero() {
		t.Error("a key written with a TTL must come back with an expiresAt")
	}

	// A key with slashes goes through the catch-all route unchanged: this is
	// the whole reason the route is a catch-all.
	type items struct {
		Count int `json:"count"`
	}
	decoded, found, err := queen.KVGetAs[items](ctx, kv, ns, "order/9f1/items")
	if err != nil || !found || decoded.Count != 2 {
		t.Fatalf("KVGetAs = %+v found=%v err=%v", decoded, found, err)
	}

	del, err := kv.Delete(ctx, ns, "order/9f1/items")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !del.Applied {
		t.Fatalf("delete = %+v", del)
	}

	gone, err := kv.Get(ctx, ns, "order/9f1/items")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if gone.Found {
		t.Fatal("the key is still there after a delete")
	}

	// A delete that hit nothing is a verdict, not an error.
	again, err := kv.Delete(ctx, ns, "order/9f1/items")
	if err != nil {
		t.Fatalf("second delete: %v", err)
	}
	if again.Applied || again.Reason != queen.KVReasonAbsent {
		t.Fatalf("second delete = %+v, want applied:false reason:absent", again)
	}
}

func TestKVPutIfAbsentIsWonExactlyOnce(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("pia")

	first, err := kv.PutIfAbsent(ctx, ns, "saga-7", map[string]interface{}{"owner": "worker-1"}, queen.TTLSeconds(120))
	if err != nil {
		t.Fatalf("first putIfAbsent: %v", err)
	}
	if !first.Applied {
		t.Fatalf("the first putIfAbsent must win: %+v (is cleanupTestData purging queen.kv?)", first)
	}

	second, err := kv.PutIfAbsent(ctx, ns, "saga-7", map[string]interface{}{"owner": "worker-2"}, queen.TTLSeconds(120))
	if err != nil {
		t.Fatalf("second putIfAbsent: %v", err)
	}
	if second.Applied {
		t.Fatal("the second putIfAbsent must lose")
	}
	if second.Reason != queen.KVReasonExists {
		t.Errorf("reason = %q, want %q", second.Reason, queen.KVReasonExists)
	}
	// The loser gets the WINNER's value in the same answer: that is the entire
	// point of the idempotency marker (§5.3).
	var owner struct {
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(second.Value, &owner); err != nil {
		t.Fatalf("the loser did not carry a value: %v", err)
	}
	if owner.Owner != "worker-1" {
		t.Errorf("the loser saw owner %q, want the winner's worker-1", owner.Owner)
	}
}

func TestKVExpectNeverCreatesTheKeyItWasFencing(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("expect")

	// The repaired bug of §5.3: expect:N>0 on an ABSENT key must be a pure
	// UPDATE and create nothing. In the naive form it falls into the INSERT arm
	// and creates the row -- in a saga, that fires the compensating command the
	// expect existed to prevent.
	missed, err := kv.Put(ctx, ns, "lock", map[string]interface{}{"by": "me"}, queen.TTLSeconds(120), queen.KVWriteOptions{Expect: queen.Expect(4)})
	if err != nil {
		t.Fatalf("put expect on an absent key: %v", err)
	}
	if missed.Applied {
		t.Fatal("expect:N>0 applied on a key that did not exist")
	}
	if missed.Reason != queen.KVReasonAbsent {
		t.Errorf("reason = %q, want %q", missed.Reason, queen.KVReasonAbsent)
	}
	probe, err := kv.Get(ctx, ns, "lock")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if probe.Found {
		t.Fatal("a failed expect CREATED the key")
	}

	// Now the ordinary fencing cycle.
	created, err := kv.Put(ctx, ns, "lock", map[string]interface{}{"by": "me"}, queen.TTLSeconds(120))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	stale, err := kv.Put(ctx, ns, "lock", map[string]interface{}{"by": "other"}, queen.TTLSeconds(120), queen.KVWriteOptions{Expect: queen.Expect(created.Version + 7)})
	if err != nil {
		t.Fatalf("put stale expect: %v", err)
	}
	if stale.Applied || stale.Reason != queen.KVReasonVersion {
		t.Fatalf("stale expect = %+v, want applied:false reason:version", stale)
	}
	fresh, err := kv.Put(ctx, ns, "lock", map[string]interface{}{"by": "me-again"}, queen.TTLSeconds(120), queen.KVWriteOptions{Expect: queen.Expect(created.Version)})
	if err != nil {
		t.Fatalf("put fresh expect: %v", err)
	}
	if !fresh.Applied {
		t.Fatalf("a matching expect must apply: %+v", fresh)
	}
}

func TestKVIncrIsARateLimiterInOneCall(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("incr")

	// §5.4 repair 2: the FIRST call of a window goes through the INSERT arm, and
	// it must be guarded too. A delta above the ceiling is refused before any
	// write -- otherwise the quota is breached on the very first request, and
	// again at every window rotation.
	over, err := kv.Incr(ctx, ns, "acme", 10, queen.TTLSeconds(60), queen.KVIncrOptions{Max: queen.Int64(3)})
	if err != nil {
		t.Fatalf("incr over the ceiling: %v", err)
	}
	if over.Applied {
		t.Fatalf("delta 10 with max 3 applied on the first call: %+v", over)
	}

	// Three admissions, then refusals, with the counter frozen at the ceiling.
	for i := 1; i <= 3; i++ {
		res, err := kv.Incr(ctx, ns, "acme", 1, queen.TTLSeconds(60), queen.KVIncrOptions{Max: queen.Int64(3)})
		if err != nil {
			t.Fatalf("incr %d: %v", i, err)
		}
		if !res.Applied || res.Value != int64(i) {
			t.Fatalf("incr %d = %+v (is cleanupTestData purging queen.kv?)", i, res)
		}
	}
	refused, err := kv.Incr(ctx, ns, "acme", 1, queen.TTLSeconds(60), queen.KVIncrOptions{Max: queen.Int64(3)})
	if err != nil {
		t.Fatalf("incr over quota: %v", err)
	}
	// With Max, `Applied` IS the admission decision: the refused request has
	// consumed no budget.
	if refused.Applied || refused.Reason != queen.KVReasonLimit || refused.Value != 3 {
		t.Fatalf("refused = %+v, want applied:false reason:limit value:3", refused)
	}
}

func TestKVReadsMultipleKeysAndPages(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("reads")

	for _, k := range []string{"quota:acme:1", "quota:acme:2", "quota:acme:3"} {
		if _, err := kv.Put(ctx, ns, k, map[string]interface{}{"k": k}, queen.TTLSeconds(120)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	many, err := kv.GetMany(ctx, ns, []string{"quota:acme:1", "quota:acme:9"})
	if err != nil {
		t.Fatalf("getMany: %v", err)
	}
	if len(many.Rows) != 1 || many.Rows[0].Key != "quota:acme:1" {
		t.Fatalf("rows = %+v", many.Rows)
	}
	// Absence is a datum, never a hole computed by difference.
	if len(many.Missing) != 1 || many.Missing[0] != "quota:acme:9" {
		t.Fatalf("missing = %v", many.Missing)
	}

	page, err := kv.GetPrefix(ctx, ns, "quota:acme:", queen.KVPrefixOptions{Limit: 2})
	if err != nil {
		t.Fatalf("getPrefix: %v", err)
	}
	if len(page.Rows) != 2 || !page.Truncated || page.NextAfter == "" {
		t.Fatalf("first page = %+v", page)
	}
	next, err := kv.GetPrefix(ctx, ns, "quota:acme:", queen.KVPrefixOptions{Limit: 2, After: page.NextAfter})
	if err != nil {
		t.Fatalf("getPrefix page 2: %v", err)
	}
	if len(next.Rows) != 1 || next.Truncated {
		t.Fatalf("second page = %+v", next)
	}
	if next.Rows[0].Key != "quota:acme:3" {
		t.Errorf("the cursor is not exclusive: %+v", next.Rows[0])
	}
}

func TestKVExpiredKeyIsNeverReturned(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("ttl")

	if _, err := kv.Put(ctx, ns, "short", map[string]interface{}{"a": 1}, queen.TTLSeconds(1)); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	// §5.7: an expired key is never returned and never counts as existing, even
	// though the sweeper has certainly not pruned it in 1.5 s. The truth is the
	// predicate, not the presence of the row.
	entry, err := kv.Get(ctx, ns, "short")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry.Found {
		t.Fatal("an expired key was returned")
	}
	// And it does not count as existing for a putIfAbsent either: the write
	// resurrects the lineage.
	resurrect, err := kv.PutIfAbsent(ctx, ns, "short", map[string]interface{}{"a": 2}, queen.TTLSeconds(60))
	if err != nil {
		t.Fatalf("putIfAbsent after expiry: %v", err)
	}
	if !resurrect.Applied {
		t.Fatalf("putIfAbsent lost against an EXPIRED row: %+v", resurrect)
	}
}

func TestKVBatchIsOneTransaction(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("batch")

	results, err := kv.Batch(ctx,
		queen.KVPutOp(ns, "a", 1, queen.TTLSeconds(60)),
		queen.KVPutOp(ns, "b", 2, queen.TTLSeconds(60)),
		queen.KVGetOp(ns, "a"),
	)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("want 3 results, got %d", len(results))
	}
	// Results come back in INPUT order even though the server applies them
	// ordered by (namespace, key).
	for i, r := range results {
		if r.Index != i {
			t.Errorf("result %d carries index %d", i, r.Index)
		}
	}
	// The get sees the put of the SAME call: one transaction, and the server
	// applies "a" before reading it.
	if !results[2].Entry().Found {
		t.Error("the read did not see the write of its own batch")
	}
}

func TestKVRequiredEscalatesOnTheStandaloneSurface(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	kv := client.KV()
	ns := kvNamespace("required")

	if _, err := kv.PutIfAbsent(ctx, ns, "s-1", map[string]interface{}{"owner": "worker-1"}, queen.TTLSeconds(120), queen.KVWriteOptions{Required: true}); err != nil {
		t.Fatalf("first required putIfAbsent: %v", err)
	}

	// The escalation the caller asked for: the whole call is rolled back and the
	// answer carries no results array at all, only the verdict.
	_, err := kv.PutIfAbsent(ctx, ns, "s-1", map[string]interface{}{"owner": "worker-2"}, queen.TTLSeconds(120), queen.KVWriteOptions{Required: true})
	var pe *queen.KVPreconditionError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want *queen.KVPreconditionError: %v", err, err)
	}
	if pe.Reason != queen.KVReasonExists || pe.FailedIndex != 0 {
		t.Errorf("precondition = %+v", pe)
	}
	var owner struct {
		Owner string `json:"owner"`
	}
	if err := json.Unmarshal(pe.Value, &owner); err != nil || owner.Owner != "worker-1" {
		t.Errorf("the error must carry the winner's value, got %s", string(pe.Value))
	}
}

// TestKVTransactionGate is the reason this feature exists: the write and the
// messages commit together, or neither does (§0, §6.3).
func TestKVTransactionGate(t *testing.T) {
	client := requireClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ns := kvNamespace("gate")
	queueName := generateQueueName("kvgate")

	if _, err := client.Queue(queueName).Create().Execute(ctx); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	commit := func() (*queen.TransactionResponse, error) {
		return client.Transaction().
			Queue(queueName).Push(map[string]interface{}{"order": 7}).
			KV(queen.KVPutIfAbsentOp(ns, "order-7", map[string]interface{}{"done": true}, queen.TTLSeconds(300), queen.KVWriteOptions{Required: true})).
			Commit(ctx)
	}

	first, err := commit()
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if !first.Success {
		t.Fatalf("first commit did not succeed: %+v", first)
	}

	// The redelivery. The marker is taken, so the bundle rolls back -- and this
	// is RETURNED, not raised: it is the expected outcome of every legitimate
	// redelivery, and putting it in the error path would put it in every
	// caller's retry policy.
	second, err := commit()
	if err != nil {
		t.Fatalf("a lost precondition must be returned, not raised: %v", err)
	}
	if second.Success {
		t.Fatal("the second bundle committed: the gate did not hold")
	}
	if !second.IsKVPrecondition() {
		t.Fatalf("reason = %q, want %q (resp %+v)", second.Reason, queen.ReasonKVPrecondition, second)
	}
	if second.KVReason != queen.KVReasonExists {
		t.Errorf("kvReason = %q, want %q", second.KVReason, queen.KVReasonExists)
	}
	// failedIndex is in the FLAT space: the push is 0, the kv rider is 1.
	if second.FailedIndex != 1 {
		t.Errorf("failedIndex = %d, want 1", second.FailedIndex)
	}

	// And the proof that the rollback was real: exactly ONE message.
	msgs, err := client.Queue(queueName).Batch(10).Pop(ctx)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want exactly 1: the refused bundle committed its push", len(msgs))
	}
}

// There used to be a surfaceSkip here, and a QUEEN_TEST_KVT escape hatch beside
// it to make the skip falsifiable on a lane that had turned the flags on. Both
// are gone with the flags themselves: there is no lane with the surface off any
// more, so there is nothing to skip for and nothing for the escape hatch to
// catch. A kv or timer route that refuses now is a failure, and this file says
// so by simply calling it.
