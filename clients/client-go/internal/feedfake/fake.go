// Package feedfake is a broker in memory, over HTTP, that speaks the wires a
// feed stream uses -- POST /api/v1/feed, /api/v1/kv, /api/v1/transaction,
// /api/v1/locks, /api/v1/ephemeral/* -- with the rules of the JS SDK's test
// broker (test-v2/feed-unit/fakeBroker.js), so the Go tests run the client's
// own KV, Lock, TransactionBuilder, Ephemeral and FeedReader against it.
//
// Every write is one log position, as on the broker: a transaction (its
// pushes share the position), a lock call. Only pushes are feed entries. The
// clock is virtual: NowMs is the wall clock, which the test moves; a write is
// stamped max(NowMs, the last write's time), and a caught-up read resolves to
// the last write's time, so the clock moves only with writes. Locks expire on
// the real clock.
//
// Each process of a test has its own client (Client(name)): its requests come
// on /c/<name>/..., so Cut(name) can make that one unreachable.
package feedfake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// T0 is the fake's default start: 2026-11-06T09:00:00.000Z.
var T0 = mustParse("2026-11-06T09:00:00.000Z")

func mustParse(s string) float64 {
	t, _ := jsv.DateParse(s)
	return t
}

// Entry is one log entry (a pushed message).
type Entry struct {
	Position      int64
	Queue         string
	Partition     string
	PartitionID   string
	Offset        int64
	ID            string
	TransactionID string
	Payload       interface{}
	TS            string
}

// Txn is one applied transaction.
type Txn struct {
	Position int64
	Items    []map[string]interface{}
	KV       []map[string]interface{}
	Written  map[string]int64
}

// Inject is something for the next transaction: Before runs first (another
// process's commit, say); Status (an HTTP status) or Reset (the connection
// dropped) is answered instead, before the transaction applies (When "" or
// "before") or after it (When "after": applied, the answer lost).
type Inject struct {
	Before func(b *Broker)
	Status int
	Reset  bool
	When   string
}

type kvRow struct {
	value   interface{}
	version int64
	// raw is the value's JSON as the client sent it (key order kept).
	raw json.RawMessage
}

// LockRow is a held lock.
type LockRow struct {
	Owner     string
	Token     int64
	ExpiresAt time.Time
}

type ephMsg struct {
	ID        string      `json:"id"`
	Partition string      `json:"partition"`
	Payload   interface{} `json:"payload"`
	Attempts  int         `json:"attempts"`
}

// Broker is the fake broker.
type Broker struct {
	mu        sync.Mutex
	NowMs     float64
	lastNowMs float64
	applied   int64
	log       []*Entry
	kv        map[string]*kvRow
	kvVersion int64
	ids       map[string]bool
	offsets   map[string]int64
	pids      map[string]string
	locks     map[string]*LockRow
	tokens    int64
	wake      chan struct{}
	txns      []Txn
	feedReads []map[string]interface{}
	injected  []Inject
	cut       map[string]bool
	ephemeral map[string][]ephMsg
	ephWake   chan struct{}
	ephSeq    int
	// NoFeed: POST /api/v1/feed answers 404 (a broker older than the feed).
	NoFeed bool
	// cutOnFeed: a client is cut at its n-th feed read (that read fails too).
	cutOnFeed map[string]int
	feedCalls map[string]int
	// cutAfter: a client is cut right after a committed transaction of its
	// own that the function accepts (that answer still goes out).
	cutAfter map[string]func(Txn) bool

	server *httptest.Server
}

// New starts a fake broker whose clock starts at start (T0 when 0).
func New(start float64) *Broker {
	if start == 0 {
		start = T0
	}
	b := &Broker{
		NowMs: start, lastNowMs: start, kv: map[string]*kvRow{}, kvVersion: 100, ids: map[string]bool{},
		offsets: map[string]int64{}, pids: map[string]string{}, locks: map[string]*LockRow{},
		wake: make(chan struct{}), cut: map[string]bool{}, ephemeral: map[string][]ephMsg{}, ephWake: make(chan struct{}),
	}
	b.server = httptest.NewServer(http.HandlerFunc(b.serve))
	return b
}

// Close stops the server.
func (b *Broker) Close() {
	b.server.CloseClientConnections()
	b.server.Close()
}

// URL is the base URL of the process name's client.
func (b *Broker) URL(name string) string { return b.server.URL + "/c/" + name }

// SetNow moves the wall clock to T0 + ms.
func (b *Broker) At(ms float64) {
	b.mu.Lock()
	b.NowMs = T0 + ms
	b.mu.Unlock()
}

// CutOnFeedCall cuts the client name at its n-th feed read (that one fails too).
func (b *Broker) CutOnFeedCall(name string, n int) {
	b.mu.Lock()
	if b.cutOnFeed == nil {
		b.cutOnFeed, b.feedCalls = map[string]int{}, map[string]int{}
	}
	b.cutOnFeed[name] = n
	b.mu.Unlock()
}

// CutAfter cuts the client name right after a committed transaction of its
// own for which when answers true: that answer still goes out, every later
// call of the client fails (a process that died between two commits).
func (b *Broker) CutAfter(name string, when func(Txn) bool) {
	b.mu.Lock()
	if b.cutAfter == nil {
		b.cutAfter = map[string]func(Txn) bool{}
	}
	b.cutAfter[name] = when
	b.mu.Unlock()
}

// Now is the wall clock (epoch ms).
func (b *Broker) Now() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.NowMs
}

// Cut makes the process name's client unreachable (true) or reachable again.
func (b *Broker) Cut(name string, cut bool) {
	b.mu.Lock()
	b.cut[name] = cut
	b.mu.Unlock()
}

func iso(ms float64) string { return jsv.MustISO(ms) }

func (b *Broker) stamp() (int64, float64) {
	b.applied++
	if b.NowMs > b.lastNowMs {
		b.lastNowMs = b.NowMs
	}
	return b.applied, b.lastNowMs
}

func (b *Broker) wakeAll() {
	close(b.wake)
	b.wake = make(chan struct{})
}

func kvKey(ns, key string) string { return ns + "\x00" + key }

func (b *Broker) appendPushes(items []map[string]interface{}, position int64, t float64) []interface{} {
	var results []interface{}
	for _, it := range items {
		q, _ := it["queue"].(string)
		part, _ := it["partition"].(string)
		if part == "" {
			part = "Default"
		}
		tx, _ := it["transactionId"].(string)
		pk := q + "\x00" + part
		if _, ok := b.pids[pk]; !ok {
			b.pids[pk] = fmt.Sprintf("pid-%d", len(b.pids)+1)
		}
		b.offsets[pk]++
		offset := b.offsets[pk]
		b.ids[pk+"\x00"+tx] = true
		b.log = append(b.log, &Entry{Position: position, Queue: q, Partition: part, PartitionID: b.pids[pk], Offset: offset,
			ID: fmt.Sprintf("m-%d-%d", position, offset), TransactionID: tx, Payload: jsv.JSONClone(it["payload"]), TS: iso(t)})
		results = append(results, map[string]interface{}{"type": "push", "status": "queued", "transactionId": tx})
	}
	return results
}

// Msg is one message a producer pushes.
type Msg struct {
	Queue         string
	Partition     string
	Data          interface{}
	TransactionID string
}

// Push is a producer's push: one position for all of msgs.
func (b *Broker) Push(msgs ...Msg) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	position, t := b.stamp()
	items := make([]map[string]interface{}, len(msgs))
	for i, m := range msgs {
		tx := m.TransactionID
		if tx == "" {
			tx = fmt.Sprintf("in-%d-%d", position, i)
		}
		items[i] = map[string]interface{}{"queue": m.Queue, "partition": m.Partition, "payload": m.Data, "transactionId": tx}
	}
	b.appendPushes(items, position, t)
	b.wakeAll()
	return position
}

// Messages is the entries of a queue, as stored.
func (b *Broker) Messages(queue string) []Entry {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Entry
	for _, e := range b.log {
		if e.Queue == queue {
			out = append(out, *e)
		}
	}
	return out
}

// Applied is the last position.
func (b *Broker) Applied() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.applied
}

// KVGet is a KV row (ok false when absent).
func (b *Broker) KVGet(ns, key string) (interface{}, int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.kv[kvKey(ns, key)]
	if !ok {
		return nil, 0, false
	}
	return jsv.JSONClone(r.value), r.version, true
}

// KVPut writes a KV row as another process's commit would (one position).
func (b *Broker) KVPut(ns, key string, value interface{}) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stamp()
	b.kvVersion++
	b.kv[kvKey(ns, key)] = &kvRow{value: jsv.JSONClone(jsv.Normalize(value)), version: b.kvVersion}
	return b.kvVersion
}

// KVRaw is a KV value's JSON as the client wrote it (nil when absent or written here).
func (b *Broker) KVRaw(ns, key string) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r, ok := b.kv[kvKey(ns, key)]; ok {
		return append([]byte(nil), r.raw...)
	}
	return nil
}

// KVDelete removes a KV row.
func (b *Broker) KVDelete(ns, key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.kv, kvKey(ns, key))
}

// KVKeys is the keys of a namespace that start with prefix, sorted.
func (b *Broker) KVKeys(ns, prefix string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.kvKeys(ns, prefix)
}

func (b *Broker) kvKeys(ns, prefix string) []string {
	var out []string
	p := ns + "\x00" + prefix
	for k := range b.kv {
		if strings.HasPrefix(k, p) {
			out = append(out, k[len(ns)+1:])
		}
	}
	sort.Strings(out)
	return out
}

// AddID marks a pushed transaction id as stored (a duplicate on the next push).
func (b *Broker) AddID(queue, partition, txID string) {
	b.mu.Lock()
	b.ids[queue+"\x00"+partition+"\x00"+txID] = true
	b.mu.Unlock()
}

// SetLock writes a lock row (somebody else holds it).
func (b *Broker) SetLock(name string, row LockRow) {
	b.mu.Lock()
	b.locks[name] = &row
	b.mu.Unlock()
}

// Txns is every applied transaction.
func (b *Broker) Txns() []Txn {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Txn(nil), b.txns...)
}

// FeedReads is the bodies of every feed read.
func (b *Broker) FeedReads() []map[string]interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]map[string]interface{}(nil), b.feedReads...)
}

// InjectNext arranges something for the next transaction.
func (b *Broker) InjectNext(i Inject) {
	b.mu.Lock()
	b.injected = append(b.injected, i)
	b.mu.Unlock()
}

func glob(pattern string) *regexp.Regexp {
	var sb strings.Builder
	sb.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	sb.WriteString("$")
	return regexp.MustCompile(sb.String())
}

func strList(v interface{}) []string {
	switch x := v.(type) {
	case string:
		return []string{x}
	case []interface{}:
		var out []string
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func numOr(v interface{}, d float64) float64 {
	if f, ok := jsv.Num(v); ok {
		return f
	}
	return d
}

// read answers a feed read (the caller holds mu; it is released while waiting).
func (b *Broker) read(ctx context.Context, body map[string]interface{}, again bool) map[string]interface{} {
	if !again {
		b.feedReads = append(b.feedReads, jsv.JSONClone(body).(map[string]interface{}))
	}
	var from int64
	switch f := body["from"].(type) {
	case nil:
		from = b.applied + 1
	case string:
		if f == "earliest" {
			from = 1
		} else {
			from = b.applied + 1
		}
	default:
		from = int64(numOr(f, 0))
	}
	var queues []*regexp.Regexp
	for _, q := range strList(body["queues"]) {
		queues = append(queues, glob(q))
	}
	parts := strList(body["partitions"])
	where, _ := feed.NormalizeWhere(body["where"])
	var since *float64
	if s, ok := body["since"].(string); ok {
		if t, ok := jsv.DateParse(s); ok {
			since = &t
		}
	}
	max := int(numOr(body["maxRecords"], 1000))
	shard, _ := body["shard"].(map[string]interface{})
	var shardQueues []*regexp.Regexp
	if shard != nil {
		for _, q := range strList(shard["queues"]) {
			shardQueues = append(shardQueues, glob(q))
		}
	}
	var out []*Entry
	next := b.applied + 1
	for _, e := range b.log {
		if e.Position < from {
			continue
		}
		if len(out) >= max && e.Position != out[len(out)-1].Position {
			next = e.Position
			break
		}
		if queues != nil && !anyMatch(queues, e.Queue) {
			continue
		}
		if parts != nil && !contains(parts, e.Partition) {
			continue
		}
		if since != nil {
			if t, _ := jsv.DateParse(e.TS); t < *since {
				continue
			}
		}
		if shard != nil && (shardQueues == nil || anyMatch(shardQueues, e.Queue)) &&
			feed.ShardOf(e.Partition, int(numOr(shard["count"], 1))) != int(numOr(shard["index"], 0)) {
			continue
		}
		msg := map[string]interface{}{"queue": e.Queue, "partition": e.Partition, "transactionId": e.TransactionID,
			"offset": float64(e.Offset), "position": float64(e.Position), "data": e.Payload}
		ok := true
		for _, c := range where {
			if !feed.ConditionHolds(c, msg) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, e)
		}
	}
	caughtUp := next > b.applied
	if len(out) == 0 && caughtUp && numOr(body["maxWaitMs"], 0) > 0 && !again {
		wake := b.wake
		wait := time.Duration(numOr(body["maxWaitMs"], 0) * float64(time.Millisecond))
		b.mu.Unlock()
		select {
		case <-wake:
		case <-time.After(wait):
		case <-ctx.Done():
		}
		b.mu.Lock()
		nb := map[string]interface{}{}
		for k, v := range body {
			nb[k] = v
		}
		nb["from"] = float64(from)
		return b.read(ctx, nb, true)
	}
	var lastTS string
	for _, e := range b.log {
		if e.Position < next {
			lastTS = e.TS
		}
	}
	resolved := b.lastNowMs
	if !caughtUp {
		if lastTS != "" {
			resolved, _ = jsv.DateParse(lastTS)
		}
	}
	entries := make([]interface{}, len(out))
	for i, e := range out {
		entries[i] = map[string]interface{}{"position": float64(e.Position), "kind": "push", "queue": e.Queue, "partition": e.Partition,
			"partitionId": e.PartitionID, "offset": float64(e.Offset), "id": e.ID, "transactionId": e.TransactionID,
			"payload": jsv.JSONClone(e.Payload), "ts": e.TS}
	}
	return map[string]interface{}{"applied": float64(b.applied), "caughtUp": caughtUp, "entries": entries,
		"next": float64(next), "resolvedTs": iso(resolved)}
}

func anyMatch(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func (b *Broker) kvCall(ops []interface{}) map[string]interface{} {
	var results []interface{}
	for index, raw := range ops {
		op, _ := raw.(map[string]interface{})
		ns, _ := op["ns"].(string)
		switch op["op"] {
		case "get":
			key, _ := op["key"].(string)
			if r, ok := b.kv[kvKey(ns, key)]; ok {
				results = append(results, map[string]interface{}{"index": float64(index), "op": "get", "key": key, "found": true,
					"value": jsv.JSONClone(r.value), "version": float64(r.version)})
			} else {
				results = append(results, map[string]interface{}{"index": float64(index), "op": "get", "key": key, "found": false})
			}
		case "getMany":
			rows := []interface{}{}
			missing := []interface{}{}
			for _, k := range strList(op["keys"]) {
				if r, ok := b.kv[kvKey(ns, k)]; ok {
					rows = append(rows, map[string]interface{}{"key": k, "value": jsv.JSONClone(r.value), "version": float64(r.version)})
				} else {
					missing = append(missing, k)
				}
			}
			results = append(results, map[string]interface{}{"index": float64(index), "op": "getMany", "rows": rows, "missing": missing, "truncated": false})
		case "getPrefix":
			prefix, _ := op["prefix"].(string)
			after, hasAfter := op["after"].(string)
			var keys []string
			for _, k := range b.kvKeys(ns, prefix) {
				if !hasAfter || k > after {
					keys = append(keys, k)
				}
			}
			limit := int(numOr(op["limit"], 100))
			if limit > 1000 {
				limit = 1000
			}
			page := keys
			if len(page) > limit {
				page = keys[:limit]
			}
			rows := []interface{}{}
			for _, k := range page {
				r := b.kv[kvKey(ns, k)]
				if op["keysOnly"] == true {
					rows = append(rows, map[string]interface{}{"key": k, "version": float64(r.version)})
				} else {
					rows = append(rows, map[string]interface{}{"key": k, "value": jsv.JSONClone(r.value), "version": float64(r.version)})
				}
			}
			truncated := len(keys) > limit
			var nextAfter interface{}
			if truncated {
				nextAfter = page[len(page)-1]
			}
			results = append(results, map[string]interface{}{"index": float64(index), "op": "getPrefix", "rows": rows, "truncated": truncated, "nextAfter": nextAfter})
		default:
			results = append(results, map[string]interface{}{"index": float64(index), "error": fmt.Sprintf("fake broker: kv op %v not served", op["op"])})
		}
	}
	return map[string]interface{}{"results": results}
}

func (b *Broker) lockRow(name string) *LockRow {
	r := b.locks[name]
	if r != nil && r.ExpiresAt.After(time.Now()) {
		return r
	}
	return nil
}

func (b *Broker) locksCall(ops []interface{}) map[string]interface{} {
	var results []interface{}
	for _, raw := range ops {
		op, _ := raw.(map[string]interface{})
		name, _ := op["name"].(string)
		owner, _ := op["owner"].(string)
		row := b.lockRow(name)
		ttl := time.Duration(numOr(op["ttlSeconds"], 0) * float64(time.Second))
		guard := func(token int64) map[string]interface{} {
			return map[string]interface{}{"op": "check", "ns": "queen-locks", "key": name, "expect": float64(token), "required": true}
		}
		token := int64(numOr(op["token"], 0))
		switch op["op"] {
		case "acquire":
			if row != nil && row.Owner != owner {
				results = append(results, map[string]interface{}{"acquired": false, "reason": "held",
					"holders": []interface{}{map[string]interface{}{"owner": row.Owner, "token": float64(row.Token)}}})
				continue
			}
			b.stamp()
			b.tokens++
			b.locks[name] = &LockRow{Owner: owner, Token: b.tokens, ExpiresAt: time.Now().Add(ttl)}
			results = append(results, map[string]interface{}{"acquired": true, "slot": 0.0, "token": float64(b.tokens), "owner": owner, "guard": guard(b.tokens)})
		case "renew":
			if row == nil || row.Token != token {
				results = append(results, map[string]interface{}{"renewed": false, "reason": "lost", "holders": []interface{}{}})
				continue
			}
			b.stamp()
			b.tokens++
			b.locks[name] = &LockRow{Owner: row.Owner, Token: b.tokens, ExpiresAt: time.Now().Add(ttl)}
			results = append(results, map[string]interface{}{"renewed": true, "slot": 0.0, "token": float64(b.tokens), "guard": guard(b.tokens)})
		case "release":
			if row == nil || row.Token != token {
				results = append(results, map[string]interface{}{"released": false, "reason": "lost"})
				continue
			}
			b.stamp()
			delete(b.locks, name)
			results = append(results, map[string]interface{}{"released": true})
		case "get":
			holders := []interface{}{}
			if row != nil {
				holders = append(holders, map[string]interface{}{"owner": row.Owner, "token": float64(row.Token)})
			}
			results = append(results, map[string]interface{}{"held": row != nil, "holders": holders})
		}
	}
	return map[string]interface{}{"results": results}
}

type httpFail struct {
	status int
	reset  bool
}

// transaction applies one transaction (mu held). A non-nil fail is answered instead of the result.
func (b *Broker) transaction(body map[string]interface{}, rawKV []json.RawMessage) (map[string]interface{}, *httpFail, bool) {
	var items []map[string]interface{}
	ops, _ := body["operations"].([]interface{})
	for _, o := range ops {
		op, _ := o.(map[string]interface{})
		if op["type"] == "push" {
			list, _ := op["items"].([]interface{})
			for _, it := range list {
				if m, ok := it.(map[string]interface{}); ok {
					items = append(items, m)
				}
			}
		}
	}
	var kvOps []map[string]interface{}
	kl, _ := body["kv"].([]interface{})
	for _, k := range kl {
		if m, ok := k.(map[string]interface{}); ok {
			kvOps = append(kvOps, m)
		}
	}
	var inj *Inject
	if len(b.injected) > 0 {
		x := b.injected[0]
		b.injected = b.injected[1:]
		inj = &x
	}
	if inj != nil && inj.Before != nil {
		before := inj.Before
		b.mu.Unlock()
		before(b)
		b.mu.Lock()
	}
	failNow := inj != nil && (inj.Status != 0 || inj.Reset)
	if failNow && inj.When != "after" {
		return nil, &httpFail{status: inj.Status, reset: inj.Reset}, false
	}
	// Duplicates first, as the broker answers them.
	seen := map[string]bool{}
	for _, it := range items {
		part, _ := it["partition"].(string)
		if part == "" {
			part = "Default"
		}
		k := jsv.String(it["queue"]) + "\x00" + part + "\x00" + jsv.String(it["transactionId"])
		if b.ids[k] || seen[k] {
			return map[string]interface{}{"success": false, "reason": "duplicate", "error": "QDUP a pushed message is a duplicate; the transaction rolled back", "results": []interface{}{}}, nil, false
		}
		seen[k] = true
	}
	if len(kvOps) > 64 {
		return map[string]interface{}{"success": false, "reason": "bad_request", "error": fmt.Sprintf("kv_too_many_ops: %d ops in one call, the ceiling is 64", len(kvOps))}, nil, false
	}
	for i, op := range kvOps {
		fail := func(kvReason string, version, value interface{}) map[string]interface{} {
			return map[string]interface{}{"success": false, "reason": "kv_precondition", "failedIndex": float64(len(items) + i), "kvReason": kvReason,
				"version": version, "value": value, "error": "QKV a required KV precondition failed; the transaction rolled back", "results": []interface{}{}}
		}
		ns, _ := op["ns"].(string)
		key, _ := op["key"].(string)
		expect, hasExpect := op["expect"]
		ev := int64(numOr(expect, 0))
		if op["op"] == "check" && ns == "queen-locks" {
			row := b.lockRow(key)
			if row == nil {
				return fail("missing", nil, nil), nil, false
			}
			if row.Token != ev {
				return fail("version", float64(row.Token), map[string]interface{}{"owner": row.Owner}), nil, false
			}
			continue
		}
		row := b.kv[kvKey(ns, key)]
		rowFail := func(reason string) map[string]interface{} {
			if row == nil {
				return fail(reason, nil, nil)
			}
			return fail(reason, float64(row.version), jsv.JSONClone(row.value))
		}
		switch op["op"] {
		case "check":
			if ev == 0 && row != nil {
				return rowFail("exists"), nil, false
			}
			if ev != 0 && (row == nil || row.version != ev) {
				if row != nil {
					return rowFail("version"), nil, false
				}
				return rowFail("missing"), nil, false
			}
			continue
		case "putIfAbsent":
			if row != nil {
				return rowFail("exists"), nil, false
			}
		case "put":
			if hasExpect {
				if row == nil {
					return rowFail("missing"), nil, false
				}
				if row.version != ev {
					return rowFail("version"), nil, false
				}
			}
		}
		if op["op"] == "put" && op["forever"] != true && numOr(op["ttlSeconds"], 0) == 0 {
			return map[string]interface{}{"success": false, "reason": "bad_request", "error": "kv_expiry_not_specified"}, nil, false
		}
	}
	position, t := b.stamp()
	for _, it := range items {
		if p, _ := it["partition"].(string); p == "" {
			it["partition"] = "Default"
		}
	}
	results := b.appendPushes(items, position, t)
	written := map[string]int64{}
	for i, op := range kvOps {
		ns, _ := op["ns"].(string)
		key, _ := op["key"].(string)
		idx := float64(len(items) + i)
		switch op["op"] {
		case "check":
			results = append(results, map[string]interface{}{"index": idx, "type": "kv", "op": "check", "key": key})
			continue
		case "delete":
			delete(b.kv, kvKey(ns, key))
			results = append(results, map[string]interface{}{"index": idx, "type": "kv", "op": "delete", "key": key, "applied": true})
			continue
		}
		b.kvVersion++
		row := &kvRow{value: jsv.JSONClone(op["value"]), version: b.kvVersion}
		if i < len(rawKV) {
			var one struct {
				Value json.RawMessage `json:"value"`
			}
			if json.Unmarshal(rawKV[i], &one) == nil {
				row.raw = one.Value
			}
		}
		b.kv[kvKey(ns, key)] = row
		written[key] = b.kvVersion
		results = append(results, map[string]interface{}{"index": idx, "type": "kv", "op": op["op"], "key": key, "version": float64(b.kvVersion), "applied": true})
	}
	b.txns = append(b.txns, Txn{Position: position, Items: items, KV: kvOps, Written: written})
	b.wakeAll()
	if failNow && inj.When == "after" {
		return nil, &httpFail{status: inj.Status, reset: inj.Reset}, true
	}
	return map[string]interface{}{"success": true, "transactionId": fmt.Sprintf("txn-%d", position), "results": results}, nil, true
}

func (b *Broker) ephemeralPush(body map[string]interface{}) map[string]interface{} {
	q, _ := body["queue"].(string)
	part, _ := body["partition"].(string)
	if part == "" {
		part = "Default"
	}
	msgs, _ := body["messages"].([]interface{})
	for _, m := range msgs {
		mm, _ := m.(map[string]interface{})
		b.ephSeq++
		b.ephemeral[q] = append(b.ephemeral[q], ephMsg{ID: fmt.Sprintf("e-%d", b.ephSeq), Partition: part, Payload: jsv.JSONClone(mm["payload"])})
	}
	close(b.ephWake)
	b.ephWake = make(chan struct{})
	return map[string]interface{}{"pushed": float64(len(msgs))}
}

func (b *Broker) ephemeralPop(ctx context.Context, q string, batch int, wait bool, timeout time.Duration) map[string]interface{} {
	take := func() []ephMsg {
		l := b.ephemeral[q]
		n := batch
		if n > len(l) {
			n = len(l)
		}
		out := l[:n]
		b.ephemeral[q] = l[n:]
		return out
	}
	got := take()
	if len(got) == 0 && wait {
		wake := b.ephWake
		b.mu.Unlock()
		select {
		case <-wake:
		case <-time.After(timeout):
		case <-ctx.Done():
		}
		b.mu.Lock()
		got = take()
	}
	if got == nil {
		got = []ephMsg{}
	}
	return map[string]interface{}{"queue": q, "messages": got}
}

var clientRe = regexp.MustCompile(`^/c/([^/]+)(/.*)$`)

func (b *Broker) serve(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	client := ""
	if m := clientRe.FindStringSubmatch(path); m != nil {
		client, path = m[1], m[2]
	}
	b.mu.Lock()
	if n, ok := b.cutOnFeed[client]; ok && r.Method == http.MethodPost && path == "/api/v1/feed" {
		b.feedCalls[client]++
		if b.feedCalls[client] == n {
			b.cut[client] = true
		}
	}
	cut := b.cut[client]
	noFeed := b.NoFeed
	b.mu.Unlock()
	if cut {
		drop(w)
		return
	}
	var body map[string]interface{}
	var rawKV []json.RawMessage
	if r.Method == http.MethodPost {
		bytes, _ := io.ReadAll(r.Body)
		var raw interface{}
		if err := json.Unmarshal(bytes, &raw); err == nil {
			body, _ = raw.(map[string]interface{})
		}
		var kvs struct {
			KV []json.RawMessage `json:"kv"`
		}
		if json.Unmarshal(bytes, &kvs) == nil {
			rawKV = kvs.KV
		}
	}
	send := func(status int, v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(jsv.MustMarshal(v))
	}
	switch {
	case r.Method == http.MethodPost && path == "/api/v1/feed":
		if noFeed {
			send(404, map[string]interface{}{"error": "Not Found"})
			return
		}
		b.mu.Lock()
		ans := b.read(r.Context(), body, false)
		b.mu.Unlock()
		send(200, ans)
	case r.Method == http.MethodPost && path == "/api/v1/kv":
		ops, _ := body["operations"].([]interface{})
		b.mu.Lock()
		ans := b.kvCall(ops)
		b.mu.Unlock()
		send(200, ans)
	case r.Method == http.MethodPost && path == "/api/v1/transaction":
		b.mu.Lock()
		ans, fail, committed := b.transaction(body, rawKV)
		if when := b.cutAfter[client]; committed && fail == nil && when != nil && when(b.txns[len(b.txns)-1]) {
			b.cut[client] = true
		}
		b.mu.Unlock()
		if fail != nil {
			if fail.reset {
				drop(w)
				return
			}
			send(fail.status, map[string]interface{}{"error": fmt.Sprintf("HTTP %d: injected", fail.status)})
			return
		}
		send(200, ans)
	case r.Method == http.MethodPost && path == "/api/v1/locks":
		ops, _ := body["operations"].([]interface{})
		b.mu.Lock()
		ans := b.locksCall(ops)
		b.mu.Unlock()
		send(200, ans)
	case r.Method == http.MethodPost && path == "/api/v1/ephemeral/push":
		b.mu.Lock()
		ans := b.ephemeralPush(body)
		b.mu.Unlock()
		send(200, ans)
	case r.Method == http.MethodGet && path == "/api/v1/ephemeral/pop":
		qv := r.URL.Query()
		batch, _ := strconv.Atoi(qv.Get("batch"))
		if batch <= 0 {
			batch = 1
		}
		timeout, _ := strconv.Atoi(qv.Get("timeout"))
		b.mu.Lock()
		ans := b.ephemeralPop(r.Context(), qv.Get("queue"), batch, qv.Get("wait") == "true", time.Duration(timeout)*time.Millisecond)
		b.mu.Unlock()
		send(200, ans)
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/api/v1/ephemeral/queue/"):
		name := strings.TrimPrefix(path, "/api/v1/ephemeral/queue/")
		b.mu.Lock()
		delete(b.ephemeral, name)
		b.mu.Unlock()
		send(200, map[string]interface{}{"queue": name, "deleted": true})
	default:
		send(404, map[string]interface{}{"error": "fake broker: no " + r.Method + " " + path})
	}
}

// drop closes the connection without an answer: the client sees it gone.
func drop(w http.ResponseWriter) {
	if hj, ok := w.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			_ = conn.Close()
			return
		}
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}
