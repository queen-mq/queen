package main

// The offline smoke test, shared by `-selftest` and `go test`.
//
// It needs no broker: it starts two IN-PROCESS fake brokers that speak the
// shapes client.go documents, mints DIFFERENT ids on each side (that is the
// point — normalization has to make two honest brokers compare equal), runs a
// short sequence through the real Runner, and asserts:
//
//   1. the generator is a pure function of the seed;
//   2. two honest sides produce NO divergence (no false positives);
//   3. a side that answers one field differently produces EXACTLY ONE
//      divergence naming that field (no false negatives);
//   4. normalization keeps the identity relation between ids;
//   5. the flag surface refuses unknown kinds and stubs.
//
// The fake is not a broker model. It is the smallest thing that keeps the
// harness honest; the real oracle is the postgres class (D22).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SelfTest runs every offline check and returns the first failure.
func SelfTest() error {
	for _, c := range []struct {
		name string
		fn   func() error
	}{
		{"generator is seeded", checkGeneratorIsSeeded},
		{"normalizer keeps id identity", checkNormalizerIdentity},
		{"comparison names the first difference", checkFirstDiffPath},
		{"mix parsing", checkMixParsing},
		{"the full message-path mix does not diverge on two honest sides", checkHonestRun},
		{"a changed field is caught", checkDishonestRun},
		{"the per-side checker log is well formed", checkCheckerLogEmission},
	} {
		if err := c.fn(); err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
	}
	return nil
}

func selfTestConfig(seed int64, a, b string) *Config {
	return &Config{
		URLA: a, URLB: b,
		NameA: "fakeA", NameB: "fakeB",
		Timeout:   5 * time.Second,
		Seed:      seed,
		Ops:       120,
		RunID:     "selftest",
		Mix:       DefaultMix(),
		Queues:    2,
		Parts:     3,
		Groups:    3,
		DupRate:   20,
		Namespace: "difffuzz-selftest",
	}
}

// pinnedMix drives only the strictly-compared path (push, pinned pop, single
// ack), so the injected difference in checkDishonestRun reliably lands on a
// compared field rather than on a relaxed wildcard pop.
func pinnedMix() Mix { return Mix{OpPush: 3, OpPop: 2, OpAck: 1} }

func checkGeneratorIsSeeded() error {
	c1 := selfTestConfig(42, "http://a", "http://b")
	c2 := selfTestConfig(42, "http://a", "http://b")
	c3 := selfTestConfig(43, "http://a", "http://b")
	s1 := opsString(NewGenerator(c1).Generate())
	s2 := opsString(NewGenerator(c2).Generate())
	s3 := opsString(NewGenerator(c3).Generate())
	if s1 != s2 {
		return fmt.Errorf("the same seed produced two different sequences")
	}
	if s1 == s3 {
		return fmt.Errorf("two different seeds produced the same sequence")
	}
	return nil
}

func opsString(ops []Op) string {
	var b strings.Builder
	for _, o := range ops {
		fmt.Fprintf(&b, "%d:%s:%s:%s:%d:%d:%d;", o.Index, o.Kind, o.Queue, o.Partition, o.Batch, o.Slot%1000, len(o.Items))
		for _, it := range o.Items {
			fmt.Fprintf(&b, "%s/%s/%s/%v,", it.Queue, it.Partition, it.TxnID, it.Duplicate)
		}
	}
	return b.String()
}

func checkNormalizerIdentity() error {
	const idA = "11111111-1111-4111-8111-111111111111"
	const idB = "22222222-2222-4222-8222-222222222222"
	// Same id twice on one side, two different ids on the other: normalization
	// must keep them different, or a lease-id bug would be invisible.
	a := json.RawMessage(fmt.Sprintf(`{"x":%q,"y":%q,"t":"2026-09-17T10:00:00Z","lagSeconds":3}`, idA, idA))
	b := json.RawMessage(fmt.Sprintf(`{"x":%q,"y":%q,"t":"2026-09-17T11:22:33.444Z","lagSeconds":9}`, idA, idB))
	na, nb := NewNormalizer(), NewNormalizer()
	va, err := na.Normalize(a)
	if err != nil {
		return err
	}
	vb, err := nb.Normalize(b)
	if err != nil {
		return err
	}
	path, _, _, differ := FirstDiff("$", va, vb)
	if !differ || path != "$.y" {
		return fmt.Errorf("expected a difference at $.y, got differ=%v path=%q", differ, path)
	}
	// And the same body on both sides must compare equal despite the clock and
	// the dropped volatile key.
	if _, _, _, d := FirstDiff("$", va, mustNormalize(a)); d {
		return fmt.Errorf("a body did not compare equal to itself")
	}
	return nil
}

func mustNormalize(raw json.RawMessage) any {
	v, err := NewNormalizer().Normalize(raw)
	if err != nil {
		panic(err)
	}
	return v
}

func checkFirstDiffPath() error {
	a := &Resp{Status: 200, Body: json.RawMessage(`{"messages":[{"deliveryAttempt":1,"offset":7}]}`)}
	b := &Resp{Status: 200, Body: json.RawMessage(`{"messages":[{"deliveryAttempt":2,"offset":7}]}`)}
	d := CompareResponses(3, "pop", "x", a, b, false)
	if d == nil {
		return fmt.Errorf("a changed field produced no divergence")
	}
	if d.Path != "$.messages[0].deliveryAttempt" {
		return fmt.Errorf("divergence path is %q, want $.messages[0].deliveryAttempt", d.Path)
	}
	same := CompareResponses(3, "pop", "x", a, a, false)
	if same != nil {
		return fmt.Errorf("identical answers produced a divergence: %s", same)
	}
	return nil
}

func checkMixParsing() error {
	if _, err := ParseMix("push=1,nosuchkind=1"); err == nil {
		return fmt.Errorf("an unknown kind was accepted")
	}
	if _, err := ParseMix("push=1,ack=1"); err != nil {
		return fmt.Errorf("a valid mix was refused: %v", err)
	}
	c := selfTestConfig(1, "http://a", "http://b")
	c.Mix = Mix{OpTxn: 1} // a documented stub
	if err := c.Validate(); err == nil {
		return fmt.Errorf("a stub kind was accepted in the mix")
	}
	return nil
}

func checkHonestRun() error {
	a, err := startFakeBroker(fakeOpts{idPrefix: 0xa1})
	if err != nil {
		return err
	}
	defer a.Close()
	// Side B is the phase-1 SUT: the resource views answer 503, so Preflight
	// picks the views-deferred mode, exactly as against the real raft1 broker.
	b, err := startFakeBroker(fakeOpts{idPrefix: 0xb2, unsupportedViews: true})
	if err != nil {
		return err
	}
	defer b.Close()

	cfg := selfTestConfig(7, a.URL, b.URL)
	cfg.StopOnDiff = false
	r := NewRunner(cfg, io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.Preflight(ctx); err != nil {
		return err
	}
	if r.views != viewsDeferred {
		return fmt.Errorf("Preflight picked views mode %d, want deferred (2) against an unsupported-views SUT", r.views)
	}
	rep, err := r.Run(ctx)
	if err != nil {
		return err
	}
	if len(rep.Divergences) != 0 {
		return fmt.Errorf("two honest sides diverged %d time(s) over the full mix; first: %s", len(rep.Divergences), rep.Divergences[0])
	}
	if rep.Executed != cfg.Ops {
		return fmt.Errorf("executed %d of %d operations", rep.Executed, cfg.Ops)
	}
	return nil
}

func checkDishonestRun() error {
	a, err := startFakeBroker(fakeOpts{idPrefix: 0xa1})
	if err != nil {
		return err
	}
	defer a.Close()
	// Side B reports one more delivery attempt than it should: a plausible
	// planner bug, invisible to a status-code check.
	b, err := startFakeBroker(fakeOpts{idPrefix: 0xb2, bumpDeliveryAttempt: true, unsupportedViews: true})
	if err != nil {
		return err
	}
	defer b.Close()

	cfg := selfTestConfig(7, a.URL, b.URL)
	cfg.Mix = pinnedMix() // the strictly-compared path, so the bump lands on a compared field
	cfg.StopOnDiff = true
	r := NewRunner(cfg, io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.Preflight(ctx); err != nil {
		return err
	}
	rep, err := r.Run(ctx)
	if err != nil {
		return err
	}
	if len(rep.Divergences) == 0 {
		return fmt.Errorf("a changed deliveryAttempt was not caught")
	}
	if !strings.Contains(rep.Divergences[0].Path, "deliveryAttempt") {
		return fmt.Errorf("the divergence names %q, want a path through deliveryAttempt", rep.Divergences[0].Path)
	}
	return nil
}

// checkCheckerLogEmission runs a short honest sequence with the per-side logs
// attached and asserts the SUT log is a well-formed checker run log: every line
// is valid JSON of a kind the checker knows, and it carries the events the
// checks read (push_ok, delivery, ack_ok) plus the drain-complete notes.
func checkCheckerLogEmission() error {
	a, err := startFakeBroker(fakeOpts{idPrefix: 0xa1})
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := startFakeBroker(fakeOpts{idPrefix: 0xb2, unsupportedViews: true})
	if err != nil {
		return err
	}
	defer b.Close()

	cfg := selfTestConfig(11, a.URL, b.URL)
	cfg.Mix = Mix{OpPush: 3, OpPop: 2, OpAck: 1}
	cfg.Ops = 80
	cfg.StopOnDiff = false
	r := NewRunner(cfg, io.Discard)
	var bufA, bufB bytes.Buffer
	r.SetLogs(NewRunLog(&bufA, "fakeA"), NewRunLog(&bufB, "fakeB"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.Preflight(ctx); err != nil {
		return err
	}
	if _, err := r.Run(ctx); err != nil {
		return err
	}
	seen, err := validateRunLog(bufB.Bytes())
	if err != nil {
		return err
	}
	for _, want := range []string{"push_ok", "delivery", "ack_ok"} {
		if seen[want] == 0 {
			return fmt.Errorf("the SUT run log carries no %q event: %v", want, seen)
		}
	}
	if seen["note"] == 0 {
		return fmt.Errorf("the SUT run log carries no drain-complete note: %v", seen)
	}
	return nil
}

// validateRunLog parses a JSONL run log the way the checker's loader does
// (comment and blank lines skipped, every kind known, seq monotone) and returns
// the count per kind. It duplicates the checker's rule on purpose: the two are
// separate modules, and this keeps the format contract pinned on the writer's
// side so a mismatch is caught here, not after a campaign.
func validateRunLog(b []byte) (map[string]int, error) {
	seen := map[string]int{}
	var lastSeq int64
	for i, raw := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var ev struct {
			Seq  int64  `json:"seq"`
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			return nil, fmt.Errorf("line %d is not valid JSON: %w", i+1, err)
		}
		if !knownLogKinds[ev.Kind] {
			return nil, fmt.Errorf("line %d has kind %q the checker does not know", i+1, ev.Kind)
		}
		if ev.Seq <= lastSeq {
			return nil, fmt.Errorf("line %d seq %d is not above the previous %d", i+1, ev.Seq, lastSeq)
		}
		lastSeq = ev.Seq
		seen[ev.Kind]++
	}
	return seen, nil
}

// ------------------------------------------------------------ the fake broker

type fakeOpts struct {
	idPrefix            byte
	bumpDeliveryAttempt bool
	// unsupportedViews models a phase-1 raft1 broker: the resource views answer
	// 503 raft_phase1_unsupported (WP-1.7a), so the runner's Preflight picks the
	// views-deferred mode, as it does against the real SUT.
	unsupportedViews bool
}

type fakeMsg struct {
	TxnID     string
	MessageID string
	Queue     string
	Partition string
	Payload   json.RawMessage
	Attempt   int
}

type fakeLease struct {
	msg   fakeMsg
	group string
}

type fakeBroker struct {
	URL  string
	opts fakeOpts

	mu       sync.Mutex
	ids      int
	seenTxn  map[string]bool
	queues   []string             // insertion order: deterministic listings
	ready    map[string][]fakeMsg // queue -> FIFO
	claimed  map[string]fakeLease // leaseKey(group,txn) -> lease
	dlq      map[string][]fakeMsg // queue -> rows
	done     map[string]int       // queue -> completed count
	partIDs  map[string]string    // queue/partition -> uuid
	groups   map[string]bool      // consumer groups seen
	srv      *http.Server
	listener net.Listener
}

func startFakeBroker(opts fakeOpts) (*fakeBroker, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	f := &fakeBroker{
		URL:      "http://" + ln.Addr().String(),
		opts:     opts,
		seenTxn:  map[string]bool{},
		ready:    map[string][]fakeMsg{},
		claimed:  map[string]fakeLease{},
		dlq:      map[string][]fakeMsg{},
		done:     map[string]int{},
		partIDs:  map[string]string{},
		groups:   map[string]bool{},
		listener: ln,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", f.handle)
	f.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = f.srv.Serve(ln) }()
	return f, nil
}

func (f *fakeBroker) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = f.srv.Shutdown(ctx)
}

func (f *fakeBroker) nextID() string {
	f.ids++
	return fmt.Sprintf("%02x000000-0000-4000-8000-%012x", f.opts.idPrefix, f.ids)
}

func (f *fakeBroker) partitionID(queue, partition string) string {
	k := queue + "/" + partition
	if id, ok := f.partIDs[k]; ok {
		return id
	}
	id := f.nextID()
	f.partIDs[k] = id
	return id
}

func (f *fakeBroker) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	// Phase-1 SUT: the resource views are not served (WP-1.7a). Answer 503
	// raft_phase1_unsupported so Preflight picks the views-deferred mode.
	if f.opts.unsupportedViews && (strings.HasPrefix(path, "/api/v1/resources") ||
		path == "/api/v1/consumer-groups" || path == "/api/v1/messages" || path == "/api/v1/dlq") {
		writeJSON(w, 503, map[string]any{"error": "raft phase 1 does not serve this route", "code": "raft_phase1_unsupported"})
		return
	}
	switch {
	case path == "/health":
		writeJSON(w, 200, map[string]any{"status": "ok", "uptimeSeconds": 12})
	case path == "/api/v1/push" && r.Method == http.MethodPost:
		f.push(w, r)
	case strings.HasPrefix(path, "/api/v1/pop/queue/") && r.Method == http.MethodGet:
		f.pop(w, r)
	case path == "/api/v1/pop" && r.Method == http.MethodGet:
		// Discovery: the fake has no namespace index, so it discovers nothing.
		writeJSON(w, 200, map[string]any{"success": true, "messages": []any{}})
	case path == "/api/v1/ack" && r.Method == http.MethodPost:
		f.ack(w, r)
	case path == "/api/v1/ack/batch" && r.Method == http.MethodPost:
		f.ackBatch(w, r)
	case strings.HasPrefix(path, "/api/v1/lease/") && strings.HasSuffix(path, "/extend") && r.Method == http.MethodPost:
		writeJSON(w, 200, map[string]any{"success": true, "renewed": 1, "newExpiresAt": "2026-09-17T10:05:00.000Z"})
	case path == "/api/v1/resources/queues":
		f.listQueues(w)
	case path == "/api/v1/consumer-groups":
		f.listGroups(w)
	case strings.HasSuffix(path, "/depth"):
		f.depth(w, strings.TrimSuffix(strings.TrimPrefix(path, "/api/v1/resources/queues/"), "/depth"))
	case path == "/api/v1/messages":
		f.messages(w, r.URL.Query().Get("queue"))
	case path == "/api/v1/dlq":
		f.dlqList(w, r.URL.Query().Get("queue"))
	default:
		writeJSON(w, 404, map[string]any{"error": "not found", "path": path})
	}
}

func (f *fakeBroker) push(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Items []PushItem `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(body.Items))
	for i, it := range body.Items {
		if _, known := f.ready[it.Queue]; !known {
			f.queues = append(f.queues, it.Queue)
			f.ready[it.Queue] = nil
		}
		status := "created"
		msgID := f.nextID()
		if f.seenTxn[it.TransactionID] {
			status = "duplicate"
		} else {
			f.seenTxn[it.TransactionID] = true
			f.ready[it.Queue] = append(f.ready[it.Queue], fakeMsg{
				TxnID: it.TransactionID, MessageID: msgID, Queue: it.Queue,
				Partition: it.Partition, Payload: it.Payload, Attempt: 0,
			})
		}
		out = append(out, map[string]any{
			"index": i, "message_id": msgID, "transaction_id": it.TransactionID,
			"queueName": it.Queue, "status": status,
		})
	}
	writeJSON(w, 201, out)
}

func (f *fakeBroker) pop(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/v1/pop/queue/")
	queue := rest
	partition := ""
	if i := strings.Index(rest, "/partition/"); i >= 0 {
		queue, partition = rest[:i], rest[i+len("/partition/"):]
	}
	group := r.URL.Query().Get("consumerGroup")
	f.groups[group] = true
	batch, _ := strconv.Atoi(r.URL.Query().Get("batch"))
	if batch <= 0 {
		batch = 1
	}
	leaseID := f.nextID()
	msgs := []map[string]any{}
	kept := f.ready[queue][:0:0]
	for _, m := range f.ready[queue] {
		if len(msgs) < batch && (partition == "" || m.Partition == partition) {
			m.Attempt++
			f.claimed[group+"|"+m.TxnID] = fakeLease{msg: m, group: group}
			attempt := m.Attempt
			if f.opts.bumpDeliveryAttempt {
				attempt++
			}
			msgs = append(msgs, map[string]any{
				"id": m.MessageID, "transactionId": m.TxnID, "data": m.Payload,
				"createdAt": "2026-09-17T10:00:00.000Z", "partition": m.Partition,
				"partitionId": f.partitionID(m.Queue, m.Partition), "leaseId": leaseID,
				"consumerGroup": group, "deliveryAttempt": attempt,
			})
			continue
		}
		kept = append(kept, m)
	}
	f.ready[queue] = kept
	writeJSON(w, 200, map[string]any{
		"success": true, "queue": queue, "partition": partition,
		"consumerGroup": group, "leaseId": leaseID, "messages": msgs,
	})
}

func (f *fakeBroker) ack(w http.ResponseWriter, r *http.Request) {
	var a AckItem
	if err := json.NewDecoder(r.Body).Decode(&a); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, []map[string]any{f.applyAck(a, 0)})
}

func (f *fakeBroker) ackBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ConsumerGroup   string    `json:"consumerGroup"`
		Acknowledgments []AckItem `json:"acknowledgments"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(body.Acknowledgments))
	for i, a := range body.Acknowledgments {
		if a.ConsumerGroup == "" {
			a.ConsumerGroup = body.ConsumerGroup
		}
		out = append(out, f.applyAck(a, i))
	}
	writeJSON(w, 200, out)
}

// applyAck is the shared verdict for one acknowledgment, so single and batch
// acks answer the same shape. The fake keys leases by (group, txn), which is
// enough for the harness: the two sides run identical logic, only their ids
// differ.
func (f *fakeBroker) applyAck(a AckItem, index int) map[string]any {
	key := a.ConsumerGroup + "|" + a.TransactionID
	lease, ok := f.claimed[key]
	if !ok {
		return map[string]any{
			"index": index, "transactionId": a.TransactionID, "success": false,
			"error": "no outstanding delivery", "leaseReleased": false, "dlq": false,
		}
	}
	delete(f.claimed, key)
	dlq := false
	switch a.Status {
	case "completed":
		f.done[lease.msg.Queue]++
	case "failed":
		f.ready[lease.msg.Queue] = append(f.ready[lease.msg.Queue], lease.msg)
	case "dlq":
		f.dlq[lease.msg.Queue] = append(f.dlq[lease.msg.Queue], lease.msg)
		dlq = true
	}
	return map[string]any{
		"index": index, "transactionId": a.TransactionID, "success": true,
		"error": nil, "leaseReleased": true, "dlq": dlq,
	}
}

func (f *fakeBroker) listQueues(w http.ResponseWriter) {
	out := []map[string]any{}
	for _, q := range f.queues {
		out = append(out, map[string]any{"name": q, "partitions": len(f.partitionsOf(q))})
	}
	writeJSON(w, 200, map[string]any{"queues": out})
}

func (f *fakeBroker) partitionsOf(queue string) []string {
	seen := map[string]bool{}
	var out []string
	for k := range f.partIDs {
		if strings.HasPrefix(k, queue+"/") {
			p := strings.TrimPrefix(k, queue+"/")
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (f *fakeBroker) listGroups(w http.ResponseWriter) {
	names := make([]string, 0, len(f.groups))
	for g := range f.groups {
		names = append(names, g)
	}
	sort.Strings(names)
	out := []map[string]any{}
	for _, g := range names {
		out = append(out, map[string]any{"name": g})
	}
	writeJSON(w, 200, map[string]any{"consumerGroups": out})
}

func (f *fakeBroker) depth(w http.ResponseWriter, queue string) {
	writeJSON(w, 200, map[string]any{
		"queue": queue, "depth": len(f.ready[queue]),
		"inFlight": f.inFlight(queue), "completed": f.done[queue], "dlq": len(f.dlq[queue]),
	})
}

func (f *fakeBroker) inFlight(queue string) int {
	n := 0
	for _, l := range f.claimed {
		if l.msg.Queue == queue {
			n++
		}
	}
	return n
}

func (f *fakeBroker) messages(w http.ResponseWriter, queue string) {
	out := []map[string]any{}
	for _, m := range f.ready[queue] {
		out = append(out, map[string]any{"transactionId": m.TxnID, "partition": m.Partition, "data": m.Payload, "state": "ready"})
	}
	for _, l := range f.claimed {
		if l.msg.Queue == queue {
			out = append(out, map[string]any{"transactionId": l.msg.TxnID, "partition": l.msg.Partition, "data": l.msg.Payload, "state": "claimed"})
		}
	}
	writeJSON(w, 200, map[string]any{"messages": out})
}

func (f *fakeBroker) dlqList(w http.ResponseWriter, queue string) {
	out := []map[string]any{}
	for _, m := range f.dlq[queue] {
		out = append(out, map[string]any{"transactionId": m.TxnID, "partition": m.Partition, "data": m.Payload})
	}
	writeJSON(w, 200, map[string]any{"messages": out})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
