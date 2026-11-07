package feedrt

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// Live state of a running feed stream, read from outside its process
// (query.js). Each active runner (each shard) answers questions on an
// ephemeral queue of its own, queen-feed-q.<hash of the queryId>.<shard>; the
// asker mints an inbox, sends the question with it as replyTo, and waits there.

const (
	defaultViewTimeoutMillis = 5000
	serveWaitMillis          = 10000
)

func sha256Hex(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// QueryQueueName is the ephemeral queue the runner of queryID (shard index) answers on.
func QueryQueueName(queryID string, index int) string {
	return "queen-feed-q." + sha256Hex("queen-feed\n" + queryID)[:16] + "." + strconv.Itoa(index)
}

// stateServer is the loop that answers questions for one active runner.
type stateServer struct {
	eph        Ephemeral
	queue      string
	answer     func(req map[string]interface{}) map[string]interface{}
	waitMillis int

	mu      sync.Mutex
	stopped bool
	running bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func newStateServer(eph Ephemeral, queue string, answer func(map[string]interface{}) map[string]interface{}, waitMillis int) *stateServer {
	if waitMillis <= 0 {
		waitMillis = serveWaitMillis
	}
	return &stateServer{eph: eph, queue: queue, answer: answer, waitMillis: waitMillis}
}

func (s *stateServer) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running || s.eph == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.stopped, s.running, s.cancel, s.done = false, true, cancel, make(chan struct{})
	go s.run(ctx, s.done)
}

// stop stops after the long poll in hand; not waited for.
func (s *stateServer) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *stateServer) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

func (s *stateServer) run(ctx context.Context, done chan struct{}) {
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
		close(done)
	}()
	backoff := 250 * time.Millisecond
	warned := false
	for !s.isStopped() {
		got, err := s.eph.Pop(ctx, s.queue, true, s.waitMillis, 32, true)
		if err != nil {
			if s.isStopped() {
				return
			}
			if errors.Is(err, ErrEphemeralUnsupported) {
				logWarn("FeedStateServer.unsupported", map[string]interface{}{"queue": s.queue, "error": err.Error()})
				return
			}
			if !warned {
				warned = true
				logWarn("FeedStateServer.pop", map[string]interface{}{"queue": s.queue, "error": err.Error()})
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			continue
		}
		backoff = 250 * time.Millisecond
		for _, payload := range got {
			req, ok := payload.(map[string]interface{})
			if !ok {
				continue
			}
			replyTo, ok := req["replyTo"].(string)
			if !ok {
				continue
			}
			reply := s.safeAnswer(req)
			out := copyMap(reply)
			setDef(out, "id", jsv.Get(req, "id"))
			if _, has := req["id"]; !has {
				delete(out, "id")
			}
			pctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.eph.Push(pctx, replyTo, []interface{}{out}); err != nil {
				logWarn("FeedStateServer.reply", map[string]interface{}{"queue": s.queue, "error": err.Error()})
			}
			cancel()
		}
	}
}

func (s *stateServer) safeAnswer(req map[string]interface{}) (reply map[string]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			reply = map[string]interface{}{"error": fmt.Sprint(r)}
		}
	}()
	return s.answer(req)
}

// FeedView asks the runners of a stream about their live state (Stream.view).
type FeedView struct {
	broker  Broker
	queryID string
	shards  int
	timeout time.Duration
}

// NewView is Stream.view(queen, queryId, { shards, timeoutMillis }): shards 0
// reads the stream's layout from KV (else 1); timeoutMillis 0 is 5000.
func NewView(b Broker, queryID string, shards int, timeoutMillis int) (*FeedView, error) {
	if b == nil || b.Ephemeral() == nil {
		return nil, errors.New("Stream.view(queen, queryId): pass the Queen client (it asks over ephemeral queues)")
	}
	if queryID == "" {
		return nil, errors.New("Stream.view: queryId is the stream's queryId")
	}
	if timeoutMillis <= 0 {
		timeoutMillis = defaultViewTimeoutMillis
	}
	return &FeedView{broker: b, queryID: queryID, shards: shards, timeout: time.Duration(timeoutMillis) * time.Millisecond}, nil
}

// ViewTimeoutError: no runner answered in time.
type ViewTimeoutError struct{ Msg string }

func (e *ViewTimeoutError) Error() string { return e.Msg }

// Code is "feed_view_timeout".
func (e *ViewTimeoutError) Code() string { return "feed_view_timeout" }

// Get is the value of one key in one step: {found, value?, shard?, position?}.
func (v *FeedView) Get(ctx context.Context, step, key string) (map[string]interface{}, error) {
	answers, err := v.ask(ctx, map[string]interface{}{"op": "get", "step": step, "key": key})
	if err != nil {
		return nil, err
	}
	for _, a := range answers {
		if jsv.Truthy(a["found"]) {
			out := map[string]interface{}{"found": true}
			setDef(out, "value", jsv.Get(a, "value"))
			setDef(out, "shard", jsv.Get(a, "shard"))
			setDef(out, "position", jsv.Get(a, "position"))
			return out, nil
		}
	}
	var firstErr interface{}
	allErr := true
	for _, a := range answers {
		if e := a["error"]; jsv.Truthy(e) {
			if firstErr == nil {
				firstErr = e
			}
		} else {
			allErr = false
		}
	}
	if firstErr != nil && allErr {
		return nil, fmt.Errorf("Stream.view: %s", jsv.String(firstErr))
	}
	return map[string]interface{}{"found": false, "answered": float64(len(answers))}, nil
}

// List is up to limit entries of one step, from every shard: {items, truncated}.
func (v *FeedView) List(ctx context.Context, step string, limit int) (map[string]interface{}, error) {
	if limit <= 0 {
		limit = 100
	}
	answers, err := v.ask(ctx, map[string]interface{}{"op": "list", "step": step, "limit": float64(limit)})
	if err != nil {
		return nil, err
	}
	items := []interface{}{}
	truncated := false
	for _, a := range answers {
		if e := a["error"]; jsv.Truthy(e) {
			return nil, fmt.Errorf("Stream.view: %s", jsv.String(e))
		}
		if l, ok := a["items"].([]interface{}); ok {
			items = append(items, l...)
		}
		truncated = truncated || jsv.Truthy(a["truncated"])
	}
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	return map[string]interface{}{"items": items, "truncated": truncated || more, "answered": float64(len(answers))}, nil
}

// Status is every shard's runner: role, position, lag, open state, per step.
func (v *FeedView) Status(ctx context.Context) ([]map[string]interface{}, error) {
	return v.ask(ctx, map[string]interface{}{"op": "status"})
}

func (v *FeedView) shardCount(ctx context.Context) int {
	if v.shards > 0 {
		return v.shards
	}
	row, err := v.broker.KVGet(ctx, PositionNamespace, v.queryID+"#layout")
	if err == nil && row.Found {
		if n := jsv.Get(row.Value, "shards"); jsv.IsInteger(n) {
			f, _ := jsv.Num(n)
			return int(f)
		}
	}
	return 1
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (v *FeedView) ask(ctx context.Context, question map[string]interface{}) ([]map[string]interface{}, error) {
	count := v.shardCount(ctx)
	inbox := "queen-feed-r." + randHex(8)
	id := randHex(6)
	eph := v.broker.Ephemeral()
	var wg sync.WaitGroup
	errs := make([]error, count)
	for i := 0; i < count; i++ {
		idx := 0
		if count > 1 {
			idx = i
		}
		q := copyMap(question)
		q["id"], q["replyTo"] = id, inbox
		wg.Add(1)
		go func(i, idx int) {
			defer wg.Done()
			errs[i] = eph.Push(ctx, QueryQueueName(v.queryID, idx), []interface{}{q})
		}(i, idx)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	var answers []map[string]interface{}
	deadline := time.Now().Add(v.timeout)
	for len(answers) < count {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		wait := left
		if wait > 30*time.Second {
			wait = 30 * time.Second
		}
		ms := int(wait / time.Millisecond)
		if ms < 1 {
			ms = 1
		}
		got, err := eph.Pop(ctx, inbox, true, ms, count, true)
		if err != nil {
			return nil, err
		}
		for _, p := range got {
			if m, ok := p.(map[string]interface{}); ok {
				if s, _ := m["id"].(string); s == id {
					answers = append(answers, m)
				}
			}
		}
	}
	// The inbox goes away with the broker's own cleanup too; this is a courtesy.
	go func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eph.Delete(dctx, inbox)
	}()
	if len(answers) == 0 {
		return nil, &ViewTimeoutError{Msg: fmt.Sprintf("Stream.view: no runner of %s answered within %d ms (is it running, with serveState on?)",
			v.queryID, int(v.timeout/time.Millisecond))}
	}
	return answers, nil
}
