package feed

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// FeedOnLeader is the code of the 421 a cluster's leader answers a feed read with.
const FeedOnLeader = "feed_on_leader"

// DefaultWaitMillis is the long poll of each read while caught up.
const DefaultWaitMillis = 5000

// MaxWaitMillis is the longest long poll the broker holds a read; the HTTP timeout is longer.
const MaxWaitMillis = 30000

// Poster sends one feed read to one node: the JSON body to POST /api/v1/feed,
// with a request timeout, and answers the response body. A refusal comes back
// as a *StatusError (anything else is a network failure); a cancelled ctx
// comes back as the ctx's error.
type Poster interface {
	PostFeed(ctx context.Context, body []byte, timeout time.Duration) ([]byte, error)
}

// ClientFor answers a Poster to one node (a follower a leader named), with the
// configured client's credentials and timeouts. A Poster that is an io.Closer
// is closed with the reader.
type ClientFor func(baseURL string) (Poster, error)

// StatusError is an HTTP answer that is not a success: its status, its code
// and its parsed body (a leader's 421 names the followers to ask).
type StatusError struct {
	Status  int
	Code    string
	Message string
	Body    map[string]interface{}
	// Err is the error this was read from.
	Err error
}

func (e *StatusError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("HTTP %d", e.Status)
}

func (e *StatusError) Unwrap() error { return e.Err }

// ErrNoFeedRoute is matched (errors.Is) by the error of a broker older than
// the feed: it answered 404 to POST /api/v1/feed.
var ErrNoFeedRoute = errors.New("no_feed_route")

// NoFeedRouteError is a 404 from the feed route: the broker predates the feed.
// A runner stops on it instead of retrying forever.
type NoFeedRouteError struct {
	Cause error
}

func (e *NoFeedRouteError) Error() string {
	return "feed: this broker has no POST /api/v1/feed (it answered 404): it is older than 2.8.0, " +
		"the first broker with the tenant feed."
}

// Code is "no_feed_route".
func (e *NoFeedRouteError) Code() string { return "no_feed_route" }

// Unwrap answers ErrNoFeedRoute and the cause.
func (e *NoFeedRouteError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrNoFeedRoute}
	}
	return []error{ErrNoFeedRoute, e.Cause}
}

// Shard asks for one slice of the partitions: the ones whose name hashes
// (FNV-1a, 32 bit, UTF-8) to Index of Count. With Queues, only those queues
// are split and the others are read whole.
type Shard struct {
	Count  int      `json:"count"`
	Index  int      `json:"index"`
	Queues []string `json:"queues,omitempty"`
}

// Options are the reader's options (JS queen.feed(options)).
type Options struct {
	// Queues: names or patterns (`orders*`); nil: every queue.
	Queues []string
	// Partitions: partition names; nil: every partition.
	Partitions []string
	// Where: conditions sent to the broker.
	Where []Condition
	// Kinds: entry kinds (the broker's; "push").
	Kinds []string
	// From: the first position: nil (default "now", or "earliest" with Since),
	// "now", "earliest", or a position (a whole number from 0).
	From interface{}
	// Since: only messages created at or after it: a time.Time, epoch
	// milliseconds, an ISO time, or a duration back from now ("1d").
	Since interface{}
	// Follow: keep waiting for new entries (nil: true); false stops the
	// iteration once caught up.
	Follow *bool
	// WaitMillis: the long poll of each read while caught up (nil: 5000, at most 30000).
	WaitMillis *int
	// MaxRecords: messages per read (0: the broker's default, 1000).
	MaxRecords int
	// MaxBytes: payload bytes per read (0: the broker's default, 1 MiB).
	MaxBytes int
	// FollowerURLs: nodes to read from first, e.g. one follower's URL.
	FollowerURLs []string
	// Shard: one slice of the partitions.
	Shard *Shard
}

// Entry is one feed entry: the message in Queen's shape, plus its position.
type Entry struct {
	Position      int64
	Kind          string
	Queue         string
	Partition     string
	PartitionID   string
	Offset        int64
	ID            string
	TransactionID string
	// Data is the payload, decoded (numbers are float64).
	Data interface{}
	// TS is when the log committed it (ISO, microseconds); CreatedAt is the same.
	TS        string
	CreatedAt string
}

// Map is the entry as the JS SDK hands it out: position, kind, queue,
// partition, partitionId, offset, id, transactionId, data, ts, createdAt
// (numbers as float64). Stream functions get messages in this shape.
func (e Entry) Map() map[string]interface{} {
	return map[string]interface{}{
		"position":      float64(e.Position),
		"kind":          e.Kind,
		"queue":         e.Queue,
		"partition":     e.Partition,
		"partitionId":   e.PartitionID,
		"offset":        float64(e.Offset),
		"id":            e.ID,
		"transactionId": e.TransactionID,
		"data":          e.Data,
		"ts":            e.TS,
		"createdAt":     e.CreatedAt,
	}
}

// Page is one read: the entries, where the next read starts, the broker's
// clock as of the read, whether it is caught up, and its applied index.
type Page struct {
	Entries    []Entry
	Next       int64
	ResolvedTS string
	CaughtUp   bool
	// Applied is the node's applied index (nil when the answer had none).
	Applied *int64
}

type follower struct {
	url    string
	client Poster
}

// Reader reads the feed by position. Safe for one reader goroutine at a time
// (Seek, Position and Stop may come from others).
type Reader struct {
	http      Poster
	clientFor ClientFor
	scheme    string

	queues     []string
	partitions []string
	where      []Condition
	kinds      []string
	since      string
	hasSince   bool
	follow     bool
	waitMillis int
	maxRecords int
	maxBytes   int
	shard      *Shard

	mu           sync.Mutex
	position     interface{} // "now" | "earliest" | int64
	stopped      bool
	followers    []follower
	followerAt   int
	fallbackTill time.Time
	// ahead is the page being read while the caller works on the one before (nil: none).
	ahead *readAhead
}

func names(v []string, what string) ([]string, error) {
	for _, x := range v {
		if x == "" {
			return nil, fmt.Errorf("feed: %s must be names (strings), got \"\"", what)
		}
	}
	if len(v) == 0 {
		return nil, nil
	}
	return append([]string(nil), v...), nil
}

func clampWait(n int) int {
	if n < 0 {
		return 0
	}
	if n > MaxWaitMillis {
		return MaxWaitMillis
	}
	return n
}

// PositionOf reads a position the way the reader takes one: "now", "earliest",
// or a whole number from 0 (any Go number kind). ok false otherwise.
func PositionOf(v interface{}) (interface{}, bool) {
	switch p := v.(type) {
	case string:
		if p == "now" || p == "earliest" {
			return p, true
		}
		return nil, false
	}
	if jsv.IsInteger(v) {
		f, _ := jsv.Num(v)
		if f >= 0 {
			return int64(f), true
		}
	}
	return nil, false
}

func shardOption(s *Shard) (*Shard, error) {
	if s == nil {
		return nil, nil
	}
	if s.Count < 1 || s.Count > 4096 {
		return nil, fmt.Errorf("feed: shard.count is a whole number from 1 to 4096, got %d", s.Count)
	}
	if s.Index < 0 || s.Index >= s.Count {
		return nil, fmt.Errorf("feed: shard.index is from 0 to %d, got %d", s.Count-1, s.Index)
	}
	out := &Shard{Count: s.Count, Index: s.Index}
	q, err := names(s.Queues, "shard.queues")
	if err != nil {
		return nil, err
	}
	out.Queues = q
	return out, nil
}

// NewReader builds a reader over http (the configured client), clientFor (a
// client to one node) and scheme ("http" or "https", from the configured URL).
func NewReader(http Poster, clientFor ClientFor, scheme string, o Options) (*Reader, error) {
	if scheme == "" {
		scheme = "http"
	}
	r := &Reader{http: http, clientFor: clientFor, scheme: scheme, follow: true, waitMillis: DefaultWaitMillis}
	var err error
	if r.queues, err = names(o.Queues, "queues"); err != nil {
		return nil, err
	}
	if r.partitions, err = names(o.Partitions, "partitions"); err != nil {
		return nil, err
	}
	r.where = append([]Condition(nil), o.Where...)
	r.kinds = o.Kinds
	if o.Since != nil && !jsv.IsUndefined(o.Since) {
		if r.since, err = SinceToISO(o.Since, time.Now()); err != nil {
			return nil, err
		}
		r.hasSince = true
	}
	if o.Follow != nil {
		r.follow = *o.Follow
	}
	if o.WaitMillis != nil {
		r.waitMillis = clampWait(*o.WaitMillis)
	}
	r.maxRecords = o.MaxRecords
	r.maxBytes = o.MaxBytes
	if r.shard, err = shardOption(o.Shard); err != nil {
		return nil, err
	}
	for _, u := range o.FollowerURLs {
		c, err := clientFor(u)
		if err != nil {
			return nil, err
		}
		r.followers = append(r.followers, follower{url: u, client: c})
	}
	switch {
	case o.From != nil && !jsv.IsUndefined(o.From):
		p, ok := PositionOf(o.From)
		if !ok {
			return nil, fmt.Errorf("feed: from must be a position, 'now' or 'earliest', got %s", jsv.MustMarshal(o.From))
		}
		r.position = p
	case r.hasSince:
		r.position = "earliest"
	default:
		r.position = "now"
	}
	return r, nil
}

// Position is where the next read starts: "now", "earliest", or (once a read
// has answered) a position, as an int64.
func (r *Reader) Position() interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.position
}

// PositionInt is Position as a number, ok false while it is "now" or "earliest".
func (r *Reader) PositionInt() (int64, bool) {
	p, ok := r.Position().(int64)
	return p, ok
}

// Seek moves the reader (a runner whose commit lost goes back to the committed position).
func (r *Reader) Seek(position interface{}) {
	if p, ok := PositionOf(position); ok {
		r.mu.Lock()
		r.position = p
		r.dropAhead()
		r.mu.Unlock()
	}
}

// readAhead is a read started early: the page at `from`, read while the
// caller works on the page before it.
type readAhead struct {
	from   int64
	done   chan struct{}
	page   Page
	err    error
	cancel context.CancelFunc
}

// Read reads one page from the reader's position and moves the reader to its
// Next. While nothing is new it waits up to waitMillis (nil: the reader's own
// waitMillis). Cancelling ctx cancels the read; the reader stays where it was.
// While the answers are not caught up, the next page is read at once, so it
// comes in while the caller works on this one.
func (r *Reader) Read(ctx context.Context, waitMillis *int) (Page, error) {
	wait := r.waitMillis
	if waitMillis != nil {
		wait = clampWait(*waitMillis)
	}
	r.mu.Lock()
	from := r.position
	ahead := r.ahead
	r.ahead = nil
	r.mu.Unlock()
	page, ok, err := r.takeAhead(ctx, ahead, from)
	if err != nil {
		return Page{}, err
	}
	if !ok {
		if page, err = r.fetch(ctx, from, wait); err != nil {
			return Page{}, err
		}
	}
	r.mu.Lock()
	r.position = page.Next
	if !page.CaughtUp && !r.stopped {
		r.ahead = r.readOn(page.Next)
	}
	r.mu.Unlock()
	return page, nil
}

// takeAhead answers the page read ahead at `from`, when there is one and it
// holds something: an empty read that was caught up waited for nothing, so
// this read waits as asked. A cancelled ctx keeps it for the next read.
func (r *Reader) takeAhead(ctx context.Context, a *readAhead, from interface{}) (Page, bool, error) {
	if a == nil {
		return Page{}, false, nil
	}
	if p, ok := from.(int64); !ok || p != a.from {
		a.cancel()
		return Page{}, false, nil
	}
	select {
	case <-a.done:
	case <-ctx.Done():
		r.mu.Lock()
		if r.ahead == nil && !r.stopped {
			r.ahead = a
		} else {
			a.cancel()
		}
		r.mu.Unlock()
		return Page{}, false, ctx.Err()
	}
	a.cancel()
	if a.err != nil || (len(a.page.Entries) == 0 && a.page.CaughtUp) {
		return Page{}, false, nil
	}
	return a.page, true, nil
}

// readOn starts reading the page at `from`, without a long poll, for the next
// Read. r.mu is held.
func (r *Reader) readOn(from int64) *readAhead {
	ctx, cancel := context.WithCancel(context.Background())
	a := &readAhead{from: from, done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(a.done)
		a.page, a.err = r.fetch(ctx, from, 0)
	}()
	return a
}

// dropAhead cancels the read started ahead, if any. r.mu is held.
func (r *Reader) dropAhead() {
	if r.ahead != nil {
		r.ahead.cancel()
		r.ahead = nil
	}
}

// fetch reads the page at `from`, waiting up to `wait` ms while nothing is new.
func (r *Reader) fetch(ctx context.Context, from interface{}, wait int) (Page, error) {
	body := jsv.Ordered{{K: "from", V: from}, {K: "maxWaitMs", V: float64(wait)}}
	if r.queues != nil {
		body = append(body, jsv.Pair{K: "queues", V: r.queues})
	}
	if r.partitions != nil {
		body = append(body, jsv.Pair{K: "partitions", V: r.partitions})
	}
	if len(r.where) > 0 {
		list := make([]interface{}, len(r.where))
		for i, c := range r.where {
			list[i] = jsv.RawJSON(mustJSON(c))
		}
		body = append(body, jsv.Pair{K: "where", V: list})
	}
	if r.kinds != nil {
		body = append(body, jsv.Pair{K: "kinds", V: r.kinds})
	}
	if r.hasSince {
		body = append(body, jsv.Pair{K: "since", V: r.since})
	}
	if r.maxRecords != 0 {
		body = append(body, jsv.Pair{K: "maxRecords", V: float64(r.maxRecords)})
	}
	if r.maxBytes != 0 {
		body = append(body, jsv.Pair{K: "maxBytes", V: float64(r.maxBytes)})
	}
	if r.shard != nil {
		sh := jsv.Ordered{{K: "count", V: float64(r.shard.Count)}, {K: "index", V: float64(r.shard.Index)}}
		if r.shard.Queues != nil {
			sh = append(sh, jsv.Pair{K: "queues", V: r.shard.Queues})
		}
		body = append(body, jsv.Pair{K: "shard", V: sh})
	}
	raw, err := r.post(ctx, jsv.MustMarshal(body), time.Duration(wait+10000)*time.Millisecond)
	if err != nil {
		return Page{}, err
	}
	return decodePage(raw)
}

func mustJSON(c Condition) []byte {
	b, _ := c.MarshalJSON()
	return b
}

// Probe is one call that reads nothing (from "now") and leaves the reader
// where it is. It fails as a read would, a broker without the feed included,
// and answers the page (Next is the position a read from "now" starts at).
func (r *Reader) Probe(ctx context.Context) (Page, error) {
	raw, err := r.post(ctx, []byte(`{"from":"now","maxWaitMs":0,"maxRecords":1}`), 10*time.Second)
	if err != nil {
		return Page{}, err
	}
	return decodePage(raw)
}

// Entries iterates every entry, in log order. It stops once caught up when
// Follow is false, at Stop, at an error (yielded once) or when ctx is done.
func (r *Reader) Entries(ctx context.Context) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		for !r.isStopped() {
			w := r.waitMillis
			if !r.follow {
				w = 0
			}
			page, err := r.Read(ctx, &w)
			if err != nil {
				yield(Entry{}, err)
				return
			}
			for _, e := range page.Entries {
				if !yield(e, nil) {
					return
				}
				if r.isStopped() {
					return
				}
			}
			if !r.follow && page.CaughtUp {
				return
			}
		}
	}
}

func (r *Reader) isStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

// Stop ends the iteration after the entry in hand.
func (r *Reader) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.dropAhead()
	r.mu.Unlock()
}

// Close stops the reader and closes the clients it opened to followers.
func (r *Reader) Close() {
	r.mu.Lock()
	r.stopped = true
	r.dropAhead()
	fs := r.followers
	r.followers = nil
	r.mu.Unlock()
	for _, f := range fs {
		if c, ok := f.client.(interface{ Close() }); ok {
			c.Close()
		}
	}
}

var schemeRe = regexp.MustCompile(`^https?://`)

func (r *Reader) learn(followers interface{}) {
	list, _ := followers.([]interface{})
	seen := map[string]bool{}
	for _, f := range r.followers {
		seen[f.url] = true
	}
	for _, f := range list {
		m, _ := f.(map[string]interface{})
		addr, _ := m["httpAddr"].(string)
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		url := addr
		if !schemeRe.MatchString(addr) {
			url = r.scheme + "://" + addr
		}
		if seen[url] {
			continue
		}
		seen[url] = true
		c, err := r.clientFor(url)
		if err != nil {
			continue
		}
		r.followers = append(r.followers, follower{url: url, client: c})
	}
}

// post sends a read: each follower once, then the configured URL; a leader's
// 421 adds the followers it names.
func (r *Reader) post(ctx context.Context, body []byte, timeout time.Duration) ([]byte, error) {
	var last error
	for attempt := 0; ; attempt++ {
		r.mu.Lock()
		n := len(r.followers)
		if attempt >= n+3 {
			r.mu.Unlock()
			break
		}
		useFollower := n > 0 && !time.Now().Before(r.fallbackTill)
		var target *follower
		if useFollower {
			f := r.followers[r.followerAt%n]
			target = &f
		}
		r.mu.Unlock()
		client := r.http
		if target != nil {
			client = target.client
		}
		raw, err := client.PostFeed(ctx, body, timeout)
		if err == nil {
			return raw, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		last = err
		var se *StatusError
		status := 0
		if errors.As(err, &se) {
			status = se.Status
		}
		if status == 421 && se.Code == FeedOnLeader {
			// A leader: read from the followers it names (this node may lead now).
			r.mu.Lock()
			r.learn(se.Body["followers"])
			if target != nil {
				r.followerAt++
			}
			r.fallbackTill = time.Time{}
			r.mu.Unlock()
			continue
		}
		if target != nil && (status == 0 || status >= 500) {
			// That follower is gone or busy: the next one, then the configured URL.
			r.mu.Lock()
			r.followerAt++
			if len(r.followers) > 0 && r.followerAt%len(r.followers) == 0 {
				r.fallbackTill = time.Now().Add(5 * time.Second)
			}
			r.mu.Unlock()
			continue
		}
		if status == 404 {
			return nil, &NoFeedRouteError{Cause: err}
		}
		return nil, err
	}
	return nil, &AllLeadersError{Cause: last}
}

// AllLeadersError: every node asked is the leader or did not answer.
type AllLeadersError struct{ Cause error }

func (e *AllLeadersError) Error() string {
	return "feed: every node asked is the leader or did not answer; the feed is read from followers " +
		"(pass followerUrls, or run the broker with QUEEN_FEED_ON_LEADER=true on a test cluster)"
}

func (e *AllLeadersError) Unwrap() error { return e.Cause }
