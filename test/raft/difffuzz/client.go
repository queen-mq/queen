package main

// The wire client. One struct per broker under test; every call returns the
// HTTP status and the raw body, because a differential fuzzer compares BYTES
// (after normalization, normalize.go), not decoded structs: a field that only
// one side emits has to show up as a difference, and a typed decode would eat
// it silently.
//
// Wire shapes are taken from the master broker, not from an SDK:
//   POST /api/v1/push        {"items":[{queue,partition?,payload,transactionId?,traceId?}]}
//                            201, TOP-LEVEL ARRAY [{index,message_id,transaction_id,queueName,status,offset?}]
//   GET  /api/v1/pop/queue/:q?consumerGroup=&batch=&partitions=&wait=&timeout=&autoAck=&leaseSeconds=
//                            200 {"success",queue,partition,partitionId,leaseId,consumerGroup,
//                                 "messages":[{id,transactionId,traceId,data,createdAt,partitionId,
//                                              partition,leaseId,consumerGroup,deliveryAttempt,offset?}]}
//   POST /api/v1/ack         {transactionId,partitionId,status,consumerGroup?,leaseId?,error?}
//                            TOP-LEVEL ARRAY [{index,transactionId,success,error,leaseReleased,dlq}]
//   POST /api/v1/ack/batch   {consumerGroup?,acknowledgments:[{transactionId,partitionId,status,leaseId?,error?}]}
//   POST /api/v1/transaction {operations:[...], kv:[...], timers:[...]}   (riders are TOP-LEVEL arrays)
//   POST /api/v1/kv          TOP-LEVEL ARRAY of ops   (also accepts {"operations":[...]})
//   POST /api/v1/timers      TOP-LEVEL ARRAY of ops   (also accepts {"operations":[...]})
//   POST /api/v1/configure   {queue, mode?:"replace", options:{...}}
//
// I15 of the plan applies to harnesses too: every call carries a deadline.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Resp is one answer from a broker: what a fuzzer can compare.
type Resp struct {
	Status  int             `json:"status"`
	Body    json.RawMessage `json:"body"`
	Elapsed time.Duration   `json:"-"`
}

// Client talks to one broker.
type Client struct {
	Name   string // "A" (postgres/single) or "B" (raft1)
	Base   string // http://host:port, no trailing slash
	Tenant string // x-queen-tenant, empty for untenanted deployments
	Token  string // Authorization: Bearer <token>, empty when auth is off
	HTTP   *http.Client
}

// NewClient builds a client with a bounded connection pool and a hard deadline
// on every request. The pool is sized for one goroutine per side: this fuzzer
// is deliberately sequential, because a differential run whose operations race
// each other cannot tell a real divergence from a scheduling accident.
func NewClient(name, base string, timeout time.Duration) *Client {
	base = strings.TrimRight(base, "/")
	return &Client{
		Name: name,
		Base: base,
		HTTP: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
				MaxIdleConns:        8,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*Resp, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("%s: marshal %s %s: %w", c.Name, method, path, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("%s: request %s %s: %w", c.Name, method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Tenant != "" {
		req.Header.Set("x-queen-tenant", c.Tenant)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	start := time.Now()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %s %s: %w", c.Name, method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("%s: %s %s: read body: %w", c.Name, method, path, err)
	}
	out := &Resp{Status: resp.StatusCode, Elapsed: time.Since(start)}
	if len(bytes.TrimSpace(raw)) == 0 {
		out.Body = json.RawMessage("null")
	} else if json.Valid(raw) {
		out.Body = json.RawMessage(raw)
	} else {
		// A non-JSON body is data, not a crash: it is exactly the kind of
		// difference this harness exists to catch, so it is carried through
		// as a JSON string rather than turned into an error.
		q, _ := json.Marshal(string(raw))
		out.Body = json.RawMessage(q)
	}
	return out, nil
}

// ----------------------------------------------------------------- write path

// PushItem is one item of POST /api/v1/push.
type PushItem struct {
	Queue         string          `json:"queue"`
	Partition     string          `json:"partition,omitempty"`
	Payload       json.RawMessage `json:"payload"`
	TransactionID string          `json:"transactionId,omitempty"`
	TraceID       string          `json:"traceId,omitempty"`
}

func (c *Client) Push(ctx context.Context, items []PushItem) (*Resp, error) {
	return c.do(ctx, http.MethodPost, "/api/v1/push", map[string]any{"items": items})
}

// PopQuery is the query string of every pop route. Zero values are omitted, so
// the request sent at the defaults is byte-identical to the one an SDK at its
// defaults sends (the reason the broker distinguishes absent from 0).
type PopQuery struct {
	ConsumerGroup    string
	Batch            int
	Partitions       int
	AutoAck          bool
	Wait             bool
	TimeoutMS        int
	LeaseSeconds     int
	SubscriptionMode string // "new" | "all"
	Namespace        string // discovery route only
	Task             string // discovery route only
}

func (p PopQuery) encode() string {
	v := url.Values{}
	if p.ConsumerGroup != "" {
		v.Set("consumerGroup", p.ConsumerGroup)
	}
	if p.Batch > 0 {
		v.Set("batch", strconv.Itoa(p.Batch))
	}
	if p.Partitions > 0 {
		v.Set("partitions", strconv.Itoa(p.Partitions))
	}
	if p.AutoAck {
		v.Set("autoAck", "true")
	}
	if p.Wait {
		v.Set("wait", "true")
	}
	if p.TimeoutMS > 0 {
		v.Set("timeout", strconv.Itoa(p.TimeoutMS))
	}
	if p.LeaseSeconds > 0 {
		v.Set("leaseSeconds", strconv.Itoa(p.LeaseSeconds))
	}
	if p.SubscriptionMode != "" {
		v.Set("subscriptionMode", p.SubscriptionMode)
	}
	if p.Namespace != "" {
		v.Set("namespace", p.Namespace)
	}
	if p.Task != "" {
		v.Set("task", p.Task)
	}
	if len(v) == 0 {
		return ""
	}
	return "?" + v.Encode()
}

func (c *Client) Pop(ctx context.Context, queue string, q PopQuery) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/pop/queue/"+url.PathEscape(queue)+q.encode(), nil)
}

func (c *Client) PopPartition(ctx context.Context, queue, partition string, q PopQuery) (*Resp, error) {
	path := "/api/v1/pop/queue/" + url.PathEscape(queue) + "/partition/" + url.PathEscape(partition)
	return c.do(ctx, http.MethodGet, path+q.encode(), nil)
}

// PopDiscover is the namespace/task route (no queue in the path).
func (c *Client) PopDiscover(ctx context.Context, q PopQuery) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/pop"+q.encode(), nil)
}

// AckItem is one acknowledgment. Status is "completed" | "failed" | "dlq"
// (the broker also accepts "retry"); Error is the nack reason recorded on the
// DLQ row when a nack exhausts the retry budget.
type AckItem struct {
	TransactionID string `json:"transactionId"`
	PartitionID   string `json:"partitionId"`
	Status        string `json:"status"`
	LeaseID       string `json:"leaseId,omitempty"`
	ConsumerGroup string `json:"consumerGroup,omitempty"`
	Error         string `json:"error,omitempty"`
}

func (c *Client) Ack(ctx context.Context, a AckItem) (*Resp, error) {
	return c.do(ctx, http.MethodPost, "/api/v1/ack", a)
}

func (c *Client) AckBatch(ctx context.Context, group string, items []AckItem) (*Resp, error) {
	body := map[string]any{"acknowledgments": items}
	if group != "" {
		body["consumerGroup"] = group
	}
	return c.do(ctx, http.MethodPost, "/api/v1/ack/batch", body)
}

// TxnBody is POST /api/v1/transaction. `kv` and `timers` are TOP-LEVEL arrays
// beside `operations`, never nested inside it: a rider one level too deep still
// answers 200 and simply has no gate (test/runners/http/kv-timers-wire.sh).
type TxnBody struct {
	Operations []json.RawMessage `json:"operations"`
	KV         []json.RawMessage `json:"kv,omitempty"`
	Timers     []json.RawMessage `json:"timers,omitempty"`
}

func (c *Client) Transaction(ctx context.Context, b TxnBody) (*Resp, error) {
	if b.Operations == nil {
		b.Operations = []json.RawMessage{}
	}
	return c.do(ctx, http.MethodPost, "/api/v1/transaction", b)
}

// KV posts a batch of KV ops as the top-level array form.
func (c *Client) KV(ctx context.Context, ops []json.RawMessage) (*Resp, error) {
	if ops == nil {
		ops = []json.RawMessage{}
	}
	return c.do(ctx, http.MethodPost, "/api/v1/kv", ops)
}

// Timers posts a batch of timer ops as the top-level array form.
func (c *Client) Timers(ctx context.Context, ops []json.RawMessage) (*Resp, error) {
	if ops == nil {
		ops = []json.RawMessage{}
	}
	return c.do(ctx, http.MethodPost, "/api/v1/timers", ops)
}

// Configure merges `options` into the queue's stored configuration. replace=true
// sends the top-level "mode":"replace" directive; the default (merge) sends NO
// mode key at all, which is the byte-identical default every SDK sends.
func (c *Client) Configure(ctx context.Context, queue string, options map[string]any, replace bool) (*Resp, error) {
	body := map[string]any{"queue": queue}
	if replace {
		body["mode"] = "replace"
	}
	if options != nil {
		body["options"] = options
	}
	return c.do(ctx, http.MethodPost, "/api/v1/configure", body)
}

// LeaseExtend renews a lease (POST /api/v1/lease/:leaseId/extend).
func (c *Client) LeaseExtend(ctx context.Context, leaseID string, seconds int) (*Resp, error) {
	body := map[string]any{}
	if seconds > 0 {
		body["leaseSeconds"] = seconds
	}
	return c.do(ctx, http.MethodPost, "/api/v1/lease/"+url.PathEscape(leaseID)+"/extend", body)
}

// ------------------------------------------------------------------ read path
//
// The views compared at the end of a run (§13.4: "the final views"). Every one
// of them is a plain GET/POST with no side effect, so a comparison run can be
// repeated without changing what it observes.

func (c *Client) Health(ctx context.Context) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/health", nil)
}

func (c *Client) ViewQueue(ctx context.Context, queue string) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/resources/queues/"+url.PathEscape(queue), nil)
}

func (c *Client) ViewDepth(ctx context.Context, queue string) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/resources/queues/"+url.PathEscape(queue)+"/depth", nil)
}

func (c *Client) ViewQueues(ctx context.Context) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/resources/queues", nil)
}

func (c *Client) ViewConsumerGroups(ctx context.Context) (*Resp, error) {
	return c.do(ctx, http.MethodGet, "/api/v1/consumer-groups", nil)
}

func (c *Client) ViewMessages(ctx context.Context, queue string, limit int) (*Resp, error) {
	v := url.Values{}
	if queue != "" {
		v.Set("queue", queue)
	}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	return c.do(ctx, http.MethodGet, "/api/v1/messages?"+v.Encode(), nil)
}

func (c *Client) ViewDLQ(ctx context.Context, queue string, limit int) (*Resp, error) {
	v := url.Values{}
	if queue != "" {
		v.Set("queue", queue)
	}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	return c.do(ctx, http.MethodGet, "/api/v1/dlq?"+v.Encode(), nil)
}

// ViewKVList is a POST because its cursor is a KEY, and a key in a query string
// lands in every access log between the browser and the database.
func (c *Client) ViewKVList(ctx context.Context, ns, prefix string, limit int) (*Resp, error) {
	body := map[string]any{"ns": ns}
	if prefix != "" {
		body["prefix"] = prefix
	}
	if limit > 0 {
		body["limit"] = limit
	}
	return c.do(ctx, http.MethodPost, "/api/v1/resources/kv/list", body)
}

func (c *Client) ViewTimers(ctx context.Context, queue string, limit int) (*Resp, error) {
	v := url.Values{}
	if limit > 0 {
		v.Set("limit", strconv.Itoa(limit))
	}
	return c.do(ctx, http.MethodGet, "/api/v1/timers/"+url.PathEscape(queue)+"?"+v.Encode(), nil)
}
