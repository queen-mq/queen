package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// qhttp talks to one Queen broker over HTTP/1.1 keep-alive (followers forward to the leader, as goload's clients
// did in the 09-30 grid: process i drives broker i % 3). No retries anywhere: a failed request is counted and, on
// the message path, left to the lease (a lease that runs out redelivers).
type qhttp struct {
	base string
	hc   *http.Client
}

func newHTTP(base string, conns int, timeout time.Duration) *qhttp {
	tr := &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        conns,
		MaxIdleConnsPerHost: conns,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		WriteBufferSize:     64 << 10,
		ReadBufferSize:      64 << 10,
	}
	return &qhttp{base: strings.TrimRight(base, "/"), hc: &http.Client{Transport: tr, Timeout: timeout}}
}

type httpErr struct {
	code int
	body string
}

func (e *httpErr) Error() string { return fmt.Sprintf("HTTP %d: %s", e.code, e.body) }

// do issues one request; a status outside ok (default 2xx) is an *httpErr carrying the body.
func (q *qhttp) do(ctx context.Context, method, path string, body []byte, ok ...int) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, q.base+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := q.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	rb, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return resp.StatusCode, nil, err
	}
	code := resp.StatusCode
	if code >= 200 && code < 300 {
		return code, rb, nil
	}
	for _, c := range ok {
		if c == code {
			return code, rb, nil
		}
	}
	b := string(bytes.TrimSpace(rb))
	if len(b) > 300 {
		b = b[:300] + "..."
	}
	return code, rb, &httpErr{code, b}
}

// ---------------------------------------------------------------------------
// wire shapes (server/src/handlers/raft.rs, the pop/ack/transaction answers)

type qmsg struct {
	ID            string          `json:"id"`
	TransactionID string          `json:"transactionId"`
	PartitionID   string          `json:"partitionId"`
	Partition     string          `json:"partition"`
	LeaseID       string          `json:"leaseId"`
	Data          json.RawMessage `json:"data"`
}

type popResp struct {
	Messages []qmsg `json:"messages"`
}

type popReq struct {
	queue     string
	group     string // "" = queue mode
	subMode   string // subscriptionMode for a new group cursor ("all")
	batch     int
	width     int
	wait      bool
	timeoutMs int
	autoAck   bool
	leaseS    int
}

func (p popReq) path() string {
	var b strings.Builder
	b.WriteString("/api/v1/pop/queue/")
	b.WriteString(p.queue)
	b.WriteString("?batch=")
	b.WriteString(strconv.Itoa(p.batch))
	if p.width > 1 {
		b.WriteString("&partitions=")
		b.WriteString(strconv.Itoa(p.width))
	}
	b.WriteString("&wait=")
	b.WriteString(strconv.FormatBool(p.wait))
	if p.wait {
		b.WriteString("&timeout=")
		b.WriteString(strconv.Itoa(p.timeoutMs))
	}
	b.WriteString("&autoAck=")
	b.WriteString(strconv.FormatBool(p.autoAck))
	if p.leaseS > 0 {
		b.WriteString("&leaseSeconds=")
		b.WriteString(strconv.Itoa(p.leaseS))
	}
	if p.group != "" {
		b.WriteString("&consumerGroup=")
		b.WriteString(p.group)
	}
	if p.subMode != "" {
		b.WriteString("&subscriptionMode=")
		b.WriteString(p.subMode)
	}
	return b.String()
}

// pop returns the messages of one pop (none on 204).
func (q *qhttp) pop(ctx context.Context, p popReq) ([]qmsg, error) {
	code, rb, err := q.do(ctx, "GET", p.path(), nil)
	if err != nil {
		return nil, err
	}
	if code == http.StatusNoContent || len(rb) == 0 {
		return nil, nil
	}
	var r popResp
	if err := json.Unmarshal(rb, &r); err != nil {
		return nil, fmt.Errorf("pop answer: %v", err)
	}
	return r.Messages, nil
}

// ackBody renders /api/v1/ack/batch for msgs (status completed).
func ackBody(msgs []qmsg, group string) []byte {
	b := make([]byte, 0, 64+len(msgs)*190)
	b = append(b, `{"acknowledgments":[`...)
	for i := range msgs {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendAckOp(b, &msgs[i], false)
	}
	b = append(b, ']')
	if group != "" {
		b = append(b, `,"consumerGroup":"`...)
		b = append(b, group...)
		b = append(b, '"')
	}
	return append(b, '}')
}

func appendAckOp(b []byte, m *qmsg, typed bool) []byte {
	b = append(b, '{')
	if typed {
		b = append(b, `"type":"ack",`...)
	}
	b = append(b, `"transactionId":"`...)
	b = append(b, m.TransactionID...)
	b = append(b, `","partitionId":"`...)
	b = append(b, m.PartitionID...)
	b = append(b, `","leaseId":"`...)
	b = append(b, m.LeaseID...)
	b = append(b, `","status":"completed"}`...)
	return b
}

type ackItem struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// ackBatch acks msgs; returns how many the broker settled.
func (q *qhttp) ackBatch(ctx context.Context, msgs []qmsg, group string) (int, error) {
	_, rb, err := q.do(ctx, "POST", "/api/v1/ack/batch", ackBody(msgs, group))
	if err != nil {
		return 0, err
	}
	var items []ackItem
	if err := json.Unmarshal(rb, &items); err != nil {
		return 0, fmt.Errorf("ack answer: %v", err)
	}
	ok := 0
	var first string
	for _, it := range items {
		if it.Success {
			ok++
		} else if first == "" {
			first = it.Error
		}
	}
	if ok < len(msgs) {
		return ok, fmt.Errorf("%d of %d acks refused, first: %s", len(msgs)-ok, len(msgs), first)
	}
	return ok, nil
}

type txnResp struct {
	Success *bool  `json:"success"`
	Reason  string `json:"reason"`
	Error   string `json:"error"`
}

type pushItem struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

func jsonString(b []byte, s string) []byte {
	q, _ := json.Marshal(s)
	return append(b, q...)
}
