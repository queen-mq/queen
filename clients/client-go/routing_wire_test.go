package queen

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
)

// Routing without copies (routing.go) against a scripted server: headers on
// every push path and every read, the filtered group's `where`, the reading
// queue's configure options, and the transaction's `link`. What is at stake is
// the wire: each of these is a body or a query the broker reads, and a key in
// the wrong place is a push without its headers or a group that reads
// everything.

func decodeMap(t *testing.T, b []byte) map[string]interface{} {
	t.Helper()
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("body is not a JSON object: %v\n%s", err, b)
	}
	return m
}

func pushItemsOf(t *testing.T, body []byte) []map[string]interface{} {
	t.Helper()
	items, _ := decodeMap(t, body)["items"].([]interface{})
	out := make([]map[string]interface{}, len(items))
	for i, it := range items {
		out[i], _ = it.(map[string]interface{})
	}
	return out
}

// ---- headers on the way in ----------------------------------------------------

func TestPushSendsHeadersAndAnItemsOwnOverThem(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`[{"status":"queued"},{"status":"queued"}]`))
	client := newWireClient(t, srv.URL)
	_, err := client.Queue("orders").Partition("order-1").
		Push([]PushItem{
			{Payload: map[string]int{"n": 1}, TransactionID: "t1"},
			{Payload: map[string]int{"n": 2}, TransactionID: "t2", Partition: "order-2",
				Headers: map[string]string{"type": "order.refunded", "traceparent": "00-ab"}},
		}).
		Headers(map[string]string{"type": "order.paid"}).
		Execute(kvCtx(t))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"items":[
		{"queue":"orders","partition":"order-1","payload":{"n":1},"transactionId":"t1","headers":{"type":"order.paid"}},
		{"queue":"orders","partition":"order-2","payload":{"n":2},"transactionId":"t2","headers":{"type":"order.refunded","traceparent":"00-ab"}}
	]}`)
}

// A push without headers is the request it was before they existed: no key.
func TestPushWithoutHeadersCarriesNoHeadersKey(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`[{"status":"queued"}]`))
	if _, err := newWireClient(t, srv.URL).Queue("orders").Push(map[string]int{"n": 1}).Execute(kvCtx(t)); err != nil {
		t.Fatalf("push: %v", err)
	}
	if items := pushItemsOf(t, srv.only(t).Body); len(items) != 1 || items[0]["headers"] != nil {
		t.Fatalf("items %v", items)
	}
	if bytes.Contains(srv.only(t).Body, []byte("headers")) {
		t.Fatalf("a push without headers names them: %s", srv.only(t).Body)
	}
}

// The plain binary body has no room for headers: such a push goes as JSON.
func TestBinaryPushWithHeadersGoesAsJSON(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`[{"status":"queued"}]`))
	client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, RetryAttempts: -1, BinaryPush: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Queue("orders").Push(map[string]int{"n": 1}).
		Headers(map[string]string{"type": "x"}).Execute(kvCtx(t)); err != nil {
		t.Fatalf("push: %v", err)
	}
	req := srv.only(t)
	if req.Header.Get("Content-Type") != "application/json" || pushItemsOf(t, req.Body)[0]["headers"] == nil {
		t.Fatalf("%s %s", req.Header.Get("Content-Type"), req.Body)
	}
}

// headerFrame is one client frame of a framed group, headers read.
type headerFrame struct {
	flags   byte
	txn     string
	block   []byte
	payload string
}

// readHeaderFrames decodes the groups at b (as readFrameGroups does), frames
// with flag 8 included: `u32 body_len | u8 flags | id[16] | u16 len, txn |
// [u16 len, block, flag 8] | payload`.
func readHeaderFrames(t *testing.T, b []byte) [][]headerFrame {
	t.Helper()
	le := binary.LittleEndian
	dec, _ := zstd.NewReader(nil)
	n := int(le.Uint32(b))
	p := 4
	var out [][]headerFrame
	for g := 0; g < n; g++ {
		l := int(le.Uint16(b[p:]))
		p += 2 + l
		l = int(le.Uint16(b[p:]))
		p += 2 + l
		count, zLen := int(le.Uint32(b[p:])), int(le.Uint32(b[p+4:]))
		p += 8
		raw, err := dec.DecodeAll(b[p:p+zLen], nil)
		if err != nil {
			t.Fatalf("group %d: zstd: %v", g, err)
		}
		p += zLen
		var frames []headerFrame
		at := 0
		for i := 0; i < count; i++ {
			bodyLen := int(le.Uint32(raw[at:]))
			f := raw[at+4 : at+4+bodyLen]
			at += 4 + bodyLen
			fr := headerFrame{flags: f[0]}
			tl := int(le.Uint16(f[17:]))
			fr.txn = string(f[19 : 19+tl])
			rest := f[19+tl:]
			if fr.flags&8 != 0 {
				hl := int(le.Uint16(rest))
				fr.block = rest[2 : 2+hl]
				rest = rest[2+hl:]
			}
			fr.payload = string(rest)
			frames = append(frames, fr)
		}
		if at != len(raw) {
			t.Fatalf("group %d: %d bytes after its frames", g, len(raw)-at)
		}
		out = append(out, frames)
	}
	if p != len(b) {
		t.Fatalf("%d bytes after the last group", len(b)-p)
	}
	return out
}

// The block the broker checks byte for byte: sorted names, u8 and u16 lengths.
func wantBlock(pairs ...string) []byte {
	le := binary.LittleEndian
	b := le.AppendUint16(nil, uint16(len(pairs)/2))
	for i := 0; i < len(pairs); i += 2 {
		b = append(b, byte(len(pairs[i])))
		b = append(b, pairs[i]...)
		b = le.AppendUint16(b, uint16(len(pairs[i+1])))
		b = append(b, pairs[i+1]...)
	}
	return b
}

func TestFramedPushCarriesTheHeadersBlock(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`[{"status":"queued"},{"status":"queued"}]`))
	client, err := New(ClientConfig{URL: srv.URL, TimeoutMillis: 2000, RetryAttempts: -1, BinaryPush: true, PushCompression: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Queue("orders").Partition("p1").Push([]PushItem{
		{Payload: map[string]int{"n": 1}, TransactionID: "t1", Headers: map[string]string{"type": "order.paid", "a.b": "1"}},
		{Payload: map[string]int{"n": 2}, TransactionID: "t2"},
	}).Execute(kvCtx(t)); err != nil {
		t.Fatalf("push: %v", err)
	}
	req := srv.only(t)
	if !bytes.HasPrefix(req.Body, []byte("QPB1\x08")) {
		t.Fatalf("not a framed push: %q", req.Body[:5])
	}
	groups := readHeaderFrames(t, req.Body[5:])
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("groups %+v", groups)
	}
	f := groups[0]
	if f[0].flags != 8 || f[0].txn != "t1" || !bytes.Equal(f[0].block, wantBlock("a.b", "1", "type", "order.paid")) || f[0].payload != `{"n":1}` {
		t.Fatalf("frame with headers %+v", f[0])
	}
	if f[1].flags != 0 || f[1].block != nil || f[1].payload != `{"n":2}` {
		t.Fatalf("frame without headers %+v", f[1])
	}
}

func TestHeaderBlock(t *testing.T) {
	block, ok := headerBlock(map[string]string{"type": "order.paid", "content-type": "application/json"})
	if !ok || !bytes.Equal(block, wantBlock("content-type", "application/json", "type", "order.paid")) {
		t.Fatalf("block %x", block)
	}
	back, ok := headersOfBlock(block)
	if !ok || !reflect.DeepEqual(back, map[string]string{"type": "order.paid", "content-type": "application/json"}) {
		t.Fatalf("decoded %v", back)
	}
	if b, ok := headerBlock(nil); b != nil || !ok {
		t.Fatal("no headers is no block")
	}
	for _, h := range []map[string]string{
		{strings.Repeat("k", 256): "v"},
		{"k": strings.Repeat("v", 70000)},
		{"": "v"},
		{"k": "\xff"},
	} {
		if _, ok := headerBlock(h); ok {
			t.Fatalf("framed headers a frame cannot carry: %v", len(h))
		}
	}
	if _, ok := headersOfBlock(block[:len(block)-1]); ok {
		t.Fatal("a cut block decoded")
	}
}

func TestTransactionPushCarriesHeaders(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"success":true,"results":[{"index":0,"type":"push","success":true}]}`))
	_, err := newWireClient(t, srv.URL).Transaction().
		Queue("out").Partition("p1").
		Push(PushItem{Payload: map[string]int{"n": 1}, TransactionID: "t1", Headers: map[string]string{"k": "v"}}).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[{"type":"push","items":[
		{"queue":"out","partition":"p1","payload":{"n":1},"transactionId":"t1","headers":{"k":"v"}}]}],
		"requiredLeases":[]}`)
}

func TestFramedTransactionCarriesHeaders(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"success":true,"results":[{"index":0,"type":"push","success":true}]}`))
	_, err := framedTxnClient(t, srv.URL).Transaction().
		Queue("out").Partition("p1").
		Push(PushItem{Payload: map[string]int{"n": 1}, TransactionID: "t1", Headers: map[string]string{"k": "v"}}).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	body := srv.only(t).Body
	if !bytes.HasPrefix(body, []byte("QTB1")) {
		t.Fatalf("not a binary transaction: %q", body[:5])
	}
	headLen := int(binary.LittleEndian.Uint32(body[5:]))
	f := readHeaderFrames(t, body[9+headLen:])[0][0]
	if f.flags != 8 || !bytes.Equal(f.block, wantBlock("k", "v")) || f.payload != `{"n":1}` {
		t.Fatalf("frame %+v", f)
	}
}

// ---- headers on the way out ---------------------------------------------------

func TestPopReadsHeadersOffsetAndGroup(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"success":true,"queue":"orders","messages":[
		{"id":"m1","transactionId":"t1","data":{"n":1},"producerSub":null,"headers":{"type":"order.paid"},
		 "createdAt":"2026-11-12T10:00:00.000Z","partitionId":"7","partition":"p1","leaseId":"l1",
		 "consumerGroup":"billing","deliveryAttempt":1,"offset":42},
		{"id":"m2","transactionId":"t2","data":{"n":2},"producerSub":null,
		 "createdAt":"2026-11-12T10:00:00.000Z","partitionId":"7","partition":"p1","leaseId":"l1",
		 "consumerGroup":"billing","deliveryAttempt":1,"offset":43}]}`))
	msgs, err := newWireClient(t, srv.URL).Queue("orders").Group("billing").Batch(2).Wait(false).Pop(kvCtx(t))
	if err != nil || len(msgs) != 2 {
		t.Fatalf("pop: %v, %d", err, len(msgs))
	}
	if !reflect.DeepEqual(msgs[0].Headers, map[string]string{"type": "order.paid"}) || msgs[0].Offset != 42 || msgs[0].ConsumerGroup != "billing" {
		t.Fatalf("first %+v", msgs[0])
	}
	if msgs[1].Headers != nil || msgs[1].Offset != 43 {
		t.Fatalf("second %+v", msgs[1])
	}
}

// testFrameWith is testFrame with any flags and the parts they announce.
func testFrameWith(id byte, flags byte, txn string, block []byte, payload string) []byte {
	le := binary.LittleEndian
	body := []byte{flags}
	idb := make([]byte, 16)
	idb[0], idb[6], idb[8] = id, 0x70, 0x80
	body = append(body, idb...)
	body = le.AppendUint16(body, uint16(len(txn)))
	body = append(body, txn...)
	if flags&8 != 0 {
		body = le.AppendUint16(body, uint16(len(block)))
		body = append(body, block...)
	}
	body = append(body, payload...)
	return append(le.AppendUint32(nil, uint32(len(body))), body...)
}

func TestFramedPopReadsTheHeadersBlock(t *testing.T) {
	frames := [][]byte{testFrameWith(1, 8, "t-10", wantBlock("type", "x"), `{"n":10}`), testFrame(2, "t-11", `{"n":11}`, "")}
	srv := newCaptureServer(t, cannedResponse{status: 200, body: string(framedAnswer(`{"frames":true}`, 10, 11, true, frames...))})
	msgs, err := framedClient(t, srv.URL).Queue("orders").Batch(2).Wait(false).Pop(kvCtx(t))
	if err != nil || len(msgs) != 2 {
		t.Fatalf("pop: %v, %d", err, len(msgs))
	}
	if !reflect.DeepEqual(msgs[0].Headers, map[string]string{"type": "x"}) || msgs[0].Data["n"] != 10.0 || msgs[0].Offset != 10 {
		t.Fatalf("first %+v", msgs[0])
	}
	if msgs[1].Headers != nil || msgs[1].Offset != 11 || msgs[1].ConsumerGroup != "__QUEUE_MODE__" {
		t.Fatalf("second %+v", msgs[1])
	}
	// A link frame never comes framed: its payload is an address, not JSON.
	link := framedAnswer(`{}`, 10, 10, false, testFrameWith(1, 16, "t-10", nil, strings.Repeat("\x00", 24)))
	if _, err := DecodePopFrames(link); err == nil {
		t.Fatal("a link frame decoded as a message")
	}
}

// A queue stream's source hands its runner the offset (what a link names) and
// the headers (what a .Where() on headers.<name> reads).
func TestStreamSourceCarriesOffsetAndHeaders(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"t1","partitionId":"7","partition":"p1",
		"leaseId":"l1","data":{"n":1},"headers":{"type":"x"},"offset":42}]}`))
	msgs, err := newWireClient(t, srv.URL).Queue("orders").Group("g").Wait(false).AsStreamSource().Pop(kvCtx(t))
	if err != nil || len(msgs) != 1 || msgs[0].Offset != 42 || !reflect.DeepEqual(msgs[0].Headers, map[string]string{"type": "x"}) {
		t.Fatalf("%v %+v", err, msgs)
	}
}

func TestFetchReadsHeaders(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"entries":[{"highWatermark":2,"logStartOffset":0,"partition":"p1","queue":"orders","records":[
		{"headers":{"type":"x"},"offset":0,"payload":{"a":1},"transactionId":"t0","ts":"2026-11-12T10:00:00.000000Z"},
		{"offset":1,"payload":{"a":2},"transactionId":"t1","ts":"2026-11-12T10:00:00.000000Z"}]}]}`))
	read, err := newWireClient(t, srv.URL).Queue("orders").Fetch(context.Background(), []string{"p1"})
	if err != nil {
		t.Fatal(err)
	}
	recs := read["p1"].Records
	if len(recs) != 2 || !reflect.DeepEqual(recs[0].Headers, map[string]string{"type": "x"}) || recs[1].Headers != nil {
		t.Fatalf("records %+v", recs)
	}
}

func TestDLQReadsHeaders(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[{"transactionId":"t1","partitionId":"7","queue":"orders","partition":"p1",
		"data":{"n":1},"headers":{"type":"x"},"errorMessage":"boom","retryCount":3}],"total":1}`))
	res, err := newWireClient(t, srv.URL).Queue("orders").DLQ("billing").Get(kvCtx(t))
	if err != nil || len(res.Messages) != 1 || !reflect.DeepEqual(res.Messages[0].Headers, map[string]string{"type": "x"}) {
		t.Fatalf("dlq: %v %+v", err, res)
	}
}

// ---- filtered groups -------------------------------------------------------------

func whereOf(t *testing.T, rawQuery string) string {
	t.Helper()
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		t.Fatalf("query %q: %v", rawQuery, err)
	}
	return q.Get("where")
}

func TestWhereGoesOnThePopOfTheGroup(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[],"filtered":true}`))
	_, err := newWireClient(t, srv.URL).Queue("order-events").Group("billing").Wait(false).
		Where("data.kind", "in", []string{"order.paid", "order.refunded"}).
		Where("headers.region", "=", "eu").
		Where("data.vip", "exists").
		Pop(kvCtx(t))
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	req := srv.only(t)
	if req.Path != "/api/v1/pop/queue/order-events" {
		t.Fatalf("path %s", req.Path)
	}
	assertJSONBody(t, []byte(whereOf(t, req.RawQuery)), `[
		{"field":"data.kind","op":"in","value":["order.paid","order.refunded"]},
		{"field":"headers.region","op":"=","value":"eu"},
		{"field":"data.vip","op":"exists"}]`)
}

func TestWhereExprGoesAsItIs(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[],"filtered":true}`))
	expr := "data.kind in ('order.paid', 'order.refunded') and data.total > 100"
	if _, err := newWireClient(t, srv.URL).Queue("order-events").Partition("p1").Group("billing").Wait(false).
		WhereExpr(expr).Pop(kvCtx(t)); err != nil {
		t.Fatalf("pop: %v", err)
	}
	req := srv.only(t)
	if req.Path != "/api/v1/pop/queue/order-events/partition/p1" || whereOf(t, req.RawQuery) != expr {
		t.Fatalf("%s ? %s", req.Path, req.RawQuery)
	}
}

// Every pop of a consumer carries the group's where, the same each time.
func TestWhereGoesOnEveryPopOfAConsumer(t *testing.T) {
	msg := `{"filtered":true,"messages":[{"transactionId":"t1","partitionId":"7","partition":"p1","leaseId":"l1","data":{"n":1},"offset":0}]}`
	srv := newCaptureServer(t, okJSON(msg), okJSON(msg))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := newWireClient(t, srv.URL).Queue("orders").Group("billing").Wait(false).Batch(1).Limit(2).AutoAck(false).
		WhereExpr("data.n = 1").
		Consume(ctx, func(context.Context, *Message) error { return nil }).Execute(ctx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	pops := 0
	for _, r := range srv.requests() {
		if strings.HasPrefix(r.Path, "/api/v1/pop") {
			pops++
			if whereOf(t, r.RawQuery) != "data.n = 1" {
				t.Fatalf("a pop without the where: %s", r.RawQuery)
			}
		}
	}
	if pops != 2 {
		t.Fatalf("%d pops", pops)
	}
}

// A broker older than filtered groups ignores the where and answers without
// the echo (or with a bodiless 204 when there is nothing): Pop and the consume
// loop stop at the first answer instead of handing the group every message.
func TestAWhereTheBrokerDoesNotEchoStops(t *testing.T) {
	unfiltered := okJSON(`{"messages":[{"transactionId":"t1","partitionId":"7","partition":"p1","leaseId":"l1","data":{"n":2}}]}`)
	for name, answer := range map[string]cannedResponse{"no echo": unfiltered, "204": {status: http.StatusNoContent}} {
		srv := newCaptureServer(t, answer)
		msgs, err := newWireClient(t, srv.URL).Queue("orders").Group("billing").Wait(false).WhereExpr("data.n = 1").Pop(kvCtx(t))
		if !errors.Is(err, ErrWhereUnsupported) || msgs != nil {
			t.Fatalf("%s: pop %v, %d messages", name, err, len(msgs))
		}
	}
	srv := newCaptureServer(t, unfiltered, unfiltered)
	handled := 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := newWireClient(t, srv.URL).Queue("orders").Group("billing").Wait(false).AutoAck(false).
		Where("data.n", "=", 1).
		Consume(ctx, func(context.Context, *Message) error { handled++; return nil }).Execute(ctx)
	if !errors.Is(err, ErrWhereUnsupported) || handled != 0 || len(srv.requests()) != 1 {
		t.Fatalf("consume: %v, handled %d, %d requests", err, handled, len(srv.requests()))
	}
	// Without a where the echo is not asked for.
	plain := newCaptureServer(t, unfiltered)
	if _, err := newWireClient(t, plain.URL).Queue("orders").Group("billing").Wait(false).Pop(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestAPopWithoutAWhereSendsNone(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"messages":[]}`))
	if _, err := newWireClient(t, srv.URL).Queue("orders").Group("billing").Wait(false).Pop(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(srv.only(t).RawQuery, "where") {
		t.Fatalf("query %s", srv.only(t).RawQuery)
	}
}

func TestWhereIsRefusedBeforeAnyRequest(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)
	ctx := kvCtx(t)
	noop := func(context.Context, *Message) error { return nil }
	cases := []struct {
		name string
		run  func() error
		want error
	}{
		{"no group", func() error {
			_, err := client.Queue("orders").Where("data.n", "=", 1).Pop(ctx)
			return err
		}, ErrWhereNeedsGroup},
		{"no group, consume", func() error {
			return client.Queue("orders").WhereExpr("data.n = 1").Consume(ctx, noop).Execute(ctx)
		}, ErrWhereNeedsGroup},
		{"namespace pop", func() error {
			_, err := client.Queue("").Namespace("billing").Group("g").Where("data.n", "=", 1).Pop(ctx)
			return err
		}, ErrWhereNeedsQueue},
		{"consume options", func() error {
			return NewConsumerManager(client.httpClient, client).Start(ctx, noop,
				ConsumeOptions{Queue: "orders", Where: "data.n = 1", Concurrency: 1})
		}, ErrWhereNeedsGroup},
	}
	for _, c := range cases {
		if err := c.run(); !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", c.name, err, c.want)
		}
	}
	// A condition Where refuses, and the two forms on one builder.
	if _, err := client.Queue("orders").Group("g").Where("data.n", "~", 1).Pop(ctx); err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatalf("bad op: %v", err)
	}
	if _, err := client.Queue("orders").Group("g").Where("data.n", "in", 1).Pop(ctx); err == nil {
		t.Fatal("in without a list")
	}
	if _, err := client.Queue("orders").Group("g").Where("data.n", "=", 1).WhereExpr("data.n = 2").Pop(ctx); err == nil {
		t.Fatal("two wheres for one group")
	}
	if n := len(srv.requests()); n != 0 {
		t.Fatalf("%d requests went out", n)
	}
}

func TestSetConsumerGroupWhere(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"success":true,"consumerGroup":"billing","queueName":"orders","where":"set"}`), okJSON(`{"success":true}`))
	admin := newWireClient(t, srv.URL).Admin()
	if _, err := admin.SetConsumerGroupWhere(kvCtx(t), "billing", "orders", "data.kind = 'order.paid'"); err != nil {
		t.Fatal(err)
	}
	c, _ := feed.WhereCondition("data.total", ">", 100)
	if _, err := admin.SetConsumerGroupWhere(kvCtx(t), "billing", "orders", []feed.Condition{c}); err != nil {
		t.Fatal(err)
	}
	reqs := srv.requests()
	if reqs[0].Method != "POST" || reqs[0].Path != "/api/v1/consumer-groups/billing/subscription" {
		t.Fatalf("%s %s", reqs[0].Method, reqs[0].Path)
	}
	assertJSONBody(t, reqs[0].Body, `{"queue":"orders","where":"data.kind = 'order.paid'"}`)
	assertJSONBody(t, reqs[1].Body, `{"queue":"orders","where":[{"field":"data.total","op":">","value":100}]}`)
	if _, err := admin.SetConsumerGroupWhere(kvCtx(t), "billing", "orders", ""); err == nil {
		t.Fatal("an empty where")
	}
	if len(srv.requests()) != 2 {
		t.Fatal("the empty where went out")
	}
}

// ---- reading queues ---------------------------------------------------------------

// A source that reading queues read is deleted only by force.
func TestDeleteASourceByForce(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"deleted":true}`), okJSON(`{"deleted":true}`))
	q := newWireClient(t, srv.URL).Queue("orders")
	if _, err := q.Delete().Execute(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Delete().Force(true).Execute(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	reqs := srv.requests()
	if reqs[0].Method != "DELETE" || reqs[0].Path != "/api/v1/resources/queues/orders" || reqs[0].RawQuery != "" {
		t.Fatalf("%s %s ?%s", reqs[0].Method, reqs[0].Path, reqs[0].RawQuery)
	}
	if reqs[1].Path != "/api/v1/resources/queues/orders" || reqs[1].RawQuery != "force=true" {
		t.Fatalf("%s ?%s", reqs[1].Path, reqs[1].RawQuery)
	}
}

func configureBody(t *testing.T, cfg QueueConfig, opts ...[2]interface{}) []byte {
	t.Helper()
	srv := newCaptureServer(t, okJSON(`{"configured":true,"route":{"from":[{"queue":"orders"}],"partitionBy":"same"},"catchingUp":false}`))
	op := newWireClient(t, srv.URL).Queue("orders-it").Config(cfg).Create()
	for _, o := range opts {
		op.Option(o[0].(string), o[1])
	}
	if _, err := op.Execute(kvCtx(t)); err != nil {
		t.Fatalf("create: %v", err)
	}
	return srv.only(t).Body
}

// A reading queue sends what was set and nothing else: none of the options the
// broker refuses beside `from` (retention, dedup, encryption, maxSize).
func TestConfigureAReadingQueue(t *testing.T) {
	body := configureBody(t, QueueConfig{
		From: ReadFrom("orders"), Where: "data.country = 'IT'", PartitionBy: "same", Start: "all",
		LeaseTime: 30, RetryLimit: 5, DeadLetterQueue: true,
	})
	assertJSONBody(t, body, `{"queue":"orders-it","options":{
		"from":"orders","where":"data.country = 'IT'","partitionBy":"same","start":"all",
		"leaseTime":30,"retryLimit":5,"deadLetterQueue":true}}`)
}

func TestConfigureAReadingQueueOfSeveralSources(t *testing.T) {
	big, _ := feed.WhereCondition("data.total", ">", 100)
	body := configureBody(t, QueueConfig{
		From: ReadSources{
			{Queue: "orders", Where: []feed.Condition{big}},
			{Queue: "refunds"},
			{Queue: "returns", Where: "headers.type = 'return.opened'"},
		},
		Where:       [][]interface{}{{"data.country", "=", "IT"}},
		PartitionBy: "headers.customer",
	})
	assertJSONBody(t, body, `{"queue":"orders-it","options":{
		"from":[{"queue":"orders","where":[{"field":"data.total","op":">","value":100}]},"refunds",
		        {"queue":"returns","where":"headers.type = 'return.opened'"}],
		"where":[{"field":"data.country","op":"=","value":"IT"}],
		"partitionBy":"headers.customer"}}`)
}

// A setting the caller spelled out still goes, and the broker answers for it.
func TestConfigureAReadingQueueKeepsWhatTheCallerSet(t *testing.T) {
	body := configureBody(t, QueueConfig{From: ReadFrom("orders"), RetentionSeconds: 60}, [2]interface{}{"from", nil})
	assertJSONBody(t, body, `{"queue":"orders-it","options":{"from":null,"retentionSeconds":60}}`)
	srv := newCaptureServer(t)
	_, err := newWireClient(t, srv.URL).Queue("q").Config(QueueConfig{From: ReadSources{{Queue: "orders", Where: 42}}}).Create().Execute(kvCtx(t))
	if err == nil || len(srv.requests()) != 0 {
		t.Fatalf("a where that is no condition: %v", err)
	}
}

// A broker older than reading queues answers a configure with `from` as it
// answers any: no route. Create says so instead of leaving an ordinary queue
// that reads nothing.
func TestConfigureAReadingQueueOnABrokerThatCannotRead(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"configured":true,"queue":"orders-it"}`), okJSON(`{"configured":true}`), okJSON(`{"configured":true}`))
	client := newWireClient(t, srv.URL)
	_, err := client.Queue("orders-it").Config(QueueConfig{From: ReadFrom("orders")}).Create().Execute(kvCtx(t))
	if !errors.Is(err, ErrReadingQueuesUnsupported) {
		t.Fatalf("from without a route: %v", err)
	}
	if _, err := client.Queue("orders-it").Create().Option("from", "orders").Execute(kvCtx(t)); !errors.Is(err, ErrReadingQueuesUnsupported) {
		t.Fatalf("Option(from) without a route: %v", err)
	}
	// Stopping (from: null) and an ordinary queue ask for no route.
	if _, err := client.Queue("orders-it").Create().Option("from", nil).Execute(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Queue("plain").Config(QueueConfig{LeaseTime: 30}).Create().Execute(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
}

func TestReadSourcesDecode(t *testing.T) {
	var one, many ReadSources
	if err := json.Unmarshal([]byte(`"orders"`), &one); err != nil || !reflect.DeepEqual(one, ReadFrom("orders")) {
		t.Fatalf("%v %+v", err, one)
	}
	if err := json.Unmarshal([]byte(`["refunds",{"queue":"orders","where":"data.n > 1"}]`), &many); err != nil ||
		len(many) != 2 || many[0].Queue != "refunds" || many[1].Queue != "orders" || many[1].Where != "data.n > 1" {
		t.Fatalf("%v %+v", err, many)
	}
	// A manifest decodes through the YAML decoder's function (yaml.v3 calls it).
	var fromYAML ReadSources
	err := fromYAML.UnmarshalYAML(func(v interface{}) error {
		*(v.(*interface{})) = []interface{}{"orders", map[string]interface{}{"queue": "refunds", "where": []interface{}{
			map[string]interface{}{"field": "data.n", "op": ">", "value": 1}}}}
		return nil
	})
	if err != nil || len(fromYAML) != 2 || fromYAML[1].Queue != "refunds" {
		t.Fatalf("%v %+v", err, fromYAML)
	}
	wire, err := json.Marshal(fromYAML)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONBody(t, wire, `["orders",{"queue":"refunds","where":[{"field":"data.n","op":">","value":1}]}]`)
	if err := json.Unmarshal([]byte(`[{"where":"x"}]`), &many); err == nil {
		t.Fatal("a source without a queue")
	}
}

// ---- link -------------------------------------------------------------------------

func TestLinkGoesBesideTheAckInTheTransaction(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"ack","success":true},{"index":1,"type":"link","success":true,"transactionId":"t1"}]}`))
	msg := &Message{TransactionID: "t1", PartitionID: "123", LeaseID: "l1", Offset: 42, ConsumerGroup: "workers", Partition: "order-1"}
	res, err := newWireClient(t, srv.URL).Transaction().
		Ack(msg, AckStatusCompleted, AckOptions{ConsumerGroup: "workers"}).
		Queue("manual-review").Partition(msg.Partition).
		Link(msg, LinkOptions{Headers: map[string]string{"review-reason": "amount"}}).
		Commit(kvCtx(t))
	if err != nil || !res.Success {
		t.Fatalf("commit: %v %+v", err, res)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[
		{"type":"ack","transactionId":"t1","partitionId":"123","leaseId":"l1","status":"completed","consumerGroup":"workers"},
		{"type":"link","queue":"manual-review","partition":"order-1",
		 "from":{"partitionId":"123","offset":42,"transactionId":"t1","consumerGroup":"workers"},
		 "headers":{"review-reason":"amount"}}],
		"requiredLeases":["l1"]}`)
}

// A feed entry's partition id is not a pop's: it is named by queue and partition.
func TestLinkAFeedEntryAndAListOfMessages(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"success":true,"results":[]}`))
	entry := feed.Entry{Queue: "orders", Partition: "c1", PartitionID: "0190-uuid", Offset: 0, TransactionID: "in-1"}
	msgs := []*Message{{TransactionID: "a", PartitionID: "7", Offset: 1}, {TransactionID: "b", PartitionID: "7", Offset: 2}}
	_, err := newWireClient(t, srv.URL).Transaction().
		Queue("copy").Partition("c1").Link(entry, LinkOptions{}).
		Queue("copy").Link(msgs, LinkOptions{}).
		Commit(kvCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	assertJSONBody(t, srv.only(t).Body, `{"operations":[
		{"type":"link","queue":"copy","partition":"c1","from":{"queue":"orders","partition":"c1","offset":0,"transactionId":"in-1"}},
		{"type":"link","queue":"copy","partition":"Default","from":{"partitionId":"7","offset":1,"transactionId":"a"}},
		{"type":"link","queue":"copy","partition":"Default","from":{"partitionId":"7","offset":2,"transactionId":"b"}}],
		"requiredLeases":[]}`)
}

// A link the target already holds commits: its result says duplicate.
func TestALinkTheTargetHoldsCommits(t *testing.T) {
	srv := newCaptureServer(t, okJSON(`{"transactionId":"tx","success":true,"results":[
		{"index":0,"type":"link","success":true,"duplicate":true,"transactionId":"t1"}]}`))
	res, err := newWireClient(t, srv.URL).Transaction().Queue("copy").
		Link(&Message{TransactionID: "t1", PartitionID: "7", Offset: 3}, LinkOptions{}).Commit(kvCtx(t))
	if err != nil || !res.Success || len(res.Results) != 1 {
		t.Fatalf("%v %+v", err, res)
	}
}

func TestLinkOfNoMessageFailsBeforeAnyRequest(t *testing.T) {
	srv := newCaptureServer(t)
	client := newWireClient(t, srv.URL)
	for _, bad := range []interface{}{&Message{}, "t1", []*Message{nil}, feed.Entry{Queue: "q"}} {
		if _, err := client.Transaction().Queue("copy").Link(bad, LinkOptions{}).Commit(kvCtx(t)); err == nil {
			t.Fatalf("linked %T", bad)
		}
	}
	if len(srv.requests()) != 0 {
		t.Fatal("a request went out")
	}
}

// In the binary transaction the head's operations are written first: a link
// before the pushes rides in it, a link after a push into its partition makes
// the transaction JSON, in the caller's order.
func TestFramedTransactionAndLinks(t *testing.T) {
	answer := okJSON(`{"success":true,"results":[]}`)
	srv := newCaptureServer(t, answer, answer)
	client := framedTxnClient(t, srv.URL)
	msg := &Message{TransactionID: "t1", PartitionID: "7", Offset: 5}
	if _, err := client.Transaction().
		Queue("out").Partition("p1").Link(msg, LinkOptions{}).
		Queue("out").Partition("p1").Push(map[string]int{"n": 1}).
		Commit(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Transaction().
		Queue("out").Partition("p1").Push(map[string]int{"n": 1}).
		Queue("out").Partition("p1").Link(msg, LinkOptions{}).
		Commit(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	reqs := srv.requests()
	first := reqs[0].Body
	if !bytes.HasPrefix(first, []byte("QTB1")) {
		t.Fatalf("a link before the push: %q", first[:5])
	}
	headLen := int(binary.LittleEndian.Uint32(first[5:]))
	assertJSONBody(t, first[9:9+headLen], `{"operations":[
		{"type":"link","queue":"out","partition":"p1","from":{"partitionId":"7","offset":5,"transactionId":"t1"}}],
		"requiredLeases":[]}`)
	if second := reqs[1].Body; second[0] != '{' || !bytes.Contains(second, []byte(`"type":"link"`)) {
		t.Fatalf("a link after a push into its partition: %q", second)
	}
}

// ---- timers ---------------------------------------------------------------------

// A schedule carries the fired message's headers, standalone and as a
// transaction's rider; a peek shows them.
func TestTimersCarryHeaders(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte(`{"orderId":7}`))
	srv := newCaptureServer(t,
		okJSON(`{"results":[{"ok":true,"status":"scheduled","queue":"reminders","timerKey":"order-7","txn":"t7"}]}`),
		okJSON(`{"success":true,"results":[{"index":0,"type":"timer","ok":true,"status":"scheduled"}]}`),
		okJSON(`{"found":true,"queue":"reminders","timerKey":"order-7","partition":"Default","txn":"t7","messageId":"m7",
			"payload":"`+payload+`","headers":{"type":"reminder"},
			"createdAt":"2026-11-12T10:00:00.000000Z","updatedAt":"2026-11-12T10:00:00.000000Z","deliverAt":"2026-11-12T11:00:00.000000Z"}`))
	client := newWireClient(t, srv.URL)
	s := TimerSchedule{Queue: "reminders", TimerKey: "order-7", Delay: time.Second, Payload: map[string]int{"orderId": 7},
		TransactionID: "t7", Headers: map[string]string{"type": "reminder"}}
	if _, err := client.Timers().Schedule(kvCtx(t), s); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Transaction().Timers(ScheduleTimerOp(s)).Commit(kvCtx(t)); err != nil {
		t.Fatal(err)
	}
	info, err := client.Timers().Peek(kvCtx(t), "reminders", "order-7")
	if err != nil || !reflect.DeepEqual(info.Headers, map[string]string{"type": "reminder"}) {
		t.Fatalf("peek: %v %+v", err, info)
	}
	op := `{"op":"schedule","queue":"reminders","timerKey":"order-7","delayMs":1000,"txn":"t7","payload":"` + payload + `","headers":{"type":"reminder"}}`
	reqs := srv.requests()
	assertJSONBody(t, reqs[0].Body, `{"operations":[`+op+`]}`)
	assertJSONBody(t, reqs[1].Body, `{"operations":[],"requiredLeases":[],"timers":[`+op+`]}`)
	// A schedule without headers is the op it was.
	if b, _ := json.Marshal(ScheduleTimerOp(TimerSchedule{Queue: "q", TimerKey: "k", TransactionID: "t"})); bytes.Contains(b, []byte("headers")) {
		t.Fatalf("%s", b)
	}
}
