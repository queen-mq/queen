package feed

import (
	"context"
	"reflect"
	"testing"
)

// Headers on the feed (catalogue version 8): an entry carries them only when
// the message has some, the entry's map hands them out under "headers", and a
// condition on headers.<name> reads them as the broker does.

func TestEntriesCarryTheirHeaders(t *testing.T) {
	with := entry(5, 1, "")
	with["headers"] = map[string]interface{}{"type": "order.paid", "trace.id": "abc"}
	http := &fakeHTTP{answers: []interface{}{page([]map[string]interface{}{with, entry(6, 2, "")}, 7, true, 6)}}
	r, _ := NewReader(http, nil, "http", Options{From: 1})
	p, err := r.Read(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.Entries[0].Headers, map[string]string{"type": "order.paid", "trace.id": "abc"}) || p.Entries[1].Headers != nil {
		t.Fatalf("headers %v / %v", p.Entries[0].Headers, p.Entries[1].Headers)
	}
	m := p.Entries[0].Map()
	if !reflect.DeepEqual(m["headers"], map[string]interface{}{"type": "order.paid", "trace.id": "abc"}) {
		t.Fatal(m["headers"])
	}
	if _, ok := p.Entries[1].Map()["headers"]; ok {
		t.Fatal("an entry without headers hands out a headers key")
	}
	// A where on a header, evaluated here on the entry's map.
	if !ConditionHolds(cond("headers.type", "=", "order.paid"), m) || !ConditionHolds(cond("headers.trace.id", "prefix", "a"), m) {
		t.Fatal("a header condition that holds")
	}
	if ConditionHolds(cond("headers.type", "!=", "x"), p.Entries[1].Map()) {
		t.Fatal("a header the message lacks holds for no op, != included")
	}
	// The same on a message whose headers are typed (a queue message's).
	q := map[string]interface{}{"headers": map[string]string{"type": "order.paid"}}
	if !ConditionHolds(cond("headers.type", "in", []interface{}{"order.paid", "order.refunded"}), q) {
		t.Fatal("typed headers")
	}
}

// The one-string where goes to the broker as it is; with conditions it is two
// wheres for one read.
func TestReaderSendsAWhereExpr(t *testing.T) {
	http := &fakeHTTP{answers: []interface{}{page(nil, 1, true, 0)}}
	expr := "data.country = 'IT' and headers.type = 'order.paid'"
	r, err := NewReader(http, nil, "http", Options{From: "earliest", WhereExpr: expr})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if http.calls[0]["where"] != expr {
		t.Fatalf("where %v", http.calls[0]["where"])
	}
	if _, err := NewReader(http, nil, "http", Options{WhereExpr: expr, Where: []Condition{cond("data.a", "=", 1)}}); err == nil {
		t.Fatal("conditions and a string")
	}
}
