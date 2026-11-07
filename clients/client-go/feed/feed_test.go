package feed

// The behaviours of clients/client-js/test-v2/feed-unit/feed.test.js
// ("where conditions", "FeedReader").

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWhereBuildsAndChecksConditions(t *testing.T) {
	c, err := WhereCondition("data.country", "=", "IT")
	if err != nil || string(mustJSON(c)) != `{"field":"data.country","op":"=","value":"IT"}` {
		t.Fatalf("%s %v", mustJSON(c), err)
	}
	c, _ = WhereCondition("data.vip", "exists")
	if string(mustJSON(c)) != `{"field":"data.vip","op":"exists"}` {
		t.Fatal(string(mustJSON(c)))
	}
	if _, err := WhereCondition("x", "~", 1); err == nil || !strings.Contains(err.Error(), "unknown op") {
		t.Fatal(err)
	}
	if _, err := WhereCondition("x", "in", 1); err == nil || !strings.Contains(err.Error(), "needs an array") {
		t.Fatal(err)
	}
	l, err := NormalizeWhere([]interface{}{"data.a", "=", 1.0})
	if err != nil || len(l) != 1 || string(mustJSON(l[0])) != `{"field":"data.a","op":"=","value":1}` {
		t.Fatal(l, err)
	}
	l, err = NormalizeWhere([]interface{}{[]interface{}{"data.a", "=", 1.0}, map[string]interface{}{"field": "queue", "op": "!=", "value": "q"}})
	if err != nil || len(l) != 2 {
		t.Fatal(l, err)
	}
}

func cond(field, op string, v ...interface{}) Condition {
	c, err := WhereCondition(field, op, v...)
	if err != nil {
		panic(err)
	}
	return c
}

func TestWhereEvaluatesLikeTheBroker(t *testing.T) {
	msg := map[string]interface{}{"queue": "orders", "partition": "booking-9", "data": map[string]interface{}{"country": "IT", "total": 40.0, "tags": []interface{}{"vip"}}}
	checks := []struct {
		c    Condition
		want bool
	}{
		{cond("data.country", "=", "IT"), true},
		{cond("data.total", ">", 100), false},
		{cond("data.tags.0", "=", "vip"), true},
		{cond("partition", "prefix", "booking-"), true},
		{cond("data.country", "in", []interface{}{"FR", "IT"}), true},
		// A missing field holds for no op, != included.
		{cond("data.missing", "!=", "x"), false},
	}
	for i, c := range checks {
		if got := ConditionHolds(c.c, msg); got != c.want {
			t.Errorf("check %d (%s): %v", i, mustJSON(c.c), got)
		}
	}
}

func TestSinceReadsADurationADateOrATime(t *testing.T) {
	now := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	for in, want := range map[interface{}]string{
		"1d":                   "2026-11-05T10:00:00.000Z",
		"15m":                  "2026-11-06T09:45:00.000Z",
		"2026-11-01T00:00:00Z": "2026-11-01T00:00:00.000Z",
	} {
		got, err := SinceToISO(in, now)
		if err != nil || got != want {
			t.Errorf("%v: %s %v", in, got, err)
		}
	}
	if got, _ := SinceToISO(now, time.Now()); got != "2026-11-06T10:00:00.000Z" {
		t.Error(got)
	}
	if _, err := SinceToISO("yesterday", now); err == nil || !strings.Contains(err.Error(), "since must be") {
		t.Error(err)
	}
}

// fakeHTTP answers from a list and records what it was asked.
type fakeHTTP struct {
	name    string
	answers []interface{}
	calls   []map[string]interface{}
}

func (f *fakeHTTP) PostFeed(_ context.Context, body []byte, _ time.Duration) ([]byte, error) {
	var b map[string]interface{}
	_ = json.Unmarshal(body, &b)
	f.calls = append(f.calls, b)
	if len(f.answers) == 0 {
		return nil, errors.New(f.name + ": no answer left")
	}
	next := f.answers[0]
	f.answers = f.answers[1:]
	if err, ok := next.(error); ok {
		return nil, err
	}
	return json.Marshal(next)
}

func entry(position, n int, partition string) map[string]interface{} {
	if partition == "" {
		partition = "c1"
	}
	return map[string]interface{}{"id": "m", "kind": "push", "offset": n, "partition": partition, "partitionId": "pid-1",
		"payload": map[string]interface{}{"n": n}, "position": position, "queue": "orders", "transactionId": "t",
		"ts": "2026-11-06T09:00:00.000000Z"}
}

func page(entries []map[string]interface{}, next int, caughtUp bool, applied int) map[string]interface{} {
	return map[string]interface{}{"applied": applied, "caughtUp": caughtUp, "entries": entries, "next": next, "resolvedTs": "2026-11-06T09:00:01.000000Z"}
}

func TestReaderSendsTheReadMovesToNextAndStopsWhenCaughtUp(t *testing.T) {
	http := &fakeHTTP{name: "main", answers: []interface{}{
		page([]map[string]interface{}{entry(5, 1, ""), entry(6, 2, "")}, 7, false, 9),
		page([]map[string]interface{}{entry(9, 3, "")}, 10, true, 9),
	}}
	f := false
	r, err := NewReader(http, func(string) (Poster, error) { return nil, errors.New("no follower") }, "http", Options{
		Queues: []string{"orders*"}, Where: []Condition{cond("data.country", "=", "IT")}, From: "earliest", Follow: &f,
	})
	if err != nil {
		t.Fatal(err)
	}
	var seen []float64
	for e, err := range r.Entries(context.Background()) {
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, e.Data.(map[string]interface{})["n"].(float64))
	}
	if !reflect.DeepEqual(seen, []float64{1, 2, 3}) {
		t.Fatal(seen)
	}
	if p, _ := r.PositionInt(); p != 10 {
		t.Fatal(p)
	}
	c := http.calls[0]
	if !reflect.DeepEqual(c["queues"], []interface{}{"orders*"}) || c["from"] != "earliest" || c["maxWaitMs"] != 0.0 {
		t.Fatal(c)
	}
	if !reflect.DeepEqual(c["where"], []interface{}{map[string]interface{}{"field": "data.country", "op": "=", "value": "IT"}}) {
		t.Fatal(c["where"])
	}
	if http.calls[1]["from"] != 7.0 {
		t.Fatal(http.calls[1])
	}
}

func TestReaderHandsEntriesOutInQueenMessageShape(t *testing.T) {
	http := &fakeHTTP{answers: []interface{}{page([]map[string]interface{}{entry(5, 1, "")}, 6, true, 5)}}
	r, _ := NewReader(http, nil, "http", Options{From: 1})
	p, err := r.Read(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := p.Entries[0].Map()
	if !reflect.DeepEqual(m["data"], map[string]interface{}{"n": 1.0}) || m["createdAt"] != m["ts"] || m["position"] != 5.0 || m["partition"] != "c1" {
		t.Fatal(m)
	}
}

func TestA421SendsTheReaderToTheFollowersItNames(t *testing.T) {
	refused := &StatusError{Status: 421, Code: "feed_on_leader", Body: map[string]interface{}{"code": "feed_on_leader",
		"followers": []interface{}{map[string]interface{}{"nodeId": 2.0, "httpAddr": "10.0.0.2:6632"}, map[string]interface{}{"nodeId": 3.0, "httpAddr": "10.0.0.3:6632"}}}}
	main := &fakeHTTP{name: "main", answers: []interface{}{refused}}
	var opened []string
	followers := map[string]*fakeHTTP{
		"https://10.0.0.2:6632": {name: "f2", answers: []interface{}{
			page([]map[string]interface{}{entry(5, 1, "")}, 6, true, 5), page([]map[string]interface{}{entry(6, 2, "")}, 7, true, 6)}},
		"https://10.0.0.3:6632": {name: "f3"},
	}
	r, _ := NewReader(main, func(u string) (Poster, error) { opened = append(opened, u); return followers[u], nil }, "https", Options{From: 1})
	first, err := r.Read(context.Background(), nil)
	if err != nil || len(first.Entries) != 1 || first.Entries[0].Data.(map[string]interface{})["n"] != 1.0 {
		t.Fatal(first, err)
	}
	if !reflect.DeepEqual(opened, []string{"https://10.0.0.2:6632", "https://10.0.0.3:6632"}) {
		t.Fatal(opened)
	}
	second, err := r.Read(context.Background(), nil)
	if err != nil || second.Entries[0].Data.(map[string]interface{})["n"] != 2.0 {
		t.Fatal(second, err)
	}
	if len(main.calls) != 1 || len(followers["https://10.0.0.2:6632"].calls) != 2 {
		t.Fatal(len(main.calls), len(followers["https://10.0.0.2:6632"].calls))
	}
}

func TestAFollowerThatDoesNotAnswerPassesTheReadOn(t *testing.T) {
	main := &fakeHTTP{answers: []interface{}{page([]map[string]interface{}{entry(5, 1, "")}, 6, true, 5)}}
	f1 := &fakeHTTP{name: "f1", answers: []interface{}{errors.New("connect ECONNREFUSED")}}
	r, _ := NewReader(main, func(string) (Poster, error) { return f1, nil }, "http", Options{From: 1, FollowerURLs: []string{"http://10.0.0.9:6632"}})
	p, err := r.Read(context.Background(), nil)
	if err != nil || len(p.Entries) != 1 {
		t.Fatal(p, err)
	}
	if len(f1.calls) != 1 || len(main.calls) != 1 {
		t.Fatal(len(f1.calls), len(main.calls))
	}
}

func TestA404IsABrokerWithoutTheFeed(t *testing.T) {
	main := &fakeHTTP{answers: []interface{}{&StatusError{Status: 404, Message: "HTTP 404: Not Found"}}}
	r, _ := NewReader(main, nil, "http", Options{})
	_, err := r.Probe(context.Background())
	var nf *NoFeedRouteError
	if !errors.As(err, &nf) || !errors.Is(err, ErrNoFeedRoute) || nf.Code() != "no_feed_route" || !strings.Contains(err.Error(), "no POST /api/v1/feed") {
		t.Fatal(err)
	}
}

func TestFNV1a32IsTheBrokersHash(t *testing.T) {
	if FNV1a32("") != 0x811c9dc5 || FNV1a32("a") != 0xe40c292c || FNV1a32("foobar") != 0xbf9cf968 {
		t.Fatal(FNV1a32(""), FNV1a32("a"), FNV1a32("foobar"))
	}
	if FNV1a32("é") != FNV1a32(string([]byte{0xc3, 0xa9})) {
		t.Fatal("utf-8")
	}
}
