// Package feed reads the tenant feed: every push to a tenant's queues, in the
// order the broker's log committed them, by position (POST /api/v1/feed).
// It is the same reader as the JS SDK's queen.feed() (client-v2/feed/Feed.js):
// the same request, the same where conditions, the same answer to a leader
// that refuses the read (421 feed_on_leader: read from the followers it names).
//
// A position is the broker log entry that wrote a message. One number orders
// every queue of the tenant, and the pushes of one transaction share theirs.
// Nothing is leased, acked or moved: the feed is a read.
//
// Most code gets a reader from the client: queen.Feed(queen.FeedOptions{...}).
package feed

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

var ops = map[string]bool{"=": true, "==": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
	"in": true, "exists": true, "prefix": true, "!prefix": true}

// Condition is one `where` condition: `{ field, op, value }`, checked. The
// broker evaluates it on a feed read; ConditionHolds evaluates it here.
//
// Fields: queue, partition, transactionId, offset, position, data,
// data.<path>. Ops: = != < <= > >= in exists prefix !prefix. A field the
// message does not have holds for no op, != and !prefix included.
type Condition struct {
	Field string
	Op    string
	// Value is the value compared (null when the caller gave none); not sent for exists.
	Value interface{}
}

// MarshalJSON writes {field, op, value} as the broker reads it (no value for exists).
func (c Condition) MarshalJSON() ([]byte, error) {
	o := jsv.Ordered{{K: "field", V: c.Field}, {K: "op", V: c.Op}}
	if c.Op != "exists" {
		v := c.Value
		if jsv.IsUndefined(v) {
			v = nil
		}
		o = append(o, jsv.Pair{K: "value", V: v})
	}
	return jsv.Marshal(o)
}

// Map is the condition as a JSON object (what the JS SDK keeps).
func (c Condition) Map() map[string]interface{} {
	m := map[string]interface{}{"field": c.Field, "op": c.Op}
	if c.Op != "exists" {
		m["value"] = c.Value
	}
	return m
}

// WhereCondition builds and checks one condition (JS whereCondition):
//
//	feed.WhereCondition("data.country", "=", "IT")
//	feed.WhereCondition("data.total", ">", 100)
//	feed.WhereCondition("partition", "prefix", "booking-")
//	feed.WhereCondition("data.vip", "exists")
func WhereCondition(field, op string, value ...interface{}) (Condition, error) {
	if field == "" {
		return Condition{}, fmt.Errorf("where: field must be a non-empty string, got %s", jsv.MustMarshal(field))
	}
	if !ops[op] {
		return Condition{}, fmt.Errorf("where: unknown op %s (= != < <= > >= in exists prefix !prefix)", jsv.MustMarshal(op))
	}
	var v interface{} = jsv.Undefined
	if len(value) > 0 {
		v = value[0]
	}
	if op == "in" && !jsv.IsArray(v) {
		return Condition{}, fmt.Errorf("where: in on %s needs an array", field)
	}
	if op == "prefix" || op == "!prefix" {
		if _, ok := v.(string); !ok {
			return Condition{}, fmt.Errorf("where: %s on %s needs a string", op, field)
		}
	}
	c := Condition{Field: field, Op: op}
	if op != "exists" {
		if jsv.IsUndefined(v) {
			v = nil
		}
		c.Value = jsv.Normalize(v)
	}
	return c, nil
}

// NormalizeWhere reads `where` as a caller wrote it: nil, a Condition, a
// [field, op, value] list, a {field, op, value} map, or a list of any of those.
func NormalizeWhere(where interface{}) ([]Condition, error) {
	if jsv.IsNullish(where) {
		return nil, nil
	}
	switch w := where.(type) {
	case Condition:
		return []Condition{w}, nil
	case []Condition:
		return append([]Condition(nil), w...), nil
	case map[string]interface{}:
		c, err := condOf(w)
		if err != nil {
			return nil, err
		}
		return []Condition{c}, nil
	}
	list, ok := jsv.Normalize(where).([]interface{})
	if !ok {
		return nil, fmt.Errorf("where: a condition is { field, op, value } or [field, op, value], got %s", jsv.MustMarshal(where))
	}
	if len(list) > 0 {
		if _, isField := list[0].(string); isField {
			list = []interface{}{list}
		}
	}
	out := make([]Condition, 0, len(list))
	for _, item := range list {
		c, err := condOf(item)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

func condOf(item interface{}) (Condition, error) {
	switch c := item.(type) {
	case Condition:
		return c, nil
	case []interface{}:
		get := func(i int) interface{} {
			if i < len(c) {
				return c[i]
			}
			return jsv.Undefined
		}
		field, _ := get(0).(string)
		op, _ := get(1).(string)
		if len(c) > 2 {
			return WhereCondition(field, op, get(2))
		}
		return WhereCondition(field, op)
	case map[string]interface{}:
		field, _ := c["field"].(string)
		op, _ := c["op"].(string)
		if v, ok := c["value"]; ok {
			return WhereCondition(field, op, v)
		}
		return WhereCondition(field, op)
	}
	return Condition{}, fmt.Errorf("where: a condition is { field, op, value } or [field, op, value], got %s", jsv.MustMarshal(item))
}

// jsonEq is the broker's equality: numbers by value, objects by their JSON.
func jsonEq(a, b interface{}) bool {
	if jsv.IsNumber(a) && jsv.IsNumber(b) {
		return jsv.Same(a, b)
	}
	if a == nil || b == nil || !jsv.IsObject(a) || !jsv.IsObject(b) {
		return jsv.Same(a, b)
	}
	ja, errA := jsv.Marshal(a)
	jb, errB := jsv.Marshal(b)
	return errA == nil && errB == nil && string(ja) == string(jb)
}

func compare(a, b interface{}) (int, bool) {
	if fa, ok := jsv.Num(a); ok {
		if fb, ok := jsv.Num(b); ok {
			switch {
			case fa < fb:
				return -1, true
			case fa > fb:
				return 1, true
			}
			if math.IsNaN(fa) || math.IsNaN(fb) {
				return 0, true
			}
			return 0, true
		}
		return 0, false
	}
	sa, ok1 := a.(string)
	sb, ok2 := b.(string)
	if ok1 && ok2 {
		return strings.Compare(sa, sb), true
	}
	return 0, false
}

func or(m map[string]interface{}, a, b string) interface{} {
	if v, ok := m[a]; ok && !jsv.IsNullish(v) {
		return v
	}
	if v, ok := m[b]; ok {
		return v
	}
	return jsv.Undefined
}

func fieldValue(field string, msg map[string]interface{}) interface{} {
	get := func(k string) interface{} {
		if v, ok := msg[k]; ok {
			return v
		}
		return jsv.Undefined
	}
	switch field {
	case "queue":
		return or(msg, "queue", "queueName")
	case "partition":
		return or(msg, "partition", "partitionName")
	case "transactionId", "offset", "position", "data":
		return get(field)
	}
	if !strings.HasPrefix(field, "data.") {
		return jsv.Undefined
	}
	cur := get("data")
	for _, key := range strings.Split(field[5:], ".") {
		switch c := cur.(type) {
		case map[string]interface{}:
			v, ok := c[key]
			if !ok {
				return jsv.Undefined
			}
			cur = v
		case []interface{}:
			i, ok := arrayIndex(key)
			if !ok || i >= len(c) {
				return jsv.Undefined
			}
			cur = c[i]
		default:
			return jsv.Undefined
		}
		if jsv.IsUndefined(cur) {
			return jsv.Undefined
		}
	}
	return cur
}

// arrayIndex is Number(key) as an index into a list.
func arrayIndex(key string) (int, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return 0, true // Number('') is 0
	}
	n := 0
	for _, r := range key {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
		if n > 1<<30 {
			return 0, false
		}
	}
	return n, true
}

// ConditionHolds says whether cond holds for a message (the feed's shape:
// queue, partition, transactionId, offset, position, data), by the rules the
// broker applies to a feed read.
func ConditionHolds(cond Condition, msg map[string]interface{}) bool {
	v := fieldValue(cond.Field, msg)
	if jsv.IsUndefined(v) {
		return false
	}
	switch cond.Op {
	case "exists":
		return true
	case "=", "==":
		return jsonEq(v, cond.Value)
	case "!=":
		return !jsonEq(v, cond.Value)
	case "in":
		list, ok := jsv.Normalize(cond.Value).([]interface{})
		if !ok {
			return false
		}
		for _, x := range list {
			if jsonEq(v, x) {
				return true
			}
		}
		return false
	case "prefix", "!prefix":
		s, ok1 := v.(string)
		p, ok2 := cond.Value.(string)
		if !ok1 || !ok2 {
			return false
		}
		if cond.Op == "prefix" {
			return strings.HasPrefix(s, p)
		}
		return !strings.HasPrefix(s, p)
	case "<", "<=", ">", ">=":
		c, ok := compare(v, cond.Value)
		if !ok {
			return false
		}
		switch cond.Op {
		case "<":
			return c < 0
		case "<=":
			return c <= 0
		case ">":
			return c > 0
		default:
			return c >= 0
		}
	}
	return false
}

var sinceRe = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$`)
var sinceUnits = map[string]float64{"ms": 1, "s": 1000, "m": 60000, "h": 3600000, "d": 86400000, "w": 604800000}

// SinceToISO is a start time as the broker reads it: a time.Time, epoch
// milliseconds, an ISO string, or a duration back from now ('30s', '15m',
// '2h', '1d', '1w').
func SinceToISO(since interface{}, now time.Time) (string, error) {
	bad := func() (string, error) {
		return "", fmt.Errorf("feed: since must be a Date, epoch ms, an ISO time or a duration like '15m', got %s", jsv.MustMarshal(since))
	}
	switch s := since.(type) {
	case time.Time:
		return jsv.ISOOf(s), nil
	case *time.Time:
		if s == nil {
			return bad()
		}
		return jsv.ISOOf(*s), nil
	case string:
		if m := sinceRe.FindStringSubmatch(s); m != nil {
			var n float64
			fmt.Sscanf(m[1], "%g", &n)
			if iso, ok := jsv.ISO(float64(now.UnixMilli()) - n*sinceUnits[m[2]]); ok {
				return iso, nil
			}
			return bad()
		}
		if t, ok := jsv.DateParse(s); ok {
			if iso, ok := jsv.ISO(t); ok {
				return iso, nil
			}
		}
		return bad()
	}
	if jsv.IsFinite(since) {
		f, _ := jsv.Num(since)
		if iso, ok := jsv.ISO(f); ok {
			return iso, nil
		}
	}
	return bad()
}

// FNV1a32 is FNV-1a, 32 bit, over the UTF-8 bytes of text: the hash the broker
// picks a shard by (feed.rs fnv1a32), so both sides agree on where a partition is.
func FNV1a32(text string) uint32 {
	h := uint32(0x811c9dc5)
	for i := 0; i < len(text); i++ {
		h ^= uint32(text[i])
		h *= 0x01000193
	}
	return h
}

// ShardOf is the shard of count that a key (a partition name, by default) belongs to.
func ShardOf(key string, count int) int {
	if count < 1 {
		count = 1
	}
	return int(FNV1a32(key) % uint32(count))
}
