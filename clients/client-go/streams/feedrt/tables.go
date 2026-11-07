package feedrt

import (
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// tableStep is .joinTable() (tables.js): the latest row per key of a table
// queue, looked up for every message that reaches the step. State key
// t\x1f<key>; a value is {v, p}: the row and its position.
type tableStep struct {
	op    *TableSpec
	state *FeedState
}

func newTableStep(op *TableSpec, state *FeedState) *tableStep {
	return &tableStep{op: op, state: state}
}

func (t *tableStep) hasQueue(q string) bool {
	for _, x := range t.op.Queues {
		if x == q {
			return true
		}
	}
	return false
}

func (t *tableStep) open() int { return t.state.Size() }

// applyRow is a message of the table's queue: the row it sets, or deletes.
func (t *tableStep) applyRow(entry map[string]interface{}) error {
	op := t.op
	var key interface{}
	if op.Key != nil {
		k, err := callValue(func() (interface{}, error) { return op.Key(entry) })
		if err != nil {
			return err
		}
		key = k
	} else {
		key = jsv.Get(entry, "partition")
	}
	if jsv.IsNullish(key) {
		return nil
	}
	var value interface{}
	if op.Value != nil {
		v, err := callValue(func() (interface{}, error) { return op.Value(entry) })
		if err != nil {
			return err
		}
		value = v
	} else {
		value = jsv.Get(entry, "data")
	}
	sk := "t" + sep + jsv.String(key)
	if jsv.IsNullish(value) {
		t.state.Delete(sk)
	} else {
		v := map[string]interface{}{"v": value}
		setDef(v, "p", jsv.Get(entry, "position"))
		t.state.Set(sk, v)
	}
	return nil
}

// apply is a stream message: the envelope with the row joined in, or none
// when the row is missing and missing is 'drop'.
func (t *tableStep) apply(env *envelope, at float64) ([]*envelope, error) {
	op := t.op
	msg := messageOf(env, at)
	key, err := callValue(func() (interface{}, error) { return op.On(msg, env.value) })
	if err != nil {
		return nil, err
	}
	var row interface{} = undefinedValue
	if !jsv.IsNullish(key) {
		if found, ok := t.state.Get("t" + sep + jsv.String(key)); ok {
			row = clone(jsv.Get(found, "v"))
		}
	}
	if jsv.IsUndefined(row) && op.Missing == "drop" {
		return nil, nil
	}
	var value interface{}
	if op.Select != nil {
		arg := row
		if jsv.IsUndefined(arg) {
			arg = nil
		}
		value, err = callValue(func() (interface{}, error) { return op.Select(env.value, arg, msg) })
		if err != nil {
			return nil, err
		}
	} else {
		m := map[string]interface{}{}
		setDef(m, "value", env.value)
		setDef(m, "row", row)
		value = m
	}
	if jsv.IsNullish(value) {
		return nil, nil
	}
	return []*envelope{env.with(value)}, nil
}

func (t *tableStep) inspect(key string) interface{} {
	found, ok := t.state.Get("t" + sep + key)
	if !ok {
		return nil
	}
	out := map[string]interface{}{"key": key}
	setDef(out, "row", clone(jsv.Get(found, "v")))
	setDef(out, "position", jsv.Get(found, "p"))
	return out
}

func (t *tableStep) list(fn func(item interface{}) bool) {
	t.state.Each(func(k string, v interface{}) bool {
		out := map[string]interface{}{"key": k[2:]}
		setDef(out, "row", clone(jsv.Get(v, "v")))
		setDef(out, "position", jsv.Get(v, "p"))
		return fn(out)
	})
}
