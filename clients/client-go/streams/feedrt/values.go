package feedrt

import (
	"fmt"
	"math"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// Undefined is JavaScript's undefined: a field that is not there (a map field
// holding it is left out of JSON). A function that answers it answers
// "nothing", as a JS function answering undefined does.
var Undefined = jsv.Undefined

var undefinedValue = jsv.Undefined

// IsUndefined reports whether v is Undefined.
func IsUndefined(v interface{}) bool { return jsv.IsUndefined(v) }

// envelope is what flows through the steps (pipeline.js): a feed message
// enters as {msg, key: partition, value: data}; a result of a stateful step
// goes on as {msg: result, value: result, key, ctx, eventTime}, derived.
type envelope struct {
	msg      interface{}
	key      string
	value    interface{}
	ctx      interface{} // Undefined for a feed message
	derived  bool
	position interface{} // Undefined, nil or a number
	et       float64     // eventTime, when hasET
	hasET    bool
}

func (e *envelope) with(value interface{}) *envelope {
	c := *e
	c.value = value
	return &c
}

// emit is what a stateful step hands on (a window closed, a match, a
// deadline, a run) or a held message an event-time step takes in its turn.
type emit struct {
	step    int
	key     string
	ctx     map[string]interface{}
	state   interface{}
	time    float64
	hasTime bool
	et      float64
	hasET   bool
	compute func() (interface{}, error)
	value   interface{}
	// A held message an event-time step released.
	release bool
	id      string
	entry   map[string]interface{}
	relEnv  *envelope
	relTS   float64
}

// callErr turns a panic in a user function into an error.
func callErr(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("%v", r)
			}
		}
	}()
	return fn()
}

func callValue(fn func() (interface{}, error)) (v interface{}, err error) {
	err = callErr(func() error {
		var e error
		v, e = fn()
		return e
	})
	return v, err
}

func callBool(fn func() (bool, error)) (b bool, err error) {
	err = callErr(func() error {
		var e error
		b, e = fn()
		return e
	})
	return b, err
}

func isoOrNil(t float64) interface{} {
	if s, ok := jsv.ISO(t); ok {
		return s
	}
	return nil
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// setDef sets m[k] unless v is Undefined (a JS object's undefined field reads the same as a missing one).
func setDef(m map[string]interface{}, k string, v interface{}) {
	if !jsv.IsUndefined(v) {
		m[k] = v
	}
}

// messageOf is the message a pattern, join, gather or table join sees for an
// envelope: the feed message itself, or, after an earlier stateful step, that
// step's result in the feed's shape (data is the result, queue is null,
// partition the key, ctx what the step said about it).
func messageOf(env *envelope, t float64) interface{} {
	if !env.derived {
		return env.msg
	}
	pos := env.position
	if jsv.IsUndefined(pos) {
		pos = nil
	}
	ctx := env.ctx
	if jsv.IsNullish(ctx) {
		ctx = nil
	}
	var ts interface{}
	if finite(t) {
		ts = isoOrNil(t)
	}
	m := map[string]interface{}{
		"position":  pos,
		"queue":     nil,
		"partition": env.key,
		"key":       env.key,
		"ctx":       ctx,
		"ts":        ts,
		"derived":   true,
	}
	setDef(m, "data", env.value)
	return m
}

type kept struct{ value interface{} }

// snapshotOf is what a waiting state keeps of a message; data is keep(msg,
// value) when the step has one.
func snapshotOf(msg interface{}, k *kept) map[string]interface{} {
	get := func(f string) interface{} { return jsv.Get(msg, f) }
	snap := map[string]interface{}{}
	setDef(snap, "position", get("position"))
	setDef(snap, "queue", get("queue"))
	setDef(snap, "partition", get("partition"))
	setDef(snap, "partitionId", get("partitionId"))
	setDef(snap, "offset", get("offset"))
	setDef(snap, "id", get("id"))
	setDef(snap, "transactionId", get("transactionId"))
	setDef(snap, "ts", get("ts"))
	created := get("createdAt")
	if jsv.IsNullish(created) {
		created = get("ts")
	}
	setDef(snap, "createdAt", created)
	if k == nil {
		setDef(snap, "data", get("data"))
	} else {
		setDef(snap, "data", k.value)
	}
	if jsv.Truthy(get("derived")) {
		snap["derived"] = true
		setDef(snap, "key", get("key"))
		if c := get("ctx"); jsv.Truthy(c) {
			snap["ctx"] = c
		}
	}
	return snap
}

func clone(v interface{}) interface{} { return jsv.Clone(v) }

// num reads a number field of a decoded-JSON object.
func num(v interface{}, k string) (float64, bool) {
	f, ok := jsv.Num(jsv.Get(v, k))
	if !ok || !finite(f) {
		return 0, false
	}
	return f, true
}

func str(v interface{}, k string) string {
	s, _ := jsv.Get(v, k).(string)
	return s
}

func asMap(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}
