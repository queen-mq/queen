package feedrt

import (
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// keptOf is what a step keeps of a message: {value} from its keep, or nil for all of it.
func keptOf(keep func(msg, value interface{}) (interface{}, error), msg, value interface{}) (*kept, error) {
	if keep == nil {
		return nil, nil
	}
	v, err := callValue(func() (interface{}, error) { return keep(msg, value) })
	if err != nil {
		return nil, err
	}
	return &kept{value: v}, nil
}

// patternEngine is .pattern() and .join() of two sides (patterns.js).
//
// State keys: p\x1f<key> for the start a pattern waits on, j\x1fL\x1f<key> /
// j\x1fR\x1f<key> for the side a join waits on. A value is {key, msg, side?,
// due}: the waiting message (or what keep kept), and due, the first
// millisecond past its deadline.
type patternEngine struct {
	op    *PatternSpec
	state *FeedState
	heap  DueHeap
}

func newPatternEngine(op *PatternSpec, state *FeedState) *patternEngine {
	e := &patternEngine{op: op, state: state}
	state.ClassifyWith(func(k string, _ interface{}) string {
		if isBufferKey(k) {
			return "held"
		}
		return "open"
	})
	return e
}

func (e *patternEngine) kind() string { return e.op.Kind }

func (e *patternEngine) test(fn func(msg interface{}) (bool, error), msg interface{}) (bool, error) {
	return callBool(func() (bool, error) { return fn(msg) })
}

// relevant: a start or then, a side. On event time only these are held.
func (e *patternEngine) relevant(env *envelope, t float64) (bool, error) {
	msg := messageOf(env, t)
	op := e.op
	if op.Kind == "pattern" {
		ok, err := e.test(op.Then, msg)
		if err != nil || ok {
			return ok, err
		}
		return e.test(op.Start, msg)
	}
	ok, err := e.test(op.Left, msg)
	if err != nil || ok {
		return ok, err
	}
	return e.test(op.Right, msg)
}

func (e *patternEngine) rebuild() {
	e.heap.clear()
	e.state.Each(func(k string, v interface{}) bool {
		if !isBufferKey(k) {
			if due, ok := num(v, "due"); ok {
				e.heap.push(due, k)
			}
		}
		return true
	})
}

func (e *patternEngine) prune() {
	for e.heap.size() > 0 {
		top := e.heap.peek()
		if v, ok := e.state.Get(top.key); ok {
			if due, ok := num(v, "due"); ok && due == top.due {
				return
			}
		}
		e.heap.pop()
	}
}

func (e *patternEngine) nextDue() (float64, bool) {
	e.prune()
	if e.heap.size() == 0 {
		return 0, false
	}
	return e.heap.peek().due, true
}

func (e *patternEngine) open() int { return e.state.Count("open") }

// advance passes every deadline at or before t: one timeout emit each when onTimeout is set.
func (e *patternEngine) advance(t float64, emits *[]*emit, limit int) {
	for n := 0; n < limit; n++ {
		e.prune()
		if e.heap.size() == 0 || e.heap.peek().due > t {
			return
		}
		sk := e.heap.pop().key
		v := e.state.Value(sk)
		e.state.Delete(sk)
		if e.op.OnTimeout == nil {
			continue
		}
		side := ""
		switch str(v, "side") {
		case "R":
			side = "right"
		case "L":
			side = "left"
		}
		key, _ := jsv.Get(v, "key").(string)
		due := mustNum(v, "due")
		waited := jsv.Get(v, "msg")
		ctx := map[string]interface{}{"kind": "timeout", "key": key, "partition": key, "deadline": due - 1}
		if side != "" {
			ctx["side"] = side
		}
		setDef(ctx, "waitedFor", waited)
		onTimeout := e.op.OnTimeout
		*emits = append(*emits, &emit{
			key: key, ctx: ctx, state: v,
			time: due, hasTime: true,
			et: due - 1, hasET: true,
			compute: func() (interface{}, error) {
				return callValue(func() (interface{}, error) { return onTimeout(clone(waited), side) })
			},
		})
	}
}

func (e *patternEngine) keyOf(env *envelope, msg interface{}) (string, error) {
	if e.op.Key == nil {
		return env.key, nil
	}
	k, err := callValue(func() (interface{}, error) { return e.op.Key(msg, env.value) })
	if err != nil {
		return "", err
	}
	return jsv.String(k), nil
}

// process takes one envelope at time t; a match is appended to emits.
func (e *patternEngine) process(env *envelope, t float64, emits *[]*emit) error {
	msg := messageOf(env, t)
	op := e.op
	if op.Kind == "pattern" {
		isThen, err := e.test(op.Then, msg)
		if err != nil {
			return err
		}
		isStart, err := e.test(op.Start, msg)
		if err != nil {
			return err
		}
		if !isThen && !isStart {
			return nil
		}
		key, err := e.keyOf(env, msg)
		if err != nil {
			return err
		}
		sk := "p" + sep + key
		waiting, has := e.state.Get(sk)
		if isThen && has {
			e.state.Delete(sk)
			k, err := keptOf(op.Keep, msg, env.value)
			if err != nil {
				return err
			}
			snap := snapshotOf(msg, k)
			due := mustNum(waiting, "due")
			wmsg := jsv.Get(waiting, "msg")
			var compute func() (interface{}, error)
			if op.OnMatch != nil {
				onMatch := op.OnMatch
				compute = func() (interface{}, error) {
					return callValue(func() (interface{}, error) { return onMatch(clone(wmsg), clone(snap)) })
				}
			} else {
				compute = func() (interface{}, error) {
					m := map[string]interface{}{"key": key, "then": snap}
					setDef(m, "start", clone(wmsg))
					return m, nil
				}
			}
			*emits = append(*emits, &emit{
				key: key, ctx: map[string]interface{}{"kind": "match", "key": key, "partition": key, "deadline": due - 1},
				state: waiting, et: t, hasET: true, compute: compute,
			})
			if isStart {
				return e.wait(sk, key, msg, env.value, t, "")
			}
		} else if isStart && (!has || op.OnRepeat == "restart") {
			return e.wait(sk, key, msg, env.value, t, "")
		}
		return nil
	}
	// join: a message that is both left and right counts as left.
	side := ""
	isLeft, err := e.test(op.Left, msg)
	if err != nil {
		return err
	}
	if isLeft {
		side = "L"
	} else {
		isRight, err := e.test(op.Right, msg)
		if err != nil {
			return err
		}
		if isRight {
			side = "R"
		}
	}
	if side == "" {
		return nil
	}
	key, err := e.keyOf(env, msg)
	if err != nil {
		return err
	}
	other := "L"
	if side == "L" {
		other = "R"
	}
	otherKey := "j" + sep + other + sep + key
	if waiting, has := e.state.Get(otherKey); has {
		e.state.Delete(otherKey)
		k, err := keptOf(op.Keep, msg, env.value)
		if err != nil {
			return err
		}
		snap := snapshotOf(msg, k)
		wmsg := jsv.Get(waiting, "msg")
		var left, right interface{} = wmsg, wmsg
		if side == "L" {
			left = snap
		} else {
			right = snap
		}
		due := mustNum(waiting, "due")
		var compute func() (interface{}, error)
		if op.Select != nil {
			sel := op.Select
			compute = func() (interface{}, error) {
				return callValue(func() (interface{}, error) { return sel(clone(left), clone(right)) })
			}
		} else {
			compute = func() (interface{}, error) {
				m := map[string]interface{}{"key": key}
				setDef(m, "left", clone(left))
				setDef(m, "right", clone(right))
				return m, nil
			}
		}
		*emits = append(*emits, &emit{
			key: key, ctx: map[string]interface{}{"kind": "match", "key": key, "partition": key, "deadline": due - 1},
			state: waiting, et: t, hasET: true, compute: compute,
		})
		return nil
	}
	myKey := "j" + sep + side + sep + key
	if !e.state.Has(myKey) || op.OnRepeat == "restart" {
		return e.wait(myKey, key, msg, env.value, t, side)
	}
	return nil
}

func (e *patternEngine) wait(sk, key string, msg, value interface{}, t float64, side string) error {
	due := t + e.op.WithinMs + 1
	k, err := keptOf(e.op.Keep, msg, value)
	if err != nil {
		return err
	}
	snap := snapshotOf(msg, k)
	v := map[string]interface{}{"key": key, "msg": snap, "due": due}
	if side != "" {
		v["side"] = side
	}
	e.state.Set(sk, v)
	e.heap.push(due, sk)
	return nil
}

func (e *patternEngine) view(v interface{}) map[string]interface{} {
	w := map[string]interface{}{"deadline": isoOrNil(mustNum(v, "due") - 1)}
	setDef(w, "msg", clone(jsv.Get(v, "msg")))
	switch str(v, "side") {
	case "L":
		w["side"] = "left"
	case "R":
		w["side"] = "right"
	}
	return w
}

func (e *patternEngine) inspect(key string) interface{} {
	keys := []string{"p" + sep + key}
	if e.op.Kind != "pattern" {
		keys = []string{"j" + sep + "L" + sep + key, "j" + sep + "R" + sep + key}
	}
	var out []interface{}
	for _, sk := range keys {
		if v, ok := e.state.Get(sk); ok {
			out = append(out, e.view(v))
		}
	}
	if len(out) == 0 {
		return nil
	}
	return map[string]interface{}{"key": key, "waiting": out}
}

func (e *patternEngine) list(fn func(item interface{}) bool) {
	e.state.Each(func(k string, v interface{}) bool {
		if isBufferKey(k) {
			return true
		}
		item := map[string]interface{}{"waiting": []interface{}{e.view(v)}}
		setDef(item, "key", jsv.Get(v, "key"))
		return fn(item)
	})
}
