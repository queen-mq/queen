package feedrt

import (
	"math"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// partID is a part's identity in a gather by count: queue, partition and
// transaction id; a step's result by its key, kind and time.
func partID(msg interface{}) string {
	get := func(k string) interface{} { return jsv.Get(msg, k) }
	if tx := get("transactionId"); jsv.Truthy(tx) {
		return jsv.String(get("queue")) + sep + jsv.String(get("partition")) + sep + jsv.String(tx)
	}
	or := func(vs ...interface{}) string {
		for _, v := range vs {
			if !jsv.IsNullish(v) {
				return jsv.String(v)
			}
		}
		return ""
	}
	if jsv.Truthy(get("derived")) {
		c := get("ctx")
		if jsv.IsNullish(c) {
			c = map[string]interface{}{}
		}
		return "d" + sep + jsv.String(get("key")) + sep + or(jsv.Get(c, "kind")) + sep +
			or(jsv.Get(c, "windowStart"), jsv.Get(c, "deadline")) + sep + or(get("ts"))
	}
	return jsv.String(get("position")) + sep + or(get("offset")) + sep + or(get("key"), get("partition"))
}

// collectEngine is .join({sides}) and .gather() (collect.js).
//
// State key c\x1f<key>; a value is {key, first, due, parts | list+ids,
// expected}, or {key, due, done: true} for a finished gather.
type collectEngine struct {
	op    *CollectSpec
	state *FeedState
	heap  DueHeap
}

func newCollectEngine(op *CollectSpec, state *FeedState) *collectEngine {
	e := &collectEngine{op: op, state: state}
	state.ClassifyWith(func(k string, v interface{}) string {
		if isBufferKey(k) {
			return "held"
		}
		if v != nil && !jsv.Truthy(jsv.Get(v, "done")) {
			return "open"
		}
		return ""
	})
	return e
}

func (e *collectEngine) kind() string { return e.op.Kind }

func (e *collectEngine) pred(p Part, msg, value interface{}) (bool, error) {
	return callBool(func() (bool, error) { return p.Pred(msg, value) })
}

// relevant: with steps or sides, one of their tests passes; by count, every message is a part.
func (e *collectEngine) relevant(env *envelope, t float64) (bool, error) {
	if e.op.Parts == nil {
		return true, nil
	}
	msg := messageOf(env, t)
	for _, p := range e.op.Parts {
		ok, err := e.pred(p, msg, env.value)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func (e *collectEngine) rebuild() {
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

func (e *collectEngine) prune() {
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

func (e *collectEngine) nextDue() (float64, bool) {
	e.prune()
	if e.heap.size() == 0 {
		return 0, false
	}
	return e.heap.peek().due, true
}

func (e *collectEngine) open() int { return e.state.Count("open") }

func (e *collectEngine) advance(t float64, emits *[]*emit, limit int) {
	for n := 0; n < limit; n++ {
		e.prune()
		if e.heap.size() == 0 || e.heap.peek().due > t {
			return
		}
		sk := e.heap.pop().key
		v := e.state.Value(sk)
		e.state.Delete(sk)
		if jsv.Truthy(jsv.Get(v, "done")) || e.op.OnTimeout == nil {
			continue
		}
		got := e.got(v)
		missing := e.missing(v)
		key, _ := jsv.Get(v, "key").(string)
		due := mustNum(v, "due")
		onTimeout := e.op.OnTimeout
		*emits = append(*emits, &emit{
			key:   key,
			ctx:   map[string]interface{}{"kind": "timeout", "key": key, "partition": key, "deadline": due - 1, "missing": missing},
			state: v, time: due, hasTime: true, et: due - 1, hasET: true,
			compute: func() (interface{}, error) {
				return callValue(func() (interface{}, error) { return onTimeout(clone(got), clone(missing), key) })
			},
		})
	}
}

func (e *collectEngine) process(env *envelope, t float64, emits *[]*emit) error {
	op := e.op
	msg := messageOf(env, t)
	part := ""
	if op.Parts != nil {
		found := false
		for _, p := range op.Parts {
			ok, err := e.pred(p, msg, env.value)
			if err != nil {
				return err
			}
			if ok {
				part, found = p.Name, true
				break
			}
		}
		if !found {
			return nil
		}
	}
	key := env.key
	if op.Key != nil {
		k, err := callValue(func() (interface{}, error) { return op.Key(msg, env.value) })
		if err != nil {
			return err
		}
		key = jsv.String(k)
	}
	sk := "c" + sep + key
	run, had := e.state.Get(sk)
	// A gather that finished: remembered until its deadline, and quiet.
	if had && jsv.Truthy(jsv.Get(run, "done")) {
		return nil
	}
	k, err := keptOf(op.Keep, msg, env.value)
	if err != nil {
		return err
	}
	snap := snapshotOf(msg, k)
	var next map[string]interface{}
	if op.Parts != nil {
		if had {
			parts := asMap(jsv.Get(run, "parts"))
			if _, in := parts[part]; in && op.OnRepeat == "keep" {
				return nil
			}
			next = copyMap(asMap(run))
			np := copyMap(parts)
			np[part] = snap
			next["parts"] = np
		} else {
			next = map[string]interface{}{"key": key, "first": t, "due": t + op.WithinMs + 1, "parts": map[string]interface{}{part: snap}}
		}
	} else {
		id := partID(msg)
		var ids []interface{}
		var list []interface{}
		var expected interface{}
		if had {
			ids, _ = jsv.Get(run, "ids").([]interface{})
			for _, x := range ids {
				if s, ok := x.(string); ok && s == id {
					return nil
				}
			}
			list, _ = jsv.Get(run, "list").([]interface{})
			expected = jsv.Get(run, "expected")
			if jsv.IsUndefined(expected) {
				expected = nil
			}
		}
		if op.Count != nil {
			n, err := callValue(func() (interface{}, error) { return op.Count(msg, env.value) })
			if err != nil {
				return err
			}
			if jsv.IsInteger(n) {
				f, _ := jsv.Num(n)
				if f > 0 {
					if expected == nil {
						expected = f
					} else {
						ef, _ := jsv.Num(expected)
						expected = math.Max(ef, f)
					}
				}
			}
		}
		if had {
			next = copyMap(asMap(run))
			next["list"] = append(append([]interface{}(nil), list...), snap)
			next["ids"] = append(append([]interface{}(nil), ids...), id)
			next["expected"] = expected
		} else {
			next = map[string]interface{}{"key": key, "first": t, "due": t + op.WithinMs + 1,
				"list": []interface{}{snap}, "ids": []interface{}{id}, "expected": expected}
		}
	}
	if e.complete(next) {
		got := e.got(next)
		due := mustNum(next, "due")
		var compute func() (interface{}, error)
		if op.OnComplete != nil {
			onComplete := op.OnComplete
			compute = func() (interface{}, error) {
				return callValue(func() (interface{}, error) { return onComplete(clone(got), key) })
			}
		} else {
			compute = func() (interface{}, error) {
				m := map[string]interface{}{"key": key}
				setDef(m, "parts", clone(got))
				return m, nil
			}
		}
		*emits = append(*emits, &emit{
			key: key, ctx: map[string]interface{}{"kind": "complete", "key": key, "partition": key, "deadline": due - 1},
			state: next, et: t, hasET: true, compute: compute,
		})
		if op.Remember {
			e.state.Set(sk, map[string]interface{}{"key": key, "due": due, "done": true})
			if !had {
				e.heap.push(due, sk)
			}
		} else {
			e.state.Delete(sk)
		}
		return nil
	}
	e.state.Set(sk, next)
	if !had {
		e.heap.push(mustNum(next, "due"), sk)
	}
	return nil
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (e *collectEngine) complete(v interface{}) bool {
	if e.op.Parts != nil {
		parts := asMap(jsv.Get(v, "parts"))
		for _, p := range e.op.Parts {
			if pv, ok := parts[p.Name]; !ok || jsv.IsUndefined(pv) {
				return false
			}
		}
		return true
	}
	expected := jsv.Get(v, "expected")
	if jsv.IsNullish(expected) {
		return false
	}
	ef, _ := jsv.Num(expected)
	list, _ := jsv.Get(v, "list").([]interface{})
	return float64(len(list)) >= ef
}

func (e *collectEngine) got(v interface{}) interface{} {
	if e.op.Parts != nil {
		return jsv.Get(v, "parts")
	}
	return jsv.Get(v, "list")
}

func (e *collectEngine) missing(v interface{}) interface{} {
	if e.op.Parts != nil {
		parts := asMap(jsv.Get(v, "parts"))
		out := []interface{}{}
		for _, p := range e.op.Parts {
			if pv, ok := parts[p.Name]; !ok || jsv.IsUndefined(pv) {
				out = append(out, p.Name)
			}
		}
		return out
	}
	expected := jsv.Get(v, "expected")
	if jsv.IsNullish(expected) {
		return nil
	}
	ef, _ := jsv.Num(expected)
	list, _ := jsv.Get(v, "list").([]interface{})
	return math.Max(0, ef-float64(len(list)))
}

func (e *collectEngine) view(v interface{}) map[string]interface{} {
	key := jsv.Get(v, "key")
	due := mustNum(v, "due")
	if jsv.Truthy(jsv.Get(v, "done")) {
		out := map[string]interface{}{"done": true, "until": isoOrNil(due - 1)}
		setDef(out, "key", key)
		return out
	}
	out := map[string]interface{}{"missing": e.missing(v), "deadline": isoOrNil(due - 1)}
	setDef(out, "key", key)
	setDef(out, "got", clone(e.got(v)))
	if l := jsv.Get(v, "list"); jsv.Truthy(l) {
		setDef(out, "expected", jsv.Get(v, "expected"))
	}
	return out
}

func (e *collectEngine) inspect(key string) interface{} {
	v, ok := e.state.Get("c" + sep + key)
	if !ok {
		return nil
	}
	return e.view(v)
}

func (e *collectEngine) list(fn func(item interface{}) bool) {
	e.state.Each(func(k string, v interface{}) bool {
		if isBufferKey(k) {
			return true
		}
		return fn(e.view(v))
	})
}
