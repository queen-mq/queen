package feedrt

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// engine is a stateful step's engine: windows, patterns, collects.
type engine interface {
	kind() string
	relevant(env *envelope, t float64) (bool, error)
	rebuild()
	nextDue() (float64, bool)
	open() int
	advance(t float64, emits *[]*emit, limit int)
	process(env *envelope, t float64, emits *[]*emit) error
	inspect(key string) interface{}
	list(fn func(item interface{}) bool)
}

const sep = "\u001f"

const weekEpochOffsetMs = 4 * 86_400 * 1000

// finalAcc is an aggregate's result, without the running sums it keeps for avg.
func finalAcc(acc interface{}, isAggregate bool) interface{} {
	m, ok := acc.(map[string]interface{})
	if !isAggregate || !ok || !jsv.Truthy(acc) {
		return clone(acc)
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if !strings.HasPrefix(k, "__avg_") {
			out[k] = v
		}
	}
	return out
}

// windowEngine is windows on the tenant feed (windows.js): tumbling,
// sliding, cron and session windows with reduce/aggregate, per key.
//
// State keys: w\x1f<start>\x1f<key> for a window of the fixed kinds,
// s\x1f<key> for a key's open session. A value is {key, start, end, due, acc}
// (a session also keeps last), where due is when it closes.
type windowEngine struct {
	op      *WindowSpec
	reducer *ReducerSpec
	state   *FeedState
	heap    DueHeap
}

func newWindowEngine(op *WindowSpec, reducer *ReducerSpec, state *FeedState) (*windowEngine, error) {
	if reducer == nil {
		return nil, fmt.Errorf("a window on a feed needs .reduce() or .aggregate() after it")
	}
	e := &windowEngine{op: op, reducer: reducer, state: state}
	state.ClassifyWith(func(k string, v interface{}) string {
		if isBufferKey(k) {
			return "held"
		}
		if strings.HasPrefix(k, "w"+sep) || strings.HasPrefix(k, "s"+sep) {
			return "open"
		}
		return ""
	})
	return e, nil
}

func (e *windowEngine) kind() string { return e.op.Kind }

func (e *windowEngine) relevant(*envelope, float64) (bool, error) { return true, nil }

func (e *windowEngine) rebuild() {
	e.heap.clear()
	e.state.Each(func(k string, v interface{}) bool {
		e.index(k, v)
		return true
	})
}

func (e *windowEngine) index(k string, v interface{}) {
	if strings.HasPrefix(k, "w"+sep) || strings.HasPrefix(k, "s"+sep) {
		if due, ok := num(v, "due"); ok {
			e.heap.push(due, k)
		}
	}
}

func (e *windowEngine) prune() {
	// A heap item whose window was closed, or moved, since it was pushed.
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

func (e *windowEngine) nextDue() (float64, bool) {
	e.prune()
	if e.heap.size() == 0 {
		return 0, false
	}
	return e.heap.peek().due, true
}

func (e *windowEngine) open() int { return e.state.Count("open") }

// advance closes every window due at or before t: one emit each.
func (e *windowEngine) advance(t float64, emits *[]*emit, limit int) {
	for n := 0; n < limit; n++ {
		e.prune()
		if e.heap.size() == 0 || e.heap.peek().due > t {
			return
		}
		sk := e.heap.pop().key
		v := e.state.Value(sk)
		e.state.Delete(sk)
		em := e.emitOf(v)
		// The result's time is when the window closed, not when this process saw it close.
		em.time, em.hasTime = mustNum(v, "due"), true
		*emits = append(*emits, em)
	}
}

func mustNum(v interface{}, k string) float64 {
	f, _ := jsv.Num(jsv.Get(v, k))
	return f
}

func (e *windowEngine) process(env *envelope, t float64, emits *[]*emit) error {
	key := env.key
	value := env.value
	if jsv.IsUndefined(value) {
		value = env.msg
	}
	if e.op.Kind == "session" {
		return e.session(key, t, value, emits)
	}
	ws, err := e.windowsOf(t)
	if err != nil {
		return err
	}
	for _, w := range ws {
		if err := e.fold(key, w[0], w[1], value); err != nil {
			return err
		}
	}
	return nil
}

func (e *windowEngine) floorToBoundary(ts float64) float64 {
	if e.op.Every == "week" {
		shifted := ts - weekEpochOffsetMs
		week := math.Floor(shifted / e.op.WindowMs)
		return week*e.op.WindowMs + weekEpochOffsetMs
	}
	return math.Floor(ts/e.op.WindowMs) * e.op.WindowMs
}

func (e *windowEngine) windowsOf(ts float64) ([][2]float64, error) {
	op := e.op
	switch op.Kind {
	case "tumbling":
		start := math.Floor(ts/op.WindowMs) * op.WindowMs
		return [][2]float64{{start, start + op.WindowMs}}, nil
	case "cron":
		start := e.floorToBoundary(ts)
		return [][2]float64{{start, start + op.WindowMs}}, nil
	case "sliding":
		latest := math.Floor(ts/op.SlideMs) * op.SlideMs
		var out [][2]float64
		for i := 0.0; i < op.WindowsPerEvent; i++ {
			start := latest - i*op.SlideMs
			if ts >= start && ts < start+op.SizeMs {
				out = append(out, [2]float64{start, start + op.SizeMs})
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("a %s window does not run on a feed", op.Kind)
}

func (e *windowEngine) initial() interface{} {
	init := e.reducer.Initial
	if jsv.IsUndefined(init) || init == nil || !jsv.IsObject(init) {
		return init
	}
	return jsv.JSONClone(init)
}

func (e *windowEngine) fold(key string, start, end float64, value interface{}) error {
	sk := "w" + sep + jsv.NumberString(start) + sep + key
	cur, had := e.state.Get(sk)
	var acc interface{}
	if had {
		acc = clone(jsv.Get(cur, "acc"))
	} else {
		acc = e.initial()
	}
	next, err := callValue(func() (interface{}, error) { return e.reducer.Fn(acc, value) })
	if err != nil {
		return err
	}
	due := end + e.op.GraceMs
	v := map[string]interface{}{"key": key, "start": start, "end": end, "due": due}
	setDef(v, "acc", next)
	e.state.Set(sk, v)
	if !had {
		e.heap.push(due, sk)
	}
	return nil
}

func (e *windowEngine) session(key string, ts float64, value interface{}, emits *[]*emit) error {
	sk := "s" + sep + key
	cur, had := e.state.Get(sk)
	gap := e.op.GapMs
	if had {
		last := mustNum(cur, "last")
		if ts > last+gap {
			// Past the gap: that session is over, and this message starts the next.
			e.state.Delete(sk)
			*emits = append(*emits, e.emitOf(cur))
			had = false
			cur = nil
		}
	}
	var acc interface{}
	if had {
		acc = clone(jsv.Get(cur, "acc"))
	} else {
		acc = e.initial()
	}
	next, err := callValue(func() (interface{}, error) { return e.reducer.Fn(acc, value) })
	if err != nil {
		return err
	}
	start, last := ts, ts
	if had {
		start = math.Min(mustNum(cur, "start"), ts)
		last = math.Max(mustNum(cur, "last"), ts)
	}
	// A message exactly gap after the last one is still in the session: it closes the millisecond after.
	due := last + gap + 1 + e.op.GraceMs
	v := map[string]interface{}{"key": key, "start": start, "last": last, "end": last + gap, "due": due}
	setDef(v, "acc", next)
	e.state.Set(sk, v)
	if !had || mustNum(cur, "due") != due {
		e.heap.push(due, sk)
	}
	return nil
}

func (e *windowEngine) view(v interface{}) map[string]interface{} {
	out := map[string]interface{}{
		"start":    isoOrNil(mustNum(v, "start")),
		"end":      isoOrNil(mustNum(v, "end")),
		"closesAt": isoOrNil(mustNum(v, "due")),
	}
	setDef(out, "value", finalAcc(jsv.Get(v, "acc"), e.reducer.IsAggregate))
	return out
}

func (e *windowEngine) inspect(key string) interface{} {
	var windows []map[string]interface{}
	var starts []float64
	e.state.Each(func(_ string, v interface{}) bool {
		if k, ok := jsv.Get(v, "key").(string); ok && k == key {
			windows = append(windows, e.view(v))
			starts = append(starts, mustNum(v, "start"))
		}
		return true
	})
	if len(windows) == 0 {
		return nil
	}
	idx := make([]int, len(windows))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		// JS compares the ISO strings, which order as the times do.
		return windows[idx[a]]["start"].(string) < windows[idx[b]]["start"].(string)
	})
	list := make([]interface{}, len(idx))
	for i, j := range idx {
		list[i] = windows[j]
	}
	return map[string]interface{}{"key": key, "windows": list}
}

func (e *windowEngine) list(fn func(item interface{}) bool) {
	e.state.Each(func(_ string, v interface{}) bool {
		k := jsv.Get(v, "key")
		if jsv.IsUndefined(k) {
			return true
		}
		return fn(map[string]interface{}{"key": k, "windows": []interface{}{e.view(v)}})
	})
}

func (e *windowEngine) emitOf(v interface{}) *emit {
	key, _ := jsv.Get(v, "key").(string)
	start := mustNum(v, "start")
	end := mustNum(v, "end")
	return &emit{
		key: key,
		// On to a later step on event time: the last millisecond the window covers.
		et: end - 1, hasET: true,
		value: finalAcc(jsv.Get(v, "acc"), e.reducer.IsAggregate),
		ctx: map[string]interface{}{
			"kind":        e.op.Kind,
			"key":         key,
			"partition":   key,
			"windowStart": start,
			"windowEnd":   end,
			"windowKey":   isoOrNil(start),
		},
		state: v,
	}
}
