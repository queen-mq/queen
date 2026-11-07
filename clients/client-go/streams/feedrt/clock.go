package feedrt

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// clock is one stateful step's clock (clock.js): the feed's, or event time.
type clock interface {
	kind() string
	needsFeedClock() bool
	accept(env *envelope, t float64, emits *[]*emit) (string, error)
	nextAt() (float64, bool)
	take(emits *[]*emit, when float64, limit int) takeResult
	watermarkAt(t float64) float64
	checkpoint() *clockCP
	restore(cp *clockCP)
	meta() map[string]interface{}
	loadMeta(m interface{})
	rebuild()
	held() int
}

type takeResult struct {
	at      float64
	release *release
}

type release struct {
	key string
	ts  float64
	env *envelope
}

type clockCP struct {
	w     float64
	t     float64
	hasT  bool
	s     float64
	hasS  bool
	n     int
	valid bool
}

// feedClock: a step's time is the feed time it is given.
type feedClock struct {
	engine engine
}

func (c *feedClock) kind() string         { return "feed" }
func (c *feedClock) needsFeedClock() bool { return true }
func (c *feedClock) accept(env *envelope, t float64, emits *[]*emit) (string, error) {
	return "ok", c.engine.process(env, t, emits)
}
func (c *feedClock) nextAt() (float64, bool) { return c.engine.nextDue() }
func (c *feedClock) take(emits *[]*emit, when float64, limit int) takeResult {
	due, _ := c.engine.nextDue()
	c.engine.advance(due, emits, limit)
	return takeResult{at: due}
}
func (c *feedClock) watermarkAt(float64) float64  { return math.NaN() }
func (c *feedClock) checkpoint() *clockCP         { return nil }
func (c *feedClock) restore(*clockCP)             {}
func (c *feedClock) meta() map[string]interface{} { return map[string]interface{}{} }
func (c *feedClock) loadMeta(interface{})         {}
func (c *feedClock) rebuild()                     {}
func (c *feedClock) held() int                    { return 0 }

// eventClock is event time: a time the messages carry (eventTime(msg)), or,
// after another step on event time, the time of that step's result. The
// step keeps a watermark: the latest event time seen minus allowedLateness.
type eventClock struct {
	engine     engine
	state      *FeedState
	fn         func(msg interface{}) (interface{}, error)
	shape      string // value (windows) or feed (patterns)
	latenessMs float64
	idleMs     *float64
	onLate     string
	ordered    bool
	upstream   *eventClock
	graceMs    float64
	heap       DueHeap
	// From the messages: the latest event time seen minus allowedLateness, as
	// of the feed time tLast; tLast unset: none yet.
	wLast float64
	tLast float64
	hasT  bool
	// The feed time of the step's last message or close: nothing it does later happens before it.
	since float64
	hasS  bool
	// The number of the next held message.
	seq int
}

type eventClockOpts struct {
	engine     engine
	state      *FeedState
	fn         func(msg interface{}) (interface{}, error)
	shape      string
	latenessMs float64
	idleMs     *float64
	onLate     string
	ordered    bool
	upstream   *eventClock
	graceMs    float64
}

func newEventClock(o eventClockOpts) (*eventClock, error) {
	if !finite(o.latenessMs) || o.latenessMs < 0 {
		return nil, fmt.Errorf("allowedLateness must be a duration of zero or more, got %s", jsv.NumberString(o.latenessMs))
	}
	if o.idleMs != nil && (!finite(*o.idleMs) || *o.idleMs <= 0) {
		return nil, fmt.Errorf("idle must be a duration above zero, got %s", jsv.NumberString(*o.idleMs))
	}
	if o.upstream != nil && o.idleMs != nil {
		return nil, fmt.Errorf("a step fed by a step on event time follows its watermark: idle goes on the first step on event time")
	}
	if o.shape == "" {
		o.shape = "feed"
	}
	if o.onLate == "" {
		o.onLate = "drop"
	}
	grace := 0.0
	if finite(o.graceMs) && o.graceMs > 0 {
		grace = o.graceMs
	}
	return &eventClock{
		engine: o.engine, state: o.state, fn: o.fn, shape: o.shape, latenessMs: o.latenessMs, idleMs: o.idleMs,
		onLate: o.onLate, ordered: o.ordered, upstream: o.upstream, graceMs: grace, wLast: math.Inf(-1),
	}, nil
}

func (c *eventClock) kind() string { return "event" }

// needsFeedClock: the feed's clock moves it, with idle, or when the step before it moves with that clock.
func (c *eventClock) needsFeedClock() bool {
	return c.idleMs != nil || (c.upstream != nil && c.upstream.needsFeedClock())
}

// watermarkAt is the watermark at feed time t (-Inf: none yet).
func (c *eventClock) watermarkAt(t float64) float64 {
	if c.upstream != nil {
		f := c.upstream.floorAt(t)
		if finite(f) {
			return math.Max(c.wLast, f-c.latenessMs)
		}
		return c.wLast
	}
	if !c.hasT || c.idleMs == nil || !finite(c.wLast) {
		return c.wLast
	}
	quiet := t - c.tLast - *c.idleMs
	if quiet > 0 {
		return c.wLast + quiet
	}
	return c.wLast
}

// readyAt is the feed time at which the watermark reaches x; ok false when
// only a message, or the step before, can take it there.
func (c *eventClock) readyAt(x float64) (float64, bool) {
	var at float64
	ok := false
	if c.hasT && x <= c.wLast {
		at, ok = c.tLast, true
	} else if c.upstream != nil {
		at, ok = c.upstream.floorReadyAt(x + c.latenessMs)
	} else if c.hasT && c.idleMs != nil && finite(c.wLast) {
		at, ok = c.tLast+*c.idleMs+(x-c.wLast), true
	}
	if !ok {
		return 0, false
	}
	if c.hasS && c.since > at {
		return c.since, true
	}
	return at, true
}

// floorAt is the oldest event time this step can still send a result with, at feed time t.
func (c *eventClock) floorAt(t float64) float64 {
	f := c.watermarkAt(t)
	c.prune()
	if c.heap.size() > 0 {
		f = math.Min(f, c.heap.peek().due)
	}
	if d, ok := c.engine.nextDue(); ok {
		f = math.Min(f, d-c.graceMs-2)
	}
	return f
}

// floorReadyAt is the feed time at which the floor reaches x; ok false while
// a held message or an open close could still send something older.
func (c *eventClock) floorReadyAt(x float64) (float64, bool) {
	c.prune()
	if c.heap.size() > 0 && c.heap.peek().due < x {
		return 0, false
	}
	if d, ok := c.engine.nextDue(); ok && d-c.graceMs-2 < x {
		return 0, false
	}
	return c.readyAt(x)
}

// extractEventTime is _windowCommon.js extractEventTime: the extractor's
// answer as a time (ok false: none).
func extractEventTime(msg interface{}, fn func(msg interface{}) (interface{}, error)) (float64, bool, error) {
	raw, err := callValue(func() (interface{}, error) { return fn(msg) })
	if err != nil {
		return 0, false, fmt.Errorf("eventTime extractor threw: %s", err.Error())
	}
	t, ok := jsv.TimeOf(raw)
	return t, ok, nil
}

func (c *eventClock) timeOf(env *envelope, t float64) (float64, error) {
	if c.fn != nil {
		var msg interface{} = env.msg
		if c.shape == "feed" {
			msg = messageOf(env, t)
		}
		ts, ok, err := extractEventTime(msg, c.fn)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("eventTime gave no time for this message (a Date, epoch milliseconds or an ISO string)")
		}
		return ts, nil
	}
	if env.hasET && finite(env.et) {
		return env.et, nil
	}
	return 0, fmt.Errorf("a result reached a step on event time without an event time")
}

// accept takes one envelope at feed time t: ignored (the step's tests do not
// take it), late, held until the watermark reaches it (in order), or taken now.
func (c *eventClock) accept(env *envelope, t float64, emits *[]*emit) (string, error) {
	if c.ordered {
		rt := t
		if env.hasET && finite(env.et) {
			rt = env.et
		}
		ok, err := c.engine.relevant(env, rt)
		if err != nil {
			return "", err
		}
		if !ok {
			return "ok", nil
		}
	}
	ts, err := c.timeOf(env, t)
	if err != nil {
		return "", err
	}
	// A fed step judges by the results it got: its watermark also follows the step before.
	w := c.wLast
	if c.upstream == nil {
		w = c.watermarkAt(t)
	}
	c.wLast = math.Max(w, ts-c.latenessMs)
	c.tLast, c.hasT = t, true
	if !c.hasS || t > c.since {
		c.since, c.hasS = t, true
	}
	if ts < w {
		if c.onLate == "include" {
			if err := c.engine.process(env, ts, emits); err != nil {
				return "", err
			}
		}
		return "late", nil
	}
	if !c.ordered {
		if err := c.engine.process(env, ts, emits); err != nil {
			return "", err
		}
		return "ok", nil
	}
	key := bufferKey(c.seq)
	c.seq++
	c.state.Set(key, map[string]interface{}{"t": ts, "e": pack(env)})
	c.heap.push(ts, key)
	return "ok", nil
}

func bufferKey(n int) string {
	s := strconv.Itoa(n)
	if len(s) < 12 {
		s = strings.Repeat("0", 12-len(s)) + s
	}
	return BufferPrefix + s
}

func (c *eventClock) prune() {
	// A held message a rollback took back out of the state, or put back under
	// the same key with another time (seq rolls back with the state).
	for c.heap.size() > 0 {
		top := c.heap.peek()
		if v, ok := c.state.Get(top.key); ok {
			if t, ok := num(v, "t"); ok && t == top.due {
				return
			}
		}
		c.heap.pop()
	}
}

type nextThing struct {
	at  float64
	key string
}

// next is the next thing due in event time: a deadline (first, at a tie) or a held message.
func (c *eventClock) next() (nextThing, bool) {
	c.prune()
	due, hasDue := c.engine.nextDue()
	if c.heap.size() > 0 {
		held := c.heap.peek()
		if !hasDue || held.due < due {
			return nextThing{at: held.due, key: held.key}, true
		}
	}
	if !hasDue {
		return nextThing{}, false
	}
	return nextThing{at: due}, true
}

func (c *eventClock) nextAt() (float64, bool) {
	n, ok := c.next()
	if !ok {
		return 0, false
	}
	return c.readyAt(n.at)
}

// take takes what is due next, at feed time when: closes the engine's
// windows or deadlines of that time, or takes the next held message out.
func (c *eventClock) take(emits *[]*emit, when float64, limit int) takeResult {
	n, _ := c.next()
	if finite(when) && (!c.hasS || when > c.since) {
		c.since, c.hasS = when, true
	}
	if n.key == "" {
		c.engine.advance(n.at, emits, limit)
		return takeResult{at: n.at}
	}
	c.heap.pop()
	v := c.state.Value(n.key)
	c.state.Delete(n.key)
	ts := mustNum(v, "t")
	return takeResult{at: n.at, release: &release{key: n.key, ts: ts, env: unpack(jsv.Get(v, "e"))}}
}

func (c *eventClock) held() int { return c.state.Count("held") }

func (c *eventClock) checkpoint() *clockCP {
	return &clockCP{w: c.wLast, t: c.tLast, hasT: c.hasT, s: c.since, hasS: c.hasS, n: c.seq, valid: true}
}

func (c *eventClock) restore(cp *clockCP) {
	if cp == nil {
		return
	}
	c.wLast, c.tLast, c.hasT, c.since, c.hasS, c.seq = cp.w, cp.t, cp.hasT, cp.s, cp.hasS, cp.n
}

// meta is the watermark and its anchors, for the snapshot pointer (JSON: no infinities).
func (c *eventClock) meta() map[string]interface{} {
	m := map[string]interface{}{"n": float64(c.seq)}
	if finite(c.wLast) {
		m["w"] = c.wLast
	}
	if c.hasT {
		m["t"] = c.tLast
	}
	if c.hasS {
		m["s"] = c.since
	}
	return m
}

func (c *eventClock) loadMeta(meta interface{}) {
	if w, ok := num(meta, "w"); ok {
		c.wLast = w
	} else if w, ok := num(meta, "watermark"); ok {
		// A record from before the anchors (the windows' own watermark).
		c.wLast = w
	} else {
		c.wLast = math.Inf(-1)
	}
	c.tLast, c.hasT = num(meta, "t")
	if s, ok := num(meta, "s"); ok {
		c.since, c.hasS = s, true
	} else {
		c.since, c.hasS = c.tLast, c.hasT
	}
	c.seq = 0
	if n := jsv.Get(meta, "n"); jsv.IsInteger(n) {
		if f, _ := jsv.Num(n); f >= 0 {
			c.seq = int(f)
		}
	}
}

// rebuild indexes the held messages of the state, after a load; numbers go on after the highest one held.
func (c *eventClock) rebuild() {
	c.heap.clear()
	c.state.Each(func(k string, v interface{}) bool {
		if !isBufferKey(k) {
			return true
		}
		t, ok := num(v, "t")
		if !ok {
			return true
		}
		c.heap.push(t, k)
		if n, err := strconv.Atoi(k[len(BufferPrefix):]); err == nil && n >= c.seq {
			c.seq = n + 1
		}
		return true
	})
}

// pack is an envelope as a held entry: the message, and only what differs from it.
func pack(env *envelope) map[string]interface{} {
	e := map[string]interface{}{"k": env.key}
	setDef(e, "m", env.msg)
	switch {
	case jsv.Same(env.value, env.msg):
		e["vm"] = 1.0
	case jsv.IsObject(env.msg) && jsv.Same(env.value, jsv.Get(env.msg, "data")):
		e["vd"] = 1.0
	default:
		setDef(e, "v", env.value)
	}
	if env.derived {
		e["d"] = 1.0
	}
	if !jsv.IsUndefined(env.ctx) {
		e["c"] = env.ctx
	}
	if !jsv.IsNullish(env.position) {
		e["p"] = env.position
	}
	if env.hasET && finite(env.et) {
		e["et"] = env.et
	}
	return e
}

func unpack(raw interface{}) *envelope {
	get := func(k string) interface{} { return jsv.Get(raw, k) }
	env := &envelope{msg: get("m"), ctx: undefinedValue, position: undefinedValue}
	env.key = jsv.String(get("k"))
	if k, ok := get("k").(string); ok {
		env.key = k
	}
	switch {
	case jsv.Truthy(get("vm")):
		env.value = env.msg
	case jsv.Truthy(get("vd")):
		if jsv.IsObject(env.msg) {
			env.value = jsv.Get(env.msg, "data")
		} else {
			env.value = undefinedValue
		}
	default:
		env.value = get("v")
	}
	if jsv.Truthy(get("d")) {
		env.derived = true
	}
	if c := get("c"); !jsv.IsUndefined(c) {
		env.ctx = c
	}
	if p := get("p"); !jsv.IsUndefined(p) {
		env.position = p
	}
	if et := get("et"); !jsv.IsUndefined(et) {
		if f, ok := jsv.Num(et); ok {
			env.et, env.hasET = f, true
		}
	}
	return env
}
