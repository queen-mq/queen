package feedrt

import (
	"fmt"
	"math"
	"strings"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// pStep is one step of the pipeline: stateless operators, or a stateful step
// with its state, its engine (windows, patterns, collects) or table, and its clock.
type pStep struct {
	kind      string
	name      string
	ops       []Op
	state     *FeedState
	engine    engine
	table     *tableStep
	clock     clock
	committed *clockCP
	label     string
}

// Pipeline is the steps of a feed stream, run in chain order (pipeline.js).
type Pipeline struct {
	steps []*pStep
	late  int
}

func stepLabel(s Step) string {
	switch s.Kind {
	case "window":
		return "the " + s.Window.Kind + " window"
	case "pattern":
		return "the " + s.Pattern.Kind
	case "collect":
		return "the " + s.Collect.Kind
	}
	return "the " + s.Kind
}

// clockOf is the clock of a step: event time when it says eventTime, or when
// the stateful step before it runs on event time (unless eventTime is off);
// else the feed's.
func clockOf(s Step, eng engine, state *FeedState, before *eventClock, fed bool) (clock, error) {
	var t TimeSpec
	grace := 0.0
	switch s.Kind {
	case "window":
		t = s.Window.Time
		grace = s.Window.GraceMs
	case "pattern":
		t = s.Pattern.Time
	case "collect":
		t = s.Collect.Time
	}
	what := stepLabel(s) + " " + string(jsv.MustMarshal(s.Name))
	// Patterns, joins, gathers and sessions depend on the order of their messages.
	ordered := s.Kind != "window" || s.Window.Kind == "session"
	onLate := t.OnLate
	if onLate == "" {
		onLate = "drop"
	}
	latenessMs := t.LatenessMs
	idleMs := t.IdleMs
	if t.EventTime != nil {
		if fed && idleMs != nil {
			return nil, fmt.Errorf("Stream.fromFeed: idle on %s, whose messages are the results of the step before it: moving on with the feed's "+
				"clock it could pass a result that step has not sent yet. Leave out its eventTime (it then takes the times of those "+
				"results and follows that step's watermark), and put idle on the first step on event time", what)
		}
		shape := "feed"
		if s.Kind == "window" {
			shape = "value"
		}
		return newEventClock(eventClockOpts{engine: eng, state: state, fn: t.EventTime, shape: shape,
			latenessMs: latenessMs, idleMs: idleMs, onLate: onLate, ordered: ordered, graceMs: grace})
	}
	if before != nil && !t.EventTimeOff {
		if idleMs != nil {
			return nil, fmt.Errorf("Stream.fromFeed: %s runs on the event time of the step before it and follows its watermark: put idle on the first step on event time", what)
		}
		return newEventClock(eventClockOpts{engine: eng, state: state, latenessMs: latenessMs, onLate: onLate,
			ordered: ordered, upstream: before, graceMs: grace})
	}
	if idleMs != nil {
		return nil, fmt.Errorf("Stream.fromFeed: idle moves a watermark on: %s runs on the feed's clock, give it eventTime (or put it after a step on event time)", what)
	}
	// A window as a pattern: both default to no lateness and 'drop', so anything else was asked for.
	if latenessMs > 0 || onLate == "include" {
		return nil, fmt.Errorf("Stream.fromFeed: allowedLateness and onLate are for event time: %s runs on the feed's clock, give it eventTime (or put it after a step on event time)", what)
	}
	return &feedClock{engine: eng}, nil
}

// NewPipeline builds the pipeline of a plan.
func NewPipeline(plan []Step) (*Pipeline, error) {
	p := &Pipeline{}
	var before *eventClock
	fed := false
	for _, s := range plan {
		if s.Kind == "ops" {
			p.steps = append(p.steps, &pStep{kind: "ops", ops: s.Ops})
			continue
		}
		state := NewFeedState()
		st := &pStep{kind: s.Kind, name: s.Name, state: state, label: stepLabel(s)}
		switch s.Kind {
		case "window":
			eng, err := newWindowEngine(s.Window, s.Reducer, state)
			if err != nil {
				return nil, err
			}
			st.engine = eng
		case "pattern":
			st.engine = newPatternEngine(s.Pattern, state)
		case "collect":
			st.engine = newCollectEngine(s.Collect, state)
		case "table":
			st.table = newTableStep(s.Table, state)
		default:
			return nil, fmt.Errorf("feed pipeline: unknown step %s", s.Kind)
		}
		if st.engine != nil {
			c, err := clockOf(s, st.engine, state, before, fed)
			if err != nil {
				return nil, err
			}
			st.clock = c
			before = nil
			if ec, ok := c.(*eventClock); ok {
				before = ec
			}
			fed = true
		}
		p.steps = append(p.steps, st)
	}
	p.CommitPoint()
	return p, nil
}

func (p *Pipeline) stateful() []*pStep {
	var out []*pStep
	for _, s := range p.steps {
		if s.state != nil {
			out = append(out, s)
		}
	}
	return out
}

// HasState: some step keeps state.
func (p *Pipeline) HasState() bool {
	for _, s := range p.steps {
		if s.state != nil {
			return true
		}
	}
	return false
}

func (p *Pipeline) tables() []*tableStep {
	var out []*tableStep
	for _, s := range p.steps {
		if s.table != nil {
			out = append(out, s.table)
		}
	}
	return out
}

func (p *Pipeline) step(name string) *pStep {
	for _, s := range p.steps {
		if s.state != nil && s.name == name {
			return s
		}
	}
	return nil
}

// ---- running ----------------------------------------------------------------

// applyRow: a table queue's message; every table of that queue takes the row.
func (p *Pipeline) applyRow(entry map[string]interface{}) error {
	q, _ := entry["queue"].(string)
	for _, s := range p.steps {
		if s.table != nil && s.table.hasQueue(q) {
			if err := s.table.applyRow(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

type outFn func(env *envelope, entry map[string]interface{}) error

// entry runs one feed message at feed time t; what reaches the end goes to out.
func (p *Pipeline) entry(entry map[string]interface{}, t float64, out outFn) error {
	part, _ := entry["partition"].(string)
	env := &envelope{msg: entry, key: part, value: jsv.Get(entry, "data"), ctx: undefinedValue, position: undefinedValue}
	return p.flow(0, []*envelope{env}, t, entry, out, entry["position"])
}

// unitFn runs one unit: the runner calls run(out) (again, to retry) and judges it.
type unitFn func(em *emit, run func(out outFn) error) (string, error)

// AdvanceOpts: MaxUnits stops after so many units (a replay stops where the
// stored record says the stream got to; <0: no cap); OnBatch is called after
// each batch, where the runner may commit in the middle of a long close
// ('abandon' stops here: the runner loaded its state again).
type AdvanceOpts struct {
	MaxUnits int
	OnBatch  func(done int) (string, error)
}

// Advance closes what is due by feed time t, in the order it fell due, across
// the steps: the earliest first, and at one time the later step first. Each
// result runs on through the steps after it as one unit.
func (p *Pipeline) Advance(t float64, unit unitFn, opts AdvanceOpts) (string, error) {
	maxUnits := opts.MaxUnits
	capped := maxUnits >= 0
	done := 0
	for {
		if capped && done >= maxUnits {
			return "ok", nil
		}
		at := -1
		when := math.Inf(1)
		for i := len(p.steps) - 1; i >= 0; i-- {
			c := p.steps[i].clock
			if c == nil {
				continue
			}
			if w, ok := c.nextAt(); ok && w <= t && w < when {
				at, when = i, w
			}
		}
		if at < 0 {
			return "ok", nil
		}
		s := p.steps[at]
		var emits []*emit
		// At most CloseBatch at once, never more than may run: no window leaves the state without its unit.
		limit := CloseBatch
		if capped && maxUnits-done < limit {
			limit = maxUnits - done
		}
		took := s.clock.take(&emits, when, limit)
		if took.release != nil {
			r, err := p.release(at, took.release, when, unit)
			if err != nil {
				return "", err
			}
			if r == "stop" {
				return "stop", nil
			}
			done++
			if opts.OnBatch != nil {
				r, err := opts.OnBatch(done)
				if err != nil {
					return "", err
				}
				if r == "abandon" {
					return "abandon", nil
				}
			}
			continue
		}
		for _, em := range emits {
			em.step = at
			e := em
			// A result happens when its window or deadline closed, whenever this process noticed.
			r, err := unit(e, func(out outFn) error {
				envs, err := p.derive([]*emit{e}, nil)
				if err != nil {
					return err
				}
				return p.flow(at+1, envs, when, nil, out, nil)
			})
			if err != nil {
				return "", err
			}
			if r == "stop" {
				return "stop", nil
			}
			done++
		}
		if opts.OnBatch != nil && len(emits) > 0 {
			r, err := opts.OnBatch(done)
			if err != nil {
				return "", err
			}
			if r == "abandon" {
				return "abandon", nil
			}
		}
	}
}

// release runs a message an event-time step held, now in its turn: through the step, and its results on.
func (p *Pipeline) release(at int, rel *release, when float64, unit unitFn) (string, error) {
	s := p.steps[at]
	env := rel.env
	kind := "message"
	if env.derived {
		kind = "result"
		if k := jsv.Get(env.ctx, "kind"); !jsv.IsNullish(k) {
			kind = jsv.String(k)
		}
	}
	em := &emit{
		step: at, key: env.key, time: rel.ts, hasTime: true, id: rel.key,
		ctx:     map[string]interface{}{"kind": kind, "key": env.key, "partition": env.key, "eventTime": rel.ts},
		state:   map[string]interface{}{"msg": env.msg},
		release: true, relEnv: env, relTS: rel.ts,
	}
	// A feed message: reported and dead-lettered as the message it is; a result, as the result it is.
	if env.derived {
		em.value = env.value
	} else {
		em.entry = asMap(env.msg)
		em.value = undefinedValue
	}
	return unit(em, func(out outFn) error {
		var emits []*emit
		if err := s.engine.process(env, rel.ts, &emits); err != nil {
			return err
		}
		var position interface{} = env.position
		if jsv.IsNullish(position) {
			position = nil
			if !env.derived {
				if pos := jsv.Get(env.msg, "position"); !jsv.IsNullish(pos) {
					position = pos
				}
			}
		}
		envs, err := p.derive(emits, position)
		if err != nil {
			return err
		}
		return p.flow(at+1, envs, when, nil, out, position)
	})
}

// NextDue is when the next window, deadline or held message is due on the feed's clock.
func (p *Pipeline) NextDue() (float64, bool) {
	min, has := 0.0, false
	for _, s := range p.steps {
		if s.clock == nil || !s.clock.needsFeedClock() {
			continue
		}
		if d, ok := s.clock.nextAt(); ok && (!has || d < min) {
			min, has = d, true
		}
	}
	return min, has
}

// NeedsFeedClock: a step closes as the feed's clock moves.
func (p *Pipeline) NeedsFeedClock() bool {
	for _, s := range p.steps {
		if s.clock != nil && s.clock.needsFeedClock() {
			return true
		}
	}
	return false
}

// WatermarkAt is the lowest event-time watermark at feed time t, for the metrics.
func (p *Pipeline) WatermarkAt(t float64) (float64, bool) {
	w, has := 0.0, false
	for _, s := range p.steps {
		if s.clock == nil || s.clock.kind() != "event" {
			continue
		}
		x := s.clock.watermarkAt(t)
		if finite(x) {
			if !has || x < w {
				w = x
			}
			has = true
		}
	}
	return w, has
}

func applyOp(op Op, env *envelope) ([]*envelope, error) {
	switch op.Kind {
	case "map":
		v, err := callValue(func() (interface{}, error) { return op.Map(env.msg, ctxArg(env.ctx)) })
		if err != nil {
			return nil, err
		}
		return []*envelope{env.with(v)}, nil
	case "filter":
		keep, err := callBool(func() (bool, error) { return op.Filter(env.msg, ctxArg(env.ctx)) })
		if err != nil {
			return nil, err
		}
		if !keep {
			return nil, nil
		}
		return []*envelope{env}, nil
	case "where":
		if jsv.IsNullish(env.msg) {
			// conditionHolds reads a field of the message: of null, JS throws.
			field := op.Where.Field
			if strings.HasPrefix(field, "data.") {
				field = "data"
			}
			what := "null"
			if jsv.IsUndefined(env.msg) {
				what = "undefined"
			}
			return nil, fmt.Errorf("Cannot read properties of %s (reading '%s')", what, field)
		}
		m, _ := env.msg.(map[string]interface{})
		if m == nil || !feed.ConditionHolds(*op.Where, m) {
			return nil, nil
		}
		return []*envelope{env}, nil
	case "flatMap":
		var out []interface{}
		err := callErr(func() error {
			var e error
			out, e = op.FlatMap(env.msg, ctxArg(env.ctx))
			return e
		})
		if err != nil {
			return nil, err
		}
		res := make([]*envelope, len(out))
		for i, v := range out {
			res[i] = env.with(v)
		}
		return res, nil
	case "keyBy":
		var key string
		err := callErr(func() error {
			var e error
			key, e = op.KeyBy(env.msg)
			return e
		})
		if err != nil {
			return nil, err
		}
		c := *env
		c.key = key
		return []*envelope{&c}, nil
	}
	return nil, fmt.Errorf("unsupported operator: %s", op.Kind)
}

// ctxArg is a step's ctx as a function gets it: nil for a feed message.
func ctxArg(ctx interface{}) interface{} {
	if jsv.IsUndefined(ctx) {
		return nil
	}
	return ctx
}

// flow runs envs through the steps from `from`. t is the feed time; entry the
// feed message they came from (nil for results of a close); position the
// position the results of a step carry.
func (p *Pipeline) flow(from int, envs []*envelope, t float64, entry map[string]interface{}, out outFn, position interface{}) error {
	cur := envs
	for i := from; i < len(p.steps) && len(cur) > 0; i++ {
		s := p.steps[i]
		if s.kind == "ops" {
			for _, op := range s.ops {
				var next []*envelope
				for _, env := range cur {
					r, err := applyOp(op, env)
					if err != nil {
						return err
					}
					next = append(next, r...)
				}
				cur = next
			}
			continue
		}
		if s.table != nil {
			var next []*envelope
			for _, env := range cur {
				r, err := s.table.apply(env, t)
				if err != nil {
					return err
				}
				next = append(next, r...)
			}
			cur = next
			continue
		}
		var emits []*emit
		for _, env := range cur {
			r, err := s.clock.accept(env, t, &emits)
			if err != nil {
				return err
			}
			if r == "late" {
				p.late++
			}
		}
		next, err := p.derive(emits, position)
		if err != nil {
			return err
		}
		cur = next
	}
	for _, env := range cur {
		if err := out(env, entry); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) derive(emits []*emit, position interface{}) ([]*envelope, error) {
	var out []*envelope
	for _, em := range emits {
		value := em.value
		if em.compute != nil {
			v, err := em.compute()
			if err != nil {
				return nil, err
			}
			value = v
			if jsv.IsNullish(value) {
				continue
			}
		}
		pos := position
		if jsv.IsUndefined(pos) {
			pos = nil
		}
		env := &envelope{msg: value, value: value, ctx: em.ctx, key: em.key, derived: true, position: pos}
		if em.hasET && finite(em.et) {
			env.et, env.hasET = em.et, true
		}
		out = append(out, env)
	}
	return out, nil
}

// ---- state --------------------------------------------------------------------

type stepCP struct {
	mark  int
	clock *clockCP
}

// Checkpoint is a point to come back to: every step's state and clock.
func (p *Pipeline) Checkpoint() []*stepCP {
	out := make([]*stepCP, len(p.steps))
	for i, s := range p.steps {
		if s.state == nil {
			continue
		}
		cp := &stepCP{mark: s.state.Mark()}
		if s.clock != nil {
			cp.clock = s.clock.checkpoint()
		}
		out[i] = cp
	}
	return out
}

// Restore goes back to a checkpoint.
func (p *Pipeline) Restore(cp []*stepCP) {
	for i, s := range p.steps {
		if s.state == nil || cp[i] == nil {
			continue
		}
		s.state.RollbackTo(cp[i].mark)
		if s.clock != nil && cp[i].clock != nil {
			s.clock.restore(cp[i].clock)
		}
	}
}

// CommitPoint: what is in now is committed; a rollback comes back here.
func (p *Pipeline) CommitPoint() {
	for _, s := range p.steps {
		if s.state == nil {
			continue
		}
		s.state.Committed()
		s.committed = nil
		if s.clock != nil {
			s.committed = s.clock.checkpoint()
		}
	}
}

// Discard goes back to the last commit point.
func (p *Pipeline) Discard() {
	for _, s := range p.steps {
		if s.state == nil {
			continue
		}
		s.state.Discard()
		if s.engine != nil {
			if s.committed != nil {
				s.clock.restore(s.committed)
			}
			s.engine.rebuild()
			s.clock.rebuild()
		}
	}
}

// Dirty: some step changed since the last commit.
func (p *Pipeline) Dirty() bool {
	for _, s := range p.steps {
		if s.state != nil && s.state.Dirty() {
			return true
		}
	}
	return false
}

// ChangedSinceSnapshot: some step changed since the last snapshot.
func (p *Pipeline) ChangedSinceSnapshot() bool {
	for _, s := range p.steps {
		if s.state != nil && s.state.ChangedSinceSnapshot() {
			return true
		}
	}
	return false
}

// SnapshotTaken: every step's state is in a snapshot.
func (p *Pipeline) SnapshotTaken() {
	for _, s := range p.steps {
		if s.state != nil {
			s.state.SnapshotTaken()
		}
	}
}

// Empty: no step holds anything.
func (p *Pipeline) Empty() bool {
	for _, s := range p.steps {
		if s.state != nil && s.state.Size() > 0 {
			return false
		}
	}
	return true
}

// StepDump is one step of a dump.
type StepDump struct {
	Name    string
	Kind    string
	Entries []KV
}

// Dump is every step's state, for a snapshot.
func (p *Pipeline) Dump() []StepDump {
	var out []StepDump
	for _, s := range p.stateful() {
		out = append(out, StepDump{Name: s.name, Kind: s.kind, Entries: s.state.Dump()})
	}
	return out
}

// Metas is the clocks' own values (event-time watermarks and their anchors), carried in the snapshot pointer.
func (p *Pipeline) Metas() []interface{} {
	out := []interface{}{}
	for _, s := range p.stateful() {
		if s.clock != nil {
			out = append(out, s.clock.meta())
		} else {
			out = append(out, map[string]interface{}{})
		}
	}
	return out
}

func metaAt(metas interface{}, i int) interface{} {
	if l, ok := metas.([]interface{}); ok && i < len(l) {
		return l[i]
	}
	return nil
}

// Load replaces every step from a dump (nil: empty) and the clocks' values.
func (p *Pipeline) Load(dump []StepDump, metas interface{}) {
	for i, s := range p.stateful() {
		var entries []KV
		if i < len(dump) && dump[i].Kind == s.kind {
			entries = dump[i].Entries
		}
		s.state.Load(entries)
		if s.engine != nil {
			s.clock.loadMeta(metaAt(metas, i))
			s.engine.rebuild()
			s.clock.rebuild()
		}
	}
	p.CommitPoint()
}

// LoadBegin starts a load in pieces (snapshot format 2).
func (p *Pipeline) LoadBegin() {
	for _, s := range p.stateful() {
		s.state.LoadBegin()
	}
}

// LoadPiece applies one piece. A piece of another kind of step than the one
// at that place in the chain is left out.
func (p *Pipeline) LoadPiece(step int, kind string, set []interface{}, del []interface{}) {
	st := p.stateful()
	if step < 0 || step >= len(st) || st[step].kind != kind {
		return
	}
	s := st[step]
	for _, kv := range set {
		pair, ok := kv.([]interface{})
		if !ok || len(pair) == 0 {
			continue
		}
		k := jsv.String(pair[0])
		var v interface{} = undefinedValue
		if len(pair) > 1 {
			v = pair[1]
		}
		s.state.LoadSet(k, v)
	}
	for _, k := range del {
		s.state.LoadDelete(jsv.String(k))
	}
}

// LoadEnd finishes a load in pieces.
func (p *Pipeline) LoadEnd(metas interface{}) {
	for i, s := range p.stateful() {
		s.state.Loaded()
		if s.engine != nil {
			s.clock.loadMeta(metaAt(metas, i))
			s.engine.rebuild()
			s.clock.rebuild()
		}
	}
	p.CommitPoint()
}

// TouchedSinceSnapshot is how many keys changed since the last snapshot, over every step.
func (p *Pipeline) TouchedSinceSnapshot() int {
	n := 0
	for _, s := range p.stateful() {
		n += s.state.TouchedSinceSnapshot()
	}
	return n
}

// Wipe empties every step (a reset): the next snapshot writes it.
func (p *Pipeline) Wipe() {
	for _, s := range p.stateful() {
		s.state.Wipe()
		if s.engine != nil {
			s.clock.loadMeta(nil)
			s.engine.rebuild()
			s.clock.rebuild()
		}
	}
	p.CommitPoint()
}

// Open is the open windows, waits, runs and rows, over every step.
func (p *Pipeline) Open() int {
	n := 0
	for _, s := range p.stateful() {
		if s.engine != nil {
			n += s.engine.open()
		} else {
			n += s.table.open()
		}
	}
	return n
}

// StateKeys is the state's keys, over every step.
func (p *Pipeline) StateKeys() int {
	n := 0
	for _, s := range p.stateful() {
		n += s.state.Size()
	}
	return n
}

// StepMetrics is one step's metrics.
type StepMetrics struct {
	Name      string  `json:"name"`
	Kind      string  `json:"kind"`
	Open      int     `json:"open"`
	Keys      int     `json:"keys"`
	Clock     string  `json:"clock,omitempty"`
	Watermark *string `json:"watermark,omitempty"`
	// HasWatermark: the step is on event time (watermark is then reported, null when none).
	HasWatermark bool `json:"-"`
	Held         *int `json:"held,omitempty"`
}

// Map is the JS shape.
func (m StepMetrics) Map() map[string]interface{} {
	out := map[string]interface{}{"name": m.Name, "kind": m.Kind, "open": float64(m.Open), "keys": float64(m.Keys)}
	if m.Clock != "" {
		out["clock"] = m.Clock
		if m.Clock == "event" {
			if m.Watermark != nil {
				out["watermark"] = *m.Watermark
			} else {
				out["watermark"] = nil
			}
			if m.Held != nil {
				out["held"] = float64(*m.Held)
			}
		}
	}
	return out
}

// StepMetrics is per step: what is open, the state's keys, and on event time the watermark and the messages held.
func (p *Pipeline) StepMetrics(t float64) []StepMetrics {
	var out []StepMetrics
	for _, s := range p.stateful() {
		m := StepMetrics{Name: s.name, Kind: s.kind, Keys: s.state.Size()}
		if s.engine != nil {
			m.Open = s.engine.open()
		} else {
			m.Open = s.table.open()
		}
		if s.clock != nil {
			m.Clock = s.clock.kind()
			if m.Clock == "event" {
				m.HasWatermark = true
				if w := s.clock.watermarkAt(t); finite(w) {
					iso := jsv.MustISO(w)
					m.Watermark = &iso
				}
				h := s.clock.held()
				m.Held = &h
			}
		}
		out = append(out, m)
	}
	return out
}

func (p *Pipeline) names() string {
	var n []string
	for _, s := range p.stateful() {
		n = append(n, s.name)
	}
	return strings.Join(n, ", ")
}

// Inspect is a live read of one key of one step: {found, value} or {found: false, error}.
func (p *Pipeline) Inspect(name, key string) map[string]interface{} {
	s := p.step(name)
	if s == nil {
		return map[string]interface{}{"found": false, "error": fmt.Sprintf("no step named %s (%s)", jsv.MustMarshal(name), p.names())}
	}
	var v interface{}
	if s.engine != nil {
		v = s.engine.inspect(key)
	} else {
		v = s.table.inspect(key)
	}
	if v == nil {
		return map[string]interface{}{"found": false}
	}
	return map[string]interface{}{"found": true, "value": v}
}

// List is a live read of up to limit entries of one step: {items, truncated, total}.
func (p *Pipeline) List(name string, limit int) map[string]interface{} {
	s := p.step(name)
	if s == nil {
		return map[string]interface{}{"error": fmt.Sprintf("no step named %s (%s)", jsv.MustMarshal(name), p.names()), "items": []interface{}{}}
	}
	items := []interface{}{}
	truncated := false
	each := func(v interface{}) bool {
		if len(items) >= limit {
			truncated = true
			return false
		}
		items = append(items, v)
		return true
	}
	if s.engine != nil {
		s.engine.list(each)
	} else {
		s.table.list(each)
	}
	held := 0
	if s.clock != nil {
		held = s.clock.held()
	}
	// The messages an event-time step holds in order are not the step's to list.
	return map[string]interface{}{"items": items, "truncated": truncated, "total": float64(s.state.Size() - held)}
}
