package operators

// The operators that run on a feed source (streams.FromFeed): pattern, join,
// gather, joinTable, name, and the feed's options of the window operators
// and of the sink. Mirror of the JS PatternOperator, CollectOperator,
// TableOperator, NameOperator, SinkOperator (routes) and _windowCommon.js
// (feedTimeOptions, stepTimeOptions).

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// Undefined is JavaScript's undefined (a field that is not there). A function
// that answers it answers "nothing", as a JS function answering undefined.
var Undefined = jsv.Undefined

// NullInitial is reduce's initial value null (JS .reduce(fn, null)); a nil
// initial is no initial at all (JS .reduce(fn)).
var NullInitial interface{} = nullInitial{}

type nullInitial struct{}

func (nullInitial) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

// EventTimeValueFn is a feed step's eventTime: the message's own time, as a
// time.Time, epoch milliseconds, or an ISO string (nil or Undefined: none).
type EventTimeValueFn func(msg interface{}) (interface{}, error)

// ---------------------------------------------------------------------------
// Durations as JS takes them
// ---------------------------------------------------------------------------

// durationMs is _windowCommon.js durationMs: a number of seconds (floor to
// ms), or a duration string ('30s', '5m', '1h30m'). ok false: not given.
func durationMs(value interface{}, name, what string) (float64, bool, error) {
	if jsv.IsNullish(value) {
		return 0, false, nil
	}
	if f, ok := jsv.Num(value); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) || f < 0 {
			return 0, false, fmt.Errorf("%s: %s is a number of seconds from 0, or a duration string such as '30s'", what, name)
		}
		return math.Floor(f * 1000), true, nil
	}
	if s, ok := value.(string); ok {
		ms, err := jsv.ParseDurationMs(s)
		if err != nil {
			return 0, false, err
		}
		return ms, true, nil
	}
	return 0, false, fmt.Errorf("%s: %s is a number of seconds, or a duration string such as '30s'", what, name)
}

// windowLatenessMs is a window's allowedLateness: seconds, or a duration string.
func windowLatenessMs(value interface{}, what string) (float64, error) {
	if jsv.IsNullish(value) {
		return 0, nil
	}
	if s, ok := value.(string); ok {
		return jsv.ParseDurationMs(s)
	}
	f, ok := jsv.Num(value)
	if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("%s: allowedLateness is a number of seconds, or a duration string such as '5m'", what)
	}
	return math.Max(0, math.Floor(f*1000)), nil
}

// ---------------------------------------------------------------------------
// Window options for a feed (feedTimeOptions)
// ---------------------------------------------------------------------------

// feedWindow is what a window keeps for a feed, beside its queue options:
// the options as given (the chain hash writes them), idle, eventTime off.
type feedWindow struct {
	graceRaw     interface{}
	latenessRaw  interface{}
	latenessMs   float64
	idleRaw      interface{}
	idleMs       *float64
	eventTimeOff bool
	eventTimeOf  EventTimeValueFn
	idleFlushRaw *float64
	optErr       error
}

// WithIdle: on a feed, the first step on event time moves its watermark on
// with the feed's clock after this many seconds without a message (JS idle).
func WithIdle(seconds float64) WindowOption {
	return func(w *windowBaseConfig) { w.feed.idleRaw = seconds }
}

// WithIdleSpan is WithIdle with a duration string ('30s', '1m').
func WithIdleSpan(span string) WindowOption {
	return func(w *windowBaseConfig) { w.feed.idleRaw = span }
}

// WithAllowedLatenessSpan is allowedLateness as a duration string ('5m'):
// the chain hash keeps the string, as the JS SDK does.
func WithAllowedLatenessSpan(span string) WindowOption {
	return func(w *windowBaseConfig) { w.feed.latenessRaw = span }
}

// WithEventTimeOf is a feed window's eventTime: the message's time as a
// time.Time, epoch milliseconds or an ISO string.
func WithEventTimeOf(fn EventTimeValueFn) WindowOption {
	return func(w *windowBaseConfig) { w.feed.eventTimeOf = fn }
}

// WithEventTimeOff keeps a feed window on the feed's clock, even after a
// step on event time (JS eventTime: false).
func WithEventTimeOff() WindowOption {
	return func(w *windowBaseConfig) { w.feed.eventTimeOff = true }
}

func (w *windowBaseConfig) finishFeed(what string) {
	f := &w.feed
	ms, err := windowLatenessMs(f.latenessRaw, what)
	if err != nil && f.optErr == nil {
		f.optErr = err
	}
	f.latenessMs = ms
	idle, given, err := durationMs(f.idleRaw, "idle", what)
	if err != nil && f.optErr == nil {
		f.optErr = err
	}
	if given {
		if idle <= 0 && f.optErr == nil {
			f.optErr = fmt.Errorf("%s: idle is a duration above zero", what)
		}
		f.idleMs = &idle
	}
	if w.EventTimeFn != nil && f.eventTimeOf == nil {
		fn := w.EventTimeFn
		f.eventTimeOf = func(msg interface{}) (interface{}, error) {
			ts, err := fn(msg)
			if err != nil {
				return nil, err
			}
			return float64(ts), nil
		}
	}
}

// FeedWindow is a window operator as a feed step sees it.
type FeedWindow struct {
	Kind            string // tumbling | sliding | cron | session
	WindowMs        float64
	SlideMs         float64
	SizeMs          float64
	WindowsPerEvent float64
	GapMs           float64
	Every           string
	GraceMs         float64
	EventTime       EventTimeValueFn
	EventTimeOff    bool
	LatenessMs      float64
	OnLate          string
	IdleMs          *float64
	// Config is the window's config as the chain hash writes it (JS insertion order).
	Config jsv.Ordered
	Err    error
}

// FeedWindowOp is implemented by the four window operators.
type FeedWindowOp interface {
	FeedWindow() FeedWindow
}

func (w *windowBaseConfig) graceJSON() interface{} {
	if w.feed.graceRaw != nil {
		return w.feed.graceRaw
	}
	return 0.0
}

func (w *windowBaseConfig) latenessJSON() interface{} {
	if w.feed.latenessRaw != nil {
		return w.feed.latenessRaw
	}
	return 0.0
}

func (w *windowBaseConfig) idleFlushJSON(dflt float64) float64 {
	if w.feed.idleFlushRaw != nil {
		return math.Max(0, math.Floor(*w.feed.idleFlushRaw))
	}
	if w.IdleFlushMs != 0 {
		return float64(w.IdleFlushMs)
	}
	return dflt
}

func (w *windowBaseConfig) feedCommon(f *FeedWindow, cfg jsv.Ordered) {
	f.GraceMs = math.Max(0, math.Floor(numOf(w.graceJSON())*1000))
	f.EventTime = w.feed.eventTimeOf
	f.EventTimeOff = w.feed.eventTimeOff
	f.LatenessMs = w.feed.latenessMs
	f.OnLate = w.OnLatePolicy
	if f.OnLate == "" {
		f.OnLate = "drop"
	}
	f.IdleMs = w.feed.idleMs
	cfg = append(cfg,
		jsv.Pair{K: "eventTime", V: f.EventTime != nil},
		jsv.Pair{K: "allowedLateness", V: w.latenessJSON()},
		jsv.Pair{K: "onLate", V: f.OnLate})
	if f.IdleMs != nil {
		cfg = append(cfg, jsv.Pair{K: "idle", V: *f.IdleMs})
	}
	f.Config = cfg
	f.Err = w.feed.optErr
}

func numOf(v interface{}) float64 {
	f, _ := jsv.Num(v)
	return f
}

// FeedWindow is the tumbling window as a feed step.
func (w *WindowTumblingOperator) FeedWindow() FeedWindow {
	f := FeedWindow{Kind: "tumbling", WindowMs: w.Seconds * msPerSec}
	w.feedCommon(&f, jsv.Ordered{{K: "kind", V: "window-tumbling"}, {K: "seconds", V: w.Seconds},
		{K: "gracePeriod", V: w.graceJSON()}, {K: "idleFlushMs", V: w.idleFlushJSON(5000)}})
	return f
}

// FeedWindow is the sliding window as a feed step.
func (w *WindowSlidingOperator) FeedWindow() FeedWindow {
	f := FeedWindow{Kind: "sliding", SizeMs: w.Size * msPerSec, SlideMs: w.Slide * msPerSec, WindowsPerEvent: w.Size / w.Slide}
	w.feedCommon(&f, jsv.Ordered{{K: "kind", V: "window-sliding"}, {K: "size", V: w.Size}, {K: "slide", V: w.Slide},
		{K: "gracePeriod", V: w.graceJSON()}, {K: "idleFlushMs", V: w.idleFlushJSON(5000)}})
	return f
}

// FeedWindow is the session window as a feed step.
func (w *WindowSessionOperator) FeedWindow() FeedWindow {
	f := FeedWindow{Kind: "session", GapMs: w.Gap * msPerSec}
	w.feedCommon(&f, jsv.Ordered{{K: "kind", V: "window-session"}, {K: "gap", V: w.Gap},
		{K: "gracePeriod", V: w.graceJSON()}, {K: "idleFlushMs", V: w.idleFlushJSON(1000)}})
	return f
}

// FeedWindow is the cron window as a feed step.
func (w *WindowCronOperator) FeedWindow() FeedWindow {
	f := FeedWindow{Kind: "cron", WindowMs: float64(everyToMs(w.Every)), Every: w.Every}
	w.feedCommon(&f, jsv.Ordered{{K: "kind", V: "window-cron"}, {K: "every", V: w.Every},
		{K: "gracePeriod", V: w.graceJSON()}, {K: "idleFlushMs", V: w.idleFlushJSON(30000)}})
	return f
}

// HasIdle: the window has a feed idle (refused on a queue source).
func (w *windowBaseConfig) HasIdle() bool { return w.feed.idleRaw != nil }

// FeedErr is an option error of the window (a bad idle or allowedLateness).
func (w *windowBaseConfig) FeedErr() error { return w.feed.optErr }

// ---------------------------------------------------------------------------
// Step time options (stepTimeOptions): pattern, join, gather
// ---------------------------------------------------------------------------

// StepTime is the event-time options of a pattern, a join or a gather.
type StepTime struct {
	// EventTime is the message's own time (a time.Time, epoch ms or an ISO
	// string); the step then runs on event time.
	EventTime EventTimeValueFn
	// EventTimeOff keeps the step on the feed's clock, even after a step on
	// event time (JS eventTime: false).
	EventTimeOff bool
	// AllowedLateness is how far out of order messages may come: nil, a
	// number of seconds, or a duration string ('5m').
	AllowedLateness interface{}
	// OnLate is "drop" (default) or "include".
	OnLate string
	// Idle (the first step on event time): after this long without a message
	// the watermark moves on with the feed's clock. nil, seconds, or a duration string.
	Idle interface{}
}

// FeedTime is a step's clock options, resolved.
type FeedTime struct {
	EventTime     EventTimeValueFn
	EventTimeOff  bool
	LatenessMs    float64
	LatenessGiven bool
	OnLate        string
	OnLateGiven   bool
	IdleMs        *float64
}

func stepTimeOptions(o StepTime, what string, cfg *jsv.Ordered) (FeedTime, error) {
	t := FeedTime{EventTime: o.EventTime, EventTimeOff: o.EventTimeOff}
	ms, given, err := durationMs(o.AllowedLateness, "allowedLateness", what)
	if err != nil {
		return t, err
	}
	t.LatenessMs, t.LatenessGiven = ms, given
	switch o.OnLate {
	case "", "drop":
		t.OnLate = "drop"
	case "include":
		t.OnLate = "include"
	default:
		return t, fmt.Errorf("onLate must be 'drop' or 'include' (got %s)", jsv.MustMarshal(o.OnLate))
	}
	t.OnLateGiven = o.OnLate != ""
	idle, idleGiven, err := durationMs(o.Idle, "idle", what)
	if err != nil {
		return t, err
	}
	if idleGiven {
		if idle <= 0 {
			return t, fmt.Errorf("%s: idle is a duration above zero", what)
		}
		t.IdleMs = &idle
	}
	if t.EventTime != nil {
		*cfg = append(*cfg, jsv.Pair{K: "eventTime", V: true})
	}
	if t.LatenessGiven {
		*cfg = append(*cfg, jsv.Pair{K: "allowedLateness", V: t.LatenessMs})
	}
	if t.OnLateGiven {
		*cfg = append(*cfg, jsv.Pair{K: "onLate", V: t.OnLate})
	}
	if t.IdleMs != nil {
		*cfg = append(*cfg, jsv.Pair{K: "idle", V: *t.IdleMs})
	}
	return t, nil
}

func parseWithin(within, what string) (float64, error) {
	if within == "" {
		return 0, fmt.Errorf("%s: within is a duration string, e.g. within: '15m'", what)
	}
	return jsv.ParseDurationMs(within)
}

// ---------------------------------------------------------------------------
// Pattern and join (two sides)
// ---------------------------------------------------------------------------

// PatternOptions are .Pattern()'s: a Start message, then a Then message for
// the same entity within Within (see the JS Stream.pattern).
type PatternOptions struct {
	Start  func(msg interface{}) (bool, error)
	Then   func(msg interface{}) (bool, error)
	Within string
	// Key is the entity: key(msg, value) (default: .KeyBy(), else the partition).
	Key func(msg, value interface{}) (interface{}, error)
	// OnMatch is what a match sends (nil/Undefined: nothing; no OnMatch: {key, start, then}).
	OnMatch func(start, then interface{}) (interface{}, error)
	// OnTimeout is what a deadline passed sends (no OnTimeout: nothing).
	OnTimeout func(start interface{}) (interface{}, error)
	// OnRepeat: "keep" (default) or "restart".
	OnRepeat string
	// Keep is what a waiting message keeps of itself, as its data.
	Keep func(msg, value interface{}) (interface{}, error)
	// Name names the step (else step1, step2, ...).
	Name string
	StepTime
}

// JoinOptions are .Join()'s of two sides: a Left and a Right message for the
// same entity, in either order, within Within.
type JoinOptions struct {
	Left   func(msg interface{}) (bool, error)
	Right  func(msg interface{}) (bool, error)
	Within string
	Key    func(msg, value interface{}) (interface{}, error)
	// Select is what a pair sends (no Select: {key, left, right}).
	Select func(left, right interface{}) (interface{}, error)
	// OnTimeout is what one left alone past its deadline sends: (msg, "left"|"right").
	OnTimeout func(msg interface{}, side string) (interface{}, error)
	OnRepeat  string
	Keep      func(msg, value interface{}) (interface{}, error)
	Name      string
	StepTime
}

// PatternOperator is .Pattern() and .Join() of two sides.
type PatternOperator struct {
	PatternKind string // pattern | join
	WithinMs    float64
	Key         func(msg, value interface{}) (interface{}, error)
	Keep        func(msg, value interface{}) (interface{}, error)
	OnTimeout   func(msg interface{}, side string) (interface{}, error)
	OnRepeat    string
	Start       func(msg interface{}) (bool, error)
	Then        func(msg interface{}) (bool, error)
	OnMatch     func(start, then interface{}) (interface{}, error)
	Left        func(msg interface{}) (bool, error)
	Right       func(msg interface{}) (bool, error)
	Select      func(left, right interface{}) (interface{}, error)
	StepName    string
	Time        FeedTime
	FeedConfig  jsv.Ordered
}

func (*PatternOperator) Kind() string { return "pattern" }

// Config is the config as JS has it (a map: for the queue chain's hash only).
func (p *PatternOperator) Config() map[string]interface{} { return orderedMap(p.FeedConfig) }

func orderedMap(o jsv.Ordered) map[string]interface{} {
	m := map[string]interface{}{}
	for _, kv := range o {
		m[kv.K] = kv.V
	}
	return m
}

func repeatOf(v, what string) (string, error) {
	switch v {
	case "", "keep":
		return "keep", nil
	case "restart":
		return "restart", nil
	}
	return "", fmt.Errorf("%s: onRepeat is 'keep' or 'restart', got %s", what, jsv.MustMarshal(v))
}

func nameOf(n string) string { return n }

// NewPatternOperator checks a pattern's options.
func NewPatternOperator(o PatternOptions) (*PatternOperator, error) {
	const what = "pattern"
	ms, err := parseWithin(o.Within, what)
	if err != nil {
		return nil, err
	}
	rep, err := repeatOf(o.OnRepeat, what)
	if err != nil {
		return nil, err
	}
	if o.Start == nil {
		return nil, errors.New("pattern: start must be a function")
	}
	if o.Then == nil {
		return nil, errors.New("pattern: then must be a function")
	}
	p := &PatternOperator{PatternKind: what, WithinMs: ms, Key: o.Key, Keep: o.Keep, OnRepeat: rep, Start: o.Start, Then: o.Then,
		OnMatch: o.OnMatch, StepName: nameOf(o.Name)}
	if o.OnTimeout != nil {
		fn := o.OnTimeout
		p.OnTimeout = func(msg interface{}, _ string) (interface{}, error) { return fn(msg) }
	}
	p.FeedConfig = jsv.Ordered{{K: "kind", V: what}, {K: "within", V: ms}, {K: "onRepeat", V: rep}, {K: "timeout", V: o.OnTimeout != nil}}
	if p.Time, err = stepTimeOptions(o.StepTime, what, &p.FeedConfig); err != nil {
		return nil, err
	}
	return p, nil
}

// NewJoinOperator checks a join of two sides.
func NewJoinOperator(o JoinOptions) (*PatternOperator, error) {
	const what = "join"
	ms, err := parseWithin(o.Within, what)
	if err != nil {
		return nil, err
	}
	rep, err := repeatOf(o.OnRepeat, what)
	if err != nil {
		return nil, err
	}
	if o.Left == nil {
		return nil, errors.New("join: left must be a function")
	}
	if o.Right == nil {
		return nil, errors.New("join: right must be a function")
	}
	p := &PatternOperator{PatternKind: what, WithinMs: ms, Key: o.Key, Keep: o.Keep, OnRepeat: rep, Left: o.Left, Right: o.Right,
		Select: o.Select, OnTimeout: o.OnTimeout, StepName: nameOf(o.Name)}
	p.FeedConfig = jsv.Ordered{{K: "kind", V: what}, {K: "within", V: ms}, {K: "onRepeat", V: rep}, {K: "timeout", V: o.OnTimeout != nil}}
	if p.Time, err = stepTimeOptions(o.StepTime, what, &p.FeedConfig); err != nil {
		return nil, err
	}
	return p, nil
}

// ---------------------------------------------------------------------------
// Join of many sides, and gather
// ---------------------------------------------------------------------------

// Part is one named part of a join of many sides or of a gather: a queue (a
// message of that queue is the part) or a test of its own.
type Part struct {
	Name  string
	Queue string
	Test  func(msg, value interface{}) (bool, error)
}

// Queues is parts named by queue: each queue's messages are the part of its name.
func Queues(names ...string) []Part {
	out := make([]Part, len(names))
	for i, n := range names {
		out[i] = Part{Name: n, Queue: n}
	}
	return out
}

// JoinSidesOptions are .JoinSides()'s: one message per side, any number of
// sides, in any order, within Within of the first (JS join({ sides })).
type JoinSidesOptions struct {
	Sides  []Part
	Within string
	Key    func(msg, value interface{}) (interface{}, error)
	// Select is what a complete set sends: (got by side name, key); none: {key, parts}.
	Select func(got interface{}, key string) (interface{}, error)
	// OnTimeout: (got, missing side names, key).
	OnTimeout func(got, missing interface{}, key string) (interface{}, error)
	// OnRepeat: "keep" (default: the first message of a side stays) or "latest".
	OnRepeat string
	Keep     func(msg, value interface{}) (interface{}, error)
	Name     string
	StepTime
}

// GatherOptions are .Gather()'s: the parts of one run, until all are in or
// the deadline passes. Steps names the parts, or Count says how many a run has.
type GatherOptions struct {
	Steps      []Part
	Count      func(msg, value interface{}) (interface{}, error)
	Key        func(msg, value interface{}) (interface{}, error)
	Within     string
	OnComplete func(got interface{}, key string) (interface{}, error)
	OnTimeout  func(got, missing interface{}, key string) (interface{}, error)
	OnRepeat   string
	Keep       func(msg, value interface{}) (interface{}, error)
	Name       string
	StepTime
}

// CollectPart is a part as the engine tests it.
type CollectPart struct {
	Name string
	Pred func(msg, value interface{}) (bool, error)
}

// CollectOperator is .JoinSides() and .Gather().
type CollectOperator struct {
	CollectKind string // join | gather
	WithinMs    float64
	Key         func(msg, value interface{}) (interface{}, error)
	Keep        func(msg, value interface{}) (interface{}, error)
	OnTimeout   func(got, missing interface{}, key string) (interface{}, error)
	OnRepeat    string
	Parts       []CollectPart
	Count       func(msg, value interface{}) (interface{}, error)
	OnComplete  func(got interface{}, key string) (interface{}, error)
	Remember    bool
	StepName    string
	Time        FeedTime
	FeedConfig  jsv.Ordered
}

func (*CollectOperator) Kind() string                     { return "collect" }
func (c *CollectOperator) Config() map[string]interface{} { return orderedMap(c.FeedConfig) }

func partsOf(spec []Part, name, what string) ([]CollectPart, error) {
	var list []CollectPart
	seen := map[string]bool{}
	for _, p := range spec {
		if p.Name == "" {
			return nil, fmt.Errorf("%s: every %s has a name", what, name)
		}
		if seen[p.Name] {
			return nil, fmt.Errorf("%s: %s %s is named twice", what, name, jsv.MustMarshal(p.Name))
		}
		seen[p.Name] = true
		if p.Test != nil {
			list = append(list, CollectPart{Name: p.Name, Pred: p.Test})
			continue
		}
		q := p.Queue
		if q == "" {
			return nil, fmt.Errorf("%s: %s %s must be a function", what, name, p.Name)
		}
		list = append(list, CollectPart{Name: p.Name, Pred: func(msg, _ interface{}) (bool, error) {
			s, ok := jsv.Get(msg, "queue").(string)
			return ok && s == q, nil
		}})
	}
	return list, nil
}

func collectRepeat(v, what string) (string, error) {
	switch v {
	case "", "keep":
		return "keep", nil
	case "latest":
		return "latest", nil
	}
	return "", fmt.Errorf("%s: onRepeat is 'keep' (the first message of a part stays) or 'latest', got %s", what, jsv.MustMarshal(v))
}

func collectConfig(c *CollectOperator, what string, names []string) {
	var parts interface{}
	if names != nil {
		l := make([]interface{}, len(names))
		for i, n := range names {
			l[i] = n
		}
		parts = l
	}
	c.FeedConfig = jsv.Ordered{{K: "kind", V: what}, {K: "within", V: c.WithinMs}, {K: "parts", V: parts},
		{K: "count", V: c.Count != nil}, {K: "onRepeat", V: c.OnRepeat}, {K: "timeout", V: c.OnTimeout != nil}}
}

// NewJoinSidesOperator checks a join of many sides.
func NewJoinSidesOperator(o JoinSidesOptions) (*CollectOperator, error) {
	const what = "join"
	ms, err := parseWithin(o.Within, what)
	if err != nil {
		return nil, err
	}
	rep, err := collectRepeat(o.OnRepeat, what)
	if err != nil {
		return nil, err
	}
	if o.Sides == nil {
		return nil, errors.New("join: side is a list of queue names or { name: test }")
	}
	parts, err := partsOf(o.Sides, "side", what)
	if err != nil {
		return nil, err
	}
	if len(parts) < 2 {
		return nil, errors.New("join: sides names at least two")
	}
	c := &CollectOperator{CollectKind: what, WithinMs: ms, Key: o.Key, Keep: o.Keep, OnTimeout: o.OnTimeout, OnRepeat: rep,
		Parts: parts, OnComplete: o.Select, StepName: o.Name}
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.Name
	}
	collectConfig(c, what, names)
	if c.Time, err = stepTimeOptions(o.StepTime, what, &c.FeedConfig); err != nil {
		return nil, err
	}
	return c, nil
}

// NewGatherOperator checks a gather.
func NewGatherOperator(o GatherOptions) (*CollectOperator, error) {
	const what = "gather"
	ms, err := parseWithin(o.Within, what)
	if err != nil {
		return nil, err
	}
	rep, err := collectRepeat(o.OnRepeat, what)
	if err != nil {
		return nil, err
	}
	hasSteps, hasCount := o.Steps != nil, o.Count != nil
	if hasSteps == hasCount {
		return nil, errors.New("gather: give steps (the parts by name) or count (how many parts a run has), one of the two")
	}
	c := &CollectOperator{CollectKind: what, WithinMs: ms, Key: o.Key, Keep: o.Keep, OnTimeout: o.OnTimeout, OnRepeat: rep,
		Count: o.Count, OnComplete: o.OnComplete, Remember: true, StepName: o.Name}
	var names []string
	if hasSteps {
		parts, err := partsOf(o.Steps, "step", what)
		if err != nil {
			return nil, err
		}
		if len(parts) == 0 {
			return nil, errors.New("gather: steps names at least one")
		}
		c.Parts = parts
		for _, p := range parts {
			names = append(names, p.Name)
		}
	}
	collectConfig(c, what, names)
	if c.Time, err = stepTimeOptions(o.StepTime, what, &c.FeedConfig); err != nil {
		return nil, err
	}
	return c, nil
}

// ---------------------------------------------------------------------------
// Table join
// ---------------------------------------------------------------------------

// TableOptions are .JoinTable()'s: every message joined with the latest row
// of a table queue (one message per change; null deletes).
type TableOptions struct {
	// Queue is the table queue (or Queues, several; no patterns).
	Queue  string
	Queues []string
	// On is the row a message looks up: on(msg, value).
	On func(msg, value interface{}) (interface{}, error)
	// Key is a row's key (default: its partition); Value what the table keeps (default: its payload).
	Key   func(row interface{}) (interface{}, error)
	Value func(row interface{}) (interface{}, error)
	// Select is what goes on: select(value, row, msg); row is nil when there
	// is none. No Select: {value, row}.
	Select func(value, row, msg interface{}) (interface{}, error)
	// Missing: "keep" (default) or "drop". Initial: "all" (default) or "now".
	Missing string
	Initial string
	Name    string
}

// TableOperator is .JoinTable().
type TableOperator struct {
	Queues     []string
	On         func(msg, value interface{}) (interface{}, error)
	KeyFn      func(row interface{}) (interface{}, error)
	ValueFn    func(row interface{}) (interface{}, error)
	SelectFn   func(value, row, msg interface{}) (interface{}, error)
	Missing    string
	Initial    string
	StepName   string
	FeedConfig jsv.Ordered
}

func (*TableOperator) Kind() string                     { return "table" }
func (t *TableOperator) Config() map[string]interface{} { return orderedMap(t.FeedConfig) }

// NewTableOperator checks a table join.
func NewTableOperator(o TableOptions) (*TableOperator, error) {
	queues := o.Queues
	if o.Queue != "" {
		queues = append([]string{o.Queue}, queues...)
	}
	bad := len(queues) == 0
	for _, q := range queues {
		if q == "" || strings.ContainsAny(q, "*?") {
			bad = true
		}
	}
	if bad {
		return nil, errors.New("joinTable: queue is the table queue's name (or a list of names, no patterns)")
	}
	var uniq []string
	seen := map[string]bool{}
	for _, q := range queues {
		if !seen[q] {
			seen[q] = true
			uniq = append(uniq, q)
		}
	}
	if o.On == nil {
		return nil, errors.New("joinTable: on must be a function")
	}
	missing := o.Missing
	if missing == "" {
		missing = "keep"
	}
	if missing != "keep" && missing != "drop" {
		return nil, fmt.Errorf("joinTable: missing is 'keep' or 'drop', got %s", jsv.MustMarshal(o.Missing))
	}
	initial := o.Initial
	if initial == "" {
		initial = "all"
	}
	if initial != "all" && initial != "now" {
		return nil, fmt.Errorf("joinTable: initial is 'all' (read the table queue from its beginning) or 'now', got %s", jsv.MustMarshal(o.Initial))
	}
	t := &TableOperator{Queues: uniq, On: o.On, KeyFn: o.Key, ValueFn: o.Value, SelectFn: o.Select, Missing: missing, Initial: initial, StepName: o.Name}
	ql := make([]interface{}, len(uniq))
	for i, q := range uniq {
		ql[i] = q
	}
	t.FeedConfig = jsv.Ordered{{K: "kind", V: "table"}, {K: "queues", V: ql}, {K: "key", V: o.Key != nil}, {K: "value", V: o.Value != nil}}
	return t, nil
}

// ---------------------------------------------------------------------------
// Name and where
// ---------------------------------------------------------------------------

var stepNameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// NameOperator is .Name(): the name of the stateful step before it.
type NameOperator struct{ StepName string }

func (*NameOperator) Kind() string { return "name" }
func (n *NameOperator) Config() map[string]interface{} {
	return map[string]interface{}{"kind": "name", "name": n.StepName}
}

// NewNameOperator checks a step name.
func NewNameOperator(name string) (*NameOperator, error) {
	if !stepNameRe.MatchString(name) {
		return nil, fmt.Errorf(".name(): a step name is 1 to 64 letters, digits, '_', '.' or '-', got %s", jsv.MustMarshal(name))
	}
	return &NameOperator{StepName: name}, nil
}

// WhereOperator is .Where(field, op, value): a filter on the source message
// (on a feed, sent to the broker when no stateful step comes before it).
type WhereOperator struct{ Cond feed.Condition }

func (WhereOperator) Kind() string { return "filter" }
func (w WhereOperator) Config() map[string]interface{} {
	return map[string]interface{}{"kind": "filter", "where": w.Cond.Map()}
}

// Apply evaluates it here (a queue source).
func (w WhereOperator) Apply(_ context.Context, env Envelope) ([]Envelope, error) {
	if env.Msg == nil || !feed.ConditionHolds(w.Cond, env.Msg) {
		return nil, nil
	}
	return []Envelope{env}, nil
}

// ---------------------------------------------------------------------------
// Sinks that route (feed sources)
// ---------------------------------------------------------------------------

// RouteFn picks where one record goes: a queue name, a Target, a QueueRef, a
// list of those, or nil/Undefined (nowhere).
type RouteFn func(value, msg interface{}) (interface{}, error)

// Routes reports whether records may go to more than one queue (a function or a list).
func (s SinkOperator) Routes() bool { return s.Route != nil || len(s.Queues) > 1 }

// ---------------------------------------------------------------------------
// ForeachBatch (feed sources)
// ---------------------------------------------------------------------------

// BatchItem is one result a .ForeachBatch() function gets: the value and the
// context .Foreach() would get, and an ID that is the same on every delivery
// of that result (a key for an upsert).
type BatchItem struct {
	Value interface{}
	Ctx   EmitCtx
	ID    string
}

// ForeachBatchFn gets up to MaxItems results at once. An error (or a panic)
// fails the call: it is tried again whole, then item by item.
type ForeachBatchFn func(items []BatchItem) error

// BatchOption is an option of .ForeachBatch().
type BatchOption func(*ForeachBatchOperator)

// DefaultBatchMaxItems is the results of one call when WithMaxItems is not given.
const DefaultBatchMaxItems = 1000

// WithMaxItems is the most results one call gets (a whole number from 1; 1000 by default).
func WithMaxItems(n int) BatchOption {
	return func(o *ForeachBatchOperator) { o.MaxItems = n }
}

// WithMaxWaitMillis keeps a batch across reads until it is full or its
// first item has waited this long (0, the default: a batch goes at the end
// of each read).
func WithMaxWaitMillis(ms int) BatchOption {
	return func(o *ForeachBatchOperator) { o.MaxWaitMillis = ms }
}

// ForeachBatchOperator is .ForeachBatch(fn, options): a terminal side effect
// in batches (JS ForeachBatchOperator). The position moves only in the
// commit right after fn returns.
type ForeachBatchOperator struct {
	Fn            ForeachBatchFn
	MaxItems      int
	MaxWaitMillis int
}

// NewForeachBatchOperator checks the function and the options.
func NewForeachBatchOperator(fn ForeachBatchFn, opts ...BatchOption) (ForeachBatchOperator, error) {
	o := ForeachBatchOperator{Fn: fn, MaxItems: DefaultBatchMaxItems}
	if fn == nil {
		return o, errors.New(".foreachBatch(fn, options): fn is a function (items) => ..., each item { value, ctx, id }")
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.MaxItems < 1 {
		return o, fmt.Errorf(".foreachBatch(fn, options): maxItems is a whole number from 1, got %d", o.MaxItems)
	}
	if o.MaxWaitMillis < 0 {
		return o, fmt.Errorf(".foreachBatch(fn, options): maxWaitMillis is a whole number from 0, got %d", o.MaxWaitMillis)
	}
	return o, nil
}

func (ForeachBatchOperator) Kind() string { return "foreachBatch" }
func (o ForeachBatchOperator) Config() map[string]interface{} {
	return map[string]interface{}{"kind": "foreachBatch", "maxItems": float64(o.MaxItems), "maxWaitMillis": float64(o.MaxWaitMillis)}
}
