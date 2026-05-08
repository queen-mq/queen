// Package operators contains the Operator implementations used by the
// fluent Stream builder. 1:1 port of the JS operators/* and Python
// queen/streams/operators/*.
//
// Each operator type implements the util.Operator interface (Kind/Config) so
// it participates in the chain's config_hash, plus its own apply-style
// method that the runtime calls to transform envelopes or compute reductions.
package operators

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Common types
// ---------------------------------------------------------------------------

// Envelope is the per-message bag of fields the runtime threads through
// operators. Mirrors the JS / Python envelope shape exactly.
type Envelope struct {
	Msg            map[string]interface{}
	Key            string
	Value          interface{}
	Ctx            map[string]interface{}
	EventTimeMs    int64
	WindowStart    int64
	WindowEnd      int64
	WindowKey      string
	OperatorTag    string
	GracePeriodMs  int64
}

// EmitCtx is the context passed to post-reducer .map(value, ctx) and
// .foreach(value, ctx) callbacks.
type EmitCtx map[string]interface{}

// MapFn is a (msg, ctx) -> value user callable. ctx may be nil for
// pre-reducer stages.
type MapFn func(msg interface{}, ctx EmitCtx) (interface{}, error)

// FilterFn returns true to keep the envelope.
type FilterFn func(msg interface{}, ctx EmitCtx) (bool, error)

// FlatMapFn returns zero or more values per input.
type FlatMapFn func(msg interface{}, ctx EmitCtx) ([]interface{}, error)

// KeyFn extracts a per-record key.
type KeyFn func(msg interface{}) (string, error)

// ReduceFn folds a value into the per-window accumulator.
type ReduceFn func(acc interface{}, value interface{}) (interface{}, error)

// ExtractorFn extracts a numeric value (count contribution, sum addend, etc.)
// from a message for AggregateOperator.
type ExtractorFn func(msg interface{}) (float64, error)

// EventTimeFn returns event-time epoch-ms for an event-time window.
type EventTimeFn func(msg interface{}) (int64, error)

// GateFn returns true to allow, false to deny.
type GateFn func(value interface{}, ctx GateContext) (bool, error)

// GateContext is the second arg passed to a gate callable. State is mutable
// and persisted on ALLOW only.
type GateContext struct {
	State        map[string]interface{}
	StreamTimeMs int64
	PartitionID  string
	Partition    string
	Key          string
}

// QueueRef is the minimal interface that .From / .To accept. Implementations
// must expose the queue name. The Queen Go client's QueueBuilder satisfies
// this via a Name() accessor.
type QueueRef interface {
	Name() string
}

// ---------------------------------------------------------------------------
// MapOperator
// ---------------------------------------------------------------------------

type MapOperator struct {
	Fn MapFn
}

func (MapOperator) Kind() string                   { return "map" }
func (MapOperator) Config() map[string]interface{} { return nil }
func (m MapOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	v, err := m.Fn(envelopeFnArg(env), env.Ctx)
	if err != nil {
		return nil, err
	}
	out := env
	out.Value = v
	return []Envelope{out}, nil
}

// envelopeFnArg returns the value passed as the first arg to user callbacks.
// Pre-stage: env.Msg is the Queen message map. Post-stage: env.Msg is nil
// and env.Value is the upstream emit (e.g. the aggregate accumulator).
func envelopeFnArg(env Envelope) interface{} {
	if env.Msg != nil {
		return env.Msg
	}
	return env.Value
}

// ---------------------------------------------------------------------------
// FilterOperator
// ---------------------------------------------------------------------------

type FilterOperator struct {
	Fn FilterFn
}

func (FilterOperator) Kind() string                   { return "filter" }
func (FilterOperator) Config() map[string]interface{} { return nil }
func (f FilterOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	keep, err := f.Fn(envelopeFnArg(env), env.Ctx)
	if err != nil {
		return nil, err
	}
	if !keep {
		return nil, nil
	}
	return []Envelope{env}, nil
}

// ---------------------------------------------------------------------------
// FlatMapOperator
// ---------------------------------------------------------------------------

type FlatMapOperator struct {
	Fn FlatMapFn
}

func (FlatMapOperator) Kind() string                   { return "flatMap" }
func (FlatMapOperator) Config() map[string]interface{} { return nil }
func (f FlatMapOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	out, err := f.Fn(envelopeFnArg(env), env.Ctx)
	if err != nil {
		return nil, err
	}
	res := make([]Envelope, len(out))
	for i, v := range out {
		e := env
		e.Value = v
		res[i] = e
	}
	return res, nil
}

// ---------------------------------------------------------------------------
// KeyByOperator
// ---------------------------------------------------------------------------

type KeyByOperator struct {
	Fn KeyFn
}

func (KeyByOperator) Kind() string                   { return "keyBy" }
func (KeyByOperator) Config() map[string]interface{} { return nil }
func (k KeyByOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	key, err := k.Fn(env.Msg)
	if err != nil {
		return nil, err
	}
	out := env
	out.Key = key
	return []Envelope{out}, nil
}

// ---------------------------------------------------------------------------
// SinkOperator (terminal .to())
// ---------------------------------------------------------------------------

type SinkOperator struct {
	QueueName         string
	PartitionResolver interface{} // string | func(value interface{}) string
}

func (SinkOperator) Kind() string { return "sink" }
func (s SinkOperator) Config() map[string]interface{} {
	return map[string]interface{}{"kind": "sink", "queue": s.QueueName}
}
func (SinkOperator) ConfigKeys() []string { return []string{"kind", "queue"} }

// PushItem is the shape of an entry in /streams/v1/cycle.push_items.
type PushItem struct {
	Queue     string                 `json:"queue"`
	Partition string                 `json:"partition,omitempty"`
	Payload   map[string]interface{} `json:"payload"`
}

// SinkEntry represents an emit handed to the sink.
type SinkEntry struct {
	Key       string
	Value     interface{}
	SourceMsg map[string]interface{}
}

// BuildPushItems shapes sink entries into push items.
func (s SinkOperator) BuildPushItems(entries []SinkEntry, sourcePartitionName string) []PushItem {
	out := make([]PushItem, 0, len(entries))
	for _, e := range entries {
		var payload map[string]interface{}
		switch v := e.Value.(type) {
		case map[string]interface{}:
			payload = v
		default:
			payload = map[string]interface{}{"value": v}
		}
		part := s.resolvePartition(e.Value, sourcePartitionName)
		out = append(out, PushItem{Queue: s.QueueName, Partition: part, Payload: payload})
	}
	return out
}

func (s SinkOperator) resolvePartition(value interface{}, sourcePartition string) string {
	switch r := s.PartitionResolver.(type) {
	case string:
		return r
	case func(interface{}) string:
		return r(value)
	}
	if sourcePartition != "" {
		return sourcePartition
	}
	return "Default"
}

// ---------------------------------------------------------------------------
// ForeachOperator (terminal .foreach())
// ---------------------------------------------------------------------------

type ForeachFn func(value interface{}, ctx EmitCtx) error

type ForeachOperator struct {
	Fn ForeachFn
}

func (ForeachOperator) Kind() string { return "foreach" }
func (ForeachOperator) Config() map[string]interface{} {
	return map[string]interface{}{"kind": "foreach"}
}

// RunEffects invokes the user fn per emit. Errors abort the cycle.
func (f ForeachOperator) RunEffects(entries []struct {
	Value interface{}
	Ctx   EmitCtx
}) error {
	for _, e := range entries {
		if err := f.Fn(e.Value, e.Ctx); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// GateOperator
// ---------------------------------------------------------------------------

type GateOperator struct {
	Fn GateFn
}

func (GateOperator) Kind() string                   { return "gate" }
func (GateOperator) Config() map[string]interface{} { return nil }
func (g GateOperator) Evaluate(env Envelope, ctx GateContext) (bool, error) {
	return g.Fn(env.Value, ctx)
}

// ---------------------------------------------------------------------------
// Window operators (tumbling, sliding, session, cron)
// ---------------------------------------------------------------------------

const msPerSec = 1000

type windowBaseConfig struct {
	GracePeriodMs    int64
	IdleFlushMs      int64
	EventTimeFn      EventTimeFn
	AllowedLatenessMs int64
	OnLatePolicy     string
}

func normaliseLate(s string) string {
	if s == "" || s == "drop" {
		return "drop"
	}
	if s == "include" {
		return "include"
	}
	panic("on_late must be 'drop' or 'include' (got " + s + ")")
}

// extractEventTime returns the event-time epoch-ms or -1 if it cannot be
// determined.
func extractEventTime(msg map[string]interface{}, fn EventTimeFn) int64 {
	if fn != nil {
		ts, err := fn(msg)
		if err != nil || ts < 0 {
			return -1
		}
		return ts
	}
	if msg == nil {
		return -1
	}
	if c, ok := msg["createdAt"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, c); err == nil {
			return t.UnixMilli()
		}
	}
	return -1
}

func toIso(ms int64) string {
	t := time.UnixMilli(ms).UTC()
	return t.Format("2006-01-02T15:04:05.000Z")
}

// WindowTumblingOperator buckets envelopes by floor(ts/N).
type WindowTumblingOperator struct {
	Seconds float64
	windowBaseConfig
}

func NewWindowTumblingOperator(seconds float64, opts ...WindowOption) *WindowTumblingOperator {
	if seconds <= 0 {
		panic("window_tumbling requires seconds > 0")
	}
	wo := applyWindowOptions(opts...)
	wo.IdleFlushMs = orDefault(wo.IdleFlushMs, 5000)
	return &WindowTumblingOperator{Seconds: seconds, windowBaseConfig: wo}
}

func (w *WindowTumblingOperator) Kind() string { return "window" }
func (w *WindowTumblingOperator) WindowKind() string { return "tumbling" }
func (w *WindowTumblingOperator) WindowMs() int64 { return int64(w.Seconds * msPerSec) }
func (w *WindowTumblingOperator) GraceMs() int64  { return w.GracePeriodMs }
func (w *WindowTumblingOperator) IdleMs() int64   { return w.IdleFlushMs }
func (w *WindowTumblingOperator) AllowedLateMs() int64 { return w.AllowedLatenessMs }
func (w *WindowTumblingOperator) OnLate() string  { return w.OnLatePolicy }
func (w *WindowTumblingOperator) EventTime() EventTimeFn { return w.EventTimeFn }
func (w *WindowTumblingOperator) OperatorTag() string {
	// Match the JS / Python format exactly: integer seconds render without
	// a trailing decimal so cross-language state keys stay identical.
	return "tumb:" + numToShort(w.Seconds)
}
func (w *WindowTumblingOperator) Config() map[string]interface{} {
	return map[string]interface{}{
		"kind":            "window-tumbling",
		"seconds":         w.Seconds,
		"gracePeriod":     float64(w.GracePeriodMs) / msPerSec,
		"idleFlushMs":     w.IdleFlushMs,
		"eventTime":       w.EventTimeFn != nil,
		"allowedLateness": float64(w.AllowedLatenessMs) / msPerSec,
		"onLate":          w.OnLatePolicy,
	}
}

// ConfigKeys returns config keys in JS insertion order so the cross-language
// config_hash matches exactly. (See util.ConfigHashOf.)
func (w *WindowTumblingOperator) ConfigKeys() []string {
	return []string{"kind", "seconds", "gracePeriod", "idleFlushMs", "eventTime", "allowedLateness", "onLate"}
}
func (w *WindowTumblingOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	ts := extractEventTime(env.Msg, w.EventTimeFn)
	if ts < 0 {
		return nil, fmt.Errorf("window_tumbling could not determine timestamp")
	}
	wm := w.WindowMs()
	ws := (ts / wm) * wm
	we := ws + wm
	out := env
	out.EventTimeMs = ts
	out.WindowStart = ws
	out.WindowEnd = we
	out.WindowKey = toIso(ws)
	out.OperatorTag = w.OperatorTag()
	out.GracePeriodMs = w.GracePeriodMs
	return []Envelope{out}, nil
}

// WindowSlidingOperator emits N=size/slide annotations per event.
type WindowSlidingOperator struct {
	Size, Slide float64
	windowBaseConfig
}

func NewWindowSlidingOperator(size, slide float64, opts ...WindowOption) *WindowSlidingOperator {
	if size <= 0 || slide <= 0 {
		panic("window_sliding requires size>0 and slide>0")
	}
	if int64(size*1000)%int64(slide*1000) != 0 {
		panic("window_sliding: size must be an integer multiple of slide")
	}
	wo := applyWindowOptions(opts...)
	wo.IdleFlushMs = orDefault(wo.IdleFlushMs, 5000)
	return &WindowSlidingOperator{Size: size, Slide: slide, windowBaseConfig: wo}
}

func (w *WindowSlidingOperator) Kind() string         { return "window" }
func (w *WindowSlidingOperator) WindowKind() string   { return "sliding" }
func (w *WindowSlidingOperator) SizeMs() int64        { return int64(w.Size * msPerSec) }
func (w *WindowSlidingOperator) SlideMs() int64       { return int64(w.Slide * msPerSec) }
func (w *WindowSlidingOperator) WindowsPerEvent() int { return int(w.Size / w.Slide) }
func (w *WindowSlidingOperator) GraceMs() int64       { return w.GracePeriodMs }
func (w *WindowSlidingOperator) IdleMs() int64        { return w.IdleFlushMs }
func (w *WindowSlidingOperator) AllowedLateMs() int64 { return w.AllowedLatenessMs }
func (w *WindowSlidingOperator) OnLate() string       { return w.OnLatePolicy }
func (w *WindowSlidingOperator) EventTime() EventTimeFn { return w.EventTimeFn }
func (w *WindowSlidingOperator) OperatorTag() string {
	return "slide:" + numToShort(w.Size) + ":" + numToShort(w.Slide)
}
func (w *WindowSlidingOperator) Config() map[string]interface{} {
	return map[string]interface{}{
		"kind":            "window-sliding",
		"size":            w.Size,
		"slide":           w.Slide,
		"gracePeriod":     float64(w.GracePeriodMs) / msPerSec,
		"idleFlushMs":     w.IdleFlushMs,
		"eventTime":       w.EventTimeFn != nil,
		"allowedLateness": float64(w.AllowedLatenessMs) / msPerSec,
		"onLate":          w.OnLatePolicy,
	}
}

func (w *WindowSlidingOperator) ConfigKeys() []string {
	return []string{"kind", "size", "slide", "gracePeriod", "idleFlushMs", "eventTime", "allowedLateness", "onLate"}
}
func (w *WindowSlidingOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	ts := extractEventTime(env.Msg, w.EventTimeFn)
	if ts < 0 {
		return nil, fmt.Errorf("window_sliding could not determine timestamp")
	}
	slideMs := w.SlideMs()
	sizeMs := w.SizeMs()
	latestStart := (ts / slideMs) * slideMs
	res := []Envelope{}
	for i := 0; i < w.WindowsPerEvent(); i++ {
		ws := latestStart - int64(i)*slideMs
		if ts < ws || ts >= ws+sizeMs {
			continue
		}
		out := env
		out.EventTimeMs = ts
		out.WindowStart = ws
		out.WindowEnd = ws + sizeMs
		out.WindowKey = toIso(ws)
		out.OperatorTag = w.OperatorTag()
		out.GracePeriodMs = w.GracePeriodMs
		res = append(res, out)
	}
	return res, nil
}

// WindowCronOperator (every: 'minute' | 'hour' | 'day' | 'week').
type WindowCronOperator struct {
	Every string
	windowBaseConfig
}

const weekEpochOffsetMs = int64(4 * 86_400 * 1000) // Jan 1 1970 UTC was Thursday

func everyToMs(every string) int64 {
	switch every {
	case "second":
		return 1000
	case "minute":
		return 60 * 1000
	case "hour":
		return 3600 * 1000
	case "day":
		return 86_400 * 1000
	case "week":
		return 7 * 86_400 * 1000
	default:
		panic("window_cron: every must be one of second|minute|hour|day|week (got " + every + ")")
	}
}

func NewWindowCronOperator(every string, opts ...WindowOption) *WindowCronOperator {
	wo := applyWindowOptions(opts...)
	wo.IdleFlushMs = orDefault(wo.IdleFlushMs, 30_000)
	return &WindowCronOperator{Every: every, windowBaseConfig: wo}
}

func (w *WindowCronOperator) Kind() string             { return "window" }
func (w *WindowCronOperator) WindowKind() string       { return "cron" }
func (w *WindowCronOperator) WindowMs() int64          { return everyToMs(w.Every) }
func (w *WindowCronOperator) GraceMs() int64           { return w.GracePeriodMs }
func (w *WindowCronOperator) IdleMs() int64            { return w.IdleFlushMs }
func (w *WindowCronOperator) AllowedLateMs() int64     { return w.AllowedLatenessMs }
func (w *WindowCronOperator) OnLate() string           { return w.OnLatePolicy }
func (w *WindowCronOperator) EventTime() EventTimeFn   { return w.EventTimeFn }
func (w *WindowCronOperator) OperatorTag() string      { return "cron:" + w.Every }
func (w *WindowCronOperator) Config() map[string]interface{} {
	return map[string]interface{}{
		"kind":            "window-cron",
		"every":           w.Every,
		"gracePeriod":     float64(w.GracePeriodMs) / msPerSec,
		"idleFlushMs":     w.IdleFlushMs,
		"eventTime":       w.EventTimeFn != nil,
		"allowedLateness": float64(w.AllowedLatenessMs) / msPerSec,
		"onLate":          w.OnLatePolicy,
	}
}

func (w *WindowCronOperator) ConfigKeys() []string {
	return []string{"kind", "every", "gracePeriod", "idleFlushMs", "eventTime", "allowedLateness", "onLate"}
}
func (w *WindowCronOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	ts := extractEventTime(env.Msg, w.EventTimeFn)
	if ts < 0 {
		return nil, fmt.Errorf("window_cron could not determine timestamp")
	}
	wm := w.WindowMs()
	var ws int64
	if w.Every == "week" {
		shifted := ts - weekEpochOffsetMs
		ws = (shifted/wm)*wm + weekEpochOffsetMs
	} else {
		ws = (ts / wm) * wm
	}
	out := env
	out.EventTimeMs = ts
	out.WindowStart = ws
	out.WindowEnd = ws + wm
	out.WindowKey = toIso(ws)
	out.OperatorTag = w.OperatorTag()
	out.GracePeriodMs = w.GracePeriodMs
	return []Envelope{out}, nil
}

// WindowSessionOperator — session windows with custom RunSession.
type WindowSessionOperator struct {
	Gap float64
	windowBaseConfig
}

func NewWindowSessionOperator(gap float64, opts ...WindowOption) *WindowSessionOperator {
	if gap <= 0 {
		panic("window_session requires gap > 0")
	}
	wo := applyWindowOptions(opts...)
	wo.IdleFlushMs = orDefault(wo.IdleFlushMs, 1000)
	return &WindowSessionOperator{Gap: gap, windowBaseConfig: wo}
}

func (w *WindowSessionOperator) Kind() string                   { return "window" }
func (w *WindowSessionOperator) WindowKind() string             { return "session" }
func (w *WindowSessionOperator) GapMs() int64                   { return int64(w.Gap * msPerSec) }
func (w *WindowSessionOperator) GraceMs() int64                 { return w.GracePeriodMs }
func (w *WindowSessionOperator) IdleMs() int64                  { return w.IdleFlushMs }
func (w *WindowSessionOperator) AllowedLateMs() int64           { return w.AllowedLatenessMs }
func (w *WindowSessionOperator) OnLate() string                 { return w.OnLatePolicy }
func (w *WindowSessionOperator) EventTime() EventTimeFn         { return w.EventTimeFn }
func (w *WindowSessionOperator) OperatorTag() string            { return "sess:" + numToShort(w.Gap) }
func (w *WindowSessionOperator) Config() map[string]interface{} {
	return map[string]interface{}{
		"kind":            "window-session",
		"gap":             w.Gap,
		"gracePeriod":     float64(w.GracePeriodMs) / msPerSec,
		"idleFlushMs":     w.IdleFlushMs,
		"eventTime":       w.EventTimeFn != nil,
		"allowedLateness": float64(w.AllowedLatenessMs) / msPerSec,
		"onLate":          w.OnLatePolicy,
	}
}

func (w *WindowSessionOperator) ConfigKeys() []string {
	return []string{"kind", "gap", "gracePeriod", "idleFlushMs", "eventTime", "allowedLateness", "onLate"}
}

func (w *WindowSessionOperator) OpenSessionStateKey(userKey string) string {
	return fmt.Sprintf("%s\u001fopen\u001f%s", w.OperatorTag(), userKey)
}

func (w *WindowSessionOperator) Apply(ctx context.Context, env Envelope) ([]Envelope, error) {
	ts := extractEventTime(env.Msg, w.EventTimeFn)
	if ts < 0 {
		return nil, fmt.Errorf("window_session could not determine timestamp")
	}
	out := env
	out.EventTimeMs = ts
	out.OperatorTag = w.OperatorTag()
	out.GracePeriodMs = w.GracePeriodMs
	return []Envelope{out}, nil
}

// SessionEmit is one closed-session emit shape.
type SessionEmit struct {
	Key         string
	WindowStart int64
	WindowEnd   int64
	WindowKey   string
	Value       interface{}
}

// SessionResult is what RunSession returns.
type SessionResult struct {
	StateOps []StateOp
	Emits    []SessionEmit
}

// RunSession applies a batch of envelopes to per-key open sessions and
// computes new state ops + emits.
func (w *WindowSessionOperator) RunSession(envelopes []Envelope, loadedState map[string]interface{}, reducerFn ReduceFn, reducerInitial func() interface{}, nowMs int64) SessionResult {
	type sessionState struct {
		Acc           interface{}
		SessionStart  int64
		LastEventTime int64
		Dirty         bool
		Seeded        bool
		Closed        bool
	}
	sessions := map[string]*sessionState{}

	for k, v := range loadedState {
		parts := strings.Split(k, "\u001f")
		if len(parts) != 3 || parts[0] != w.OperatorTag() || parts[1] != "open" {
			continue
		}
		userKey := parts[2]
		s := &sessionState{Seeded: true, Acc: reducerInitial()}
		if obj, ok := v.(map[string]interface{}); ok {
			if a, ok := obj["acc"]; ok {
				s.Acc = a
			}
			if ss, ok := obj["sessionStart"].(float64); ok {
				s.SessionStart = int64(ss)
			}
			if le, ok := obj["lastEventTime"].(float64); ok {
				s.LastEventTime = int64(le)
			}
		}
		sessions[userKey] = s
	}

	var emits []SessionEmit

	for _, env := range envelopes {
		userKey := env.Key
		ts := env.EventTimeMs
		s := sessions[userKey]
		if s == nil || s.LastEventTime == 0 {
			seeded := false
			if s != nil {
				seeded = s.Seeded
			}
			s = &sessionState{
				Acc:           reducerInitial(),
				SessionStart:  ts,
				LastEventTime: ts,
				Dirty:         true,
				Seeded:        seeded,
			}
			sessions[userKey] = s
		} else if s.LastEventTime+w.GapMs() >= ts {
			if ts > s.LastEventTime {
				s.LastEventTime = ts
			}
			s.Dirty = true
		} else {
			emits = append(emits, w.buildEmit(userKey, *s))
			s = &sessionState{
				Acc:           reducerInitial(),
				SessionStart:  ts,
				LastEventTime: ts,
				Dirty:         true,
				Seeded:        false,
			}
			sessions[userKey] = s
		}
		valueArg := env.Value
		if valueArg == nil {
			valueArg = env.Msg
		}
		newAcc, err := reducerFn(s.Acc, valueArg)
		if err == nil {
			s.Acc = newAcc
		}
	}

	flushRef := nowMs
	if flushRef == 0 {
		flushRef = time.Now().UnixMilli()
	}
	for userKey, s := range sessions {
		if s.LastEventTime == 0 {
			continue
		}
		sessionEnd := s.LastEventTime + w.GapMs()
		if sessionEnd+w.GracePeriodMs <= flushRef {
			emits = append(emits, w.buildEmit(userKey, *s))
			s.Closed = true
		}
	}

	var stateOps []StateOp
	for userKey, s := range sessions {
		stateKey := w.OpenSessionStateKey(userKey)
		if s.Closed {
			if s.Seeded {
				stateOps = append(stateOps, StateOp{Type: "delete", Key: stateKey})
			}
		} else if s.Dirty {
			stateOps = append(stateOps, StateOp{
				Type: "upsert",
				Key:  stateKey,
				Value: map[string]interface{}{
					"acc":           s.Acc,
					"sessionStart":  s.SessionStart,
					"lastEventTime": s.LastEventTime,
				},
			})
		}
	}

	return SessionResult{StateOps: stateOps, Emits: emits}
}

func (w *WindowSessionOperator) buildEmit(userKey string, s struct {
	Acc           interface{}
	SessionStart  int64
	LastEventTime int64
	Dirty         bool
	Seeded        bool
	Closed        bool
}) SessionEmit {
	return SessionEmit{
		Key:         userKey,
		WindowStart: s.SessionStart,
		WindowEnd:   s.LastEventTime + w.GapMs(),
		WindowKey:   toIso(s.SessionStart),
		Value:       s.Acc,
	}
}

// ---------------------------------------------------------------------------
// ReduceOperator
// ---------------------------------------------------------------------------

const stateKeySep = "\u001f"

type StateOp struct {
	Type  string      `json:"type"`            // "upsert" | "delete"
	Key   string      `json:"key"`
	Value interface{} `json:"value,omitempty"`
}

type ReduceEmit struct {
	Key         string
	WindowStart int64
	WindowEnd   int64
	WindowKey   string
	Value       interface{}
}

type ReduceResult struct {
	StateOps []StateOp
	Emits    []ReduceEmit
}

type ReduceOperator struct {
	Fn      ReduceFn
	Initial interface{}
}

func (r ReduceOperator) Kind() string { return "reduce" }
func (r ReduceOperator) Config() map[string]interface{} {
	return map[string]interface{}{"hasInitial": r.Initial != nil}
}

// Run folds a batch onto the loaded state. Mirror of ReduceOperator.run().
func (r ReduceOperator) Run(envelopes []Envelope, loadedState map[string]interface{}, streamTimeMs int64, operatorTag string, gracePeriodMs int64) ReduceResult {
	type entry struct {
		Key, WindowKey         string
		WindowStart, WindowEnd int64
		Acc                    interface{}
		Touched, Seeded        bool
	}
	accs := map[string]*entry{}

	for k, v := range loadedState {
		parts := ParseStateKey(k)
		if parts == nil {
			continue
		}
		if strings.HasPrefix(parts.OperatorTag, "__") {
			continue
		}
		if operatorTag != "" && parts.OperatorTag != operatorTag {
			continue
		}
		var ws, we int64
		var acc interface{} = v
		if obj, ok := v.(map[string]interface{}); ok {
			if x, ok := obj["windowStart"].(float64); ok {
				ws = int64(x)
			}
			if x, ok := obj["windowEnd"].(float64); ok {
				we = int64(x)
			}
			if a, ok := obj["acc"]; ok {
				acc = a
			}
		}
		accs[k] = &entry{
			Key: parts.UserKey, WindowKey: parts.WindowKey,
			WindowStart: ws, WindowEnd: we, Acc: acc, Seeded: true,
		}
	}

	var batchMaxStart int64 = -1 << 62
	for _, env := range envelopes {
		tag := env.OperatorTag
		if tag == "" {
			tag = operatorTag
		}
		sk := StateKeyFor(tag, env.WindowKey, env.Key)
		e, ok := accs[sk]
		if !ok {
			e = &entry{
				Key: env.Key, WindowKey: env.WindowKey,
				WindowStart: env.WindowStart, WindowEnd: env.WindowEnd,
				Acc: cloneJSON(r.Initial), Seeded: false,
			}
			accs[sk] = e
		}
		va := env.Value
		if va == nil {
			va = env.Msg
		}
		newAcc, err := r.Fn(e.Acc, va)
		if err == nil {
			e.Acc = newAcc
		}
		e.Touched = true
		if env.WindowStart > batchMaxStart {
			batchMaxStart = env.WindowStart
		}
	}

	effective := streamTimeMs
	if effective == -1<<62 || effective == 0 {
		effective = batchMaxStart
	}

	var ops []StateOp
	var emits []ReduceEmit
	for sk, e := range accs {
		closed := (e.WindowEnd + gracePeriodMs) <= effective
		if closed {
			emits = append(emits, ReduceEmit{
				Key: e.Key, WindowStart: e.WindowStart,
				WindowEnd: e.WindowEnd, WindowKey: e.WindowKey, Value: e.Acc,
			})
			if e.Seeded {
				ops = append(ops, StateOp{Type: "delete", Key: sk})
			}
		} else if e.Touched {
			ops = append(ops, StateOp{
				Type: "upsert", Key: sk,
				Value: map[string]interface{}{
					"acc": e.Acc, "windowStart": e.WindowStart, "windowEnd": e.WindowEnd,
				},
			})
		}
	}
	return ReduceResult{StateOps: ops, Emits: emits}
}

// StateKey parts.
type StateKey struct {
	OperatorTag string
	WindowKey   string
	UserKey     string
}

// StateKeyFor builds an operator-scoped state key.
func StateKeyFor(operatorTag, windowKey, userKey string) string {
	if userKey == "" {
		return operatorTag + stateKeySep + windowKey
	}
	return operatorTag + stateKeySep + windowKey + stateKeySep + userKey
}

// ParseStateKey splits an operator-scoped state key. Returns nil for malformed.
func ParseStateKey(k string) *StateKey {
	parts := strings.Split(k, stateKeySep)
	if len(parts) == 3 {
		return &StateKey{OperatorTag: parts[0], WindowKey: parts[1], UserKey: parts[2]}
	}
	if len(parts) == 2 {
		return &StateKey{OperatorTag: "", WindowKey: parts[0], UserKey: parts[1]}
	}
	return nil
}

func cloneJSON(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch v.(type) {
	case int, int64, float64, string, bool:
		return v
	}
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

// ---------------------------------------------------------------------------
// AggregateOperator
// ---------------------------------------------------------------------------

type AggregateOperator struct {
	ReduceOperator
	Fields []string
}

// NewAggregateOperator builds an aggregate from a name->extractor map.
// Iteration over Go maps is random, so the caller must explicitly pass a
// deterministic field order if they care about config_hash matching another
// language. fieldOrder is optional — if empty, alphabetical order is used.
func NewAggregateOperator(extractors map[string]ExtractorFn, fieldOrder ...string) *AggregateOperator {
	if len(extractors) == 0 {
		panic("aggregate requires at least one extractor")
	}
	fields := fieldOrder
	if len(fields) == 0 {
		fields = make([]string, 0, len(extractors))
		for k := range extractors {
			fields = append(fields, k)
		}
		sort.Strings(fields)
	}
	initial := map[string]interface{}{}
	for _, name := range fields {
		switch name {
		case "min", "max":
			initial[name] = nil
		case "avg":
			initial["__avg_sum"] = float64(0)
			initial["__avg_count"] = float64(0)
			initial["avg"] = float64(0)
		default:
			initial[name] = float64(0)
		}
	}
	fn := func(acc interface{}, msg interface{}) (interface{}, error) {
		var nxt map[string]interface{}
		if a, ok := acc.(map[string]interface{}); ok {
			nxt = make(map[string]interface{}, len(a))
			for k, v := range a {
				nxt[k] = v
			}
		} else {
			nxt = map[string]interface{}{}
		}
		for _, name := range fields {
			ex := extractors[name]
			v, err := ex(msg)
			if err != nil {
				return nil, err
			}
			switch name {
			case "count":
				nxt["count"] = numAdd(nxt["count"], v)
			case "sum":
				nxt["sum"] = numAdd(nxt["sum"], v)
			case "min":
				nxt["min"] = numMin(nxt["min"], v)
			case "max":
				nxt["max"] = numMax(nxt["max"], v)
			case "avg":
				nxt["__avg_sum"] = numAdd(nxt["__avg_sum"], v)
				nxt["__avg_count"] = numAdd(nxt["__avg_count"], 1)
				if c := numAsFloat(nxt["__avg_count"]); c > 0 {
					nxt["avg"] = numAsFloat(nxt["__avg_sum"]) / c
				}
			default:
				nxt[name] = numAdd(nxt[name], v)
			}
		}
		return nxt, nil
	}
	return &AggregateOperator{
		ReduceOperator: ReduceOperator{Fn: fn, Initial: initial},
		Fields:         fields,
	}
}

func (a *AggregateOperator) Kind() string { return "aggregate" }
func (a *AggregateOperator) Config() map[string]interface{} {
	return map[string]interface{}{"fields": a.Fields}
}

func numAsFloat(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}
func numAdd(a, b interface{}) float64 { return numAsFloat(a) + numAsFloat(b) }
func numMin(a, b interface{}) interface{} {
	if a == nil {
		return numAsFloat(b)
	}
	bf := numAsFloat(b)
	af := numAsFloat(a)
	if bf < af {
		return bf
	}
	return af
}
func numMax(a, b interface{}) interface{} {
	if a == nil {
		return numAsFloat(b)
	}
	bf := numAsFloat(b)
	af := numAsFloat(a)
	if bf > af {
		return bf
	}
	return af
}

// ---------------------------------------------------------------------------
// Window option helpers
// ---------------------------------------------------------------------------

// WindowOption configures a window operator. Use With* helpers.
type WindowOption func(*windowBaseConfig)

func WithGracePeriod(seconds float64) WindowOption {
	return func(w *windowBaseConfig) { w.GracePeriodMs = int64(seconds * msPerSec) }
}
func WithIdleFlushMs(ms int64) WindowOption {
	return func(w *windowBaseConfig) { w.IdleFlushMs = ms }
}
func WithEventTime(fn EventTimeFn) WindowOption {
	return func(w *windowBaseConfig) { w.EventTimeFn = fn }
}
func WithAllowedLateness(seconds float64) WindowOption {
	return func(w *windowBaseConfig) { w.AllowedLatenessMs = int64(seconds * msPerSec) }
}
func WithOnLate(policy string) WindowOption {
	return func(w *windowBaseConfig) { w.OnLatePolicy = normaliseLate(policy) }
}

func applyWindowOptions(opts ...WindowOption) windowBaseConfig {
	w := windowBaseConfig{OnLatePolicy: "drop"}
	for _, o := range opts {
		o(&w)
	}
	if w.OnLatePolicy == "" {
		w.OnLatePolicy = "drop"
	}
	return w
}

func orDefault(v, dflt int64) int64 {
	if v == 0 {
		return dflt
	}
	return v
}

// numToShort renders a float64 the same way JS String(num) does:
// integer values produce "60" instead of "60.000000".
func numToShort(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
