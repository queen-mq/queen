package feedrt

// What a feed stream is, for whoever looks at it from outside (the console):
// the queues it reads, its steps in order, where its results go, and what it
// does with a message that keeps failing (about.js). The active runner writes
// it to queen-feed / <queryId>#about in its first commit after it becomes
// active, under the lock like the rest of the commit (one record per query:
// every shard writes the same one). Nothing reads it back to run the stream.

import (
	"math"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// AboutKey is the key of a query's description in queen-feed.
func AboutKey(queryID string) string { return queryID + "#about" }

const (
	aboutFormat  = 1
	clipMaxText  = 200
	clipMaxList  = 50
	clipMaxDepth = 4
	modulePath   = "github.com/smartpricing/queen/clients/client-go/v2"
	// sdkVersion is this client's version when the build says none (a local build).
	sdkVersion = "2.8.1"
)

var clientName = sync.OnceValue(func() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Path == modulePath && info.Main.Version != "" && info.Main.Version != "(devel)" {
			return "client-go " + strings.TrimPrefix(info.Main.Version, "v")
		}
		for _, d := range info.Deps {
			if d.Path == modulePath && d.Version != "" && d.Version != "(devel)" {
				return "client-go " + strings.TrimPrefix(d.Version, "v")
			}
		}
	}
	return "client-go " + sdkVersion
})

// cutText cuts a string at max JS characters (UTF-16 units) and marks the cut.
func cutText(s string, max int) string {
	n := 0
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		w := 1
		if r >= 0x10000 {
			w = 2
		}
		if n+w > max {
			return s[:i] + "…"
		}
		n += w
		i += size
	}
	return s
}

// clip is a JSON-safe copy, cut to a size worth storing: long strings, long
// lists, deep objects, no functions (about.js clip).
func clip(v interface{}, depth int) interface{} {
	if jsv.IsNullish(v) {
		return nil
	}
	switch x := v.(type) {
	case string:
		return cutText(x, clipMaxText)
	case bool:
		return x
	case time.Time:
		if x.IsZero() {
			return nil
		}
		return jsv.ISOOf(x)
	case feed.Condition:
		return whereOf(x)
	case jsv.Ordered:
		if depth >= clipMaxDepth {
			return "…"
		}
		out := jsv.Ordered{}
		for _, p := range x {
			if jsv.IsUndefined(p.V) || isFunc(p.V) {
				continue
			}
			out = append(out, jsv.Pair{K: p.K, V: clip(p.V, depth+1)})
		}
		return out
	case map[string]interface{}:
		if depth >= clipMaxDepth {
			return "…"
		}
		out := map[string]interface{}{}
		for k, e := range x {
			if jsv.IsUndefined(e) || isFunc(e) {
				continue
			}
			out[k] = clip(e, depth+1)
		}
		return out
	case []interface{}:
		n := len(x)
		if n > clipMaxList {
			n = clipMaxList
		}
		out := make([]interface{}, 0, n+1)
		for _, e := range x[:n] {
			out = append(out, clip(e, depth+1))
		}
		if len(x) > clipMaxList {
			out = append(out, "… "+jsv.NumberString(float64(len(x)-clipMaxList))+" more")
		}
		return out
	case []string:
		l := make([]interface{}, len(x))
		for i, e := range x {
			l[i] = e
		}
		return clip(l, depth)
	}
	if f, ok := jsv.Num(v); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return jsv.NumberString(f)
		}
		return f
	}
	if isFunc(v) {
		return "fn"
	}
	if jsv.IsArray(v) || jsv.IsObject(v) {
		return clip(jsv.Normalize(v), depth)
	}
	return jsv.String(v)
}

func isFunc(v interface{}) bool {
	return v != nil && !jsv.IsUndefined(v) && reflect.ValueOf(v).Kind() == reflect.Func
}

func whereOf(c feed.Condition) interface{} {
	o := jsv.Ordered{{K: "field", V: c.Field}, {K: "op", V: c.Op}}
	if c.Op != "exists" {
		v := c.Value
		if jsv.IsUndefined(v) {
			v = nil
		}
		o = append(o, jsv.Pair{K: "value", V: v})
	}
	return clip(o, 0)
}

func listOrNull(l []string) interface{} {
	if l == nil {
		return nil
	}
	return clip(l, 0)
}

// stepOf is one compiled step as the description says it; pipe gives the clock it runs on.
func stepOf(s Step, pipe *pStep) interface{} {
	if s.Kind == "ops" {
		ops := make([]interface{}, len(s.Ops))
		var where []interface{}
		for i, op := range s.Ops {
			ops[i] = op.Kind
			if op.Kind == "where" && op.Where != nil {
				where = append(where, whereOf(*op.Where))
			}
		}
		out := jsv.Ordered{{K: "kind", V: "ops"}, {K: "ops", V: ops}}
		if len(where) > 0 {
			out = append(out, jsv.Pair{K: "where", V: where})
		}
		return out
	}
	clockKind := ""
	if pipe != nil && pipe.clock != nil {
		clockKind = pipe.clock.kind()
	}
	if s.Kind == "window" {
		reducer := jsv.Ordered{}
		if c, ok := clip(s.ReducerConfig, 0).(jsv.Ordered); ok {
			reducer = c
		}
		out := jsv.Ordered{{K: "kind", V: "window"}, {K: "name", V: s.Name}, {K: "window", V: clip(s.Config, 0)}, {K: "reducer", V: reducer}}
		if clockKind != "" {
			out = append(out, jsv.Pair{K: "clock", V: clockKind})
		}
		return out
	}
	config := jsv.Ordered{}
	if c, ok := clip(s.Config, 0).(jsv.Ordered); ok {
		config = c
	}
	// pattern, join (two sides or many), gather, table: the name the chain gave it.
	kind := s.Kind
	if s.Kind != "table" {
		if k, ok := config.Get("kind").(string); ok && k != "" {
			kind = k
		}
	}
	out := jsv.Ordered{{K: "kind", V: kind}, {K: "name", V: s.Name}, {K: "config", V: config}}
	if clockKind != "" {
		out = append(out, jsv.Pair{K: "clock", V: clockKind})
	}
	return out
}

func sinkOf(s *SinkSpec) interface{} {
	if s == nil {
		return nil
	}
	if s.Kind == "foreach" {
		return jsv.Ordered{{K: "kind", V: "foreach"}}
	}
	if s.Kind == "foreachBatch" {
		return jsv.Ordered{{K: "kind", V: "foreachBatch"}, {K: "maxItems", V: float64(s.MaxItems)}, {K: "maxWaitMillis", V: float64(s.MaxWaitMillis)}}
	}
	if s.Route != nil {
		return jsv.Ordered{{K: "kind", V: "route"}}
	}
	queues := s.Queues
	if queues == nil {
		queues = []string{}
	}
	return jsv.Ordered{{K: "kind", V: "queues"}, {K: "queues", V: clip(queues, 0)}}
}

// durationText is a lock lifetime as a JS duration string ('15s', '1m', '250ms').
func durationText(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return jsv.NumberString(float64(d/time.Hour)) + "h"
	case d%time.Minute == 0:
		return jsv.NumberString(float64(d/time.Minute)) + "m"
	case d%time.Second == 0:
		return jsv.NumberString(float64(d/time.Second)) + "s"
	}
	return jsv.NumberString(float64(d)/float64(time.Millisecond)) + "ms"
}

// describe is the description of the runner's stream (about.js describeFeedStream).
func (r *Runner) describe() interface{} {
	src := r.stream.Source
	where := []interface{}{}
	for _, c := range r.stream.Pushdown {
		where = append(where, whereOf(c))
	}
	reads := jsv.Ordered{{K: "queues", V: listOrNull(listOrNil(src.Queues))}, {K: "partitions", V: listOrNull(listOrNil(src.Partitions))},
		{K: "where", V: where}}
	if src.Kinds != nil {
		reads = append(reads, jsv.Pair{K: "kinds", V: clip(src.Kinds, 0)})
	}
	if src.Since != nil && !jsv.IsUndefined(src.Since) {
		reads = append(reads, jsv.Pair{K: "since", V: clip(src.Since, 0)})
	}
	if len(r.tableQueues) > 0 {
		reads = append(reads, jsv.Pair{K: "tables", V: clip(r.tableQueues, 0)})
	}
	steps := make([]interface{}, len(r.stream.Steps))
	for i, s := range r.stream.Steps {
		var pipe *pStep
		if i < len(r.pipeline.steps) {
			pipe = r.pipeline.steps[i]
		}
		steps[i] = stepOf(s, pipe)
	}
	var onPoison interface{} = r.poison.kind
	if r.poison.kind == "deadLetter" {
		onPoison = jsv.Ordered{{K: "queue", V: r.poison.queue}}
	}
	var shards interface{}
	if r.shard != nil {
		by := "partition"
		if r.shardBy != nil {
			by = "function"
		}
		shards = jsv.Ordered{{K: "count", V: float64(r.shard.Count)}, {K: "by", V: by}}
	}
	var hash interface{}
	if r.hash != "" {
		hash = r.hash
	}
	return jsv.Ordered{
		{K: "v", V: float64(aboutFormat)},
		{K: "queryId", V: r.queryID},
		{K: "hash", V: hash},
		{K: "client", V: clientName()},
		{K: "reads", V: reads},
		{K: "steps", V: steps},
		{K: "sink", V: sinkOf(r.sink)},
		{K: "onPoison", V: onPoison},
		{K: "shards", V: shards},
		{K: "exclusive", V: r.opts.exclusive},
		{K: "lockTtl", V: clip(durationText(r.opts.lockTTL), 0)},
		{K: "positionCommitMillis", V: r.opts.positionCommitMillis},
	}
}

// aboutValue is the description, made once per process (nil when it cannot be made).
func (r *Runner) aboutValue() interface{} {
	if !r.aboutMade {
		r.aboutMade = true
		func() {
			defer func() {
				if p := recover(); p != nil {
					logWarn("FeedRunner.about", map[string]interface{}{"queryId": r.id})
					r.about = nil
				}
			}()
			r.about = r.describe()
		}()
	}
	return r.about
}
