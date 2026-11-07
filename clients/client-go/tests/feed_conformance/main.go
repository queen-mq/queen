// The Go SDK's feed-stream conformance runner (clients/conformance/feed/README.md
// sections 1 and 3): builds a feed stream from a case file with this SDK's
// streams.FromFeed and runs it, speaking JSON lines on stdout and taking
// commands on stdin.
//
//	go-runner run  --url <url> --case <case.json> [--runner <name>] [--run '<json>']
//	go-runner view --url <url> --query-id <id> --op get|list|status [--step s] [--key k] [--limit n] [--shards n] [--timeout-ms n]
//
// Build (from clients/client-go):
//
//	go build -o ../conformance/feed/.bin/go-runner ./tests/feed_conformance
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
	"github.com/smartpricing/queen/clients/client-go/v2/streams"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/feedrt"
	"github.com/smartpricing/queen/clients/client-go/v2/streams/operators"
)

var outMu sync.Mutex

// say writes one JSON line on stdout (undefined fields left out).
func say(v interface{}) {
	b, err := jsv.Marshal(v)
	if err != nil {
		b = jsv.MustMarshal(map[string]interface{}{"event": "failed", "message": err.Error()})
	}
	outMu.Lock()
	os.Stdout.Write(append(b, '\n'))
	outMu.Unlock()
}

func arg(name, fallback string) string {
	for i, a := range os.Args {
		if a == "--"+name && i+1 < len(os.Args) {
			return os.Args[i+1]
		}
	}
	return fallback
}

// ---- functions from expressions -------------------------------------------------

// arg0 is a function argument as JS has it: a Go nil ctx (a feed message's) is undefined.
func undef(v interface{}) interface{} {
	if v == nil {
		return jsv.Undefined
	}
	if c, ok := v.(streams.EmitCtx); ok {
		if c == nil {
			return jsv.Undefined
		}
		return map[string]interface{}(c)
	}
	return v
}

func fn1(e interface{}) func(a interface{}) (interface{}, error) {
	return func(a interface{}) (interface{}, error) { return evaluate(e, []interface{}{a}) }
}

func fn2(e interface{}) func(a, b interface{}) (interface{}, error) {
	return func(a, b interface{}) (interface{}, error) { return evaluate(e, []interface{}{a, b}) }
}

func fn3(e interface{}) func(a, b, c interface{}) (interface{}, error) {
	return func(a, b, c interface{}) (interface{}, error) { return evaluate(e, []interface{}{a, b, c}) }
}

func test1(e interface{}) func(a interface{}) (bool, error) {
	return func(a interface{}) (bool, error) {
		v, err := evaluate(e, []interface{}{a})
		return jsv.Truthy(v), err
	}
}

func test2(e interface{}) func(a, b interface{}) (bool, error) {
	return func(a, b interface{}) (bool, error) {
		v, err := evaluate(e, []interface{}{a, b})
		return jsv.Truthy(v), err
	}
}

func has(o jsv.Ordered, k string) bool { return !jsv.IsUndefined(o.Get(k)) }

func str(o jsv.Ordered, k string) string {
	s, _ := o.Get(k).(string)
	return s
}

func optOf(o jsv.Ordered, k string) jsv.Ordered {
	x, _ := o.Get(k).(jsv.Ordered)
	return x
}

// stepTime reads the time options of a pattern, a join or a gather.
func stepTime(o jsv.Ordered) streams.StepTime {
	var t streams.StepTime
	if et := o.Get("eventTime"); et == false {
		t.EventTimeOff = true
	} else if !jsv.IsUndefined(et) && et != nil {
		f := fn1(et)
		t.EventTime = func(msg interface{}) (interface{}, error) { return f(msg) }
	}
	if v := o.Get("allowedLateness"); !jsv.IsUndefined(v) {
		t.AllowedLateness = plain(v)
	}
	t.OnLate = str(o, "onLate")
	if v := o.Get("idle"); !jsv.IsUndefined(v) {
		t.Idle = plain(v)
	}
	return t
}

func windowOpts(o jsv.Ordered) []operators.WindowOption {
	var out []operators.WindowOption
	if v, ok := jsv.Num(o.Get("gracePeriod")); ok {
		out = append(out, operators.WithGracePeriod(v))
	}
	if v, ok := jsv.Num(o.Get("idleFlushMs")); ok {
		out = append(out, operators.WithIdleFlushMs(int64(v)))
	}
	if et := o.Get("eventTime"); et == false {
		out = append(out, streams.WithEventTimeOff())
	} else if !jsv.IsUndefined(et) && et != nil {
		f := fn1(et)
		out = append(out, streams.WithEventTimeOf(func(msg interface{}) (interface{}, error) { return f(msg) }))
	}
	switch v := o.Get("allowedLateness").(type) {
	case string:
		out = append(out, streams.WithAllowedLatenessSpan(v))
	case float64:
		out = append(out, operators.WithAllowedLateness(v))
	}
	if s := str(o, "onLate"); s != "" {
		out = append(out, operators.WithOnLate(s))
	}
	switch v := o.Get("idle").(type) {
	case string:
		out = append(out, streams.WithIdleSpan(v))
	case float64:
		out = append(out, streams.WithIdle(v))
	}
	return out
}

func parts(spec interface{}) []streams.Part {
	switch s := spec.(type) {
	case []interface{}:
		var out []streams.Part
		for _, x := range s {
			n := jsv.String(x)
			out = append(out, streams.Part{Name: n, Queue: n})
		}
		return out
	case jsv.Ordered:
		var out []streams.Part
		for _, p := range s {
			if q, ok := p.V.(string); ok {
				out = append(out, streams.Part{Name: p.K, Queue: q})
			} else {
				out = append(out, streams.Part{Name: p.K, Test: test2(p.V)})
			}
		}
		return out
	}
	return nil
}

func num(v interface{}) float64 {
	f, _ := jsv.Num(v)
	return f
}

// build makes the stream of a case's chain.
func build(q *queen.Queen, c jsv.Ordered) (s *streams.Stream, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	src := plain(c.Get("source"))
	sm, _ := src.(map[string]interface{})
	fs := streams.FeedSource{From: sm["from"], Since: sm["since"]}
	fs.Queues = strs(sm["queues"], sm["queue"])
	fs.Partitions = strs(sm["partitions"], sm["partition"])
	fs.FollowerURLs = strs(sm["followerUrls"], nil)
	fs.Kinds = strs(sm["kinds"], nil)
	if v, ok := jsv.Num(sm["maxRecords"]); ok {
		fs.MaxRecords = int(v)
	}
	s = streams.FromFeed(q, fs)
	chain, _ := c.Get("chain").([]interface{})
	for _, raw := range chain {
		st, _ := raw.(jsv.Ordered)
		fe := st.Get("fn")
		opts := optOf(st, "opts")
		switch str(st, "op") {
		case "where":
			args, _ := st.Get("args").([]interface{})
			field, _ := args[0].(string)
			op, _ := args[1].(string)
			if len(args) == 2 {
				s = s.Where(field, op)
			} else {
				s = s.Where(field, op, plain(args[2]))
			}
		case "filter":
			f := fn2(fe)
			s = s.Filter(func(msg interface{}, ctx streams.EmitCtx) (bool, error) {
				v, err := f(msg, undef(ctx))
				return jsv.Truthy(v), err
			})
		case "map":
			f := fn2(fe)
			s = s.Map(func(msg interface{}, ctx streams.EmitCtx) (interface{}, error) { return f(msg, undef(ctx)) })
		case "flatMap":
			f := fn2(fe)
			s = s.FlatMap(func(msg interface{}, ctx streams.EmitCtx) ([]interface{}, error) {
				v, err := f(msg, undef(ctx))
				if err != nil {
					return nil, err
				}
				l, ok := v.([]interface{})
				if !ok {
					t := "object"
					switch {
					case jsv.IsUndefined(v):
						t = "undefined"
					case jsv.IsNumber(v):
						t = "number"
					case v == true || v == false:
						t = "boolean"
					}
					if _, isStr := v.(string); isStr {
						t = "string"
					}
					return nil, fmt.Errorf("flatMap fn must return an array (got %s)", t)
				}
				return l, nil
			})
		case "keyBy":
			f := fn1(fe)
			s = s.KeyBy(func(msg interface{}) (string, error) {
				v, err := f(msg)
				return jsv.String(v), err
			})
		case "windowTumbling":
			s = s.WindowTumbling(num(opts.Get("seconds")), windowOpts(opts)...)
		case "windowSliding":
			s = s.WindowSliding(num(opts.Get("size")), num(opts.Get("slide")), windowOpts(opts)...)
		case "windowSession":
			s = s.WindowSession(num(opts.Get("gap")), windowOpts(opts)...)
		case "windowCron":
			s = s.WindowCron(str(opts, "every"), windowOpts(opts)...)
		case "reduce":
			f := fn2(fe)
			var initial interface{}
			if has(st, "initial") {
				initial = plain(st.Get("initial"))
				if initial == nil {
					initial = streams.NullInitial
				}
			}
			s = s.Reduce(func(acc, v interface{}) (interface{}, error) { return f(acc, v) }, initial)
		case "aggregate":
			fields, _ := st.Get("fields").(jsv.Ordered)
			var fs []streams.AggregateField
			for _, p := range fields {
				fs = append(fs, streams.AggregateField{Name: p.K, Fn: fn1(p.V)})
			}
			s = s.AggregateFields(fs...)
		case "pattern":
			o := streams.PatternOptions{Within: str(opts, "within"), OnRepeat: str(opts, "onRepeat"), Name: str(opts, "name"), StepTime: stepTime(opts)}
			if has(opts, "start") {
				o.Start = test1(opts.Get("start"))
			}
			if has(opts, "then") {
				o.Then = test1(opts.Get("then"))
			}
			if has(opts, "key") {
				o.Key = fn2(opts.Get("key"))
			}
			if has(opts, "keep") {
				o.Keep = fn2(opts.Get("keep"))
			}
			if has(opts, "onMatch") {
				o.OnMatch = fn2(opts.Get("onMatch"))
			}
			if has(opts, "onTimeout") {
				o.OnTimeout = fn1(opts.Get("onTimeout"))
			}
			s = s.Pattern(o)
		case "join":
			if has(opts, "sides") {
				o := streams.JoinSidesOptions{Sides: parts(opts.Get("sides")), Within: str(opts, "within"), OnRepeat: str(opts, "onRepeat"),
					Name: str(opts, "name"), StepTime: stepTime(opts)}
				if has(opts, "key") {
					o.Key = fn2(opts.Get("key"))
				}
				if has(opts, "keep") {
					o.Keep = fn2(opts.Get("keep"))
				}
				if has(opts, "select") {
					f := fn2(opts.Get("select"))
					o.Select = func(got interface{}, key string) (interface{}, error) { return f(got, key) }
				}
				if has(opts, "onTimeout") {
					f := fn3(opts.Get("onTimeout"))
					o.OnTimeout = func(got, missing interface{}, key string) (interface{}, error) { return f(got, missing, key) }
				}
				s = s.JoinSides(o)
				continue
			}
			o := streams.JoinOptions{Within: str(opts, "within"), OnRepeat: str(opts, "onRepeat"), Name: str(opts, "name"), StepTime: stepTime(opts)}
			if has(opts, "left") {
				o.Left = test1(opts.Get("left"))
			}
			if has(opts, "right") {
				o.Right = test1(opts.Get("right"))
			}
			if has(opts, "key") {
				o.Key = fn2(opts.Get("key"))
			}
			if has(opts, "keep") {
				o.Keep = fn2(opts.Get("keep"))
			}
			if has(opts, "select") {
				o.Select = fn2(opts.Get("select"))
			}
			if has(opts, "onTimeout") {
				f := fn2(opts.Get("onTimeout"))
				o.OnTimeout = func(msg interface{}, side string) (interface{}, error) { return f(msg, side) }
			}
			s = s.Join(o)
		case "gather":
			o := streams.GatherOptions{Within: str(opts, "within"), OnRepeat: str(opts, "onRepeat"), Name: str(opts, "name"), StepTime: stepTime(opts)}
			if has(opts, "steps") {
				o.Steps = parts(opts.Get("steps"))
			}
			if has(opts, "count") {
				o.Count = fn2(opts.Get("count"))
			}
			if has(opts, "key") {
				o.Key = fn2(opts.Get("key"))
			}
			if has(opts, "keep") {
				o.Keep = fn2(opts.Get("keep"))
			}
			if has(opts, "onComplete") {
				f := fn2(opts.Get("onComplete"))
				o.OnComplete = func(got interface{}, key string) (interface{}, error) { return f(got, key) }
			}
			if has(opts, "onTimeout") {
				f := fn3(opts.Get("onTimeout"))
				o.OnTimeout = func(got, missing interface{}, key string) (interface{}, error) { return f(got, missing, key) }
			}
			s = s.Gather(o)
		case "joinTable":
			o := streams.TableOptions{Missing: str(opts, "missing"), Initial: str(opts, "initial"), Name: str(opts, "name")}
			switch qv := plain(opts.Get("queue")).(type) {
			case string:
				o.Queue = qv
			case []interface{}:
				for _, x := range qv {
					o.Queues = append(o.Queues, jsv.String(x))
				}
			}
			if has(opts, "on") {
				o.On = fn2(opts.Get("on"))
			}
			if has(opts, "key") {
				o.Key = fn1(opts.Get("key"))
			}
			if has(opts, "value") {
				o.Value = fn1(opts.Get("value"))
			}
			if has(opts, "select") {
				f := fn3(opts.Get("select"))
				// A missing row is nil here, undefined in JS.
				o.Select = func(v, row, msg interface{}) (interface{}, error) { return f(v, undef(row), msg) }
			}
			s = s.JoinTable(o)
		case "name":
			s = s.Name(str(st, "name"))
		case "to":
			var target interface{}
			switch t := st.Get("target").(type) {
			case string:
				target = t
			case []interface{}:
				var l []string
				for _, x := range t {
					l = append(l, jsv.String(x))
				}
				target = l
			case jsv.Ordered:
				f := fn2(t.Get("fn"))
				target = streams.RouteFn(func(v, msg interface{}) (interface{}, error) { return f(v, undef(msg)) })
			}
			var to streams.ToOptions
			switch p := st.Get("partition").(type) {
			case string:
				to.Partition = p
			case jsv.Ordered:
				to.Partition = fn1(p.Get("fn"))
			}
			s = s.ToWith(target, to)
		case "foreach":
			e := st.Get("fn")
			s = s.Foreach(func(value interface{}, ctx streams.EmitCtx) error {
				say(map[string]interface{}{"event": "foreach", "value": value, "ctx": undef(ctx)})
				if jsv.IsUndefined(e) {
					return nil
				}
				_, err := evaluate(e, []interface{}{value, undef(ctx)})
				return err
			})
		case "foreachBatch":
			e := st.Get("fn")
			var bopts []streams.BatchOption
			if o, ok := st.Get("opts").(jsv.Ordered); ok {
				if v, ok := jsv.Num(o.Get("maxItems")); ok {
					bopts = append(bopts, streams.WithMaxItems(int(v)))
				}
				if v, ok := jsv.Num(o.Get("maxWaitMillis")); ok {
					bopts = append(bopts, streams.WithMaxWaitMillis(int(v)))
				}
			}
			s = s.ForeachBatch(func(items []streams.BatchItem) error {
				// The items as JSON (as the JS runner prints them), then the expression with them as its one argument.
				list := make([]interface{}, len(items))
				for i, it := range items {
					list[i] = map[string]interface{}{"value": it.Value, "ctx": undef(it.Ctx), "id": it.ID}
				}
				say(map[string]interface{}{"event": "foreachBatch", "items": list})
				if jsv.IsUndefined(e) {
					return nil
				}
				_, err := evaluate(e, []interface{}{jsv.JSONClone(list)})
				return err
			}, bopts...)
		default:
			return nil, fmt.Errorf("case: unknown chain op %s", str(st, "op"))
		}
	}
	return s, nil
}

func strs(v, alt interface{}) []string {
	if jsv.IsNullish(v) {
		v = alt
	}
	switch x := v.(type) {
	case string:
		return []string{x}
	case []interface{}:
		var out []string
		for _, e := range x {
			out = append(out, jsv.String(e))
		}
		return out
	}
	return nil
}

func intPtr(v interface{}) *int {
	if f, ok := jsv.Num(v); ok {
		n := int(f)
		return &n
	}
	return nil
}

func boolPtr(v interface{}) *bool {
	if b, ok := v.(bool); ok {
		return &b
	}
	return nil
}

// runOptions maps the case's run options (JS names) to the Go ones.
func runOptions(c jsv.Ordered, overrides jsv.Ordered) (streams.FeedRunOptions, error) {
	merged := jsv.Ordered{}
	if r, ok := c.Get("run").(jsv.Ordered); ok {
		for _, p := range r {
			merged = merged.Set(p.K, p.V)
		}
	}
	for _, p := range overrides {
		merged = merged.Set(p.K, p.V)
	}
	var o streams.FeedRunOptions
	get := func(k string) interface{} { return plain(merged.Get(k)) }
	o.QueryID, _ = get("queryId").(string)
	if v := get("from"); !jsv.IsUndefined(v) {
		o.From = v
	}
	o.WaitMillis = intPtr(get("waitMillis"))
	if f, ok := jsv.Num(get("maxRecords")); ok {
		o.MaxRecords = int(f)
	}
	if f, ok := jsv.Num(get("maxBytes")); ok {
		o.MaxBytes = int(f)
	}
	switch r := get("reset").(type) {
	case bool:
		o.Reset = r
	case string:
		o.ResetToken = r
	}
	o.PositionCommitMillis = intPtr(get("positionCommitMillis"))
	o.CommitEveryMillis = intPtr(get("commitEveryMillis"))
	o.MaxPendingOutputs = intPtr(get("maxPendingOutputs"))
	o.Exclusive = boolPtr(get("exclusive"))
	switch t := get("lockTtl").(type) {
	case string:
		ms, err := jsv.ParseDurationMs(t)
		if err != nil {
			return o, err
		}
		o.LockTTL = time.Duration(ms * float64(time.Millisecond))
	}
	switch p := get("onPoison").(type) {
	case string:
		o.OnPoison = p
	case map[string]interface{}:
		if n, ok := p["name"].(string); ok {
			o.OnPoison = n
		}
	}
	o.Retries = intPtr(get("retries"))
	o.RetryDelayMillis = intPtr(get("retryDelayMillis"))
	o.SnapshotEveryMillis = intPtr(get("snapshotEveryMillis"))
	o.SnapshotMaxEntries = intPtr(get("snapshotMaxEntries"))
	if f, ok := jsv.Num(get("snapshotChunkChars")); ok {
		o.SnapshotChunkChars = int(f)
	}
	if f, ok := jsv.Num(get("snapshotPieceBytes")); ok {
		o.SnapshotPieceBytes = int(f)
	}
	if sh, ok := get("shard").(map[string]interface{}); ok {
		o.Shard = &streams.ShardSpec{Index: int(num(sh["index"])), Count: int(num(sh["count"]))}
	}
	if f, ok := jsv.Num(get("shards")); ok {
		o.Shards = int(f)
	}
	if sb := merged.Get("shardBy"); !jsv.IsUndefined(sb) && sb != "partition" && sb != nil {
		f := fn1(sb)
		o.ShardBy = func(msg interface{}) (interface{}, error) { return f(msg) }
	}
	o.ServeState = boolPtr(get("serveState"))
	if f, ok := jsv.Num(get("serveWaitMillis")); ok {
		o.ServeWaitMillis = int(f)
	}
	o.OnError = func(err error, ctx map[string]interface{}) {
		ev := map[string]interface{}{"event": "error", "message": err.Error(), "phase": ctx["phase"], "action": ctx["action"]}
		if code := feedrt.ErrorCode(err); code != "" {
			ev["code"] = code
		}
		if ev["action"] == nil {
			delete(ev, "action")
		}
		say(ev)
	}
	return o, nil
}

func fail(err error) {
	ev := map[string]interface{}{"event": "failed", "message": err.Error()}
	if code := feedrt.ErrorCode(err); code != "" {
		ev["code"] = code
	}
	say(ev)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go-runner run|view --url <url> ...")
		os.Exit(2)
	}
	mode := os.Args[1]
	url := arg("url", "")
	if url == "" {
		fail(errors.New("--url is required"))
		os.Exit(2)
	}
	q, err := queen.New(url)
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	if mode == "view" {
		shards, _ := strconv.Atoi(arg("shards", "0"))
		timeout, _ := strconv.Atoi(arg("timeout-ms", "3000"))
		view, err := streams.View(q, arg("query-id", ""), streams.ViewOptions{Shards: shards, TimeoutMillis: timeout})
		if err != nil {
			fail(err)
			os.Exit(0)
		}
		ctx := context.Background()
		var answer interface{}
		switch arg("op", "status") {
		case "get":
			answer, err = view.Get(ctx, arg("step", ""), arg("key", ""))
		case "list":
			limit, _ := strconv.Atoi(arg("limit", "100"))
			answer, err = view.List(ctx, arg("step", ""), limit)
		default:
			var l []map[string]interface{}
			l, err = view.Status(ctx)
			list := make([]interface{}, len(l))
			for i, x := range l {
				list[i] = x
			}
			answer = list
		}
		if err != nil {
			fail(err)
		} else {
			say(map[string]interface{}{"event": "view", "answer": answer})
		}
		os.Exit(0)
	}
	if mode != "run" {
		fail(errors.New("mode is run or view"))
		os.Exit(2)
	}
	raw, err := os.ReadFile(arg("case", ""))
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	cv, err := decodeOrdered(raw)
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	c, _ := cv.(jsv.Ordered)
	var overrides jsv.Ordered
	if s := arg("run", ""); s != "" {
		ov, err := decodeOrdered([]byte(s))
		if err != nil {
			fail(err)
			os.Exit(2)
		}
		overrides, _ = ov.(jsv.Ordered)
	}
	opts, err := runOptions(c, overrides)
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	s, err := build(q, c)
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	h, err := func() (h streams.FeedHandle, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("%v", r)
			}
		}()
		return s.RunFeed(context.Background(), opts)
	}()
	if err != nil {
		fail(err)
		os.Exit(2)
	}
	m := h.Metrics()
	role := m.Role
	if m.Group {
		role = "standby"
		if m.Running > 0 {
			role = "active"
		}
	}
	say(map[string]interface{}{"event": "started", "role": role})

	var endOnce sync.Once
	end := func(event string) {
		endOnce.Do(func() {
			if event == "stopped" {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				_ = h.Stop(ctx)
				cancel()
			}
			say(map[string]interface{}{"event": event, "metrics": h.Metrics().Map()})
			os.Exit(0)
		})
	}
	go func() {
		<-h.Done()
		end("done")
	}()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	for in.Scan() {
		fields := strings.Fields(strings.TrimSpace(in.Text()))
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "stop":
			end("stopped")
		case "metrics":
			say(map[string]interface{}{"event": "metrics", "metrics": h.Metrics().Map()})
		case "state":
			step := ""
			if len(fields) > 1 {
				step = fields[1]
			}
			key := ""
			if len(fields) > 2 {
				key = strings.Join(fields[2:], " ")
			}
			say(map[string]interface{}{"event": "state", "answer": h.State(step).Get(key)})
		case "list":
			step := ""
			if len(fields) > 1 {
				step = fields[1]
			}
			limit := 100
			if len(fields) > 2 {
				if n, err := strconv.Atoi(fields[2]); err == nil {
					limit = n
				}
			}
			say(map[string]interface{}{"event": "state", "answer": h.State(step).List(limit)})
		}
	}
	end("stopped")
}
