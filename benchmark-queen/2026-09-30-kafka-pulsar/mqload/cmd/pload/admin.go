package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"

	"mqload/internal/core"
)

// padmin talks to the Pulsar admin REST API (-admin). The broker redirects
// (307) topic calls to the bundle owner; net/http follows them, bodies
// included (bytes.Reader bodies are replayable).
type padmin struct {
	pc *pulsarFlags
	hc *http.Client
}

func newAdmin(pc *pulsarFlags) *padmin {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 2 * pc.adminConc
	return &padmin{pc: pc, hc: &http.Client{Timeout: 120 * time.Second, Transport: tr}}
}

type httpError struct {
	code int
	body string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.code, e.body) }

// call issues one admin request, retrying network errors and 5xx (bundle
// ownership moves, topic loading) with a short backoff. Returns the status
// and body; non-2xx statuses other than the ones in ok are errors.
func (a *padmin) call(ctx context.Context, method, path string, body any, ok ...int) (int, []byte, error) {
	var bs []byte
	if body != nil {
		var err error
		if bs, err = json.Marshal(body); err != nil {
			return 0, nil, err
		}
	}
	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
			}
		}
		var rd io.Reader
		if bs != nil {
			rd = bytes.NewReader(bs)
		}
		req, err := http.NewRequestWithContext(ctx, method, a.pc.admin+path, rd)
		if err != nil {
			return 0, nil, err
		}
		if bs != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := a.hc.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return 0, nil, err
			}
			continue
		}
		rb, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		code := resp.StatusCode
		if code >= 200 && code < 300 || slices.Contains(ok, code) {
			return code, rb, nil
		}
		lastErr = &httpError{code, string(bytes.TrimSpace(rb))}
		if code < 500 {
			break
		}
	}
	return 0, nil, fmt.Errorf("%s %s: %w", method, path, lastErr)
}

// parallel runs fn(0..n-1) with at most conc in flight; first error wins.
func parallel(ctx context.Context, n, conc int, fn func(ctx context.Context, i int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	// One concrete type in the Value: atomic.Value panics when two workers store
	// errors of different types (10-01, 1M partitions: *httpError beside a
	// context error), which hid the error that mattered.
	var first atomic.Pointer[error]
	var wg sync.WaitGroup
	for w := 0; w < min(conc, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n || ctx.Err() != nil {
					return
				}
				if err := fn(ctx, i); err != nil {
					first.CompareAndSwap(nil, &err)
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	if e := first.Load(); e != nil {
		return *e
	}
	return nil
}

func (a *padmin) topicPath(topic string) string {
	return "/admin/v2/persistent/" + a.pc.tenant + "/" + a.pc.namespace + "/" + topic
}

// setupTopics creates tenant, namespace, topics and the subscription (this
// process with -create), then verifies the subscription on every partition
// before any producer exists. Without -create it waits until the topics and
// the subscription exist.
func (a *padmin) setupTopics(ctx context.Context, run *core.Run, names []string, extraSubs map[string][]string) error {
	cfg, pc := run.Cfg, a.pc
	if pc.subType == "exclusive" && cfg.ConsTotal > cfg.Topics && (cfg.Partitions == 0 || !pc.consumerSlices) {
		return fmt.Errorf("-sub-type exclusive on whole topics allows one consumer per topic, but cons-total %d > topics %d", cfg.ConsTotal, cfg.Topics)
	}
	ctx, cancel := context.WithTimeout(ctx, pc.topicTimeout)
	defer cancel()
	t := time.Now()
	if !cfg.Create {
		return a.waitExisting(ctx, run, names)
	}
	// cluster, tenant, namespace
	_, b, err := a.call(ctx, "GET", "/admin/v2/clusters", nil)
	if err != nil {
		return err
	}
	var clusters []string
	if err := json.Unmarshal(b, &clusters); err != nil || len(clusters) == 0 {
		return fmt.Errorf("clusters: %s", b)
	}
	code, _, err := a.call(ctx, "PUT", "/admin/v2/tenants/"+pc.tenant,
		map[string]any{"adminRoles": []string{}, "allowedClusters": clusters[:1]}, 409)
	if err != nil {
		return err
	}
	fmt.Printf("  [admin] cluster %s, tenant %s %s\n", clusters[0], pc.tenant, map[bool]string{true: "exists", false: "created"}[code == 409])
	nsPath := "/admin/v2/namespaces/" + pc.tenant + "/" + pc.namespace
	code, _, err = a.call(ctx, "PUT", nsPath, map[string]any{"bundles": map[string]any{"numBundles": pc.bundles}}, 409)
	if err != nil {
		return err
	}
	var bundles struct {
		NumBundles int `json:"numBundles"`
	}
	if _, b, err := a.call(ctx, "GET", nsPath+"/bundles", nil); err == nil {
		_ = json.Unmarshal(b, &bundles)
	}
	fmt.Printf("  [admin] namespace %s/%s %s with %d bundles\n", pc.tenant, pc.namespace, map[bool]string{true: "exists", false: "created"}[code == 409], bundles.NumBundles)
	run.SetInfo("cluster", clusters[0])
	run.SetInfo("namespace_bundles", bundles.NumBundles)
	if pc.persistence != "" {
		if _, _, err := a.call(ctx, "POST", nsPath+"/persistence", map[string]any{
			"bookkeeperEnsemble": pc.persist[0], "bookkeeperWriteQuorum": pc.persist[1], "bookkeeperAckQuorum": pc.persist[2],
			"managedLedgerMaxMarkDeleteRate": 0}); err != nil {
			return err
		}
	}
	if _, b, err := a.call(ctx, "GET", nsPath+"/persistence", nil, 404); err == nil {
		v := string(bytes.TrimSpace(b))
		if v == "" || v == "null" {
			v = "(not set: broker managedLedgerDefault* ensemble/write/ack quorum)"
		}
		fmt.Printf("  [admin] namespace persistence: %s\n", v)
		run.SetInfo("namespace_persistence", v)
	}

	// topics
	var created, existed atomic.Int64
	err = parallel(ctx, len(names), pc.adminConc, func(ctx context.Context, i int) error {
		p := a.topicPath(names[i])
		var code int
		var err error
		if cfg.Partitions > 0 {
			code, _, err = a.call(ctx, "PUT", p+"/partitions", cfg.Partitions, 409)
		} else {
			code, _, err = a.call(ctx, "PUT", p, nil, 409)
		}
		if err != nil {
			return err
		}
		if code == 409 {
			existed.Add(1)
			if cfg.Partitions > 0 {
				n, err := a.partitionsOf(ctx, names[i])
				if err != nil {
					return err
				}
				if n != cfg.Partitions {
					return fmt.Errorf("topic %s exists with %d partitions, want %d", names[i], n, cfg.Partitions)
				}
			}
		} else {
			created.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}
	createS := time.Since(t).Seconds()
	fmt.Printf("  [admin] %d topics created, %d existed (%d partitions each) in %.1fs\n", created.Load(), existed.Load(), cfg.Partitions, createS)

	// the subscription, before any producer sends
	if err := a.createSubscription(ctx, run, names, pc.sub); err != nil {
		return err
	}
	for topic, subs := range extraSubs {
		for _, sub := range subs {
			if err := a.createSubscription(ctx, run, []string{topic}, sub); err != nil {
				return err
			}
		}
	}
	run.SetInfo("topic_create_s", createS)
	return nil
}

// createSubscription creates sub (earliest) on every topic of names and verifies it on every partition.
func (a *padmin) createSubscription(ctx context.Context, run *core.Run, names []string, sub string) error {
	cfg, pc := run.Cfg, a.pc
	ts := time.Now()
	earliest := map[string]any{"ledgerId": -1, "entryId": -1, "partitionIndex": -1}
	var fanoutFailed atomic.Int64
	err := parallel(ctx, len(names), pc.adminConc, func(ctx context.Context, i int) error {
		_, _, err := a.call(ctx, "PUT", a.topicPath(names[i])+"/subscription/"+sub, earliest, 409)
		// On a partitioned topic the broker fans this PUT out to every partition inside one admin
		// request; at 50k partitions it answers 500 (10-01). verifySubscription creates the
		// subscription on each partition that missed it, one bounded request at a time, so a
		// failed fan-out only costs time.
		var he *httpError
		if err != nil && cfg.Partitions > 0 && errors.As(err, &he) && (he.code >= 500 || he.code == 412) { // 412: "unable to persist readPosition for cursor reset" at 10k partitions with transactions on (10-01)
			fanoutFailed.Add(1)
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	if n := fanoutFailed.Load(); n > 0 {
		fmt.Printf("  [admin] subscription fan-out failed on %d of %d topics (HTTP 5xx): creating it partition by partition\n", n, len(names))
	}
	subS := time.Since(ts).Seconds()
	tv := time.Now()
	parts, fixed, err := a.verifySubscription(ctx, names, cfg.Partitions, sub)
	if err != nil {
		return err
	}
	verifyS := time.Since(tv).Seconds()
	fmt.Printf("  [admin] subscription %q on %s created in %.1fs and verified on all %d partitions in %.1fs (%d re-created on a partition)\n",
		sub, strings.Join(names, ","), subS, parts, verifyS, fixed)
	key := ""
	if sub != pc.sub {
		key = "_" + sub
	}
	run.SetInfo("subscription_create_s"+key, subS)
	run.SetInfo("subscription_verify_s"+key, verifyS)
	return nil
}

func (a *padmin) partitionsOf(ctx context.Context, topic string) (int, error) {
	_, b, err := a.call(ctx, "GET", a.topicPath(topic)+"/partitions", nil)
	if err != nil {
		return 0, err
	}
	var md struct {
		Partitions int `json:"partitions"`
	}
	if err := json.Unmarshal(b, &md); err != nil {
		return 0, fmt.Errorf("partitions of %s: %s", topic, b)
	}
	return md.Partitions, nil
}

// verifySubscription checks the subscription on every partition (the
// partitioned-topic PUT fans out to the partitions; a partition that missed it
// gets it directly) and returns the partitions checked and re-created.
func (a *padmin) verifySubscription(ctx context.Context, names []string, P int, sub string) (int, int, error) {
	per := max(P, 1)
	var fixed, done atomic.Int64
	// At 100k+ partitions this takes minutes: say how far it got, so a failure
	// shows where it stopped.
	if total := len(names) * per; total >= 100_000 {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					fmt.Printf("  [admin] subscription: %d of %d partitions checked (%d re-created)\n", done.Load(), total, fixed.Load())
				}
			}
		}()
	}
	err := parallel(ctx, len(names)*per, a.pc.adminConc, func(ctx context.Context, i int) error {
		defer done.Add(1)
		topic := names[i/per]
		if P > 0 {
			topic = fmt.Sprintf("%s-partition-%d", topic, i%per)
		}
		p := a.topicPath(topic)
		for attempt := 0; ; attempt++ {
			_, b, err := a.call(ctx, "GET", p+"/subscriptions", nil)
			if err != nil {
				return err
			}
			var subs []string
			if err := json.Unmarshal(b, &subs); err != nil {
				return fmt.Errorf("subscriptions of %s: %s", topic, b)
			}
			if slices.Contains(subs, sub) {
				return nil
			}
			if attempt > 0 {
				return fmt.Errorf("subscription %q missing on %s after re-creating it", sub, topic)
			}
			fixed.Add(1)
			if _, _, err := a.call(ctx, "PUT", p+"/subscription/"+sub, map[string]any{"ledgerId": -1, "entryId": -1, "partitionIndex": -1}, 409); err != nil {
				return err
			}
		}
	})
	return len(names) * per, int(fixed.Load()), err
}

// waitExisting (processes without -create): every topic has the partitions
// and the subscription (checked at topic level) before this process goes on.
func (a *padmin) waitExisting(ctx context.Context, run *core.Run, names []string) error {
	cfg := run.Cfg
	t := time.Now()
	var lastLog time.Time
	for {
		var missing atomic.Int64
		err := parallel(ctx, len(names), a.pc.adminConc, func(ctx context.Context, i int) error {
			if cfg.Partitions > 0 {
				n, err := a.partitionsOf(ctx, names[i])
				if err != nil || n != cfg.Partitions {
					missing.Add(1)
					return nil
				}
			}
			// A partitioned topic's /subscriptions aggregates every partition in one admin request: at
			// 50k partitions it never answers (10-01: the load processes waited 27 min). The -create
			// process verified the subscription on every partition before any of these started, so
			// three partitions (first, middle, last) show the setup is visible from here.
			paths := []string{a.topicPath(names[i])}
			if P := cfg.Partitions; P > 0 {
				paths = paths[:0]
				for _, k := range []int{0, P / 2, P - 1} {
					paths = append(paths, a.topicPath(fmt.Sprintf("%s-partition-%d", names[i], k)))
				}
			}
			for _, p := range paths {
				_, b, err := a.call(ctx, "GET", p+"/subscriptions", nil, 404)
				var subs []string
				if err != nil || json.Unmarshal(b, &subs) != nil || !slices.Contains(subs, a.pc.sub) {
					missing.Add(1)
					return nil
				}
			}
			return nil
		})
		if err == nil && missing.Load() == 0 {
			fmt.Printf("  [admin] %d topics with %d partitions and subscription %q exist (checked in %.1fs)\n", len(names), cfg.Partitions, a.pc.sub, time.Since(t).Seconds())
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("topics/subscription not there within -topic-timeout: %d of %d topics missing", missing.Load(), len(names))
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			fmt.Printf("  [admin] waiting for %d of %d topics (partitions + subscription %q)\n", missing.Load(), len(names), a.pc.sub)
		}
		time.Sleep(time.Second)
	}
}

// warm sends ONE message without "ts" to every partition (explicit router)
// and consumes + acks them on the subscription, so the load meets partitions
// whose ledgers, cursors and producer/consumer registrations exist.
func warm(ctx context.Context, run *core.Run, pc *pulsarFlags, client pulsar.Client, names []string) error {
	cfg := run.Cfg
	ctx, cancel := context.WithTimeout(ctx, pc.topicTimeout)
	defer cancel()
	P := max(cfg.Partitions, 1)
	want := len(names) * P
	t := time.Now()
	val := run.Pool.Warm()
	var sent, failed atomic.Int64
	var first atomic.Value
	var wg sync.WaitGroup
	err := parallel(ctx, len(names), pc.adminConc, func(ctx context.Context, i int) error {
		prod, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic:                   pc.fullName(names[i]),
			MessageRouter:           routeExplicit,
			CompressionType:         pc.compressionV,
			BatchingMaxPublishDelay: pc.batchDelay,
			SendTimeout:             pc.sendTimeout,
		})
		if err != nil {
			return fmt.Errorf("warm producer %s: %v", names[i], err)
		}
		msgs := make([]routedMsg, P)
		var pwg sync.WaitGroup
		for p := range msgs {
			m := &msgs[p]
			m.magic, m.part = routedMagic, p
			m.Payload = val
			pwg.Add(1)
			wg.Add(1)
			prod.SendAsync(ctx, &m.ProducerMessage, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
					failed.Add(1)
					first.CompareAndSwap(nil, err.Error())
				} else {
					sent.Add(1)
				}
				pwg.Done()
				wg.Done()
			})
		}
		pwg.Wait()
		prod.Close()
		return nil
	})
	wg.Wait()
	if err != nil {
		return err
	}
	if failed.Load() > 0 {
		return fmt.Errorf("warm: %d of %d sends failed, first: %v", failed.Load(), want, first.Load())
	}
	sendS := time.Since(t).Seconds()
	tc := time.Now()
	topics := make([]string, len(names))
	for i, n := range names {
		topics[i] = pc.fullName(n)
	}
	opts := pulsar.ConsumerOptions{SubscriptionName: pc.sub, Type: pc.subTypeV, Name: "pload-warm",
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest, ReceiverQueueSize: pc.receiverQueue}
	if len(topics) == 1 {
		opts.Topic = topics[0]
	} else {
		opts.Topics = topics
	}
	cons, err := client.Subscribe(opts)
	if err != nil {
		return fmt.Errorf("warm consumer: %v", err)
	}
	got, other := 0, 0
	for got < want {
		select {
		case <-ctx.Done():
			cons.Close()
			return fmt.Errorf("warm: consumed %d of %d warm-up messages within -topic-timeout", got, want)
		case cm := <-cons.Chan():
			if _, _, isLoad := core.ParseStamp(cm.Payload()); isLoad {
				other++
			} else {
				got++
			}
			if err := cons.Ack(cm.Message); err != nil {
				return fmt.Errorf("warm ack: %v", err)
			}
		}
	}
	cons.Close() // flushes the grouped acks
	consS := time.Since(tc).Seconds()
	fmt.Printf("  [warm] one message (no \"ts\") into each of %d partitions in %.1fs; consumed and acked in %.1fs%s\n",
		want, sendS, consS, map[bool]string{true: fmt.Sprintf(" (+%d load messages left from earlier)", other), false: ""}[other > 0])
	run.SetInfo("warm_send_s", sendS)
	run.SetInfo("warm_consume_s", consS)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	return nil
}
