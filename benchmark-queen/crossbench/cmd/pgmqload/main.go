// pgmqload — raw throughput loader for pgmq, shaped like goload's openloop mode.
//
// The channel-manager harness (cmbench) measures a 12-stage topology with a
// fan-out of 6 deliveries per ingress event. That is the wrong shape for asking
// "what raw message rate does pgmq sustain". This tool asks that question
// directly, with the same semantics the Queen soak uses: open-loop push at a
// target rate, explicit consume, explicit ack.
//
// Mapping onto pgmq:
//
//	push  -> pgmq.send_batch(queue, msgs[], headers[])   (group in the header)
//	pop   -> pgmq.read_grouped_head(queue, vt, qty)      (<=1 msg per group)
//	ack   -> pgmq.delete(queue, msg_ids[])               (delete IS the ack)
//
// Open-loop: the pacer pushes at -rate regardless of whether the readers keep
// up. A rate is "sustained" when pop/s matches push/s and lag stays flat.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type stats struct {
	pushed, popped, deleted atomic.Int64
	pushErr, popErr, ackErr atomic.Int64
	mu                      sync.Mutex
	lat                     []float64 // ms, sampled
}

func (s *stats) addLat(ms float64) {
	s.mu.Lock()
	if len(s.lat) < 200000 {
		s.lat = append(s.lat, ms)
	}
	s.mu.Unlock()
}

func (s *stats) drainLat() []float64 {
	s.mu.Lock()
	v := s.lat
	s.lat = nil
	s.mu.Unlock()
	sort.Float64s(v)
	return v
}

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	i := int(float64(len(v)) * p)
	if i >= len(v) {
		i = len(v) - 1
	}
	return v[i]
}

func main() {
	dsn := flag.String("dsn", "postgres://postgres:postgres@127.0.0.1:5432/postgres", "postgres DSN")
	queue := flag.String("queue", "soakq", "pgmq queue name")
	groups := flag.Int("groups", 200, "distinct ordering groups (the pgmq analogue of partitions)")
	rate := flag.Int("rate", 100000, "target push rate, msg/s (open loop)")
	pushBatch := flag.Int("push-batch", 100, "messages per send_batch call")
	pushers := flag.Int("pushers", 32, "concurrent push goroutines")
	readers := flag.Int("readers", 200, "concurrent reader goroutines")
	readQty := flag.Int("read-qty", 500, "qty passed to read_grouped_head")
	vt := flag.Int("vt", 30, "visibility timeout seconds")
	payload := flag.Int("payload", 256, "payload bytes")
	duration := flag.Int("duration", 120, "seconds")
	report := flag.Int("report", 10, "report interval seconds")
	maxConns := flag.Int("max-conns", 160, "pool size")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dsn:", err)
		os.Exit(1)
	}
	cfg.MaxConns = int32(*maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Fresh queue + the FIFO index the grouped read expects.
	_, _ = pool.Exec(ctx, `SELECT pgmq.drop_queue($1)`, *queue)
	if _, err := pool.Exec(ctx, `SELECT pgmq.create($1)`, *queue); err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	if _, err := pool.Exec(ctx, `SELECT pgmq.create_fifo_index($1)`, *queue); err != nil {
		fmt.Fprintln(os.Stderr, "fifo index:", err)
		os.Exit(1)
	}

	fmt.Printf("pgmqload: queue=%s groups=%d rate=%d/s push-batch=%d pushers=%d readers=%d read-qty=%d payload=%dB dur=%ds\n",
		*queue, *groups, *rate, *pushBatch, *pushers, *readers, *readQty, *payload, *duration)

	st := &stats{}
	blob := make([]byte, *payload)
	for i := range blob {
		blob[i] = 'x'
	}
	blobStr := string(blob)

	var wg sync.WaitGroup
	deadline := time.Now().Add(time.Duration(*duration) * time.Second)

	// ---- pushers: open-loop pacer -----------------------------------------
	batchesPerSec := float64(*rate) / float64(*pushBatch)
	perPusher := batchesPerSec / float64(*pushers)
	interval := time.Duration(float64(time.Second) / perPusher)
	for p := 0; p < *pushers; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)*7919 + 13))
			tick := time.NewTicker(interval)
			defer tick.Stop()
			msgs := make([]string, *pushBatch)
			hdrs := make([]string, *pushBatch)
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if time.Now().After(deadline) {
						return
					}
					now := time.Now().UnixMicro()
					for i := 0; i < *pushBatch; i++ {
						g := rng.Intn(*groups)
						m, _ := json.Marshal(map[string]any{"t": now, "b": blobStr})
						msgs[i] = string(m)
						hdrs[i] = fmt.Sprintf(`{"x-pgmq-group":"g%d"}`, g)
					}
					_, err := pool.Exec(ctx,
						`SELECT pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[])`,
						*queue, msgs, hdrs)
					if err != nil {
						st.pushErr.Add(1)
						continue
					}
					st.pushed.Add(int64(*pushBatch))
				}
			}
		}(p)
	}

	// ---- readers: read_grouped_head -> delete ------------------------------
	for r := 0; r < *readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil || time.Now().After(deadline) {
					return
				}
				rows, err := pool.Query(ctx,
					`SELECT msg_id, message FROM pgmq.read_grouped_head($1::text, $2::int, $3::int)`,
					*queue, *vt, *readQty)
				if err != nil {
					st.popErr.Add(1)
					time.Sleep(2 * time.Millisecond)
					continue
				}
				var ids []int64
				var lats []float64
				for rows.Next() {
					var id int64
					var msg []byte
					if err := rows.Scan(&id, &msg); err != nil {
						continue
					}
					ids = append(ids, id)
					var m struct {
						T int64 `json:"t"`
					}
					if json.Unmarshal(msg, &m) == nil && m.T > 0 {
						lats = append(lats, float64(time.Now().UnixMicro()-m.T)/1000.0)
					}
				}
				rows.Close()
				if len(ids) == 0 {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				st.popped.Add(int64(len(ids)))
				for _, l := range lats {
					st.addLat(l)
				}
				if _, err := pool.Exec(ctx, `SELECT pgmq.delete($1::text, $2::bigint[])`, *queue, ids); err != nil {
					st.ackErr.Add(1)
					continue
				}
				st.deleted.Add(int64(len(ids)))
			}
		}()
	}

	// ---- reporter ---------------------------------------------------------
	go func() {
		t := time.NewTicker(time.Duration(*report) * time.Second)
		defer t.Stop()
		var lastPush, lastPop, lastDel int64
		last := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				now := time.Now()
				dt := now.Sub(last).Seconds()
				last = now
				pu, po, de := st.pushed.Load(), st.popped.Load(), st.deleted.Load()
				lat := st.drainLat()
				var depth int64
				_ = pool.QueryRow(ctx,
					fmt.Sprintf(`SELECT count(*) FROM pgmq.q_%s`, *queue)).Scan(&depth)
				fmt.Printf("[%s] push=%8.0f/s pop=%8.0f/s ack=%8.0f/s | depth=%9d | p50=%8.2f p99=%9.2f ms | errs push=%d pop=%d ack=%d\n",
					now.Format("15:04:05"),
					float64(pu-lastPush)/dt, float64(po-lastPop)/dt, float64(de-lastDel)/dt,
					depth, pct(lat, 0.50), pct(lat, 0.99),
					st.pushErr.Load(), st.popErr.Load(), st.ackErr.Load())
				lastPush, lastPop, lastDel = pu, po, de
			}
		}
	}()

	wg.Wait()
	cancel()
	var depth int64
	c2, cc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cc()
	_ = pool.QueryRow(c2, fmt.Sprintf(`SELECT count(*) FROM pgmq.q_%s`, *queue)).Scan(&depth)
	fmt.Printf("[final] pushed=%d popped=%d acked=%d depth=%d | errs push=%d pop=%d ack=%d\n",
		st.pushed.Load(), st.popped.Load(), st.deleted.Load(), depth,
		st.pushErr.Load(), st.popErr.Load(), st.ackErr.Load())
}
