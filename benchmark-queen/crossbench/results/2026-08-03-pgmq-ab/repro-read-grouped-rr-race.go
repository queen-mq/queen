// pgmqrepro: adversarial repro of the read_grouped_rr per-group exclusivity
// hole, mirroring the crossbench pgmq adapter's readLoop shape exactly
// (grouped read -> per-key concurrent handlers -> single batched delete).
//
// Detection is exact, not statistical: a global in-flight table records every
// (group, seq) between "read returned it" and "delete committed". A HANDOFF
// VIOLATION is a read that returns seq S of group G while some seq < S of the
// same G is in flight on a DIFFERENT read call. That is precisely the contract
// read_grouped_rr claims to enforce via the head-visibility gate + advisory
// lock, and precisely what the campaign verifier sees as an order violation
// when the two handlers' recordings invert.
//
// Usage: pgmqrepro -fn read_grouped_rr|read_grouped_head -dur 60s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type inflightEntry struct {
	reader int
	readID int64
	seq    int64
	at     time.Time
}

type tracker struct {
	mu       sync.Mutex
	inflight map[string]map[int64]inflightEntry // group -> seq -> holder
	maxFirst map[string]int64                   // verifier-style first-occurrence high-water
	seen     map[string]map[int64]bool
	handoff  int64
	verViol  int64
	dups     int64
	events   []string
}

func newTracker() *tracker {
	return &tracker{
		inflight: map[string]map[int64]inflightEntry{},
		maxFirst: map[string]int64{},
		seen:     map[string]map[int64]bool{},
	}
}

// onRead registers a delivery and checks both contracts.
func (t *tracker) onRead(g string, seq int64, reader int, readID int64, readCt int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// smoking gun: some EARLIER seq of this group is in flight on another read
	if m := t.inflight[g]; m != nil {
		for s, e := range m {
			if s < seq && e.readID != readID {
				t.handoff++
				if len(t.events) < 20 {
					t.events = append(t.events, fmt.Sprintf(
						"HANDOFF: reader %d read %s seq %d (read_ct=%d) while seq %d in flight on reader %d for %s",
						reader, g, seq, readCt, s, e.reader, time.Since(e.at).Round(time.Millisecond)))
				}
			}
		}
	}

	// verifier-style: first occurrence of a lower seq after a higher one
	if t.seen[g] == nil {
		t.seen[g] = map[int64]bool{}
	}
	if t.seen[g][seq] {
		t.dups++
	} else {
		t.seen[g][seq] = true
		if seq < t.maxFirst[g] {
			t.verViol++
		}
		if seq > t.maxFirst[g] {
			t.maxFirst[g] = seq
		}
	}

	if t.inflight[g] == nil {
		t.inflight[g] = map[int64]inflightEntry{}
	}
	t.inflight[g][seq] = inflightEntry{reader: reader, readID: readID, seq: seq, at: time.Now()}
}

func (t *tracker) onDeleted(g string, seq int64) {
	t.mu.Lock()
	delete(t.inflight[g], seq)
	t.mu.Unlock()
}

type payload struct {
	G   string `json:"g"`
	Seq int64  `json:"seq"`
}

func main() {
	fn := flag.String("fn", "read_grouped_rr", "grouped read function under test")
	dur := flag.Duration("dur", 60*time.Second, "measured duration")
	groups := flag.Int("groups", 100, "FIFO groups")
	rate := flag.Int("rate", 2000, "aggregate inserts/s across groups")
	readers := flag.Int("readers", 8, "concurrent read loops")
	qty := flag.Int("qty", 100, "read batch size")
	vt := flag.Int("vt", 30, "visibility timeout seconds")
	workMs := flag.Int("work", 3, "simulated per-key handler work ms")
	seed := flag.Int("seed", 0, "invisible junk rows to pre-load: fifo_groups scans them regardless of visibility, so they widen the statement's snapshot-to-eligibility window without ever being delivered")
	dsn := flag.String("dsn", "postgres://postgres:postgres@localhost:5462/postgres", "DSN")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pc, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		panic(err)
	}
	pc.MaxConns = 60
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	const q = "repro_q"
	pool.Exec(ctx, `SELECT pgmq.drop_queue($1::text)`, q)
	if _, err := pool.Exec(ctx, `SELECT pgmq.create($1::text)`, q); err != nil {
		panic(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pgmq.create_fifo_index($1::text)`, q); err != nil {
		panic(err)
	}

	if *seed > 0 {
		if _, err := pool.Exec(ctx, fmt.Sprintf(`
			SELECT pgmq.send_batch('%s',
				ARRAY(SELECT '{"junk":true}'::jsonb FROM generate_series(1, %d)),
				ARRAY(SELECT format('{"x-pgmq-group":"junk%%s"}', g %% 500)::jsonb FROM generate_series(1, %d) g))`,
			q, *seed, *seed)); err != nil {
			panic(err)
		}
		// park the junk out of visibility: it stays in the table (and in every
		// fifo_groups scan) but its group heads are never eligible
		if _, err := pool.Exec(ctx,
			`UPDATE pgmq.q_repro_q SET vt = clock_timestamp() + interval '2 hours' WHERE message ? 'junk'`); err != nil {
			panic(err)
		}
		fmt.Printf("seeded %d invisible junk rows across 500 junk groups\n", *seed)
	}

	tr := newTracker()
	var inserted, delivered, reads, deletes atomic.Int64

	// producers: one goroutine per group => per-group insert order is structural
	var pwg sync.WaitGroup
	perGroupInterval := time.Duration(float64(time.Second) * float64(*groups) / float64(*rate))
	for gi := 0; gi < *groups; gi++ {
		pwg.Add(1)
		go func(gi int) {
			defer pwg.Done()
			g := fmt.Sprintf("g%03d", gi)
			hdr := fmt.Sprintf(`{"x-pgmq-group":%q}`, g)
			var seq int64
			for ctx.Err() == nil {
				seq++
				msg := fmt.Sprintf(`{"g":%q,"seq":%d}`, g, seq)
				if _, err := pool.Exec(ctx,
					`SELECT pgmq.send($1::text, $2::jsonb, $3::jsonb, 0)`, q, msg, hdr); err != nil {
					if ctx.Err() != nil {
						return
					}
					seq-- // retry same seq, order preserved (serial in this goroutine)
					time.Sleep(2 * time.Millisecond)
					continue
				}
				inserted.Add(1)
				time.Sleep(perGroupInterval + time.Duration(rand.IntN(3))*time.Millisecond)
			}
		}(gi)
	}

	// readers: faithful mirror of the crossbench adapter readLoop
	readSQL := fmt.Sprintf(`SELECT msg_id, read_ct, message FROM pgmq.%s($1::text, $2::int, $3::int)`, *fn)
	var readID atomic.Int64
	var rwg sync.WaitGroup
	for r := 0; r < *readers; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			for ctx.Err() == nil {
				rows, err := pool.Query(ctx, readSQL, q, *vt, *qty)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(2 * time.Millisecond)
					continue
				}
				type row struct {
					id     int64
					readCt int
					g      string
					seq    int64
				}
				var got []row
				for rows.Next() {
					var id int64
					var ct int
					var raw []byte
					if err := rows.Scan(&id, &ct, &raw); err != nil {
						break
					}
					var p payload
					if json.Unmarshal(raw, &p) != nil {
						continue
					}
					got = append(got, row{id: id, readCt: ct, g: p.G, seq: p.Seq})
				}
				rows.Close()
				reads.Add(1)
				if len(got) == 0 {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				rid := readID.Add(1)
				for _, x := range got {
					tr.onRead(x.g, x.seq, r, rid, x.readCt)
				}
				delivered.Add(int64(len(got)))

				// per-key concurrent handlers, like the adapter
				byKey := map[string][]row{}
				for _, x := range got {
					byKey[x.g] = append(byKey[x.g], x)
				}
				var kwg sync.WaitGroup
				for _, xs := range byKey {
					kwg.Add(1)
					go func(xs []row) {
						defer kwg.Done()
						time.Sleep(time.Duration(*workMs) * time.Millisecond)
					}(xs)
				}
				kwg.Wait()

				ids := make([]int64, 0, len(got))
				for _, x := range got {
					ids = append(ids, x.id)
				}
				if _, err := pool.Exec(ctx, `SELECT pgmq.delete($1::text, $2::bigint[])`, q, ids); err == nil {
					deletes.Add(int64(len(ids)))
					for _, x := range got {
						tr.onDeleted(x.g, x.seq)
					}
				}
			}
		}(r)
	}

	fmt.Printf("pgmqrepro fn=%s groups=%d rate=%d readers=%d qty=%d vt=%ds work=%dms dur=%s\n",
		*fn, *groups, *rate, *readers, *qty, *vt, *workMs, *dur)
	deadline := time.After(*dur)
	tick := time.NewTicker(10 * time.Second)
	for done := false; !done; {
		select {
		case <-tick.C:
			tr.mu.Lock()
			h, v, d := tr.handoff, tr.verViol, tr.dups
			tr.mu.Unlock()
			fmt.Printf("  t+ ins=%d del=%d reads=%d handoff=%d verViol=%d dups=%d\n",
				inserted.Load(), delivered.Load(), reads.Load(), h, v, d)
		case <-deadline:
			done = true
		}
	}
	cancel()
	pwg.Wait()
	rwg.Wait()

	tr.mu.Lock()
	defer tr.mu.Unlock()
	fmt.Printf("\n=== RESULT fn=%s ===\n", *fn)
	fmt.Printf("inserted=%d delivered=%d reads=%d deleted=%d\n",
		inserted.Load(), delivered.Load(), reads.Load(), deletes.Load())
	fmt.Printf("HANDOFF violations (later seq served while earlier in flight elsewhere): %d\n", tr.handoff)
	fmt.Printf("verifier-style first-occurrence order violations: %d\n", tr.verViol)
	fmt.Printf("dups: %d\n", tr.dups)
	sort.Strings(tr.events)
	for _, e := range tr.events {
		fmt.Println("  " + e)
	}
	if tr.handoff > 0 || tr.verViol > 0 {
		os.Exit(3)
	}
}
