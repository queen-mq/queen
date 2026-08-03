// Package pgmq adapts pgmq (Postgres Message Queue) to the CM-BENCH contract.
//
// pgmq is the most informative comparison in the campaign because it shares
// Queen's substrate: same Postgres, same durability tier, same fsync. Anything
// that differs is architecture, not storage engine — and the architectural
// metrics (write amplification, row churn, backend count) are exact and
// hardware-independent.
//
// Two structural facts drive this adapter:
//
//  1. pgmq HAS per-key ordering. read_grouped_* drains one message per group,
//     so "1000 ordered properties" is expressible. This adapter does not get to
//     pretend otherwise.
//
//  2. pgmq has NO consumer groups. The five terminal groups on a fan-out topic
//     each need their own physical queue, and every derived message is INSERTed
//     once per group — the 1+FanOut publish factor of SPEC.md §2. On top of
//     that pgmq's read+delete cycle costs an UPDATE (visibility timeout) and a
//     DELETE per delivery, so the row churn per delivered message is 3 versions
//     against Queen's 1.
//
// Read function: the default is read_grouped_rr, which takes a per-group
// advisory lock so concurrent consumers get DISJOINT groups. That is what makes
// "a key is never in flight in two handlers at once" actually true. With
// read_grouped_head, consumers reach for the same group heads, and once a head
// is invisible under its visibility timeout the NEXT message of that group can
// be handed to another consumer — which would reorder the key. Do not switch
// the default without re-reading that sentence.
package pgmq

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crossbench/internal/broker"
	"crossbench/internal/workload"
)

// Config tunes the adapter.
type Config struct {
	DSN string

	// ReadFn is the grouped-read function. Keep read_grouped_rr unless you have
	// re-derived the exclusivity argument in the package comment.
	ReadFn string

	// VisibilityTimeoutSec is the lease: how long a read message stays hidden
	// before it redelivers. It plays the role of Queen's lease time.
	VisibilityTimeoutSec int

	// MaxConns bounds the pool. pgmq is a library, not a broker: every
	// concurrent operation is a Postgres backend unless a pooler coalesces
	// them, so this number IS part of the result.
	MaxConns int32

	// Readers is how many concurrent read loops to run per stage.
	Readers int

	// EmptyBackoff is the pause after a read that returned nothing.
	EmptyBackoff time.Duration
}

// DefaultConfig is the campaign baseline.
func DefaultConfig(dsn string) Config {
	return Config{
		DSN:                  dsn,
		ReadFn:               "read_grouped_rr",
		VisibilityTimeoutSec: 60,
		// Matched to Queen's DB_POOL_SIZE=160 on a postgres.conf that allows 400.
		// At 64 the pool was smaller than Queen's while serving 96 readers and
		// 128 shard publishers, so the "pgmq saturates at ~2450 ev/s" ceiling
		// that set the campaign's design point was measured against a pool we
		// imposed rather than against pgmq.
		MaxConns:     160,
		Readers:      8,
		EmptyBackoff: 5 * time.Millisecond,
	}
}

// Broker is the pgmq system under test.
type Broker struct {
	cfg  Config
	pool *pgxpool.Pool
	topo workload.Topology

	// lanes is the number of ordered groups. pgmq groups are free (a header
	// value plus an index, not an object), but the campaign compares systems at
	// MATCHED lane counts, so this must be honoured — an adapter that pins
	// itself to one property per group makes its column incomparable with the
	// systems that were run at 200. Same defect the Queen adapter had.
	lanes int

	queues     atomic.Int64
	reads      atomic.Int64
	rowsRead   atomic.Int64
	deletes    atomic.Int64
	inserts    atomic.Int64
	logicalPub atomic.Int64
	readers    atomic.Int64
	emptyRead  atomic.Int64
}

// New opens the pool and checks the extension is present.
func New(ctx context.Context, cfg Config) (*Broker, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, pool: pool}
	if err := b.preflight(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return b, nil
}

// preflight fails LOUDLY and early if the installed pgmq cannot express the
// workload. A silently-degraded run that produces plausible numbers is far
// worse than a run that refuses to start.
func (b *Broker) preflight(ctx context.Context) error {
	var ext bool
	err := b.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgmq')`).Scan(&ext)
	if err != nil {
		return fmt.Errorf("pgmq preflight: %w", err)
	}
	if !ext {
		return fmt.Errorf("pgmq preflight: extension 'pgmq' is not installed in this database")
	}

	var hasRead bool
	err = b.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		  WHERE n.nspname = 'pgmq' AND p.proname = $1
		)`, b.cfg.ReadFn).Scan(&hasRead)
	if err != nil {
		return fmt.Errorf("pgmq preflight: %w", err)
	}
	if !hasRead {
		return fmt.Errorf(
			"pgmq preflight: pgmq.%s() not found — this pgmq build cannot do per-group FIFO, "+
				"so it cannot express the CM workload's per-property ordering. "+
				"Install a pgmq with grouped reads, or record the run as 'cannot express the workload' (SPEC.md §6.1)",
			b.cfg.ReadFn)
	}
	return nil
}

func (b *Broker) Name() string { return "pgmq" }

// group maps an ordering key onto a pgmq group. Identity when lanes ==
// properties; otherwise properties share a group exactly as they share a Kafka
// partition, with the same head-of-line blocking.
func (b *Broker) group(key string) string {
	if b.lanes >= b.topo.Properties {
		return key
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return "l" + strconv.Itoa(int(h.Sum32()%uint32(b.lanes)))
}

// queueName maps (topic, group) onto a pgmq queue. pgmq queue names become
// table names, so the hyphens in the logical topic names have to go.
func queueName(topic, group string) string {
	s := topic + "_" + group
	return strings.ReplaceAll(s, "-", "_")
}

func (b *Broker) Setup(ctx context.Context, t workload.Topology, o broker.SetupOpts) error {
	b.topo = t
	b.lanes = o.PhysicalLanes
	if b.lanes < 1 || b.lanes > t.Properties {
		b.lanes = t.Properties
	}
	for _, topic := range t.Topics() {
		for _, g := range t.GroupsFor(topic) {
			qn := queueName(topic, g)
			if o.Reset {
				// drop_queue errors when the queue does not exist; that is fine.
				_, _ = b.pool.Exec(ctx, `SELECT pgmq.drop_queue($1::text)`, qn)
			}
			if _, err := b.pool.Exec(ctx, `SELECT pgmq.create($1::text)`, qn); err != nil {
				return fmt.Errorf("pgmq create %s: %w", qn, err)
			}
			// The FIFO index is what makes a grouped head read cheap; without it
			// every read degrades to a scan and we would be benchmarking a
			// missing index rather than pgmq.
			if _, err := b.pool.Exec(ctx, `SELECT pgmq.create_fifo_index($1::text)`, qn); err != nil {
				return fmt.Errorf("pgmq create_fifo_index %s (required for per-group FIFO): %w", qn, err)
			}
			b.queues.Add(1)
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, topic, key string, payload []byte) error {
	return b.PublishBatch(ctx, topic, key, [][]byte{payload})
}

// PublishBatch inserts the slice into every group queue of the topic, in ONE
// round trip. The round trip is single so pgmq is not handicapped on latency;
// the N physical INSERTs are unavoidable and are the measured cost.
func (b *Broker) PublishBatch(ctx context.Context, topic, key string, payloads [][]byte) error {
	groups := b.topo.GroupsFor(topic)
	if len(groups) == 0 {
		return fmt.Errorf("pgmq: no groups for topic %s", topic)
	}

	// $1 is a JSON ARRAY of the payload documents, which is what
	// jsonb_array_elements expands; a text[] of documents would not cast.
	// This mirrors the SQL shape already reviewed in benchmark-queen/pgmq.
	var ja strings.Builder
	ja.WriteByte('[')
	for i, p := range payloads {
		if i > 0 {
			ja.WriteByte(',')
		}
		ja.Write(p)
	}
	ja.WriteByte(']')

	// The group header is what read_grouped_* orders on: it carries the
	// property, so one property is one FIFO lane.
	header := fmt.Sprintf(`{"x-pgmq-group":%q}`, b.group(key))

	// One statement, one CTE per group queue: a single round trip that performs
	// len(groups) batched inserts.
	//
	// EVERY CTE MUST BE REFERENCED BY THE FINAL SELECT. Postgres prunes a CTE
	// that holds only a plain SELECT when nothing reads it, so a trailing
	// "SELECT 1" makes the whole statement succeed while inserting NOTHING —
	// silently, at full speed, with the client counting publishes that never
	// happened. (Measured 2026-08-02: it produced an empty queue table and a
	// spurious PASS.) Summing counts over the CTEs is what forces evaluation.
	var sb strings.Builder
	var tail strings.Builder
	args := []any{ja.String(), header}
	sb.WriteString("WITH ")
	for i, g := range groups {
		if i > 0 {
			sb.WriteString(", ")
			tail.WriteString(" + ")
		}
		args = append(args, queueName(topic, g))
		fmt.Fprintf(&sb, "g%d AS (SELECT pgmq.send_batch($%d::text, "+
			"ARRAY(SELECT jsonb_array_elements($1::jsonb)), "+
			"ARRAY(SELECT $2::jsonb FROM generate_series(1, jsonb_array_length($1::jsonb)))) AS id)",
			i, len(args))
		fmt.Fprintf(&tail, "(SELECT count(*) FROM g%d)", i)
	}
	sb.WriteString(" SELECT ")
	sb.WriteString(tail.String())

	if _, err := b.pool.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("pgmq send_batch: %w", err)
	}
	b.logicalPub.Add(int64(len(payloads)))
	b.inserts.Add(int64(len(payloads) * len(groups)))
	return nil
}

// Consume runs Readers loops over one stage's queue. Each read returns at most
// one message per GROUP, so a returned row set is a set of distinct keys: pgmq
// gives breadth across properties, never depth within one.
func (b *Broker) Consume(ctx context.Context, topic, group string,
	o broker.ConsumeOpts, h broker.Handler) error {

	stats := o.AckStats()
	qn := queueName(topic, group)
	qty := o.BatchSize
	if qty < 1 {
		qty = 100
	}
	readers := b.cfg.Readers
	if readers < 1 {
		readers = 1
	}

	readSQL := fmt.Sprintf(
		`SELECT msg_id, message FROM pgmq.%s($1::text, $2::int, $3::int)`, b.cfg.ReadFn)

	var wg sync.WaitGroup
	errCh := make(chan error, readers)
	for i := 0; i < readers; i++ {
		b.readers.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.readLoop(ctx, qn, readSQL, qty, h, stats); err != nil && ctx.Err() == nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (b *Broker) readLoop(ctx context.Context, qn, readSQL string, qty int,
	h broker.Handler, stats *workload.StageCounters) error {

	for ctx.Err() == nil {
		rows, err := b.pool.Query(ctx, readSQL, qn, b.cfg.VisibilityTimeoutSec, qty)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("pgmq read %s: %w", qn, err)
		}

		type row struct {
			id  int64
			msg []byte
		}
		var got []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.msg); err != nil {
				rows.Close()
				return fmt.Errorf("pgmq scan: %w", err)
			}
			got = append(got, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		b.reads.Add(1)
		if len(got) == 0 {
			b.emptyRead.Add(1)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(b.cfg.EmptyBackoff):
			}
			continue
		}
		b.rowsRead.Add(int64(len(got)))

		// One row per group: build one single-message batch per key.
		byKey := map[string][]broker.Message{}
		idsByKey := map[string][]int64{}
		var order []string
		for _, r := range got {
			st, err := workload.DecodeStamp(r.msg)
			if err != nil {
				continue
			}
			k := workload.PartitionKey(st.Prop)
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], broker.Message{Stamp: st, Payload: r.msg})
			idsByKey[k] = append(idsByKey[k], r.id)
		}

		var kwg sync.WaitGroup
		var okIDs []int64
		var okMu sync.Mutex
		for _, k := range order {
			kwg.Add(1)
			go func(key string, msgs []broker.Message, ids []int64) {
				defer kwg.Done()
				if err := h(ctx, &broker.Batch{Key: key, Msgs: msgs}); err != nil {
					return // no delete: the visibility timeout redelivers it
				}
				okMu.Lock()
				okIDs = append(okIDs, ids...)
				okMu.Unlock()
			}(k, byKey[k], idsByKey[k])
		}
		kwg.Wait()

		if len(okIDs) == 0 {
			continue
		}
		// The ack is a DELETE. Together with the visibility-timeout UPDATE the
		// read performed, that is 2 extra row versions per delivered message on
		// top of the INSERT — the churn number for the cost table.
		if _, err := b.pool.Exec(ctx, `SELECT pgmq.delete($1::text, $2::bigint[])`, qn, okIDs); err != nil {
			stats.AckErr.Add(int64(len(okIDs)))
			if ctx.Err() != nil {
				return nil
			}
			continue
		}
		b.deletes.Add(int64(len(okIDs)))
		stats.Acked.Add(int64(len(okIDs)))
	}
	return nil
}

func (b *Broker) Provisioned() broker.Provisioned {
	return broker.Provisioned{
		// Ordered lanes are groups, which exist per queue: properties x queues.
		OrderedLanes:   b.lanes * int(b.queues.Load()),
		PhysicalQueues: int(b.queues.Load()),
		// pgmq has no consumers, only pollers: every reader is a backend.
		ConsumerMembers:          int(b.readers.Load()),
		Connections:              int(b.pool.Stat().TotalConns()),
		PublishesPerIngressEvent: 1 + float64(workload.FanOut),
		BuiltSemantics: []string{
			"fan-out materialised: no consumer groups, so every derived message is INSERTed once PER GROUP",
			"ack is a DELETE and the read is an UPDATE: 3 row versions per delivered message vs 1 INSERT",
			"no depth batching: a grouped read returns at most ONE message per property, so a property's backlog cannot be drained in one round trip",
			"no broker-side dedup: a dedup window would need an external store",
			"consumption is polling, not push: empty reads cost a query",
		},
	}
}

func (b *Broker) Stats() map[string]any {
	st := b.pool.Stat()
	return map[string]any{
		"pgmq_read_fn":            b.cfg.ReadFn,
		"pgmq_lanes":              b.lanes,
		"pgmq_props_per_lane":     float64(b.topo.Properties) / float64(max(b.lanes, 1)),
		"pgmq_queues":             b.queues.Load(),
		"pgmq_reads":              b.reads.Load(),
		"pgmq_empty_reads":        b.emptyRead.Load(),
		"pgmq_rows_read":          b.rowsRead.Load(),
		"pgmq_deletes":            b.deletes.Load(),
		"pgmq_physical_inserts":   b.inserts.Load(),
		"pgmq_logical_publishes":  b.logicalPub.Load(),
		"pgmq_readers":            b.readers.Load(),
		"pgmq_pool_total_conns":   st.TotalConns(),
		"pgmq_pool_acquired":      st.AcquiredConns(),
		"pgmq_visibility_timeout": b.cfg.VisibilityTimeoutSec,
	}
}

func (b *Broker) Close() error {
	b.pool.Close()
	return nil
}
