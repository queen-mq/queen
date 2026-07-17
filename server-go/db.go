package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionLookup encapsulates the PUSHPOPLOOKUPSOL machinery that
// pop_unified_batch_v4 depends on. Faithful to libqueen's design: a COALESCING
// buffer (latest-wins per partition) flushed on a short tick as ONE combined
// update_partition_lookup_v1 call — NOT one call per push batch (that floods a
// single updater, lets partition_lookup go stale, and starves wildcard pop).
// A periodic reconcile is the safety net (mirrors PartitionLookupReconcileService).
type PartitionLookup struct {
	pool   *pgxpool.Pool
	stmtTO time.Duration
	flush  time.Duration

	mu      sync.Mutex
	pending map[string]puEntry // key: partition_id, value: latest update
}

type puEntry struct {
	QueueName     string `json:"queue_name"`
	PartitionID   string `json:"partition_id"`
	LastMessageID string `json:"last_message_id"`
	LastCreatedAt string `json:"last_message_created_at"`
}

func NewPartitionLookup(pool *pgxpool.Pool, stmtTO, flush time.Duration) *PartitionLookup {
	if flush <= 0 {
		flush = 100 * time.Millisecond
	}
	pl := &PartitionLookup{
		pool:    pool,
		stmtTO:  stmtTO,
		flush:   flush,
		pending: make(map[string]puEntry, 4096),
	}
	go pl.loop()
	return pl
}

// Fire is called from the push batcher with push_messages_v3's partition_updates
// array. It merges into the coalescing buffer (latest created_at wins per
// partition) — cheap, no PG call on the hot path.
func (pl *PartitionLookup) Fire(pu json.RawMessage) {
	var entries []puEntry
	if err := json.Unmarshal(pu, &entries); err != nil || len(entries) == 0 {
		return
	}
	pl.mu.Lock()
	for _, e := range entries {
		if e.PartitionID == "" {
			continue
		}
		if cur, ok := pl.pending[e.PartitionID]; !ok || e.LastCreatedAt >= cur.LastCreatedAt {
			pl.pending[e.PartitionID] = e
		}
	}
	pl.mu.Unlock()
}

func (pl *PartitionLookup) loop() {
	t := time.NewTicker(pl.flush)
	defer t.Stop()
	for range t.C {
		pl.mu.Lock()
		if len(pl.pending) == 0 {
			pl.mu.Unlock()
			continue
		}
		batch := make([]puEntry, 0, len(pl.pending))
		for _, e := range pl.pending {
			batch = append(batch, e)
		}
		pl.pending = make(map[string]puEntry, 4096)
		pl.mu.Unlock()

		payload, err := json.Marshal(batch)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), pl.stmtTO)
		_, err = pl.pool.Exec(ctx, "SELECT queen.update_partition_lookup_v1($1::jsonb)", string(payload))
		cancel()
		if err != nil {
			log.Printf("[partition_lookup] update failed (%d partitions): %v", len(batch), err)
		}
	}
}

// StartReconcile runs the periodic partition_lookup reconcile safety net.
func StartReconcile(pool *pgxpool.Pool, interval time.Duration, lookback int, stmtTO time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for range t.C {
			ctx, cancel := context.WithTimeout(context.Background(), stmtTO)
			_, err := pool.Exec(ctx, "SELECT queen.reconcile_partition_lookup_v1($1)", lookback)
			cancel()
			if err != nil {
				log.Printf("[partition_lookup] reconcile failed: %v", err)
			}
		}
	}()
}

// NewPool builds a dedicated pgx pool with a fixed connection count. Each op
// type gets its own so they never contend for connections (per-function engine
// isolation, à la Queen 0.16).
func NewPool(ctx context.Context, connString string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	if maxConns < 1 {
		maxConns = 1
	}
	cfg.MaxConns = int32(maxConns)
	cfg.MinConns = int32(maxConns)
	return pgxpool.NewWithConfig(ctx, cfg)
}
