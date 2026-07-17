package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// TypePolicy mirrors libqueen's per-type BatchPolicy (lib/queen/batch_policy.hpp).
// Sizes are counted in JOBS (HTTP requests), exactly like should_fire()'s
// queue_size — NOT in messages. Defaults are the validated benchmark values
// (see benchmark-queen soak start-broker.sh / queen-inspect.json).
type TypePolicy struct {
	Preferred     int           // fire when >= this many jobs are queued
	MaxBatch      int           // hard cap on jobs coalesced into one SP call
	MaxHold       time.Duration // otherwise fire when oldest job waited this long
	MaxConcurrent int           // ceiling on in-flight batches (gated by pool too)
}

type Config struct {
	Port string

	PGConnString string
	DBPoolSize   int

	Push TypePolicy
	Pop  TypePolicy
	Ack  TypePolicy

	PushMaxPartitionsPerBatch int

	// Unified engine: shared PG concurrency budget across push/pop/ack, and
	// per-lane fair-share weights (fire permits per round-robin turn).
	GlobalConcurrency int // total shared PG concurrency budget across all shards
	EngineShards      int // parallel scheduler goroutines (à la NUM_WORKERS)
	PipelineDepth     int // SP calls pipelined per connection (pgx SendBatch)
	PushWeight        int
	PopWeight         int
	AckWeight         int

	StmtTimeout time.Duration

	// Long-poll pop (wait=true): park an empty pop server-side and re-check on a
	// backoff cadence instead of returning empty immediately. Mirrors the C++
	// POP_WAIT_* knobs (soak defaults).
	PopWaitInitialMs int
	PopWaitThreshold int
	PopWaitMultipl   int
	PopWaitMaxMs     int
	PopDefaultTimeoutMs int

	// PUSHPOPLOOKUPSOL: coalescing-buffer flush interval for the per-push
	// partition_lookup follow-up (latest-wins per partition), plus the periodic
	// reconcile safety net mirroring the C++ PartitionLookupReconcileService.
	PartitionFlush    time.Duration
	ReconcileInterval time.Duration
	ReconcileLookback int
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func LoadConfig() Config {
	host := envStr("PG_HOST", "localhost")
	port := envStr("PG_PORT", "5432")
	user := envStr("PG_USER", "postgres")
	pass := envStr("PG_PASSWORD", "postgres")
	db := envStr("PG_DATABASE", "postgres")

	poolSize := envInt("DB_POOL_SIZE", 50)

	// Base DSN WITHOUT a pool size — each op type (push/pop/ack) gets its OWN
	// pgx pool so a hot push path can never starve pop of connections. This
	// mirrors Queen 0.16's "three shared per-function engine instances", each
	// with dedicated DB connections. Pool sizes are set per-batcher from their
	// MaxConcurrent (see main.go). DB_POOL_SIZE is kept only as a legacy hint.
	conn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, pass, db,
	)
	_ = poolSize

	// Defaults from lib/queen/batch_policy.hpp default_batch_policy_for(), which
	// are exactly the env values captured in the soak (queen-inspect.json):
	//   PUSH {preferred 50, hold 20ms, maxbatch 500, concurrent 24}
	//   POP  {preferred 20, hold  5ms, maxbatch 500, concurrent 16}
	//   ACK  {preferred 50, hold 20ms, maxbatch 500, concurrent 16}
	push := TypePolicy{
		Preferred:     envInt("QUEEN_PUSH_PREFERRED_BATCH_SIZE", 50),
		MaxHold:       time.Duration(envInt("QUEEN_PUSH_MAX_HOLD_MS", 20)) * time.Millisecond,
		MaxBatch:      envInt("QUEEN_PUSH_MAX_BATCH_SIZE", 500),
		MaxConcurrent: envInt("QUEEN_PUSH_MAX_CONCURRENT", 24),
	}
	pop := TypePolicy{
		Preferred:     envInt("QUEEN_POP_PREFERRED_BATCH_SIZE", 20),
		MaxHold:       time.Duration(envInt("QUEEN_POP_MAX_HOLD_MS", 5)) * time.Millisecond,
		MaxBatch:      envInt("QUEEN_POP_MAX_BATCH_SIZE", 500),
		MaxConcurrent: envInt("QUEEN_POP_MAX_CONCURRENT", 16),
	}
	ack := TypePolicy{
		Preferred:     envInt("QUEEN_ACK_PREFERRED_BATCH_SIZE", 50),
		MaxHold:       time.Duration(envInt("QUEEN_ACK_MAX_HOLD_MS", 20)) * time.Millisecond,
		MaxBatch:      envInt("QUEEN_ACK_MAX_BATCH_SIZE", 500),
		MaxConcurrent: envInt("QUEEN_ACK_MAX_CONCURRENT", 16),
	}

	return Config{
		Port:              envStr("PORT", "6632"),
		PGConnString:      conn,
		DBPoolSize:        poolSize,
		Push:                      push,
		Pop:                       pop,
		Ack:                       ack,
		PushMaxPartitionsPerBatch: envInt("QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH", 8),
		GlobalConcurrency:         envInt("QUEEN_GLOBAL_CONCURRENCY", 48),
		EngineShards:              envInt("QUEEN_ENGINE_SHARDS", 8),
		PipelineDepth:             envInt("QUEEN_PIPELINE_DEPTH", 1),
		PushWeight:                envInt("QUEEN_PUSH_WEIGHT", 1),
		PopWeight:                 envInt("QUEEN_POP_WEIGHT", 1),
		AckWeight:                 envInt("QUEEN_ACK_WEIGHT", 1),
		StmtTimeout:         time.Duration(envInt("QUEEN_STMT_TIMEOUT_MS", 30000)) * time.Millisecond,
		PopWaitInitialMs:    envInt("POP_WAIT_INITIAL_INTERVAL_MS", 10),
		PopWaitThreshold:    envInt("POP_WAIT_BACKOFF_THRESHOLD", 5),
		PopWaitMultipl:      envInt("POP_WAIT_BACKOFF_MULTIPLIER", 2),
		PopWaitMaxMs:        envInt("POP_WAIT_MAX_INTERVAL_MS", 100),
		PopDefaultTimeoutMs: envInt("POP_DEFAULT_TIMEOUT_MS", 2000),
		PartitionFlush:      time.Duration(envInt("PARTITION_LOOKUP_FLUSH_MS", 100)) * time.Millisecond,
		ReconcileInterval: time.Duration(envInt("RECONCILE_INTERVAL_MS", 2000)) * time.Millisecond,
		ReconcileLookback: envInt("RECONCILE_LOOKBACK_SECONDS", 30),
	}
}
