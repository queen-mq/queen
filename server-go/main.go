// go-hotpath-spike — a Go reimplementation of Queen MQ's hot path (push / pop /
// ack) that keeps the PostgreSQL stored procedures AS-IS and replicates
// libqueen's batch-fusion + tuning (see config.go). Purpose: a like-for-like
// throughput comparison against the C++ broker using the same goload loader and
// the same SQL. NOT production-complete (no auth/encryption/DLQ-UI/streams/
// retention-enforcement — retention is left to Postgres/ops for the spike).
package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := LoadConfig()

	ctx := context.Background()

	// Per-shard DB pools (like Queen's per-worker connections): each engine shard
	// owns its own connections so shards never contend on a shared pool mutex,
	// and each can independently keep many queries active at Postgres. Plus a
	// small aux pool for partition_lookup/reconcile/configure.
	nShards := cfg.EngineShards
	if nShards < 1 {
		nShards = 1
	}
	perShardBudget := cfg.GlobalConcurrency / nShards
	if perShardBudget < 1 {
		perShardBudget = 1
	}
	enginePools := make([]*pgxpool.Pool, nShards)
	for i := 0; i < nShards; i++ {
		p, perr := NewPool(ctx, cfg.PGConnString, perShardBudget+2)
		if perr != nil {
			log.Fatalf("engine pool %d init failed: %v", i, perr)
		}
		enginePools[i] = p
		defer p.Close()
	}
	auxPool, err := NewPool(ctx, cfg.PGConnString, 8)
	if err != nil {
		log.Fatalf("aux pool init failed: %v", err)
	}
	defer auxPool.Close()

	// Wait for Postgres to accept connections (docker-compose startup race).
	if err := waitForPG(auxPool, 60*time.Second); err != nil {
		log.Fatalf("postgres not ready: %v", err)
	}

	pl := NewPartitionLookup(auxPool, cfg.StmtTimeout, cfg.PartitionFlush)
	StartReconcile(auxPool, cfg.ReconcileInterval, cfg.ReconcileLookback, cfg.StmtTimeout)

	metrics := NewMetrics()
	specs := []laneSpec{
		lanePush: {
			name: "push", sql: "SELECT queen.push_messages_v3($1::jsonb)",
			policy: cfg.Push, weight: cfg.PushWeight, metrics: metrics.Push,
			gate: true, maxParts: cfg.PushMaxPartitionsPerBatch, pushObject: true, onPU: pl.Fire,
		},
		lanePop: {
			name: "pop", sql: "SELECT queen.pop_unified_batch_v4($1::jsonb)",
			policy: cfg.Pop, weight: cfg.PopWeight, metrics: metrics.Pop,
		},
		laneAck: {
			name: "ack", sql: "SELECT queen.ack_messages_v2($1::jsonb)",
			policy: cfg.Ack, weight: cfg.AckWeight, metrics: metrics.Ack,
		},
	}
	shards := nShards
	perShard := perShardBudget
	engine := NewEngines(shards, enginePools, cfg.StmtTimeout, perShard, cfg.PipelineDepth, specs)

	auth := NewAuth(os.Getenv("QUEEN_JWT_SECRET"))
	crypto := NewCrypto(os.Getenv("QUEEN_ENCRYPTION_KEY"))
	if auth != nil {
		log.Println("[go-hotpath] auth enabled (per-request JWT HS256 verify)")
	}
	if crypto != nil {
		log.Println("[go-hotpath] encryption enabled (AES-256-GCM per message)")
	}
	s := &Server{cfg: cfg, pool: auxPool, engine: engine, metrics: metrics, notifier: NewNotifier(), auth: auth, crypto: crypto}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/push", s.handlePush)
	mux.HandleFunc("GET /api/v1/pop/queue/{queue}", s.handlePop)
	mux.HandleFunc("GET /api/v1/pop/queue/{queue}/partition/{partition}", s.handlePop)
	mux.HandleFunc("POST /api/v1/ack", s.handleAckSingle)
	mux.HandleFunc("POST /api/v1/ack/batch", s.handleAckBatch)
	mux.HandleFunc("POST /api/v1/configure", s.handleConfigure)
	mux.HandleFunc("GET /api/v1/status", s.handleStatus)
	mux.HandleFunc("GET /metrics/prometheus", s.handlePrometheus)

	// pprof on :6060 for goroutine/CPU profiling during investigation.
	if os.Getenv("QUEEN_PPROF") != "" {
		go func() { log.Println(http.ListenAndServe(":6060", nil)) }()
	}

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Printf("[go-hotpath] :%s | sharded engine: shards=%d global=%d(perShard=%d) pipeDepth=%d ceilings{push=%d,pop=%d,ack=%d} weights{push=%d,pop=%d,ack=%d} pref{push=%d,pop=%d}",
		cfg.Port, shards, cfg.GlobalConcurrency, perShard, cfg.PipelineDepth,
		cfg.Push.MaxConcurrent, cfg.Pop.MaxConcurrent, cfg.Ack.MaxConcurrent,
		cfg.PushWeight, cfg.PopWeight, cfg.AckWeight,
		cfg.Push.Preferred, cfg.Pop.Preferred)
	log.Fatal(srv.ListenAndServe())
}

func waitForPG(pool *pgxpool.Pool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lastErr = pool.Ping(ctx)
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return lastErr
}
