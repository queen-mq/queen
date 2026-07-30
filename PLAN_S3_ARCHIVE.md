# PLAN_S3_ARCHIVE — PG-tail + S3 archive tier for the log engine

Rev 1.0 — 2026-07-30. Grounded in a three-track reconnaissance of branch `rustproxy`
(SQL layer, Rust broker, external constraints). File:line references are from that survey.

## 0. Goal and non-goals

**Goal.** Messages stay in Postgres only for a short tail window; a background **mover**
relocates old segment blobs into fat immutable objects on S3-compatible storage
(DO Spaces primary; AWS S3 / versitygw / Garage compatible). Pop/read paths resolve
archived bytes via broker-side read-through with a cache. This buys retention of days
or weeks at object-storage cost, keeps PG small and fast, and keeps brokers stateless.

**Non-goals (v1).**
- No change to hot-path semantics or latency: push ack = PG commit, exactly as today.
- No S3 in the push/ack critical path, ever.
- No multipart upload (objects ≤ 64 MB target, single PUT; S3/Spaces cap is 5 GB).
- No LIST-dependent logic (PG is the index; GC works by explicit keys).
- No object compaction (objects are born fat by construction; revisit only if measured).
- No hard push-shed on tail overflow (v1 = cap metric + alert; see §8).

**Deployment stance.** Feature is default-OFF (`QUEEN_ARCHIVE=false`). Cloud cells turn
it on. Self-host can use any S3-compatible endpoint or leave it off. One engine, no fork.

## 1. Architecture (decided)

```
offset →  0 ····································→ head
          [······ S3 objects (archived) ······)[·· PG tail (blob) ··]
          ^ log_start (unchanged semantics)     ^ mover boundary (age-based)
```

1. **Pointer-in-place.** Archived rows STAY in `queen.log_segments`: `blob` becomes
   NULL, pointer columns are set. No separate archive table. Rationale (survey): the
   pop scan tolerates retention gaps **silently by design** (`041_log_schema.sql:10-12`,
   `043_log_pop.sql:342-344`) — rows that vanish are indistinguishable from retained-away
   data, which is correct for retention and data loss for archival. Keeping the row
   preserves, for free: the PK-as-pop-path scan, the head probe, `log_start` invariants
   (load-bearing in pop seeding, stats math `048:16-27`, and retention `045:102-105`),
   `created_at` timestamp seeks, and "earliest" resolution.
2. **Two-phase pop.** `log_pop_v1` cannot fetch S3 inside SQL (it would hold the consumer
   row lock + a pooled PG connection through a GET; the file's own header rules this out,
   `043_log_pop.sql:73-82`). So: SQL claims exactly as today and returns archived rows as
   pointers (NULL blob); the broker resolves bytes **after** the transaction, from cache
   or S3, then renders. If resolution fails, the pop returns a retryable error — the
   claim is covered by existing lease/redelivery semantics (hotlist wheel revisit is
   already capped at 1 s by the re-arm fix, so no 300 s stall).
3. **Objects = concatenated segment blobs + footer.** Stored blobs are already
   `pack_frames + zstd` and structurally self-describing (`frames.rs:1-7`); segment size
   in PG is emergent and NOT tunable (`QUEEN_V2_FUSION_FRAMES` is dead, `fusion.rs:361`),
   so fat objects are built by **concatenation** in the mover. Footer carries per segment:
   `partition_id, base_offset, end_offset, created_at, byte_off, byte_len, xxh3_64`.
   The footer makes each object self-contained (DR: pointers rebuildable from objects).
4. **Checksums are mandatory.** `zstd_decompress` swallows errors (`unwrap_or_default`,
   `frames.rs:353`) and the pop path `continue`s on undecodable blobs — a corrupt ranged
   GET would silently deliver 0 messages. Every archive read verifies xxh3 before decode;
   mismatch = hard error (retryable), never a skip.
5. **Grouping: one object per tenant** (fan-in across that tenant's partitions/queues).
   Mixed-tenant objects would break per-tenant deletion (GDPR = delete by prefix) and
   per-tenant accounting. Shared-cell tenants are small; per-tenant batches still reach
   tens of MB by covering minutes of data. Key layout:
   `q/{tenant_id}/{partition_id_first}/{base_offset_first}.qar`
   — deterministic from the batch's first segment, so a crash-retry re-PUTs the same key
   (overwrite of an orphan attempt is benign; pointers always reference the attempt that
   committed). Do NOT rely on conditional PUT (`If-None-Match` is UNVERIFIED on Spaces);
   verify ETag/length after upload instead.
6. **A small object catalog**, `queen.log_objects`
   `(obj_key PK, tenant_id, queue_id, bytes, seg_count, live_count, created_at)`,
   maintained by the mover and decremented by retention deletes. It drives S3 GC
   (`live_count = 0` → DELETE object) and O(objects) `archived_bytes` accounting.
   To keep per-queue retention clean, a batch never mixes queues: **object per
   (tenant, queue)** is the practical grain.

## 2. Schema changes (all catalog-only, applied dark)

```sql
ALTER TABLE queen.log_segments ALTER COLUMN blob DROP NOT NULL;
ALTER TABLE queen.log_segments
    ADD COLUMN IF NOT EXISTS obj_key  TEXT,
    ADD COLUMN IF NOT EXISTS obj_off  BIGINT,
    ADD COLUMN IF NOT EXISTS obj_len  INTEGER,   -- compressed blob bytes (= old octet_length)
    ADD COLUMN IF NOT EXISTS obj_sum  BIGINT;    -- xxh3_64 of the blob
CREATE TABLE IF NOT EXISTS queen.log_objects (...);  -- §1.6
```

- No new index on `log_segments` (zero-secondary-indexes is an explicit, measured design
  decision, `041:96-97`). Mover eligibility walks the PK per partition (§4).
- Invariant: `blob IS NULL ⇔ obj_key IS NOT NULL`. CHECK constraint.
- `queen.queues`: add `archive_enabled BOOLEAN DEFAULT false` (wire: `archiveEnabled`
  via `configure_queue_v1`). Archive knobs live in `queen.queues` ONLY — the survey
  flagged that `configure_queue_v1` never writes `log_queues` (dedup window travels via
  a separate Rust write, `handlers/queues.rs:135`); don't add a second split-brain knob.
- Stats: `queen.stats` gains `archived_bytes`; `048_log_stats.sql` keeps
  `retained_bytes` = live blobs (`octet_length`, unchanged cheapness via
  `STORAGE EXTERNAL`) and adds `archived_bytes` from `log_objects` (O(objects), not
  O(segments)). The proxy storage quota reads both; without this, archiving would
  silently deflate `retained_bytes` and the quota (survey Q7).

## 3. Retention-semantics split (Phase 0, ships before any S3 code)

Today `retention.rs:74-76` computes the `log_txns` purge cutoff as
`GREATEST(dedup_window, completed_retention, 900)`. With week-scale retention this
would drag weeks of hash rows (16 B/frame + row + PK overhead → tens of GB) and re-open
the O(rate×window) class of problem. Split into two knobs:

- **payload retention** (replay horizon): drives segment rows/objects. May be weeks.
- **hash/ack window** (`log_txns`): stays short — `GREATEST(dedup_window, ACK_HORIZON, 900)`
  where ACK_HORIZON covers lease + retry chains (minutes), NOT replay.

**✅ V1 — VERIFIED 2026-07-30 (read-only investigation). Verdict: neither (a) nor (b) —
the positional path exists but does not cover replay. The split is safe ONLY together
with an ack-by-offset extension (Phase 0b).**

Verified mechanics:
- Positional acks exist and skip `log_txns` entirely: `log_ack_v1` / `log_ack_at_v1`
  ("NO segment reads", `044:63-79`, `:160-176`). The broker reaches them ONLY through
  the AckRegistry fast path, which fires iff: leaseId present + **every** item in the
  (partition, worker) group is `completed` + the acked txn-hash set EXACTLY equals the
  delivered batch's set (`ack_registry.rs:21-40`, `data.rs:2252-2265`). The registry is
  RAM-only, LRU/TTL-bounded, "an OPTIMIZATION ONLY" — every miss falls to by-hash.
- Everything else — partial acks, per-message acks with batch>1, every explicit signal
  (`failed`/`retry`/`dlq`), lease-less acks, registry misses (restart/eviction/HA) —
  resolves through `log_ack_by_hash_v1` against `log_txns`. Unresolvable (purged)
  hashes "count as NOT acked: the cursor stops before their position and those frames
  redeliver — redelivery over data loss, by design" (`044:417-419`, `045:227-229`).
- **Why `completed_retention` is in the GREATEST formula — now proven:** the hash
  window ≥ replay horizon *by construction*, so that seek-back consumption within
  retention stays ackable. Long payload retention without the split would therefore
  drag the hash sidecar along (storage) AND make lease-less by-hash acks unnest an
  unbounded window (`044:530` — a CPU bomb at weeks scale). The split is mandatory.
- **Reporting hole (severity: high).** On `ok:true` without `dlq`, the broker marks
  ALL group items `success=true` and only corrects `noopHashes`/`staleHashes`
  (`data.rs:2449-2479`). Purged-hash items appear in NEITHER list (`eff IS NULL AND
  below=false`) → **the client is told the ack succeeded while the cursor did not
  move** → silent redelivery loop. Worse: a `failed`/`dlq` status with a purged hash
  never even becomes a signal (`sig` CTE requires `eff IS NOT NULL`, `044:542-548`)
  → no retry charge, no DLQ — a poison message in a replay can never be dead-lettered.
- **This is a LIVE bug today, without archive:** on queues with `retention_enabled`
  false (the default), segments live forever but the hash window floors at
  `GREATEST(dedup_window, 900)` (~1 h default). A timestamp-seek further back than
  that + per-message acking = the silent livelock, today. (Flagged as a standalone
  fix, independent of this plan.)
- Registry-miss on a whole-batch `completed` replay ack *converges* (redelivery
  re-pops → registry repopulated → next ack hits): one wasted redelivery, not a
  livelock. The livelock is confined to partial/per-message/nack/lease-less forms —
  which includes the JS client's per-message handler mode (`ConsumerManager.js:193`
  acks one message per request → set≠batch → by-hash always).

**Resolution — Phase 0b (prerequisite of the split), options analyzed:**
- **(A) RECOMMENDED — ack-by-offset (the deferred "D2-lite"):** the pop wire already
  exposes each segment's `base_offset` as the opaque `seq` token (`data.rs:497`,
  `:534`) and frames are index-ordered, so the broker can emit a per-message opaque
  offset token; SDKs echo it in acks. New SQL `log_ack_at_multi_status_v1` = the
  by-hash decision procedure (implicit-ack, signal clamp, retry budget, DLQ hand-off)
  minus hash resolution. All ack forms become positional and replay-safe forever;
  `log_txns` shrinks to `GREATEST(dedup_window, ack_horizon, 900)`. Backward
  compatible: by-hash stays for old SDKs (within the short window); **replay support
  is gated on updated SDKs** — acceptable, replay is a new feature.
- (B) Registry stores per-hash offset maps (RAM): covers partial/nack on registry-hit
  leases only; still fails on restart/HA; nacks after a miss stay stuck. Weaker, not
  chosen.
- (C) Hash sidecar in the archive footer + broker-side pre-resolution: still needs
  (A)'s new SQL function anyway — dominated by (A).
- In every option: **fix the reporting hole** — unresolvable hashes must return as an
  explicit per-item error list (`unresolvedHashes`), never silent success. **SHIPPED
  2026-07-30** (uncommitted): 044 returns `unresolvedHashes`, the broker maps them to
  per-item failures; empty-partition cursor seal landed in 043 in the same pass (the
  Rust edition of the C++ pop_unified_batch_v4 starvation fix). Tests: JS
  test-v2/ackwindow.js (3), Go tests/ack_window_test.go, Py tests/test_ack_window.py.
  Phase 0b's remaining scope is the ack-by-offset structural half only.

`ack_horizon` contract note: `leaseTime` has NO cap in `configure_queue_v1`
(`012:45`), so the short window must be computed per queue, e.g.
`GREATEST(dedup_window, lease_time × (retry_limit + 1) + slack, 900)`.

## 4. The mover (`server/src/archive.rs`)

Copy the `retention.rs` shape verbatim (spawn / run_loop / leader / step): session
advisory lock **737_003** (737_001 retention, 737_002 stats are taken), fixed cadence
from cycle start, Sampler-gated errors, idle cycles at DEBUG, no shutdown hook.

- **Work list** (one query per cycle, `retention.rs:69` style): queues with
  `archive_enabled AND storage='segments'`, joined `queues ⋈ log_queues ⋈ log_partitions`
  **with the tenant equality on the join** — the survey pinned the cross-tenant-by-name
  bug class and its unit test (`retention.rs:60-67`, `:400`); replicate both.
- **Eligibility walk**: per partition, PK walk from `log_start` forward while
  `blob IS NOT NULL AND created_at < now() - archive_age`. `created_at` is monotone per
  partition in commit order (PUSHSER invariant, `041:12-14`) so the walk stops early.
  No new column, no new index.
- **Batch build**: concatenate blobs per (tenant, queue) up to `QUEEN_ARCHIVE_OBJECT_MB`
  (default 64) or cutoff exhaustion; footer + xxh3 per segment; single PUT; **verify
  ETag/length**; then ONE transaction: `UPDATE log_segments SET blob=NULL, obj_*=...`
  for the batch + `INSERT log_objects`. Crash anywhere = re-run is idempotent (§1.5).
- **Error taxonomy**: `Transient` (503 Slow Down → exponential backoff; network) vs
  `Permanent` (4xx auth/config → circuit-break + alarm), file_buffer's
  `classify_push_error` precedent. **Zero unwraps** — `panic = "abort"` means a mover
  panic kills the whole broker (`obs.rs:26-38`).
- **Self-throttle**: token-bucket ops cap well under Spaces' 800 ops/s bucket limit;
  the mover is naturally low-rate (64 MB objects ≈ 1 PUT per several seconds even at
  high ingest).

## 5. S3 client — hand-rolled SigV4, not a crate (decided by repo policy)

The repo's dependency philosophy is explicit in three places (`server/Cargo.toml:23-26`,
`:41-44`, `proxy/Cargo.toml:8-10`): cmake-free, rustls-only, no reqwest, no second
TLS backend. That rules out `object_store` (pulls reqwest), `aws-sdk-s3` (aws-lc-rs/cmake,
huge tree) and `rust-s3` (native-tls leakage, weak maintenance). Meanwhile the SigV4
toolkit is **already** in the direct deps: `hmac`, `sha2`, `hex`.

Build `server/src/s3.rs`: SigV4 signing (~300-500 lines, test vectors from AWS docs) +
a minimal HTTP/1.1 client with PUT / ranged GET / DELETE, keep-alive pooling and
`Connection: close` fallback, over `tokio-rustls` — following the `queen_proxy` precedent
(`hyper 1` + `hyper-util` + `http-body-util` are acceptable additions if hand-rolling
HTTP/1.1 request framing is judged not worth it; both options are cmake-free).
`httpget.rs` is NOT reusable as-is (GET-only, no headers, no pooling, 1 MiB cap).

Config (`ArchiveConfig::from_env` per the `config.rs` idiom, §7 of survey):
`QUEEN_ARCHIVE`, `QUEEN_ARCHIVE_ENDPOINT`, `QUEEN_ARCHIVE_BUCKET`, `QUEEN_ARCHIVE_REGION`,
`QUEEN_ARCHIVE_ACCESS_KEY` / `_SECRET_KEY` (logged via `mask()` only),
`QUEEN_ARCHIVE_AGE_MS` (default 1800000), `QUEEN_ARCHIVE_OBJECT_MB` (64),
`QUEEN_ARCHIVE_INTERVAL_MS` (5000), `QUEEN_ARCHIVE_CACHE_MB` (256),
`QUEEN_ARCHIVE_TAIL_CAP_GB`, `QUEEN_ARCHIVE_REPLAY_CONCURRENCY` (4).

## 6. Read-through

**Resolver** (`server/src/archive_read.rs`): given `(obj_key, off, len, sum)` → chunked
LRU cache (keyed by object+chunk, NOT by consumer — WarpStream's lesson; chunk 4-8 MB)
→ ranged GET on miss → xxh3 verify → bytes. Sequential replay gets one-chunk readahead.
A global semaphore (`REPLAY_CONCURRENCY`) isolates replay I/O from the hot path
(KIP-405's measured lesson: historical reads starving produce).

**Pop integration.** SQL: `log_pop_v1` returns pointer columns alongside `r_blob`
(NULL for archived); all THREE wrappers updated — `log_pop_wildcard_bin_v1` (bytea[]
transport), `log_pop_wildcard_wire_v1` and `log_pop_discover_wire_v1` (base64-in-JSON
transport). Broker: an **async pre-pass** in the callers fills the missing entries of
the positional `bin_blobs` slice before calling `render_pop_parts` — which is sync and
stays sync (`data.rs:1797`). Both transports must be covered (survey surprise #2) or
specific-partition/discovery pops silently miss the archive.

**Secondary decode sites** (survey §11), all via the same resolver:
1. `dlq_file_head` (`data.rs:2584`, **ack path**) — must read-through: today an archived
   poison segment would silently lose its DLQ payload snapshot (`Ok(false)` skip). Rare
   (archive age ≫ retry horizon) but a correctness hole.
2. `resolve_dup_mids` (`fusion.rs:951`, **push path**) — dedup window (hours) exceeds
   archive age (minutes), so duplicate-mid of an archived segment is reachable. Policy:
   bounded resolver timeout (~250 ms), then the existing `zero_uuid` degrade. The push
   path never stalls on S3.
3. `handle_get_message` (`messages.rs:35`) — read-through for the direct lookup; the
   purged-txn fallback (`seg_scan_segments`, LIMIT 5000, `messages.rs:75`) is bounded to
   live rows only (`AND blob IS NOT NULL`); archived+purged-txn lookup is unsupported in
   v1 (management nicety, returns a clear error).
4. `enrich_segment_payloads` (`messages.rs:262`) — read-through with its existing
   per-request cache, capped at ≤8 objects/request; beyond the cap, messages return
   without payload preview + a flag.

## 7. Retention, GC, quota, metering

- **Retention** (`045` + `retention.rs`): the whole-segment delete step is unchanged
  (rows with pointers delete like rows with blobs); the step additionally decrements
  `log_objects.live_count` for touched `obj_key`s. A new mover-side GC pass DELETEs
  objects with `live_count = 0` and removes catalog rows. Bucket lifecycle expiration
  (supported on Spaces, day granularity, API-only) is configured as a **backstop** at
  `max(payload_retention) + slack`, not as the primary GC.
- **Quota**: proxy storage quota = `retained_bytes` (hot) + `archived_bytes` (new), both
  from `queen.stats`. Pricing may weight them differently (hot vs archive tiers).
- **Metering** (survey surprise #6: the broker has NO byte counters and NO pxdb channel;
  mover traffic never crosses the proxy): broker-side per-tenant counters
  (`bytes_archived`, `bytes_replayed`) next to `PerQueue` (already tenant-keyed),
  flushed syscollect-style into a `queen.*` table; the proxy's existing rollup imports
  them into `queen_proxy.usage_minutes` under new op classes (`Archive`, `Replay`).
  Mapping tenant_id↔cluster_id already lives in the proxy registry.

## 8. Failure modes (explicit decisions)

| Failure | Behavior |
|---|---|
| S3 down | Mover circuit-breaks and pauses; tail grows in PG. `tail_backlog` metric + WARN transition (spool-flip style). v1 = alert at `TAIL_CAP`; no push-shed (consistent with `maxSize` being unenforced today — revisit post-pilot). |
| S3 down during replay | Archived pops return retryable errors; tail consumers unaffected. |
| Corrupt object / bad range | xxh3 mismatch → hard retryable error, Sampler-logged. Never a silent skip. |
| Crash mid-PUT / mid-swap | Deterministic key re-PUT; pointer swap is transactional; idempotent. |
| 503 Slow Down | Exponential backoff + self-throttle under the 800 ops/s cap. |
| Mover bug | No panic path (taxonomy §4); poison batch quarantined (skip + alarm), FIFO not blocked. |

## 9. Security

- Per-frame AES-256-GCM happens at the push handler, **before** packing (`data.rs:200`),
  so archived objects carry ciphertext for encrypted queues and the mover never sees
  plaintext or keys. BUT encryption is per-queue opt-in and the key is process-global:
  non-encrypted queues archive **plaintext to S3**. Decision needed: recommend/force
  bucket-level SSE for cloud cells, or force `encryption_enabled` for archive-enabled
  queues. (Also inherit the known sharp edge: a missing/invalid `QUEEN_ENCRYPTION_KEY`
  silently disables encryption with only a sampled warn.)
- Per-tenant prefixes make GDPR deletion = catalog-driven DELETE by prefix + PG rows.

## 10. Observability

Follow the `obs.rs` house style: `Arc<ArchiveStats>` snapshot in `ReporterHandles`;
`rates` fields `arch_mb_s, arch_obj_s, replay_hit_pct, replay_gets_s`; `sizes` fields
`tail_backlog_mb, arch_cache_mb, objects, archived_gb`; on-change transitions for
S3 healthy/degraded. Per-message-path logs only via `Sampler`.

## 11. Test plan

- **Unit**: SigV4 official test vectors; footer pack/unpack roundtrip; xxh3 corruption
  detection; eligibility-walk boundaries; work-list tenant-join (mirror `retention.rs:400`).
- **Harness**: S3 target in `test/run.sh` compose = **versitygw** (Apache-2.0, active;
  MinIO images are frozen/archived since late 2025 — pin `RELEASE.2025-04-22` only as
  fallback). CI also runs the resolver against a filesystem fake for speed.
- **Integration**: archive-aggressive mode (`AGE_MS=2000`) so every suite pop exercises
  read-through; dedicated tests: push → archived (blob NULL, catalog row) → full replay
  from earliest → ack correctness (the §3 V1 scenario!) → retention → object deleted.
  DLQ-of-archived; duplicate-mid-of-archived; get_message/enrich caps.
- **Fault injection**: kill S3 mid-PUT; corrupt a byte; S3 down during replay; broker
  crash between PUT and swap; 503 storms.
- **Parity gate**: full six-suite matrix with flag OFF must remain bit-identical to
  baseline; full matrix with flag ON must be green.
- **Perf gate** (bench VM): hot-path throughput/latency with mover ON = baseline within
  noise; replay of N GB while hot load runs (semaphore proves isolation); meter integrity.

## 12. Phases & estimates

| Phase | Content | Est. |
|---|---|---|
| 0 | Contracts: retention split design (§3 — gated on 0b), DDL dark, `archiveEnabled` knob, byte-counter plumbing decision | 3-5 gg |
| 0b | Ack-by-offset (§3 option A) + `unresolvedHashes` reporting fix: positional-with-status SQL fn, per-message offset token on the pop wire, JS SDK echo, tests | ~1 sett |
| 1 | S3 client: SigV4 + minimal HTTP, retry/backoff, config+mask, versitygw harness | 1 sett |
| 2 | Mover: work-list, eligibility walk, batch+footer+PUT+verify, pointer swap, catalog, obs, 737_003 | 1.5 sett |
| 3 | Read-through: SQL contract (log_pop_v1 + 3 wrapper, 2 transports), async pre-pass, resolver+cache+readahead+semaphore, 4 secondary sites | 1.5-2 sett |
| 4 | Retention/GC, archived_bytes, quota, metering import | 1 sett |
| 5 | Hardening: fault matrix, parity+perf gates, soak on bench VM, adversarial review, docs | 2 sett |

**Total ≈ 8-9 settimane** to production-credible on cloud cells (0b added after the
gate verification resolved against the free lunch). Phases 0/0b are useful even if the
rest is deferred (contract work + a live-bug fix). Phases 1-2 ship dark
(mover behind flag, nothing reads pointers yet); Phase 3 is the risk center — plan the
adversarial review there.

## 13. Open decisions (Alice)

1. §3 is resolved (option A). Remaining choice: ship Phase 0b inside this plan vs as a
   standalone pre-launch item — the `unresolvedHashes` reporting fix is worth shipping
   regardless (live bug on retention-disabled queues, see the spawned task).
2. Plaintext-on-S3 policy for non-encrypted queues (§9).
3. Tail-cap enforcement level v1 (alert-only vs push-shed).
4. Replay billing: meter `bytes_replayed` as billable (Spaces egress to droplets is free
   in-region, so this is pricing policy, not cost recovery).
5. Client HTTP leg: hand-rolled HTTP/1.1 vs adding `hyper 1` (queen_proxy precedent).
6. Sequencing vs launch: this plan is buildable now, but Phases 1-5 add ~7 weeks of
   surface before the 24-48 h soak and the pilot. Recommended: Phase 0 now (days, closes
   real gaps regardless), Phases 1+ scheduled explicitly against the launch calendar.
