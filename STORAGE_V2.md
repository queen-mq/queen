# STORAGE_V2.md — raft class disk-write efficiency (PLAN_RAFT.md phase 1)

Design note for the phase-1 "performance package" storage-efficiency round.
Goal (Alice's direction): bring the raft class's disk **write amplification**
from HEAD's ~5x toward Kafka's ~1–2x, which the evidence says also shrinks the
durable-point fsync and the tail. Every lever is behind an env knob defaulting
to today's behaviour until a VM measure justifies flipping it. Correctness
(I1/I2/I4, difffuzz 0 divergences vs the pg oracle, the crash matrix,
dropped-write durability) is non-negotiable and gates every lever.

Do NOT edit PLAN_RAFT.md from here (§0.3: ratified decisions change only with
Alice). This note is the SSOT for the round; results land in RAFT_STATUS.md
(M12 + performance-package rows).

---

## 1. Measured baseline (HEAD `485b4ebc`, VM 164.90.215.224, ext4, default knobs)

Short like-for-like runs, consume-matched, retention **off** both (the soak
confound is removed in the re-test by turning retention **on** for both classes).
Gross write is device-level (`/proc/diskstats` sectors ×512), because LMDB
dirties pages through its mmap and the kernel flusher writes them back **without
attributing them to the process** — `/proc/pid/io write_bytes` undercounts the
store by ~2x, so it is reported only as a cross-check.

| shape | offered | gross dev MB/s | seg net | store net | NET MB/s | **WA (dev/net)** | durable p50 | push p50/p99 | sys CPU |
|---|---|---|---|---|---|---|---|---|---|
| A20k  | 20k msg/s, batch 10, 100 part, 256 B | **42.0** | 6.96 | 1.97 | 8.93 | **4.71x** | 16.8 ms | 5.4 / 17.5 | 58% |
| C1000 | 3k msg/s, batch 1, 1000 part, 256 B  | **26.3** | 1.15 | 1.46 | 2.61 | **10.07x** | 33.6 ms | 3.9 / 35.1 | 38% |

(FAT100 saturation shape: loader invocation still being calibrated; write-amp
on FAT is payload-dominated ≈ the seg ~1.36x floor, not the scatter problem.)

The 3 h raft-vs-pg soak (`/root/raft/soak-20260920-0807`) on the same HEAD:
96 G raft vs 2 G pg over 10800 s → **75 G segments + 22 G store(data.mdb) + 33 M
log**. Net growth ~8.9 MB/s; the log is truncated after apply (33 M tail), so the
payload's raft-log copy is **transient gross churn, not net growth**.

## 2. Where the writes go (verified in code + on the VM)

Per second at A20k the device writes ~42 MB to persist ~8.9 MB of net state:

- **Segments ~7 MB/s** — the payload's permanent home, append-only, ~1.36x over
  the 5.1 MB/s raw payload (frame header + 16 B hash + length). This is the
  Kafka-equivalent floor and is FINE.
- **Raft log ~7 MB/s (transient)** — every `Append` effect carries `blob` (the
  packed frames); the entry is written+fsynced to `data/log/*.qlog`
  (`replicator/local.rs`) then truncated after apply. So the payload is written
  to disk **twice** at the gross level (log + segment), once permanently.
- **LMDB store ~28 MB/s to persist ~2 MB/s net = ~14x** — the dominant term and
  the amplifier. This is **not** commit frequency alone; it is **scatter under
  copy-on-write**. Per `Append` (apply.rs), keyed by `pid`/`(pid,base_offset)`,
  so scattered across the 100–1000 partitions:
  - `txns (pid, base_offset)` → `[end][created][hashes]` — the dedup authority
    (PERF-E), ~50–180 B. **The biggest per-append value.**
  - `seg_loc (pid, base_offset)` → `(bucket,file_id,offset,len)` — node-local
    location.
  - `partition (pid)` — the partition row (watermarks), rewritten every append.
  - `pending (tenant,queue,group,pid)` — per subscribed group.
  - per **command** (not per append): `request_ids (id)` (a **random** 16-byte
    key, D6, kept 600 s) + `request_expiry (now,id)`.

  LMDB commits are non-durable `mdb_txn_commit` every `store_commit_ms=4` ms
  (~250/s) and a durable `mdb_env_sync` every `durable_every_ms=1000` ms. Each
  commit CoW-rewrites the meta page, the B-tree root and the **whole
  root-to-leaf path of every dirtied key**; scattered keys across N partitions
  dirty ~N distinct leaves + their ancestor paths, and the durable point fsyncs
  the lot. C1000 (batch 1, 1000 partitions = maximum scatter) is the worst:
  **10x**.

## 3. Rejected lever: batch the store commit (`QUEEN_RAFT_STORE_COMMIT_MS`)

Measured A/B on the existing knob (A20k, consume-matched):

| store_commit_ms | 4 (HEAD) | 40 | 100 | 1000 |
|---|---|---|---|---|
| gross dev MB/s | 42.0 | 39.9 | 38.3 | 27.7 |
| WA (dev/net) | 4.71x | 4.47x | 4.29x | 2.95x |
| push p50 ms | 5.4 | 8.0 | 8.9 | **22.7** |
| durable p50 ms | 16.8 | 33.6 | 33.6 | 33.6 |

Frequency batching pays off only at extreme windows (1000 ms cuts WA to 2.95x)
and **immediately regresses latency** (the apply thread stalls on a bigger
inline flush; the overlay deepens). The per-commit fixed CoW cost (meta+root+
inner) is small relative to the scattered LEAF writes, so cutting frequency
10–25x barely helps until the overlay-bloat latency cost has already landed.
**Verdict: not a good lever; do not flip the default.** The scatter, not the
frequency, is the amplifier — attack the scatter.

## 4. Chosen levers — remove scattered per-append writes (Kafka-shaped)

The `.qidx` segment index (`segments/index.rs`) is already the Kafka pattern:
one immutable index file beside each **sealed** segment, the active file's index
in RAM, written **sequentially** once at seal. Its record already carries
`pid | base_offset | end | created_at | offset | count | len` — i.e. **all of
`seg_loc` and most of `txns`** (only the hash list is missing). The store keeps
`seg_loc`/`txns` as a **second, scattered, CoW-amplified copy** of data the
segment index already owns. Both are removable.

### Lever 1 — drop the `seg_loc` LMDB write (`QUEEN_RAFT_SEG_LOC_STORE`, default on)

`seg_loc` is **node-local** (Scope::NodeLocal — not in the §12.9 digest) and is
**rebuilt from segment files** on snapshot install (`clear_node_local`), so it
is a pure derived cache. The planner never reads it (planner/mod.rs:35); the pop
path locates bytes from the claim's `(bucket,pid,offset)` via the segment index,
not `seg_loc`. Its only readers are apply's retention/GC/partition-delete
(`scan_seg_loc`), which can read the same `(offset,len,bucket,file_id)` from the
segment index (RAM active + `.qidx` sealed). Knob off = don't write the
`SegLoc` keyspace; serve those reads from the index. Digest-invariant (I2), one
fewer scattered write per append. Correctness: node-local, so no difffuzz
divergence possible; covered by the crash/recovery matrix (reopen rebuilds the
index from files already).

### Lever 2 — dedup authority into the segment (`QUEEN_RAFT_DEDUP_INDEX=segment`, default `txns`)

The task's #1 lever. Extend the segment index to carry the append's **hash
list** (a variable-length `.qhash` sidecar beside each `.qidx`, plus the active
file's hashes in RAM), and make it the committed dedup authority. Apply's
`dedup::record` then writes **zero** LMDB rows on the hot path (today `txns`
writes one scattered `(pid,base_offset)` row per append — the biggest per-append
value). The planner's dedup resolve and the 005 ack-by-hash path scan the
segment's records in the window (they already scan `txns` there; now they read
the segment index the pop path already uses), gated by PERF-B's RAM bloom.

- **Determinism/digest.** `txns` is currently Scope::Replicated (digested). The
  hash content is a deterministic function of the replicated `Append` effects,
  but the sidecar's file layout is node-local. Resolution: the dedup DECISION is
  what must be deterministic, and it is (same effects → same in-window
  `(offset,hash)` set on every node). The `.qhash` becomes node-local like
  `.qidx`/`seg_loc`; the digest drops the `Txns` keyspace. This is an I2-surface
  change and is proven, not asserted, by a `sidecar`-vs-`txns` differential
  fuzzer (the `rows`-vs-`txns` template PERF-E already built in
  `tests/dedup_txns.rs`) at **0 divergences**, plus the crash matrix and a
  dropped-write reopen.
- **Retention/GC.** `txns` purge (`006`) becomes a segment-scoped sweep; the
  window bound (D10, dedup_window_seconds) is unchanged.
- **Recovery.** The `.qhash` is rebuilt by scanning the segment file's frames
  (which already carry the per-frame transaction id / hash) exactly as `.qidx`
  is rebuilt (§11.5), so a stale/missing sidecar is not a durability hole.

Expected: removes the two largest scattered per-append writes → C1000 ~10x and
A20k ~4.7x both down substantially, durable-point fsync smaller, tail flat or
better (fewer dirty pages per point).

### Not in this round (documented, higher risk / owed a WP)

- **Kill the payload log double-write** (reference-in-entry / serve-from-log).
  ~7 MB/s transient at A20k. Inherent to D3 (entries carry effects) and the
  "log is truth" recovery; changing it rewrites the crash contract and paints
  phase 3 (followers must materialize from the entry) into a corner. Owed a WP.
- **`request_ids` random-key churn** — D6 idempotency, correctness-critical; a
  random-insert tree is the worst CoW case but cannot be dropped. Owed analysis.
- **Store env-sync off the apply thread** — R-125, Alice's D9 gate.

## 5. Correctness gate (every lever, before it is called done)

1. `cargo test -p queen-engine --lib rsm::` green; `cargo build --lib` green after
   every edit (own only these files: `rsm/apply.rs`, `rsm/dedup.rs`,
   `rsm/segments/*`, `rsm/planner/{pop,push,ack,mod}.rs`, `rsm/store/*`,
   `rsm/tests/*`; NOT the batcher/replicator/handlers owned by prior WPs unless a
   mechanical struct-field add is forced).
2. clippy + rustfmt clean on touched files; the I2 `disallowed_methods` deny-gate
   (`test/raft/lint/deny-bites.sh`) green.
3. A `sidecar`-vs-`txns` (and `seg_loc` on-vs-off) differential fuzzer at 0
   divergences; the crash matrix (`test/raft/crash`) green; a dropped-write
   reopen holds.
4. VM ablation: knob off == HEAD numbers; knob on == the new numbers; write-amp
   and disk MB/s down substantially, durable-point p50 and push p99 down or flat,
   throughput up or flat. Gate = all four green.

## 6. Re-test plan

Short first (A20k / C1000 / FAT100, 60–75 s, knob off vs on, same box, retention
matched), reporting consume-matched push p50/p99/p999, throughput, pops/s,
lag, ERRLINES, **system CPU**, **iostat write MB/s**, and **write amplification
(gross ÷ net)**. Stop and report at the short gate. Only if good: a fair
raft-vs-pg soak, 3 h each, retention **ON for both**, detached, sampled every
60 s with system CPU + iostat + disk, a DONE marker and a raft-vs-pg
SOAK-SUMMARY.md. Kill only recorded PIDs; work under `/root/raft/<dir>` only.
