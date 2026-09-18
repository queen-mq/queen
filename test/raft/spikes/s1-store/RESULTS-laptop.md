# S1 laptop smoke — redb 2.6.3 vs fjall 2.11.2 vs heed 0.22.1 (LMDB)

**Smoke only.** macOS serialises `F_FULLFSYNC` (PLAN_RAFT.md §0.3), and a
durable point here fsyncs one file per segment bucket touched, so every durable
number below is a macOS artefact, not a Linux number. What the laptop *does*
say, because it is the same work on the same data for all three engines: the
per-entry commit cost, write amplification, RSS, export cost, reclamation
behaviour, iteration speed, and — the PASS/FAIL of WP-0.3 — what each engine's
reopened state says after `kill -9`. The 2 h matrix at 20k/50k/100k and the 100
kill runs plus dm-flakey are WP-0.3 part 2, on the Linux VM.

Host: MacBook (Darwin 24.5.0, arm64, APFS), Rust 1.94.0 release build.
Date: 2026-09-17. Crate: `test/raft/spikes/s1-store` at
`--rate 20000 --duration 60 --segment-bytes 262144` (a small roll size so files
roll and reclamation can be observed inside 60 s; the plan's default is 64 MiB),
`--cache-mb 256`, payload 512 B, batch 10 → 2000 entries/s, 4096 partitions,
64 queues, 1 group, KV 8000 ops/s over 200k keys, timers 400/s, request-id
window 60 s.

Commands (exact):

```
cargo build --release
./target/release/s1-store run --engine <E> --dir <data>/s-<E>-<T> \
    --rate 20000 --duration 60 --segment-bytes 262144 --fsync-threads <T>
./kill-loop.sh 10            # ENGINE=<E> RATE=20000 MIN_MS=3000 MAX_MS=12000 MODE=mixed
./target/release/s1-store iter --engine <E> --dir <data>/iter-<E> --iter-keys 10000000
cargo +1.88 check --release  # MSRV C-3
```

## 1. 60 s apply stream at 20k msg/s, one fsync thread

| | redb 2.6.3 | fjall 2.11.2 | heed 0.22.1 |
|---|---|---|---|
| achieved | 2 660 msg/s (266 entries/s of 2000) | **17 945 msg/s** (1795 entries/s) | 4 880 msg/s (488 entries/s) |
| store ops/s | 6 831 | **46 127** | 12 625 |
| non-durable commit p50 | 0.511 ms | **0.033 ms** | 0.139 ms |
| non-durable commit p99 | 8.959 ms | **0.375 ms** | 0.655 ms |
| non-durable commit p99.9 | 18.9 ms | 2.6 ms | 4.5 ms |
| entry total p99 (append + commit + CAS reads) | 9.2 ms | 1.1 ms | 8.7 ms |
| durable point p50 / p99 | 1 442 / 2 359 ms | 2 032 / 3 539 ms | 1 278 / 1 999 ms |
| of which segment fsync (p99) | 2 064 ms | 3 539 ms | 1 966 ms |
| KV get p99 | 0.063 ms | 0.021 ms | **0.018 ms** |
| KV prefix list p99 (100 rows) | 0.103 ms | 0.106 ms | **0.038 ms** |
| **WA, kernel write counter** | **34.1x** | **1.15x** | 3.35x |
| WA, allocated bytes on disk | 4.11x | 0.90x | 1.03x |
| store growth vs its own rows | **18.2x** (17.4 MiB of rows → 317 MiB) | 0.32x (115 MiB → 37 MiB, LSM + lz4) | 1.02x (31.5 MiB → 32.1 MiB) |
| RSS max | 313 MiB | 140 MiB | **40 MiB** |
| consistent export | logical dump 12.2 MiB in 159 ms | logical dump 74.1 MiB in 585 ms | **`mdb_env_copy` 32.1 MiB in 60 ms (537 MiB/s)** |
| reopen after clean close | 9 ms | 21 ms | 7 ms |

Raw logs: `results/smoke-<engine>-t1.log` (every number above is a line in
those files; the last line of each is the machine-readable `RESULT` line).

### Reclamation after deletes (delete the older half of the segments)

| | redb | fjall | heed |
|---|---|---|---|
| rows deleted / time | 7 591 / 72 ms | 57 864 / 88 ms | 26 516 / 32 ms |
| store before → after deletes → after maintenance | 317.8 → 317.8 → **34.5 MiB** (`Database::compact()`, 1 552 ms) | 30.9 → 34.5 → **26.0 MiB** (`major_compact`, 831 ms) | 32.1 → 32.1 → **32.1 MiB** (no in-place compaction; a compacting copy would be 26 MiB, 223 ms) |
| segment files unlinked | 5 files, 1.2 MiB (at 266 entries/s few buckets had rolled) | 1 034 files, 258 MiB freed | 405 files, 101 MiB freed |

Three different stories, all of which matter for §11.7:

- **redb** does not free pages at `Durability::None` commits (its own
  documentation says so), and the file it left behind was 317 MiB for 17 MiB of
  live rows. `compact()` brought it back to 34.5 MiB, but it is a stop-the-world pass (1.55 s here, on a 317 MiB file).
- **fjall** frees by compaction; the store briefly *grows* while tombstones and
  the new SSTs live next to the old ones, then drops below where it started.
- **heed** never shrinks: freed pages go on LMDB's free list and are reused, so
  the file is a high-water mark. That is fine for a broker whose volume is
  bounded by retention, and bad for one that has a spike and then idles.

## 2. Does parallelising the segment fsyncs help? (macOS: no)

`--fsync-threads 8` against the same run:

| engine | msg/s t1 → t8 | durable p50 t1 → t8 | fsync p99 t1 → t8 |
|---|---|---|---|
| redb | 2 660 → 2 239 | 1 442 → 1 212 ms | 2 064 → **6 160** ms |
| fjall | 17 945 → 18 460 | 2 032 → **639** ms | 3 539 → 1 835 ms |
| heed | 4 880 → **1 620** | 1 278 → 311 ms | 1 966 → 918 ms |

The p50 drops and the tail explodes: `F_FULLFSYNC` is serialised by the drive,
so eight threads queue behind each other and the slowest durable point gets
worse. Whether fsync fan-out pays has to be measured on the VM (ext4,
`fdatasync` ≈ 1 ms, which the plan says scales in parallel).

**The finding that survives the platform**: the durable point's cost is
dominated by *one fsync per segment bucket touched since the last one*, not by
bytes. At 2 000 entries/s over 4 096 partitions every one of the 256 buckets is
touched every second (measured: 194–360 files fsynced per durable point). The
store commit itself was 0.3 s of redb's 1.4 s and 8 ms of fjall's 2.0 s. §11.4
should say how many buckets a durable point may touch, or the design should let
an entry's payloads land in fewer buckets between durable points.

## 3. PASS/FAIL: kill -9 during non-durable commits (10 runs per engine)

`kill-loop.sh 10`, alternating an external `kill -9` at a random moment in
[3 s, 12 s] with the harness raising SIGKILL itself at a random point of an
entry (`append` / `precommit` / `postcommit`). After each kill the store is
reopened and `verify` checks that every segment the reopened state references is
present with a matching xxh3, that `segments` and `seg_loc` hold the same key
set (no torn entry), and whether the applied index is ahead of the last durable
index.

| engine | runs | PASS | FAIL | reopened past the last durable commit | max segments referenced beyond the durable file lengths | torn entries | reopen after kill -9 |
|---|---|---|---|---|---|---|---|
| redb 2.6.3 | 10 | **10** | 0 | **0 / 10** (applied index always equals the durable index) | 0 | 0 | 45–111 ms |
| fjall 2.11.2 | 10 | **10** | 0 | **6 / 10** | 368 | 0 | 35–67 ms |
| heed 0.22.1 | 10 | **10** | 0 | **6 / 10** | 251 | 0 | **4–8 ms** |

All three agree with their segment files after `kill -9`, and no entry was ever
torn across the 13 keyspaces — as expected, because a process kill leaves the
page cache intact, so unflushed bytes are still readable. The interesting column
is the third one:

- **redb reverts to its last durable commit.** `Durability::None` does not
  publish a new root that survives a crash, so the reopened state is exactly the
  last `Immediate` commit, and everything applied after it is gone. For the RSM
  that means recovery replays from the durable index and never sees state ahead
  of the files — the simplest possible §11.5, at the cost of replaying up to one
  durable interval of committed entries at every start.
- **fjall and heed reopen PAST the last durable commit** (6 of 10 runs each; the
  other 4 killed close enough to a durable point that the two indexes coincided).
  fjall replays its journal, LMDB's committed meta page is in the mmap. In those
  runs the reopened state referenced up to **368 segments (fjall) / 251 (heed)
  that lie beyond the file lengths recorded at the last durable point** — the
  exact situation I11 describes. Two consequences for the design:
  1. recovery must **not** truncate segment files to the lengths recorded at the
     last durable point; it must use the lengths implied by the state the store
     reopens with (in this harness, `max(offset+len)` per file over the `seg_loc`
     rows the reopened state holds, which is the per-entry equivalent of "every
     apply commit records the lengths of the files it touched");
  2. every frame past the durable point must be checksum-verified on recovery,
     because after a **power loss** (not a process kill) those bytes can be
     missing while the store still references them. That case is `flaky.sh` on
     the VM (part 2); the laptop cannot produce it.

Command: `ENGINE=<E> RATE=20000 MIN_MS=3000 MAX_MS=12000 MODE=mixed ./kill-loop.sh 10`
(5 external `kill -9` + 5 in-process SIGKILL at `append`/`precommit`/`postcommit`).
Full lines in `results/killloop-<engine>.log`.

## 4. Ordered iteration over 10M keys

`iter` bulk-loads 10 000 000 rows of 16 B key → 40 B value (the `segments`
shape, `(pid, base_offset)` in key order) in transactions of 10 000, a durable
commit every `--iter-durable-txns` (10) transactions, then measures a full
ordered scan and 100 range scans of 1 000 rows.

| | redb 2.6.3 | fjall 2.11.2 | heed 0.22.1 |
|---|---|---|---|
| keys loaded | **2 000 000** (see below) | 10 000 000 | 10 000 000 |
| bulk load | 42.8 s = 47 k keys/s | 47.5 s = **210 k keys/s** | 74.8 s = 134 k keys/s |
| bytes on disk per key (logical 56 B) | **326 B** | **20 B** (lz4 on very compressible synthetic values) | 139 B |
| full ordered scan | 0.18 s = 11.2 M rows/s, 598 MiB/s | 5.20 s = 1.9 M rows/s, 103 MiB/s | **0.08 s = 119 M rows/s, 6 351 MiB/s** |
| range scan of 1 000 rows, p50 / p99 | 0.025 / 0.051 ms | **0.815 / 3.263 ms** | **0.013 / 0.017 ms** |

- **redb could not finish the 10M load on this laptop.** The first attempt
  (durable commit every 1M keys) ran ~11 minutes at 7 % CPU — fsync-bound — had
  written 1.4 GiB and was still going, on a disk with 7 GiB free, so it was
  killed (`results/iter-redb-10M-aborted.log`). The durable interval matters for
  redb specifically, because it frees pages only at commits above
  `Durability::None`, so the default became every 10 transactions (100k keys);
  even so, 2M keys took 42.8 s and 326 B of file per 56 B row — the same
  copy-on-write amplification the 60 s run showed. The 10M figure for redb has
  to come from the VM.
- **fjall's scan is 60x slower per row than heed's** and its 1 000-row range
  scan is 60x slower than heed's. An LSM has to merge and decompress; LMDB
  hands back pointers into the mmap. This is the number that matters for the
  replicated digest (§12.9), for chunked deletes and for a logical snapshot
  export, all of which walk the keyspace in order.
- Both scan passes are warm (the laptop cannot drop the page cache without
  root); the VM run should report a cold pass.

Logs: `results/iter-<engine>.log`, plus `results/iter-redb-10M-aborted.log`
for the abandoned 10M redb attempt.

## 5. MSRV, build

- `cargo build --release` with Rust 1.94: clean. `cargo test --release`: 4 tests
  (histogram bounds, segment frame round-trip + checksum detection, file roll and
  recorded lengths), all pass.
- `cargo +1.88 check --release`: **rc=0** (`results/msrv-1.88.log`) with the pins of `DEPS.md`
  (redb 2.6.3, fjall 2.11.2, heed 0.22.1 / lmdb-master-sys 0.2.6). The newest
  redb (4.3.0) and fjall (3.1.10) declare MSRV 1.90 and do not build on 1.88.
- No cmake anywhere: heed compiles LMDB with `cc`, redb and fjall have no build
  script at all.
- Linux build: not exercised here (part 2 does, on the VM). The platform-
  specific code is `sys.rs` (`/proc/self/{status,io}` vs `proc_pid_rusage`) and
  `seg.rs::fsync_fd` (`fdatasync`/`fsync` vs `F_FULLFSYNC`).

## 6. What this smoke already says about the choice

1. **redb 2.6.3 is the weakest fit for this write pattern.** 34x write
   amplification and a 18x file-to-rows ratio come from copy-on-write B-tree
   commits at 266 entries/s: every entry rewrites the path to root of 13
   trees, and `Durability::None` never frees the old pages until the next
   durable commit. Its non-durable commit p99 (9 ms) is 24x fjall's. Add the
   MSRV ladder (1.88 pins us to a 2025-08 release) and it is hard to justify.
2. **fjall 2.11.2 is the fastest by a wide margin** on exactly the shape of
   §6.1 (many small ordered writes across 13 keyspaces in one atomic batch),
   with WA 1.15x and the only story for incremental snapshots (immutable SSTs).
   Its costs are RAM (140 MiB vs heed's 40 MiB, tunable), compaction pauses
   that have to be measured under a 2 h run, journal replay on open, and
   **ordered reads**: its full scan is 60x slower per row than LMDB's and its
   1 000-row range scan is 60x slower (0.8 ms vs 13 µs), which is felt by the
   digest, chunked deletes, `pending`/ready-ring rebuilds and logical exports.
3. **heed/LMDB is the most predictable**: lowest RSS, lowest read latency, a
   native consistent export that is 9x faster than a logical dump, and a store
   file that tracks its rows 1:1. It pays 3.35x write amplification for
   page-granular COW on random keys (which on the VM's ext4 is the number to
   watch: 3.35x of a 20k msg/s stream is ~35 MB/s of extra disk traffic), it
   cannot give space back in place, and it brings a C dependency and a fixed
   map size. Its recovery after `kill -9` was also the fastest to reopen
   (4–8 ms).

The decision needs the VM: durable-point cost with `fdatasync`, the 2 h
compaction and RSS behaviour of fjall, and the dropped-unflushed-writes verdict
(`flaky.sh`) that this laptop cannot produce.

---

## 2026-09-18 — the refutation round's laptop half

Two adversarial reviews of `MEMO.md` came back `refuted`; the measurement answer
is in `RESULTS-refutation.md`, and its VM half is the one that counts. The
laptop contributed one thing the VM confirmed and then moderated: the **D15
read path in heed's two reader modes**, `results/refutation/reads-heed-laptop.log`.

```
./target/release/s1-store reads --engine heed --dir <a store a ratified run left> \
    --read-threads {1,2,4,8} --duration 4 --heed-no-tls {0,1} --heed-max-readers 512
```

| reader threads | `WithTls` gets/s | `WithoutTls` (MDB_NOTLS) gets/s |
|---|---|---|
| 1 | 3 220 688 | 1 311 104 |
| 2 | 6 452 080 | 1 251 184 |
| 4 | 12 204 352 | 639 664 |
| 8 | 11 403 088 | **293 168** |

On macOS the `Send`-capable reader mode scales **backwards** — 11x slower at 8
threads than at 1, and 39x behind the thread-local mode. On the VM (Linux, 8
vCPU) the same measurement is flat rather than negative, 2.06 → 2.08 M gets/s
from 1 to 8 threads against 9.96 M with TLS, so the *size* of the penalty is an
OS-scheduler property; the direction is the same on both hosts and the cause is
the same line of `mdb.c` (`mdb_txn_renew0` takes `env->me_rmutex` and scans the
reader table on every begin when there is no thread-local slot). Two ceilings,
both measured here first:

```
READERS engine=heed asked=200 opened=1  err="MDB_BAD_RSLOT: Invalid reuse of reader locktable slot"
READERS engine=heed asked=100 opened=64 err="MDB_READERS_FULL: Environment maxreaders limit reached"   (max_readers=64)
```

As always, macOS numbers are smoke: `F_FULLFSYNC` is serialised, so every
durable point here is 1.1–2.4 s and no throughput figure from this host means
anything. The read benchmark touches no fsync, which is why it is reported.
