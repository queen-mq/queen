# NIGHT-C — dropped unflushed writes at pipeline=4 (D-01..D-03, the WP-1.11 §4 re-run)

PLAN_RAFT.md §13.6/§13.7 (the dropped-unflushed-writes proof owed by
R-106/R-111/R-114), §7.1–7.3, I3/I5/D4, §11.3/§11.4/§11.8, Appendix G, O14, O19,
G-3/I8; RAFT_STATUS.md D-01/D-02/D-03 (the WP-0.3 "heed 1-in-10 store would not
reopen", unexplained) and F-1/R-117, F-2/R-119. Night of 2026-09-18/19, VM
`root@164.90.215.224` (`queenpgless-01`, Ubuntu 24.04, 8 vCPU, 15 GB, ext4
`/dev/vda1`). All work under `/root/raft/night/c/`. Ran after NIGHT-A/NIGHT-B.

**WP-1.11 §4 proved the dropped-writes durability at pipeline=1, 20/20. This is
the owed pipeline=4 re-run on the F-1-fixed binary.** It hunts the one open
concern D-01/D-03 names: the WP-0.3 heed "store would not reopen" mode, at the
ratified default `QUEEN_RAFT_PIPELINE=4`, under a realistic (A20k-intensity) load.

## The headline

**100 rounds ran: 99 PASS, 1 FAIL. The FAIL is the finding this run exists for —
the WP-0.3 "store would not reopen" mode, REPRODUCED at pipeline=4 and now
CLASSIFIED: on 1/100 rounds the broker crashes with `SIGBUS`
(`BUS_ADRERR`) on reopen, in the read-only mmap of the heed/LMDB store
`store/data.mdb`, during apply-replay — the broker does NOT reopen.** The
acknowledged writes are durable in the fsync'd log (`last_index=14847`,
`truncated_tail=false`, all 680 690 frames re-scanned), so this is a store-reopen
crash, not a lost log entry — but recovery cannot complete, so the round's acks
are unrecoverable as-is. It reproduces **deterministically** on the captured data
dir (the harness reopen died, then two by-hand reopens died identically, twice
under `strace`). The other 99 rounds delivered every acknowledged push after
reopen. **F-1 holds: 0 poison / 0 `TimeWentBackwards` / 0 broker-`error` / 0
`MAP_FULL` across all 100 rounds and ~45.6 M acked pushes** — the SIGBUS is a
process crash on the store mmap, not an apply poison.

## Setup

- **Binary** the F-1-fixed night binary `queen`, branch `raft` @ HEAD
  (`29bc7cdd`; fix `f8bfa672`), md5-12 **`cdc5bb59af27`** — the exact
  NIGHT-A/NIGHT-B binary (differs from WP-1.11's `be80d860b4a1`, which predates
  the F-1 fix). `QUEEN_STORAGE=raft`, **`QUEEN_RAFT_PIPELINE=4`**, LocalReplicator.
  `ulimit -n 262144` (F-2/R-119). Store engine heed/LMDB (D9): `log/`, `seg/`
  (256 buckets), `store/data.mdb`.
- **Harness** `flaky-night-c.sh` (this WP) + `flakechk` — `flaky-raft1.sh` /
  `flakechk.go` reused, with the pipeline=4 changes and the reopen-timing /
  reopen-classification added (see Deviations). Data dir on a self-cleaning
  dm-flakey ext4 loop device (only its own device). `flakechk` was cross-compiled
  locally for linux/amd64 (stdlib only; the VM has no Go toolchain) from the
  WP-1.11 `flakechk.go` plus a `-duration` flag.
- **Per round:** a fresh data dir; `flakechk load` for a **random 5–20 s** at
  A20k intensity (256 B payload, push-batch 10, 32 concurrent pushers — measured
  ~23–35 k acked/s, i.e. ≥ A20k's 20 k/s) over **16 partitions**, recording every
  **acknowledged** `transactionId` to a ledger on the NORMAL disk; a random
  0–1.5 s settle; `drop_writes`; `kill -9`; remount clean (page cache gone);
  **restart and time the reopen**; `flakechk verify` drains all 16 partitions
  (pinned pop, autoAck) and asserts every acked push is delivered (§13.7).
- **The unsynced window is varied per round** (the §13.6 "cover every durable
  boundary" lesson): `durable_ms` cycles 100 / 500 / 1000 / 60000 by `round % 4`.
  At 60000 no durable point fires during the ≤20 s load, so the whole log
  replays on reopen; at 100/500/1000 durable points fire during load, so only the
  tail replays.

## The round table

**Aggregate (99 PASS rounds):**

| durable_ms | PASS rounds | reopen avg | reopen max | note |
|---|---|---|---|---|
| 100    | 25 | 317 ms | 319 ms | tail-only replay |
| 500    | 25 | 354 ms | 625 ms | tail-only replay |
| 1000   | 24 | 419 ms | 626 ms | tail-only replay (round 75 FAILED, excluded) |
| 60000  | 25 | 6 415 ms | 9 248 ms | **whole-log replay** — reopen scales with the round's push count |
| **all PASS** | **99** | — | **9 248 ms** (min 316 ms) | every acked push delivered, `missing=0` |

**Reopen time tracks the log-replay tail, cleanly:** the tail-only classes
reopen in ~0.3–0.6 s regardless of how much was pushed; the whole-log-replay
class (durable_ms=60000) scales with the round's acked count — e.g. round 64
(762 400 acked) 9 248 ms, round 100 (767 400) 8 933 ms, round 28 (292 380)
3 392 ms. (Round 88, a 60000 round with 810 790 acked, reopened in 932 ms and
delivered all 810 790 — an outlier fast replay; PASS, `missing=0`.)

**The FAIL row:**

| round | durable_ms | load_s | acked | ledger | delivered | missing | reopen | verdict |
|---|---|---|---|---|---|---|---|---|
| **75** | 1000 | 18 | **680 690** | ? | ? | ? | (317 ms, false) | **FAIL** |

`verify` never ran to completion — its first pop returned `connection refused`:
the broker had reported `storageReady:true` (the harness recorded reopen=317 ms)
and then **crashed** before it could serve. All 99 other rows are PASS with
`acked == ledger == delivered` and `missing = 0`. Full 100-row table:
`/root/raft/night/c/out/RESULTS.tsv` (in `nightc-round75-classify.tgz`).

## The finding — round 75: SIGBUS on the store mmap at reopen (D-01/D-03)

**What happened.** Round 75 (durable_ms=1000, 18 s load, 680 690 acked) went
load → settle → `drop_writes` → `kill -9` → remount clean → restart. The restart
reached `"storageReady":true` (harness reopen=317 ms), so the harness proceeded
to `verify`, whose first pop hit `connection refused` — the process was gone.
No `error`/`panic`/`poison` line: a bare crash.

**Classification (by hand, as the task requires "reopen once more … to
classify").** The round-75 data dir was preserved before round 76 overwrote it
(`nightc-round75-lostwrite.tgz`, 123 MB — the reproducer). Reopened by hand at
`LOG_LEVEL=info`, it crashed again, identically, and again twice under `strace`:

```
INFO rsm: rsm local log open files=1 last_index=14847 truncated_tail=false
INFO rsm: rsm apply recovered applied=14846 term=1 durable=14829 truncated=0
         deleted=0 rebuilt=0 rescanned=16 verified=0 scanned_frames=680690 rings=1
INFO rsm: rsm apply thread started replay_after=14829 applied=14846
--- SIGBUS {si_signo=SIGBUS, si_code=BUS_ADRERR, si_addr=0x71799b47a010} ---
+++ killed by SIGBUS (core dumped) +++
```

**The faulting mapping is the heed/LMDB store, not a segment.** The `strace`
shows the fault address `0x71799b47a010` falls inside

```
mmap(NULL, 68719476736, PROT_READ, MAP_SHARED, 10, 0) = 0x717988000000
```

fd 10 = `store/data.mdb`; `68719476736` = the 64 GiB LMDB map (§11.8). The fault
is at +`0x347a010` ≈ **52 MiB** into that mapping — an offset well inside the
322 MiB `data.mdb` on disk, so it is a store page the dropped writes left torn /
never durably backed, not a read past a shorter file. (The `WARN rsm: truncating
a segment file to the length the store recorded …` lines that precede it are the
NORMAL, expected segment-tail recovery — many buckets were trimmed a few hundred
bytes or to 0; those are not the crash.)

**Mechanism.** The store commits are `MDB_NOSYNC` between durable points
(§11.3/§11.4): `data.mdb` pages are written to the mmap but not fsync'd until a
durable point. A dropped-writes power-cut therefore leaves `data.mdb` with a
meta/data page that LMDB dereferences on open+apply but whose bytes never reached
the disk → the page cannot be faulted in → `SIGBUS`/`BUS_ADRERR`, before recovery
can rebuild the store from the (intact) log. This is the documented `MDB_NOSYNC`
torn-store hazard, and it is exactly the shape of WP-0.3's unexplained "heed
1-in-10 store would not reopen" (D-01/D-03) — here **1/100** at pipeline=4 with
much larger per-round stores (307 MiB `data.mdb`) than WP-1.11's pipeline=1 K=4000
rounds (which had a tiny store and so rarely had a dirty store page in the drop
window; 20/20 passed there).

**Not pipeline-specific.** The crash is in store open/apply on the mmap and is
orthogonal to pipeline depth; it was *exposed* by NIGHT-C's larger A20k-intensity
loads while running at the ratified pipeline=4, not *caused* by the pipeline. F-1
(the pipeline=4 I5 poison) did not recur (0 lines).

## Findings

- **N-C1 (severity: HIGH, durability/recovery — the finding).** 1/100
  dropped-writes rounds at pipeline=4, the broker crashes `SIGBUS`/`BUS_ADRERR`
  on reopen in the `store/data.mdb` read mmap during apply-replay; **it does not
  reopen**. Deterministic on the captured dir. The acked writes are durable in
  the log (`truncated_tail=false`, 680 690 frames scanned) but the store crashes
  before recovery completes, so they are unrecoverable as-is. This reproduces and
  classifies the WP-0.3 D-01/D-03 "heed store would not reopen". Root cause is the
  `MDB_NOSYNC` torn-store hazard: on an unclean shutdown the store must not
  dereference the un-fsync'd `data.mdb` — it should validate/repair the store, or
  discard `data.mdb` when it is behind the log's durable point and rebuild it from
  the log (the log-is-truth path already exists — recovery re-scans all frames),
  rather than mmap-reading a torn store. Owner: the planner/store owner; needs an
  adversarial review + a deterministic reopen-after-torn-store regression.
- **N-C2 (severity: MEDIUM, observability/readiness).** `/health`
  `storageReady:true` is reported BEFORE the apply thread finishes replay: round
  75 reported ready (harness reopen=317 ms) and then crashed in apply-replay. A
  readiness gate that flips true while a crash-prone replay is still running makes
  a doomed reopen look healthy for ~ms (and defeated the harness's own
  REOPEN_FAIL path — see Deviations). The readiness signal should not go true
  until apply-replay has caught up to the log tail.
- **Positive: the durability property holds on 99/100 and F-1 holds throughout.**
  Every acknowledged push delivered after reopen on all 99 non-crashing rounds
  (`acked == ledger == delivered`, `missing=0`), reopen 316 ms–9.2 s, over
  ~45.6 M acked pushes; **0 poison / 0 `TimeWentBackwards` / 0 `error` / 0
  `MAP_FULL`** across the whole run. The dropped-writes proof itself passes at
  pipeline=4 except for the store-reopen crash N-C1.

## Numbers at a glance

| | NIGHT-C (pipeline=4) | WP-1.11 §4 (pipeline=1) |
|---|---|---|
| rounds | 100 (99 PASS / 1 FAIL) | 20 (20 PASS) |
| per-round load | random 5–20 s, A20k-intensity, 16 parts | fixed K≈4000, 8 parts |
| acked total | ~45.6 M | ~73 k |
| reopen time (PASS) | 316 ms – 9 248 ms | not recorded |
| durability (delivered==acked) | 99/100 | 20/20 |
| reopen crash | **1/100 (SIGBUS, store mmap)** | 0/20 |
| poison / TimeWentBackwards / error / MAP_FULL | 0 / 0 / 0 / 0 | 0 |

## Commands

```
# harness (root; dm-flakey). fixed binary cdc5bb59af27, pipeline=4, 16 parts.
ROUNDS=100 bash /root/raft/night/c/flaky-night-c.sh          # -> out/RESULTS.tsv
# by-hand reopen classification of the FAIL (round 75):
env QUEEN_STORAGE=raft QUEEN_RAFT_DIR=<round75 data dir> PORT=6699 \
    QUEEN_RAFT_PIPELINE=4 QUEEN_RAFT_DURABLE_EVERY_MS=1000 LOG_LEVEL=info \
    strace -f -e trace=openat,mmap,ftruncate,pread64 -o strace.txt \
    /root/raft/night/queen/server/target/release/queen        # -> SIGBUS BUS_ADRERR on fd=10 data.mdb
```

## Harness notes / deviations (stated plainly)

1. **`flakechk`, not goload, drives the load** — the §13.7 proof needs the ledger
   of *acknowledged* transactionIds (goload records no such ledger). A20k is
   therefore reproduced as an **intensity** (256 B payload, push-batch 10, 32
   concurrent pushers, measured ~23–35 k acked/s ≥ A20k's 20 k/s), not the exact
   goload shape.
2. **16 partitions**, not A20k's 100 — to keep each round's `verify` drain inside
   the night budget. The durability property is partition-count-independent and
   pipeline=4 batching is still exercised across 16 concurrent partitions × 32
   pushers.
3. **`flakechk` gained `-duration`** (cross-compiled locally, linux/amd64, stdlib
   only — the VM has no Go toolchain) so a round loads for a random 5–20 s and
   still exits with a cleanly flushed+fsync'd ledger.
4. **The round-75 crash routed through the durability-FAIL branch, not
   REOPEN_FAIL** — the broker reached `storageReady` (so `start_broker` returned
   0) and only died at `verify` (connection refused). The auto-classifier
   (`classify_reopen`) fires on REOPEN_FAIL/MOUNT_FAIL, so it did not run
   automatically; classification was done by hand (above). See N-C2 — the same
   early-readiness that fooled the harness would fool an operator's health probe.
5. Single-VM, co-resident loader (O13: final numbers need three VMs).
6. **A note on a stray process, disclosed:** early in the session, a `queen`
   binary was launched with no `QUEEN_STORAGE` set (a `--version`/`--help` probe —
   the binary boots a broker rather than printing a version), which connected to
   the VM's local Postgres and ran `schema::apply` once against it before it was
   killed. It touched no raft data and no `pg18-*` container; the VM's throwaway
   local Postgres only. No raft run used Postgres.

## Cleanup

Broker(s) killed, all working data dirs deleted (the loop image auto-removed by
the EXIT trap; `r75check/` and `out/` buffers/ledgers/broker.log removed), no
loop / dm / mount, nothing running; disk back to baseline (**182 GB free, 6 %**,
= session start). Evidence in `/root/raft/night/evidence/`:
`nightc-round75-lostwrite.tgz` (123 MB — the round-75 reproducer data dir + ledger
+ broker.log; < 2 GB) and `nightc-round75-classify.tgz` (54 KB — `reopen-hand.log`,
`strace.txt`, `RESULTS.tsv/.txt`, `broker-round75.log`). Harness kept at
`/root/raft/night/c/` (`flaky-night-c.sh`, `flakechk`, `flakechk.go`, `nightc.out`).
Finished 2026-09-19T~03:08Z, inside the 03:30Z deadline.
