# S1 decision memo — which embedded ordered store carries the RSM (D9)

WP-0.3 of PLAN_RAFT.md. Candidates: **redb 2.6.3**, **fjall 2.11.2**,
**heed 0.22.1** (LMDB 0.9). Written 2026-09-17 from two measurement campaigns:
`RESULTS-laptop.md` (macOS, smoke: relative cost, reclamation, iteration, 10
kill runs per engine) and `RESULTS-vm.md` (Linux VM, the numbers that count:
9-cell matrix, 45 kill runs, 20 dropped-write runs, a 10-minute soak).
**Revised 2026-09-18** after two adversarial reviews came back `refuted` (one
*major*, one *blocker*) and a third measurement round answered part of them:
`RESULTS-refutation.md` (Linux VM, the ratified store shape, the LMDB flag and
reader mode that had never been measured). §6 lists every finding and what
happened to it. The harness is this crate; every command line is in those three
files.

**Recommendation in one line (revised 2026-09-18): keep heed/LMDB for D9 but pin
its configuration and its read-path rule, make redb — not fjall — the documented
alternative and the engine for single-voter / embedded (D2, raft1), and change
§11.5 so recovery repairs short segment files from the Raft log instead of
discarding the state directory.** What changed and why is in §6.0; the reasons,
numbers and residual risks are below. D9 was ratified on 2026-09-18 while its
own stated precondition was still unrun (R1.1), so this memo treats the pin as
provisional until the two runs of §6.9 exist.

---

## 1. The mandatory criterion first (WP-0.3: "PASS/FAIL, not if feasible")

> after kill -9 during non-durable commits AND after dropped unflushed writes,
> the applied index the reopened store reports and the segment bytes it
> references agree (I11); record for each engine whether it reopens past its
> last durable commit after a process crash.

| engine | `kill -9` (Linux, 15 runs) | dropped unflushed writes (dm-flakey) | reopens past its last durable commit | verdict |
|---|---|---|---|---|
| **redb 2.6.3** | **15/15 PASS**, 0 torn | **5/5 PASS** | **never** (0/15 kill, 0/5 flaky, 0/10 on macOS) | **PASS** |
| **fjall 2.11.2** | **15/15 PASS**, 0 torn | **1/5 PASS, 4/5 FAIL** | **yes**: 14/15 kill, 4/5 flaky | **FAIL** |
| **heed 0.22.1** | **15/15 PASS**, 0 torn | **9/10 PASS, 1/10 would not reopen at all** | after kill -9 yes (12/15); after dropped writes **never** (0 of the 9 that reopened) | **PASS with a caveat** |

**Read that table with its sample sizes in view, because they are small and
WP-0.3 asked for larger ones.** 15 kill runs against the 100 the work package
mandates, and 5 dropped-write runs per engine (10 for heed) against 20. Worse,
the kill half cannot falsify anything: `RESULTS-vm.md` §2 says it itself — "a
process kill leaves the page cache intact" — and indeed all 45 kill runs passed
for all three engines. **Every bit of discrimination above comes from 20
dm-flakey runs**, 5 + 5 + 10. On n = 10 a single observed failure has a 95%
interval of about 0.3%–45%: these runs cannot tell "1 in 10" from "1 in 3", and
they cannot tell 0/10 from a rate of a few percent. And the dm-flakey runs are
20 s long on a VM whose `dirty_expire_centisecs` is 3000, so almost nothing
reaches the platter except at each durable point's explicit `force_sync` —
"heed never reopened past its durable point after dropped writes (0/10)" is
therefore partly a property of the run length, not only of LMDB
(`RESULTS-refutation.md` §5). The table is the best evidence there is; it is not
strong evidence.

What the failures look like (raw lines in `RESULTS-vm.md` §3):

- **fjall**: the reopened state is ahead of the durable point (journal records
  that reached the platter replay) while the segment files are short of what it
  references — `shortfile=256…1700` rows whose `(file, offset, len)` lies past
  the end of the file, `beyond_recorded=1085…1891`, `missing=0 badsum=0
  torn=false`. The bytes are not corrupt, they were never written: a segment
  file is fsynced only at a durable point.
- **heed**: 9 runs came back exactly at the durable point with every referenced
  byte present (LMDB's meta page on the platter is the one `mdb_env_sync` put
  there). One run would not open at all and left no error text. A second 5-run
  loop with the exit status captured did not reproduce it, so **the failure mode
  is unexplained** (see §5, risk 2). The first version of this memo called it
  "the corruption LMDB's own documentation warns about for that flag"; that was
  wrong for this configuration, and R1.5 is right to say so. `lmdb.h` 590–602
  warns about corruption under MDB_NOSYNC in general and then narrows it:
  "**However, if the filesystem preserves write order and the #MDB_WRITEMAP flag
  is not used**, transactions exhibit ACI […] and only lose D." The harness opens
  without `WRITE_MAP`, so for this env the header predicts integrity — unless
  ext4 did not preserve write order, which would be a production hazard rather
  than a test artifact. Either way the cause is open. `lmdb.h` 582–589 documents
  a flag with no such conditional, **MDB_NOMETASYNC** ("maintains database
  integrity, but a system crash may undo the last committed transaction" — which
  is §11.4's durable-point semantics exactly); it was never measured before
  2026-09-18 and its cost is now in `RESULTS-refutation.md` §3.
- **redb**: `Durability::None` never publishes a crash-visible root, so the
  store always comes back at its last durable commit. Recovery can never see
  state ahead of the files. 20/20 runs across both fault types, on both hosts.

**This is the finding that should drive D9, and it is not only about engines.**
Two of the three candidates can reopen with committed state whose payload bytes
were never fsynced, and **neither fjall nor LMDB can be rolled back** to the
durable point. As §11.5 stands today the node must then discard its state
directory and install a snapshot (raft3) or refuse to start (raft1) — with
fjall that is 4 power losses in 5, and in raft1 / embedded mode (D2) there is no
snapshot source, so it is data loss.

### A repair that makes the choice much less sharp

The entries above the durable point are still in the **Raft log**, which is
fsynced on append and purged only behind snapshots (§11.4), and their effects
carry the payload bytes: the `Append` effect is
`pid, bucket, base_offset, count, created_at, hashes[16×count], blob` (§5.2) —
the blob is in the log, the segment file is only a local copy of it. Apply is
deterministic (I2) and every crash left the store state a consistent, untorn
prefix of entries (torn=0 in all 45 kill and 20 flaky runs). So recovery can
**replay the log tail into the segment files** instead of discarding the state:
for every entry above `durable_index`, rewrite its payload frames and fix the
node-local `seg_loc` (positions are node-local, D8/I7, so rewriting them at
different offsets is legal). Proposed change to
§11.5 step 3: a short or missing frame above the last durable point is repaired
from the log; only a frame that is missing *below* it, or corrupt with the log
already purged, forces the snapshot install. This is a design change for G0,
not something this spike can ratify — **and as of 2026-09-18 it has not been
made: §11.5 step 3 in the ratified plan still says "discard the state directory
and install a snapshot (raft3), or refuse to start (raft1)".**

**This repair is not symmetric between the two engines that need it, and the
first version of this memo blurred that.** The repair fixes exactly one failure
class: *state ahead of the durable point, store intact*. That is fjall's
measured failure — `flaky-fjall-20260917-141859.log` run 5,
`shortfile=1700 beyond_recorded=1568 missing=0 badsum=0 torn=false`: the store
opens fine, the frames it names are simply not there, and the log has them.
Under the repair, fjall's 4-in-5 becomes recoverable and its criterion failures
go to 0. heed's measured failure is a different class:
`flaky-heed-20260917-142056.log` run 4, `VERIFY DID NOT RUN (store would not
reopen)`. There is nothing for a log-tail repair to repair — the state store is
gone. So the sentence in §4 item 4 that this repair "is what makes a 1-in-10 or
4-in-5 'reopened past the durable point' tolerable at all" conflated the two:
heed's 1-in-10 was **not** a past-durable reopen (heed is 0/10 past-durable
after dropped writes). Corrected: **with the repair adopted, fjall's failures
become recoverable and heed's does not.** That is the opposite of the ranking
the first version drew from it, and it is R1.3 and R2.6's point.

The scenario that follows is concrete and it is D2's default for embedded:
**one voter, raft1, power loss.** There is no peer and no snapshot source, so
§11.5 step 3's second branch — refuse to start with a clear error — is the only
outcome, and the data is gone. heed reaches that state 1 time in 10 in the only
samples that exist; redb cannot reach it at all.

---

## 2. Every measured quantity, per engine

Linux VM, 90 s per cell, batch 10, payload 512 B, 4096 partitions, KV mix,
durable point every 1 s, 1 fsync thread, `--segment-bytes 262144`. Full tables,
including the 50k cells and the macOS comparison, in `RESULTS-vm.md` §1 and
`RESULTS-laptop.md`.

> **⚠ This table is the PRE-G0 store shape and three of its rows are now
> superseded.** It was measured on 2026-09-17 with one store write transaction
> **per applied entry**, with `segments` rows in the store, and with no dedup
> keyspace. G0 (2026-09-18) amended §11.3 to commit every 4 ms / 256 entries,
> took `segments` out of the store (§6.1) and ratified D10 option (a), a dedup
> index of uniformly random `(pid, hash)` keys. Re-measured in that shape
> (`RESULTS-refutation.md` §2): **fjall and heed are unmoved** (same rates ±2%,
> same write amplification ±0.6 points), but **redb is not**: 20k → 19 466 msg/s
> (97% of offered, was 10 046), 50k → 25 895 (was 10 260), kernel write
> amplification **8.66x** (was 29.31x). A 2×2 isolates the cause to the commit
> cadence and not to the keyspaces. Read the "achieved" and "write
> amplification" rows for redb below as historical.

| | redb 2.6.3 | fjall 2.11.2 | heed 0.22.1 (LMDB) |
|---|---|---|---|
| achieved at 20k / 50k / 100k msg/s | **10 046 / 10 260 / 9 909** | **20 000 / 49 905 / 67 641** | **20 000 / 48 783 / 58 958** |
| non-durable commit p50 / p99 (20k) | 0.399 / 0.543 ms | **0.047 / 0.295 ms** | 0.065 / 0.122 ms |
| durable point p50 (20k / 50k) | 1409 / 1311 ms | **360 / 573 ms** | 475 / 901 ms |
| of which segment fsyncs | 336 / 311 ms | 352 / 541 ms | 344 / 573 ms |
| ⇒ **the engine's own durable commit** | **≈ 1073 / 1000 ms** | **≈ 8 / 32 ms** | ≈ 131 / 328 ms |
| write amplification, kernel counter | **29.3x** | **1.35–1.38x** | 2.80–3.30x |
| store bytes per byte of its own rows | 8.7–8.9x | **0.28–0.30x** (lz4 on very compressible synthetic values) | 0.87–0.88x |
| RSS max (20k / 50k / 100k) | 250 / 251 / 250 MiB | 155 / 554 / **717 MiB** | 182 / 445 / 528 MiB (page cache of the mmap) |
| RSS over a 10-min soak at 50k | not run | **4 → 986 MiB, still rising** | not run |
| KV get p99 / prefix list p99 | 24 / 41 µs | 32–38 / 97–119 µs | **2–3 / 11 µs** — single-threaded, with heed's default thread-local reader slots. In the mode where `RoTxn` is `Send` the read path does not scale past one core (§3, `RESULTS-refutation.md` §4) |
| ordered scan of `segments` (**a keyspace §6.1 no longer keeps in the store**) | 10.5–11.5 M rows/s | 2.1–4.8 M rows/s (1.35 M/s at 2.4 M rows) | **42–44 M rows/s** — over 180 001 rows / **9.6 MiB, fully in page cache**; heed was never scanned beyond that and never soaked |
| consistent export (snapshot §11.6) | logical dump, 181–265 MiB/s | logical dump, 91–123 MiB/s (1.1 GiB in 12.1 s in the soak) | **`mdb_env_copy`, 524–556 MiB/s** |
| incremental export possible? (**I8 says S1 "prefers engines with incremental checkpoints"**) | no | **yes in principle** (immutable SSTs; no checkpoint API in 2.11) | **no, always O(state)** — the recommendation below is made *against* this stated preference, which the first version of this memo failed to say (R2.7) |
| writer pause for the export | none | none | none (but a long read txn pins the free list → the file grows) |
| chunked delete (older half of the 90 s run) | 316 ms / 52.6k rows | 145 ms / 92.8k rows; 1.78 s / 540k rows in the soak | 178 ms / 91.8k rows |
| space actually given back | `compact()` 855 → 146 MiB, **843 ms stop-the-world** | `major_compact` 55 → 40 MiB, **1.3 s at 90 s scale, 12.3 s in the soak** | **never in place** (free list reused); compacting copy 116–305 ms |
| reopen: clean / after kill -9 | 1–2 ms / 57–465 ms | 3–5 ms / 234–967 ms (journal replay) | **0–2 ms / 0–70 ms** |
| build dependencies (C-2) | pure Rust, no build script | pure Rust, no build script | `cc` compiles LMDB's 2 C files; `doxygen-rs` rewrites doc comments; **no cmake**, no bindgen by default |
| MSRV (C-3 = 1.88) | 2.6.3 is the newest that builds on 1.88 (2025-08-23); 3.1.3 declares 1.89, 4.x 1.90 | 2.11.2 declares 1.76; 3.x needs 1.90 | declares none; builds on 1.88 (`results/msrv-1.88.log`) |
| license | MIT OR Apache-2.0 | MIT OR Apache-2.0 | heed MIT, lmdb-master-sys Apache-2.0, LMDB itself OpenLDAP Public License |
| maintenance (2026-09-17) | 100+ commits/3 mo, 7 open issues, **one maintainer**, 2.x line is EOL | 40 + 33 commits/3 mo (fjall + lsm-tree), 41 open issues, **one maintainer**, young (first release 2023-12) | **0 commits/3 mo** (frozen C engine, last heed release 2026-04), 51 open issues, Meilisearch team |

### The same cells in the ratified store shape (2026-09-18, 40 s per cell)

§11.3 batched commits (4 ms / 256 entries), no `segments` rows, a `dedup`
keyspace of random `(pid, hash)` keys. Full table and the isolation 2×2 in
`RESULTS-refutation.md` §2.

| | redb 2.6.3 | fjall 2.11.2 | heed 0.22.1 | heed, MDB_NOMETASYNC |
|---|---|---|---|---|
| achieved at 20k / 50k | **19 466 / 25 895** | **20 000 / 49 019** | **20 000 / 47 425** | 17 309 / not run |
| store commit p50 / p99 (20k) | 3.52 / 32.26 ms | **0.61 / 9.47 ms** | 0.66 / 11.78 ms | 7.04 / **122.88 ms** |
| store commits held per second (20k) | 51 | 138 | 134 | 18 |
| ⇒ the engine's own durable commit (20k) | ≈ 336 ms | **≈ 17 ms** | ≈ 139 ms | **≈ 0 ms** |
| write amplification, kernel counter | 8.66 / 7.88x | **1.33x** | 3.91 / 3.00x | 7.04x |
| RSS max (20k / 50k) | 260 / 261 MiB | **129 / 195 MiB** | 163 / 349 MiB | 153 MiB |
| KV get p99 / prefix list p99 (20k) | 21 / 40 µs | 31 / 126 µs | **4 / 13 µs** | 7 / 17 µs |
| ordered scan (`seg_loc`) | 7.1–7.4 M rows/s | 2.1–2.7 M rows/s | **41.5 M rows/s** | 41.4 M/s |
| reopen, clean | 1 ms | 231–457 ms | **0–1 ms** | 4 ms |

fjall and heed are within ±2% of their legacy-shape rates; **redb is a different
engine here** (§3). The crash verdicts of §1 were measured in the legacy shape
and were not re-run — §6.9 lists that as the first thing WP-1.2 needs.

Two measurements belong to the design, not to any engine:

- **The durable point is the segment-file fan-out.** 293–484 file fsyncs per
  durable point (all 256 buckets every second, plus the files that rolled), at
  ≈1.2–1.8 ms each on this ext4 = **340–580 ms of every second**, for all three
  engines. §11.4 must bound how many buckets a durable point may touch, or
  entries must land in fewer buckets between durable points. No engine choice
  fixes this.
- **Nothing reached 100k msg/s** in a single apply thread with one write
  transaction per entry (68k fjall, 59k heed, 10k redb).

---

## 3. Reading of the three

**redb 2.6.3 — the best crash story, and the engine this memo was wrong to drop
(rewritten 2026-09-18).** It is the only candidate that passes the mandatory
criterion outright, and its recovery is trivial (§11.5 collapses to "replay from
the durable index"). The first version of this memo then dropped it on a ~10 000
msg/s ceiling and 29x kernel write amplification, and wrote its own escape
clause: "it deserves a re-measurement with a longer durable interval and fewer
keyspaces before anyone believes the ceiling is final". **G0 fired that clause
and the re-measurement now exists** (`RESULTS-refutation.md` §2). One write
transaction per applied entry is indeed exactly what copy-on-write B-trees are
worst at — and §11.3 as amended no longer does it. At 20k, 40 s, the same VM:

| redb 2.6.3, 20k target | per-entry commit | §11.3 batched (4 ms / 256) |
|---|---|---|
| legacy keyspaces | 10 046 msg/s, WA 29.31x | **19 539 msg/s, WA 8.33x** |
| ratified keyspaces (dedup, no `segments`) | 9 843 msg/s, WA 26.81x | **19 466 msg/s, WA 8.66x** |

The cadence explains the entire effect; the keyspaces explain none of it. So
"the only one that cannot carry the load" is no longer true: redb clears the 20k
target at 97% and reaches 25 895 msg/s at the 50k target. What is still true is
weaker and narrower: it is **roughly half of fjall and heed above 25k** (25.9k
against 47–49k at the 50k target), it writes **8.3–8.7 bytes per logical byte**
against fjall's 1.3 and heed's 3.0–3.9, its store commit at the 50k target has a
p50 of 29.7 ms (against 1.7–1.8 ms), and its 2.x line gets no more releases
while 3.x needs MSRV 1.89 and 4.x needs 1.90. That is a capacity argument for
the 3-voter broker, not a disqualification — and for the profile where the
mandatory criterion bites hardest, single-voter / embedded (D2, raft1, no
snapshot source), 19.5k msg/s at 97% of offered is more than that profile asks
for. See §4 item 2.

**fjall 2.11.2 — the fastest writer, the weakest recovery, and the memory
question.** Best throughput (68k at the 100k target), best write amplification
(1.35x), an essentially free durable commit (8–32 ms), immutable SSTs (the only
incremental-snapshot story, I8), pure Rust. Against it: it **fails the mandatory
criterion 4 times in 5**; its `major_compact` is a single-threaded
stop-the-world pass that took **12.3 s** in the soak (on the apply thread, the
only mutator, that is a 12-second node stall); its ordered scans are 10–20x
slower than LMDB's, which is what the digest (§12.9), chunked deletes, ready-ring
rebuilds and logical exports pay; and its **RSS climbed from 4 MiB to 986 MiB in
ten minutes at 50k msg/s and was still rising**, with the delete phase not
lowering it — G-3 and I8 ask for RAM bounded independently of retained volume
and a 10-minute soak cannot yet show that fjall gives it.

**heed 0.22.1 / LMDB — the most predictable, with explicit operational rules
(qualified 2026-09-18).** It never reopened past its durable point after dropped
writes (0/10, in 20 s runs — see §1's caveat), its ordered reads are 15–20x the
others' (41.5 M rows/s in the ratified shape, 4 µs gets, 13 µs prefix lists), it
has a native consistent export at 556 MiB/s for §11.6, and it reopens in 0–4 ms
clean and under 70 ms after a kill. Two claims the first version made for it do
not survive the refutation as written:

- **The read numbers are true in one reader mode only.** They were taken
  single-threaded with heed's default `EnvOpenOptions::new()`, i.e. `WithTls`
  (env_open_options.rs:29,39,462), where `RoTxn` is **not `Send`**
  (txn.rs:237 implements `Send` only for `WithoutTls`): it cannot cross an
  `.await` or move between tokio workers, and a second read transaction on one
  thread fails outright — measured, `MDB_BAD_RSLOT` on the 2nd of 200
  (`RESULTS-refutation.md` §4). The mode that lifts both, `read_txn_without_tls()`
  (MDB_NOTLS), **does not scale past one core**: 2.06 / 1.97 / 2.08 M gets/s at
  1 / 4 / 8 threads on the VM, against 2.10 / 8.18 / 9.96 M with TLS, because
  `mdb_txn_renew0` then takes `env->me_rmutex` and scans the reader table on
  every begin. Plus an unexercised ceiling: `max_readers` defaults to 126 and
  `MDB_READERS_FULL` is a hard error on a read path D15 makes ordinary. heed is
  still the fastest reader here **by a lot**, but only under a rule the design
  has to adopt (§4 item 1b).
- **"Its memory is page cache the kernel can reclaim rather than heap" is a
  mechanism argument, not a measurement.** §2's own row reads "RSS over a 10-min
  soak at 50k: fjall 4 → 986 MiB still rising; heed **not run**". At 40 s in the
  ratified shape heed's RSS max is *higher* than fjall's (163 vs 129 MiB at 20k,
  349 vs 195 at 50k). Neither figure says anything about G-3's "RSS flat ±5%
  across 60 minutes"; for an mmap engine RSS is resident mapped pages and climbs
  with the touched working set. Until the 2 h soak of §6.9 exists, this reason
  should not be used.

The rest of the case against it stands and grows slightly: 3.0–3.9x write
amplification in the ratified shape (the dedup keyspace's random keys cost it
3.30x → 3.91x at 20k), a 139–197 ms store commit at the durable point, a file
that is a high-water mark (no in-place compaction), a map size fixed at open
(and no backpressure anywhere in the plan for `MDB_MAP_FULL`, risk 7), no
incremental checkpoint although I8 prefers one, a C dependency (cc only — C-2 is
satisfied, and the build image question is now closed: both
`/Users/alice/Work/queen/Dockerfile` and `server/Dockerfile` build on
`rust:1-bookworm`, which carries gcc), an upstream with no commits in three
months, and the **one run in ten that would not open at all**, still unexplained.

---

## 4. Recommendation for D9 (revised 2026-09-18)

1. **Engine for the 3-voter broker: heed 0.22.1 (the thin wrapper over LMDB),
   with its configuration pinned.** The unqualified "heed" of the first version
   was a recommendation for a configuration nobody had measured. Pin three
   things with it, or the numbers it was chosen on are not the numbers it will
   deliver:

   a. **`MDB_NOSYNC`, not `MDB_NOMETASYNC`, until the crash evidence says
      otherwise.** NOMETASYNC is the flag whose documented guarantee matches
      §11.4 with no conditional, and it was tempting; measured, it costs 13% of
      the 20k rate (17 309 vs 20 000 msg/s), 1.8x the kernel writes (7.04x vs
      3.91x) and a store-commit **p99 of 123 ms** on the apply thread, the only
      mutator. That is a node stall, not a capacity cost. Its crash behaviour
      under dropped writes has not been measured either, so it buys a documented
      promise and an unmeasured one at the same time. Decide it with §6.9's
      flaky loop, not on the header text.
   b. **A read-path rule, in §11.3 and WP-1.2: a local read begins and ends
      inside one blocking call, on a thread that keeps its LMDB reader slot;
      a `RoTxn` is never held across an `.await`; `max_readers` is set to at
      least the blocking pool size and `MDB_READERS_FULL` is a refusal, not a
      panic.** With thread-local slots (the fast mode) a second *transaction* on
      one thread is `MDB_BAD_RSLOT` — measured, the 2nd of 200. That is a
      constraint on the store adapter, not on the read shape: the scan-then-
      lookup the planner and D15 want is legal as long as the scan and the
      lookups share **one** `RoTxn` (LMDB allows many cursors in one read txn),
      which means `rsm/store/` must expose a read-transaction handle rather than
      the free-standing `get` / `scan` this spike's trait has — that trait is
      exactly why the harness had to work around it (`src/main.rs` ≈776). Get
      that seam wrong and the design lands on MDB_NOTLS, where the read path
      does not scale past one core (§3).
   c. **Keep the `rsm/store/` adapter seam** (item 6 below). It is the only
      reason this memo could be revised instead of rewritten, and it is what
      makes item 2 a real option rather than a sentence.

2. **Documented alternative: redb 2.6.3, not fjall — and redb is the engine to
   reach for in single-voter / embedded mode (D2, raft1).** This reverses the
   first version. Three facts moved it: (i) redb's exclusion rested on a ~10k
   msg/s ceiling that the ratified write shape removes (19 466 msg/s at the 20k
   target, WA 29.31x → 8.66x, isolated to the commit cadence by a 2×2, §3);
   (ii) redb is the only candidate that passes the mandatory criterion, and it
   passes it **by construction** rather than by observation — `Durability::None`
   never publishes a crash-visible root, so it cannot reach the state §11.5's
   escape hatch is written for, while heed's 0/10 is 10 short samples and its
   1/10 unopenable store is unexplained; (iii) in raft1 there is no snapshot
   source, so an unopenable store is data loss and no log-tail repair can touch
   it. redb's remaining cost is capacity (≈26k msg/s against 47–49k) and write
   amplification (8.3–8.7x), and embedded is exactly the profile that does not
   need the capacity.

3. **fjall 2.11.2 stays on the list, but behind redb**, and only if §11.5's
   log-tail repair lands: it fails the mandatory criterion 4 times in 5, and the
   repair is what makes that recoverable (which, unlike for heed, it genuinely
   is — §1). Reconsider it for the high-rate profile if (a) a 2-hour soak shows
   its RSS plateaus (10 minutes at 50k took it from 4 to 986 MiB, still rising),
   and (b) §11.7's compaction can be made incremental and budgeted so no
   `major_compact` ever runs on the apply thread (12.3 s in the soak). Its write
   cost really is 2–3x better than anyone else's.

4. **Change §11.5 (and I11) to repair rather than discard**: replay the Raft log
   tail into short segment files above the durable point; discard-and-install
   only below it. **Decide this FIRST, before WP-1.2 freezes the engine**,
   because it changes the ranking: with the repair, fjall's 4-in-5 becomes
   recoverable and heed's 1-in-10 unopenable store does not (§1). §11.5 step 3
   is unchanged in the ratified plan, so today the escape hatch is still
   "discard or refuse to start", and in raft1 that is "refuse to start".

5. **Bound the durable point's bucket fan-out in §11.4** (352–573 ms of every
   second in the ratified shape, unchanged by the store), independently of the
   engine.

6. **Write the engine behind the `rsm/store/` adapter this crate already
   models** (now 14 keyspaces with `dedup`, a batched write txn per §11.3,
   durable commit, ordered scan, export, maintenance). The spike kept all three
   engines behind one trait with no contortions, and adding a second LMDB reader
   mode and a second flag set cost one enum.

7. **New, and not an engine question: give §11.8 a map-size rule.** LMDB's limit
   is the map size fixed at open, and §11.8 gates only on disk usage percent.
   `MDB_MAP_FULL` on the apply path is a node-local liveness cliff the leader
   cannot see: report LMDB map usage in `Status` beside disk percent, gate the
   planner on the highest map usage of any voter, bound read-transaction
   lifetime (a long read txn pins the free list and grows the file — and §11.6
   step 3's `mdb_env_copy` runs inside one), and define what a node does when
   apply hits `MDB_MAP_FULL`. Without that rule, item 1 buys a failure mode with
   no backpressure.

**D9 was ratified on 2026-09-18 with this memo's own precondition unrun.** §4 of
the first version ended: "Before G0 ratifies this, one cheap run is worth waiting
for: 20 dropped-write runs on heed". `RESULTS-vm.md` §7 lists it as deferred, it
is not in `results-vm/`, and PLAN_RAFT.md §2 demoted it to "residual risk to
close in WP-1.2". That is a conditional recommendation ratified without its
condition. §6.9 says what has to exist before WP-1.2 writes a line of
heed-specific code.

## 5. Residual risks

1. **The escape hatch becomes the normal path.** With fjall, ~80% of power
   losses leave state referencing bytes that were never written; with heed that
   number was 0/10, but 1/10 left a store that would not open. Until §11.5 can
   repair from the log, both mean "discard the state directory and install a
   snapshot", which in raft1 / embedded (D2) is refuse-to-start.
2. **heed's unexplained failure — and the inference that named it was wrong.**
   One run in ten left no error text and the rerun did not reproduce it. The
   guess was a torn meta page under `MDB_NOSYNC` with the process faulting on
   the mmap. `lmdb.h` does not support that for this env: without
   `MDB_WRITEMAP`, and on a filesystem that preserves write order, NOSYNC
   "preserves the ACI … but not D" (590–602). So either the cause is something
   else, or ext4 on this VM did not preserve write order — which would be a
   production hazard, not a test artifact. **The failure has no name and it is
   the single fact that decides D9 against redb.** A faulting mmap would also
   kill the whole process, including the parts I15 wants responsive.
3. **LMDB's operational rules.** Map size fixed at open (`MDB_MAP_FULL` until a
   restart with a bigger map); the file never shrinks, so §11.8's disk
   thresholds must be read against a high-water mark; a long read transaction
   pins the free list and grows the file while it lives (§11.6 already forbids
   holding one across a transfer). None of the three was measured: the file
   growth under a long read txn, the map-full path, and the reader-slot table
   are all named and untested except for the slot ceiling, which now is
   (`MDB_READERS_FULL` at `max_readers`, §3). See §4 item 7: today `MDB_MAP_FULL`
   on the apply path stops that node (I16) with nothing upstream to slow the
   leader down, and two such nodes lose quorum.
4. **A C dependency and a frozen upstream.** `cc` only, no cmake (C-2 holds).
   The build-image question is **closed**: both `/Users/alice/Work/queen/Dockerfile`
   (`server-builder`, `kafka-builder`, `sqs-builder`, `s3-builder`) and
   `server/Dockerfile` build on `rust:1-bookworm`, a glibc image with gcc, so
   nothing has to change; DEPS.md's "verify before relying on it" is answered.
   An LMDB bug is still ours to carry, against an upstream with no commits in
   three months.
5. **MSRV (C-3).** Both surviving candidates are pinned to older releases to
   hold 1.88 (redb 2.6.3 is a year old, fjall 2.x is superseded by 3.x). Heed
   0.22.1 declares no MSRV, which means nothing stops a future release from
   raising it silently. The plan should decide whether the server moves to 1.90.
6. **Every number here is short, and two pillars rest on the shortest ones.**
   90 s cells (40 s in the 2026-09-18 round), a 10-minute soak, 15 kill runs,
   20 dm-flakey runs in all. Two specific holes are load-bearing: heed's
   ordered-read pillar is established over **9.6 MiB of `segments` fully in page
   cache** (`heed-20000.log`: 180 001 rows, 9.6 MiB, 0.00 s) and heed was never
   scanned beyond 180k rows and never soaked, while G-3 targets hundreds of
   millions of messages and LMDB's mmap scan is exactly what degrades past RAM;
   and heed's RAM pillar has no long-run evidence at all. Also unmeasured:
   fjall's compaction backlog over hours, LMDB's file growth over hours, the
   durable-point cost once the store is tens of GiB, the dedup keyspace at its
   real size (~72 M random keys at A20k with a 1 h window — the 2026-09-18 round
   wrote 800k of them into a 145 MiB file that never left page cache), the
   10M-key iteration for redb, three brokers sharing one disk (§13.6).
7. **The harness is not the broker.** One thread, a synthetic key distribution
   over 4096 partitions, and a
   payload mix that is far more compressible than real messages (fjall's lz4
   gains are flattered). The relative ordering it produced is credible; the
   absolute rates are not a broker throughput prediction.

---

## 6. Refutations (2026-09-18)

Two adversarial reviews of the 2026-09-17 version came back `refuted`: one
*major* (10 findings, R1.*) and one *blocker* (9 findings, R2.*). **Every
finding is upheld.** Not one was wrong on its facts, and I could not defend a
single one of them away. What follows is each finding, what was done about it,
and what is still open. The measurements are in `RESULTS-refutation.md`; the
corrections are in §1–§5 above and in `RESULTS-vm.md`.

### 6.0 What changed in the recommendation, in three lines

- **redb is back**, as the documented alternative and as the engine for
  single-voter / embedded (D2, raft1). It was dropped on a throughput ceiling
  that the G0 amendment to §11.3 removes — measured, not argued (R2.1).
- **heed stays for the 3-voter broker, but pinned**: `MDB_NOSYNC`, thread-local
  readers, and a read-path rule without which its read numbers do not exist
  (R1.6). Its "RAM bounded" and "ordered reads win" pillars are demoted to
  unproven until the 2 h soak (R1.7, R1.8, R2.7).
- **The §11.5 log-tail repair has to be decided before WP-1.2 picks an engine**,
  because it helps fjall and does nothing for heed — the opposite of what the
  first version claimed (R1.3, R2.6).

### 6.1 Findings fixed by re-measurement

| id | finding | what was done | outcome |
|---|---|---|---|
| **R2.1** | The comparison measures a design that no longer exists: §11.3 now commits every 4 ms / 256 entries, and redb was dropped on the per-commit cost a 256:1 batch amortizes | Added `--store-commit-ms` / `--store-commit-entries` and re-ran the redb decision cells on the VM, plus a 2×2 that separates cadence from keyspaces (`RESULTS-refutation.md` §2) | **Upheld, and it changes the answer.** 10 046 → **19 539** msg/s, WA 29.31x → **8.33x**, with the same keyspaces; ratified keyspaces at per-entry cadence reproduce the old numbers (9 843, 26.81x). §3 and §4 item 2 rewritten; redb re-admitted |
| **R2.2** | The ordered-scan pillar is measured on `segments`, which the §6.1 amendment removed from the store | Added `--shape ratified` (no `segments` rows) and re-ran the matrix; the equivalent walk is now over `seg_loc` | **Upheld.** heed 41.5 M rows/s vs fjall 2.1–2.7 M — the 15–20x gap survives the keyspace change, so the *conclusion* holds, but the number in §2's table was measured on a table that will not exist. §2 carries a warning header |
| **R2.3** | The store's new dominant keyspace (D10 option (a): random `(pid, hash)`) was never in the harness | Added a `dedup` keyspace, one row per message, uniformly random 16-byte keys, with an optional window prune; it is on in `--shape ratified` | **Upheld, partly closed.** It costs heed 3.30x → **3.91x** write amplification at the same rate and roughly doubles its store growth rate. But 800k keys in a 145 MiB file is still entirely page-cached; the real regime (~72 M rows) is unmeasured and is now named in risk 6 |
| **R1.5** | The spike measured one LMDB configuration and never MDB_NOMETASYNC, and the memo's causal claim is not what `lmdb.h` says for a non-WRITEMAP env | Added `--heed-flags`; ran NOMETASYNC on the VM; quoted both header paragraphs exactly (`RESULTS-refutation.md` §3) | **Upheld.** The wrong causal claim is removed from §1 and risk 2. NOMETASYNC measured: **17 309 msg/s (87%), WA 7.04x, store commit p99 123 ms** — real cost, and its crash behaviour is still unmeasured, so §4 item 1a keeps NOSYNC for now |
| **R1.6** | heed's read numbers come from a thread-local reader configuration the async broker cannot adopt; `max_readers` never exercised | Added `--heed-no-tls` / `--heed-max-readers` and a `reads` subcommand with N threads and a reader-slot probe (`RESULTS-refutation.md` §4) | **Upheld, and the measurement is worse than the finding guessed.** In the `Send`-capable mode heed's read path **does not scale at all** (2.06 / 1.97 / 2.08 M gets/s at 1 / 4 / 8 threads) against 2.10 / 8.18 / **9.96** M with TLS. `MDB_BAD_RSLOT` on the 2nd simultaneous read txn on one thread; `MDB_READERS_FULL` at `max_readers`. §4 item 1b is the new rule |
| **R1.9(b)** | `RESULTS-vm.md` §3 says heed had "3 of the 5 runs" reopen at the durable point; the log shows 4 of 5 | Checked `flaky-heed-20260917-142056.log` (runs 1, 2, 3, 5 PASS) | **Upheld, fixed in `RESULTS-vm.md` §3** |
| **R2.9** | "5–7x below at 50k and 100k" is wrong: the 50k ratios are 4.86x and 4.75x | Recomputed from `RESULTS-vm.md` §1 | **Upheld.** The whole sentence is gone: §3's redb paragraph is rewritten around the new numbers |
| **R2.9 / DEPS.md** | DEPS.md's open item: does the build image carry a C toolchain? | Read both Dockerfiles | **Closed in heed's favour.** Both build on `rust:1-bookworm` (glibc, gcc). Recorded in risk 4 |

### 6.2 Findings fixed by correcting the argument

| id | finding | resolution |
|---|---|---|
| **R1.3 / R2.6** | The memo applies the §11.5 log-tail repair asymmetrically: it fixes fjall's failure (state ahead of an intact store) and cannot fix heed's (store will not open), yet §4 item 4 used it to excuse heed's 1-in-10 | **Upheld and decisive.** §1 now states the two failure classes, quotes both raw lines, and says plainly that with the repair adopted fjall's failures become recoverable and heed's does not. §4 item 4 says to decide the repair FIRST because it changes the ranking |
| **R1.4 / R2.6** | D2 single-voter / embedded: with 1 voter there is no snapshot source, so heed's unopenable store is unrecoverable data loss; redb is 20/20 | **Upheld.** This is now §4 item 2's third reason and the reason redb — not fjall — is the documented alternative and the embedded engine |
| **R2.4** | redb's 0/15 + 0/5 is structural (`Durability::None` never publishes a crash-visible root); heed's 0/10 is an observation. The memo read them as the same kind of fact | **Upheld.** §4 item 2 (ii) now distinguishes "passes by construction" from "passed 10 times" |
| **R2.7 (first half)** | I8 says "S1 prefers engines with incremental checkpoints"; heed is the only candidate that cannot, and the memo never said it was choosing against the invariant | **Upheld.** §2's row now carries the I8 preference and says the recommendation is made against it |
| **R1.2 / R2.5** | The kill -9 half cannot falsify (page cache intact), so the verdict rests on 5–10 dm-flakey samples; and those runs are 20 s on a VM with `dirty_expire_centisecs=3000`, which biases them toward the result they reported | **Upheld, cannot be closed in this budget.** §1 now carries the sample sizes, the ~0.3%–45% interval on n=10, and the run-length bias. §6.9 and `RESULTS-refutation.md` §5 say what would close it |
| **R1.7 / R2.7** | The ordered-read pillar is 9.6 MiB fully cached; heed was never scanned beyond 180k rows, never soaked; the RAM pillar has no long-run evidence and is used to disqualify the one engine whose RAM *was* measured | **Upheld.** §3's heed paragraph demotes both, §4 item 1 no longer leans on "memory is page cache", risk 6 names both holes with the exact figures |
| **R2.8** | `MDB_MAP_FULL` is an apply-path failure with no backpressure: §11.8 gates on disk percent, the map limit is node-local and invisible to the leader, and D15 makes long read txns normal | **Upheld.** It is a plan gap, not a memo error. §4 item 7 is new and asks for a map-size rule in §11.8; risk 3 states the liveness consequence |
| **R1.10 / R2 closing** | What could not be refuted: the durable-point ordering, `verify --verify-all`, the symmetry of the three adapters, the honesty of `RESULTS-vm.md` §5's cut list, DEPS.md's facts, the histogram's documented ±3.2% | Recorded, with thanks. The adapter seam both reviews call "the only reason this is recoverable rather than fatal" is §4 item 6, and it is what made this revision possible at all |

### 6.3 Findings I cannot fix from here — they belong to the coordinator

| id | finding | what it needs |
|---|---|---|
| **R1.9(a)** | PLAN_RAFT.md D9 says heed is "the only candidate that always reopens consistent after kill -9 and after dropped writes". Both words are false against the evidence D9 cites: **redb is 20/20 across both fault types with 0 past-durable**, a strictly better record than heed's 15/15 + 9/10, and **heed reopened past its durable point in 12 of 15 kill runs** | An edit to PLAN_RAFT.md §2, which this task may not make. Proposed text: "S1: heed reopened at its durable point in 9 of 10 dropped-write runs (1 of 10 would not reopen, unexplained) and past it in 12 of 15 kill -9 runs; redb passed both fault types 20/20 and cannot reopen past its durable point by construction, but its throughput was re-measured only after G0 (see RESULTS-refutation.md). Sample sizes: 15 kill and 10 dropped-write runs, against the 100 + 20 WP-0.3 mandates." Also: the redb clause "capped at ~10k msg/s with 29× write amplification" is now false for §11.3 as amended |
| **R2.9(b)** | `RAFT_STATUS.md:48` still records D9 as *proposed* while PLAN_RAFT.md §2 marks it RATIFIED (heed). The working record and the plan disagree about the decision about to be built on | An edit to RAFT_STATUS.md, which this task may not make |
| **R1.1 / WP-0.3 exit criterion** | WP-0.3's PASS/FAIL exit criterion is not met at the mandated sample size (15 kill and 20 dm-flakey runs in all, against 100 per engine plus an equivalent dropped-write sample), so the WP is not done as written even though its memo exists | A line in RAFT_STATUS.md next to WP-0.3 |

### 6.9 What must exist before WP-1.2 writes heed-specific code

In priority order. Together they are about 4 hours of VM time; the first is 45
minutes and is the one this memo made its own precondition on 2026-09-17.

1. **`./flaky.sh 20` for all three engines, at equal n** (the current 5 / 5 / 10
   cannot support a ranking), with the patched script that records rc and the
   tail of a failed verify. Accept heed only if its no-reopen rate is 0/20 in
   the configuration the broker will ship.
2. **The same loop with `vm.dirty_expire_centisecs=100`,
   `vm.dirty_writeback_centisecs=50`, `RUN_S ≥ 120`** — the only configuration
   that can produce heed's dangerous case at all. Needs Alice: a sysctl is
   outside `/root`.
3. **`./flaky.sh 20` for heed with `--heed-flags nometasync`.** Its cost is now
   known (§4 item 1a); its crash behaviour is not, and the header's
   unconditional integrity claim is the only reason to pay that cost.
4. **A 2 h soak on heed at 50k** with RSS sampled, the `data.mdb` high-water
   mark, the scan rate at 10M+ rows and the durable-point cost once the store is
   tens of GiB — before "RAM bounded" or "ordered reads win" are used as reasons
   again (R1.7, R1.8).
5. **Decide §11.5's log-tail repair**, then re-score the mandatory criterion with
   it applied to all three engines (§4 item 4).
6. If redb is to be more than a fallback, **a ≥ 30 min ratified-shape cell per
   rate** to see whether its 8.3–8.7x write amplification and its 25.9k ceiling
   at the 50k target hold at steady state, and whether C-3 is worth one minor
   version (redb 3.1.3 declares MSRV 1.89, 4.x declares 1.90).
