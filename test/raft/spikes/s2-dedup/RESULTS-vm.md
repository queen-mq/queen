# S2 VM results — dedup: store index (a) vs hash lists + blooms (b)

The half of WP-0.4 that counts (PLAN_RAFT.md §0.3: numbers to quote come from
the Linux VM). Host: `queenpgless-01`, 164.90.215.224, Ubuntu 24.04,
6.8.0-124-generic, 8 vCPU, 15 GB, ext4 on `/dev/vda1`, rustc 1.98.1, release
build. Crate: `test/raft/spikes/s2-dedup`.

Two campaigns, both driven by scripts kept in this directory:

| campaign | date (UTC) | script | cells |
|---|---|---|---|
| first | 2026-09-17 15:45–16:15 | `s2-vm.sh` (coordinator's, on the VM) | b-heed, a-fjall, b-fjall — **a-heed died at once**, and **every rebuild step exited 1** |
| continuation | 2026-09-18 05:40–05:55 | `s2-vm-resume.sh` (this directory) | a-heed, plus two 120 s cells that settle what the rebuild step was really reporting |

Load, identical in every 600 s cell: 50 000 msg/s offered, batches of 10
messages per Append, 10 Appends per entry → 500 entries/s, 96 B payloads, 4096
partitions, segment files rolling at 2 MiB, `--fsync-mode data` (`fdatasync`)
with 8 fsync threads, one non-durable store commit per entry, a durable point
every 1000 ms, dedup window **360 s**, txns window **432 s**, retention **72 s**
(shorter than the window, so every cell covers the retention-deleted-segment
case D10 and §11.7 name), 1 % duplicates at ages 0–420 s, ack-by-hash probes
below the cursor at a nominal 5000/s. Raw logs: `results/vm/*.log`, one
`RESULT` line per run.

> **§8 (2026-09-18) revises two things in §1–§7.** The ack-by-hash workload of
> these cells asked ONE hash per call, where `log_ack_by_hash_v1` resolves a
> whole ack in one pass: §2's "option (b) was over budget from the first
> minute" is measured against a workload no client generates, and §8.4 shows
> what the same path costs when it is batched the way 005 batches it. And §1's
> `wrong=76..91` for option (b) is now settled: option (b) restarts clean
> (§8.4). Read §1–§7 with §8 beside them.

Two things to read before the numbers:

- **The ack-by-hash load is per ENTRY, not per second** (10 attempts per entry,
  of which ~4–5 find a sample). A cell that manages fewer entries per second
  therefore also does fewer ack resolutions per second: 1825/s in a-heed,
  1055/s in b-heed. The slow cells got the easier absolute ack load, so the gap
  between the options is if anything understated.
- **The 10-minute run is not steady state at these windows.** The dedup window
  fills at t=360 s, the txns prune starts at t=432 s. Only the last ~2.8
  minutes of each cell are in the regime a real deployment lives in
  permanently. §2 is about exactly that.

---

## 1. The four cells

| | (a) a-heed | (a) a-fjall | (b) b-heed | (b) b-fjall |
|---|---|---|---|---|
| achieved rate | 37 565 msg/s | **39 855** | 24 036 | 21 294 |
| dedup probe p50 / p99 (one push, 10 hashes) | **0.013 / 0.024 ms** | 0.021 / 0.084 | 0.070 / 0.575 | 0.074 / 0.767 |
| probe mean | **0.013 ms** | 0.027 | 0.084 | 0.093 |
| of which reached the files | — (always the store) | — | n=77 272 (**5.4 %** of pushes), p50 0.463, p99 0.815 ms | n=62 928 (4.9 %), p50 0.623, p99 1.087 ms |
| ack-by-hash p50 / p99 | **0.002 / 0.003 ms** | 0.029 / 0.167 | 0.423 / 0.767 | 0.591 / 1.055 |
| store commit per entry, mean | 0.706 ms | 0.487 | 0.286 | **0.261** |
| durable point p50 / mean | **1310.7 / 1227.4 ms** | 96.3 / 100.6 | 176.1 / 184.7 | 75.8 / 77.8 |
| store on disk at the end | 3133.1 MiB | 1569.6 MiB | 177.2 MiB | **146.5 MiB** |
| segment files at the end | 344.6 MiB | **364.1** (payload only) | 488.8 (payload + 16 B/msg of hashes, after hash-only compaction) | 459.4 |
| live dedup records | 17 603 161 rows | 19 156 919 rows | **721 657** `txns` rows (one per Append) | 607 783 |
| dedup's own logical bytes per message | 79.6 B | 79.6 B | **21.3 B** | 21.3 B |
| RAM the design holds | **0** (it is all in the store) | **0** | 87.5 MiB (blooms 23.3 + cache 64.2) | 85.6 MiB (21.4 + 64.2) |
| process RSS, max | 3156 MiB | 1370 MiB | **290 MiB** | 986 MiB |
| kernel bytes written | 94 785 MiB (158.0 MiB/s) | 14 364 MiB (23.9) | 19 943 MiB (33.2) | **2907 MiB (4.8)** |
| prune step per entry, p50 / p99 | **0.001 / 0.335 ms** | 0.009 / 5.119 | 0.034 / **0.049** | 0.179 / 0.431 |
| hash-only compaction | — | — | 613 rewrites, 1225.6 → 214.7 MiB (5.7×), p50 9.7 ms | 529, 1057.7 → 185.4 MiB, p50 14.8 ms |
| **exact (both directions)** | **yes** | **yes** | **yes** | **yes** |
| rebuild: ready to probe | **20 ms** | 1848 ms | 212 ms (blooms) / 466 ms (files) | 4005 / 2110 ms |
| rebuild: samples wrong | **0 of 3967** (fixed check) | 302 of 4096 | 91 of 4096 | 76 of 4096 |

The three `wrong=` counts in the last row are **the pre-fix check, not a defect
in the designs** — §3 proves it. a-heed is the only cell whose rebuild ran the
corrected check, and it is clean.

### What the two designs cost, per message, at this window

| | (a) | (b) |
|---|---|---|
| store bytes per message pushed | 145.8 B (heed) / 68.8 B (fjall) | 12.9 B (heed) / 12.0 B (fjall) |
| segment-file bytes per message | 16.0 B | 35.5 / 37.7 B |
| resident bloom bytes per message in the window | 0 | 1.69 / 1.76 B |

Option (a)'s store number is the one that does not extrapolate. At a 360 s
window it is already 17.6 M live rows in 3133 MiB on LMDB (19.2 M in 1570 MiB on
fjall), with RSS 3156 / 1370 MiB. The window D10 names is **1 h, ten times this**: ~176 M
rows, and every voter carries the whole thing. Option (b)'s store did not grow
with the window at all — it is one 54 B locator row per *Append*.

---

## 2. Why nothing held the offered 50 000 msg/s (measured, not guessed)

The driver is one thread. Per entry it does, serially: 10 probes (10 hashes
each), ~4–5 ack-by-hash resolutions, the appends, one store commit, one prune
step, one retention step, and — once a second — a durable point that fsyncs
every dirty bucket file and commits the store durably. At 500 entries/s the
budget for all of that is **2.000 ms per entry**. Multiplying each histogram's
mean by its count per entry accounts for 96 % of the measured entry time:

| ms per entry | laptop a-short¹ | VM a-heed | VM a-fjall | VM b-heed | VM b-fjall |
|---|---|---|---|---|---|
| probe (×10) | 0.150 | 0.130 | 0.270 | 0.840 | 0.930 |
| ack-by-hash (×4–8.7) | 0.095 | 0.010 | 0.196 | **1.875** | **2.391** |
| store commit | 0.358 | 0.706 | 0.487 | 0.286 | 0.261 |
| prune + retention | 0.609 | 0.044 | 0.526 | 0.039 | 0.234 |
| durable point, amortized | 0.026 | **1.465** | 0.229 | 0.647 | 0.338 |
| hash-only compaction, amortized | — | — | — | 0.047 | 0.067 |
| rest (append, bookkeeping, oracle) | 0.154 | 0.099 | 0.155 | 0.127 | 0.152 |
| **measured entry mean** | **1.392** | **2.454** | **1.863** | **3.861** | **4.373** |
| busy share of the wall clock | 70 % | 92 % | 74 % | 93 % | 93 % |

¹ `results/a-short.log`, the laptop cell that did hold 50 000 msg/s, at the
laptop's 60 s window. It is in the table as the reference, not as a VM number.

Read across the row and the two options fail the target for two different
reasons:

**Option (b) was over budget from the first minute.** Probe + ack alone are
2.7 ms (heed) and 3.3 ms (fjall) against a 2.0 ms budget, and the cells were
93 % busy for the whole run — there was never a phase where they kept up. The
cause is the one the laptop already measured and the VM reproduces: resolving
one hash below the cursor means testing the bucket's file blooms and then
reading *every frame of that partition* in each file that hit. b-heed read
74 008 955 frames (14 353 MiB) in 600 s, and 633 278 of the 710 550 reads that
touched a file were ack resolutions, not probes: **94.6 % of pushes were still
answered without opening a file** (77 272 of 1 442 180 reached the files). This
is the same ceiling as the laptop's 25 253 msg/s, on different hardware, so it
is the design's, not the machine's.

**Superseded in part by §8.4.** Every one of those 633 278 resolutions was a
separate call, each paying its own bloom union, its own `txns` scan and its own
frame reads. 005 resolves the whole `p_hashes` array of one ack in ONE pass. A
cell that does that (`v-b-ack10`, and on the laptop `r-b-ack10`) reads 2.1×
fewer frames for the same nominal hash rate, costs 6.9× less per hash, and runs
54 % faster. The ceiling in this paragraph belongs to the harness, not to option
(b); what survives is a 24–30× gap against option (a) on the same path.

**Option (a) held the target and then lost it as its index grew.** The
per-minute progress line the harness now prints (`results/vm/a-heed.log`) is
the measurement:

| minute | msg/s | entry mean | busy | store | RSS | pacing lag |
|---|---|---|---|---|---|---|
| 1 | **49 489** | 1.056 ms | 52 % | 501 MiB | 510 MiB | 1 s |
| 2 | **50 144** | 1.590 ms | 80 % | 980 MiB | 992 MiB | 0 s |
| 3 | **49 158** | 1.842 ms | 91 % | 1433 MiB | 1447 MiB | 1 s |
| 4 | 48 771 | **2.049 ms** | 100 % | 1882 MiB | 1898 MiB | 3 s |
| 5 | 45 218 | 2.211 ms | 100 % | 2299 MiB | 2316 MiB | 9 s |
| 6 | 42 548 | 2.349 ms | 100 % | 2691 MiB | 2710 MiB | 18 s |
| 7 | 41 228 | 2.425 ms | 100 % | 3078 MiB | 3099 MiB | 29 s |
| 8 | **18 837** | 5.308 ms | 100 % | 3133 MiB | 3156 MiB | 67 s |
| 9 | 14 228 | 7.027 ms | 100 % | 3133 MiB | 3156 MiB | 111 s |

Option (a) **did** sustain the offered 50 000 msg/s on this VM — for three and a
half minutes. It crosses the 2.000 ms budget in minute 4, at a store of ~1.9 GB,
and it falls off a cliff in minute 8: that is `t = 432 s`, when the **txns prune
starts** and the store stops growing (3078 → 3133 MiB, flat thereafter) because
deletes now match inserts. Paying for both at once costs it 60 % of its rate.

So the honest answer to "why did the laptop's option (a) hold 50 k and the VM's
not" is three measured differences, in order of size:

1. **The window is six times wider** (360 s vs 60 s), and option (a)'s cost is
   its index. Same design, same engine family, 17.6 M live rows instead of
   3.6 M — and the run only reaches the pruning regime in its last 2.8 minutes,
   where it does 14–19 k msg/s.
2. **A durable point on Linux is real.** `fdatasync` on ext4 costs ~1 ms per
   file (S1's finding) and every durable point syncs every dirty bucket file;
   on macOS with plain `fsync` it costs almost nothing. Amortized per entry:
   0.026 ms on the laptop, 0.229–1.465 ms on the VM. For a-heed it is the
   single largest item (60 % of the entry): `mdb_env_sync` on a 3.1 GB store
   with `MDB_NOSYNC` writes back every page dirtied since the last sync, and it
   took **1227 ms on average** — the harness asked for one per second and got
   269 of them in 600 s. That is also where a-heed's 94.8 GiB of kernel writes
   comes from: 158 MiB/s of disk traffic while the run appended 2148 MiB of
   payload and 1712 MiB of dedup rows, 6.4 MiB/s of logical data in all.
3. **The VM's cores are slower.** Visible in the probe (0.013–0.027 ms vs
   0.011–0.015 laptop) and in the commit, but it is the smallest of the three.

None of this says 50 000 msg/s is out of reach for the broker: the harness is
one thread doing planning, applying, fsyncing and pruning in series (§7.1 puts
the durable point and file maintenance on the apply thread *between* entries,
and §11.4 is explicitly allowed to bound the bucket fan-out, which S1 already
flagged). It does say that **at the spec's window, option (a)'s index — not the
probe — is what a node spends its time on.**

---

## 3. The rebuild step: `wrong=76..302` was the harness, and here is the proof

Every rebuild step of the first campaign exited 1. The failing samples all had
the same shape:

```
MISMATCH pid=4058 off=26 created_age=597 s inside=false got=Some(2657)
```

— a sample created in the first seconds of the run, expected to answer "not a
duplicate" (its age, 597 s, is far past the 360 s window), answering instead
with an offset two orders of magnitude further along the partition.

**Diagnosis.** `rebuild` now reports, for every mismatch, every occurrence of
that hash the design can still find, with its age. Reproduced on the laptop
first (`repro-samples.sh`: window 40 s, txns 50 s, retention 10 s, duplicate
ages 0–90 s, 120 s, option a on fjall — scaled so a 2-minute run has the shape
of a 10-minute one at the campaign's windows; logs in
`results/repro-samples-*.log`), then on the VM:

```
MISMATCH pid=2604 off=2 created_age=119 s inside=false got=Some(868)
  occurrences still in the store: [off=868 age=39 s] -> a younger one exists: true
```

- laptop, pre-fix selection: **459 of 4096 wrong, 459 of 459 with a younger
  occurrence** of that hash still in the store;
- VM, pre-fix selection (`results/vm/samples-legacy*.log`): **258 of 4096
  wrong, 258 of 258 with a younger occurrence**.

The mechanism is the run's own duplicate injection. A duplicate whose age is
past the dedup window is **accepted and appended** — that is 003's rule, and the
exactness oracle checks it (a-heed: 5591 of them, none falsely reported). From
that moment the hash has a second, younger occurrence, and the probe rightly
answers it. But the saved sample was written from the *first* occurrence, so its
expectation ("no answer") is stale. The campaign's windows make this likely:
the oracle's ring keeps a sample takeable for `max(dup_max_age, txns) = 432 s`,
so a hash saved at t≈80 s can be re-pushed as late as t≈430 s, and a re-push
after t=240 s is still inside the 360 s window when the run ends at t=600 s. The
laptop campaign never saw it because its ring was 72 s wide and its run 300 s
long: every re-push had expired again long before the check.

**A second defect in the same place, found while fixing the first.** The saved
samples were "the first 4096 ack probes", and ack probes only start once the
oracle holds samples old enough (`retention_s + 5 … 0.8 × txns_s`), so all 4096
were created in the first seconds of the run. By the end, *every one of them*
was past the window: the restart check asked the `expect no answer` direction
4096 times and the `expect the original offset` direction **zero** times. It
could not have caught a design that forgot a live record across a restart.

**The fix** (`src/main.rs`): the samples are a reservoir over the whole run, and
a sample whose hash the run re-pushed out of window is dropped at save time (the
run prints how many). **Proof, same parameters, same host, same binary:**

| cell | selection | samples | of which "expect the original offset" | wrong |
|---|---|---|---|---|
| `samples-legacy` (VM, 120 s) | pre-fix | 4096 | **0** | **258** (258 with a younger occurrence) |
| `samples-fixed` (VM, 120 s) | fixed | 3539 | **547** | **0** |
| `repro-samples-legacy` (laptop, 120 s) | pre-fix | 4096 | 0 | 459 (459 younger) |
| `repro-samples-fixed` (laptop, 120 s) | fixed | 3167 | 583 | **0** |
| `a-heed` (VM, 600 s, campaign windows) | fixed | 3967 | **803** | **0** |

**Verdict: a harness artefact, not a loss of exactness after a restart.** The
corrected check is also strictly stronger than the one that was failing: it asks
both directions, and a-heed passed it 3967/3967 at the campaign's own windows.

Two honest caveats:

- the three pre-fix cells (a-fjall, b-heed, b-fjall) **cannot be re-checked**:
  `s2-vm.sh` deletes each data directory after its rebuild. The claim that their
  `wrong=` counts are the same artefact rests on the identical mismatch
  signature, on the counts tracking the number of out-of-window duplicates each
  cell injected (6337 → 302, 4176 → 91, 3244 → 76), and on the reproduction
  above. It is not a re-measurement of those three cells.
- dropping re-pushed samples costs coverage (129 of 4096 in a-heed, 929 in the
  laptop repro). Re-stamping the sample with the new occurrence would keep it;
  dropping was the smaller change. WP-1.x should re-stamp.

---

## 4. The a-heed cell: `MDB_BAD_VALSIZE`, and why only option (a) hit it

The first campaign's a-heed cell died before its first entry:

```
ERROR: heed: MDB_BAD_VALSIZE: Unsupported size of key/DB name/data, or wrong DUPFIXED size
```

**Cause:** option (a)'s prune walks its expiry index from the beginning on its
first step (`dedup.rs` `StoreIndex::prune`, `from = b""`), and the heed adapter
turned that into `Bound::Included(b"")`. LMDB rejects a zero-length key —
`mdb_cursor_get(MDB_SET_RANGE)` checks `key->mv_size == 0 || > maxkeysize` and
answers `MDB_BAD_VALSIZE` — so the error arrives before the first row. It is the
only call site in the crate that passes an empty `from`, which is why option (b)
ran fine on the same engine, and why fjall (`range(from.to_vec()..)`) and redb
(`range(from..)`) accept it.

**Fix** (`src/engines/heed_eng.rs`): the trait's "empty = from the beginning" is
`Bound::Unbounded` for LMDB. Applied to `range` and `prefix_count`. Reproduced
on the laptop in one second, fixed, re-run there, then run on the VM for the
full 600 s cell in §1.

**Note for S1 (not changed here).** `test/raft/spikes/s1-store/src/engines/heed_eng.rs`
has the same `Bound::Included(prefix)` in `prefix_count`. It is latent, not
triggered: S1's two call sites pass 8-byte prefixes. S1's sources were left
untouched on purpose (that spike's files are not this task's).

---

## 5. Exactness (the PASS/FAIL of WP-0.4)

Every cell: `exact=true`. The duplicate is drawn first and pushed to the
ORIGINAL's partition, since 003 keys dedup on `(partition, hash)`.

| cell | duplicates inside the window | detected | missed | wrong original offset | outside the window | falsely reported | fresh hashes falsely reported |
|---|---|---|---|---|---|---|---|
| a-heed | 104 444 | **104 444** | 0 | 0 | 5 591 | **0** | 0 |
| a-fjall | 127 808 | **127 808** | 0 | 0 | 6 337 | **0** | 0 |
| b-heed | 64 702 | **64 702** | 0 | 0 | 4 176 | **0** | 0 |
| b-fjall | 53 329 | **53 329** | 0 | 0 | 3 244 | **0** | 0 |

The case D10 and §11.7 exist for — retention has deleted the segment, the hashes
must still answer — is the ack-by-hash column, checked per probe (the `segments`
row is read back and confirmed absent before probing):

| cell | ack-by-hash probes below the cursor | resolved | unresolved | of which the segment was already deleted by retention |
|---|---|---|---|---|
| a-heed | 1 095 115 | **1 095 115** | 0 | 1 095 109 (99.999 %) |
| a-fjall | 1 338 943 | **1 338 943** | 0 | 1 338 943 (100 %) |
| b-heed | 633 278 | **633 278** | 0 | 633 278 (100 %) |
| b-fjall | 511 013 | **511 013** | 0 | 511 000 (99.997 %) |

The planner overlay (§7.2) earned its place again: 188–266 duplicates per cell
were same-entry duplicates that committed state could not yet see. The
exhaustive `--audit-misses` scan reported `0 were THERE, 0 were genuinely
absent` in every cell.

---

## 6. Restart

`rebuild` reopens the store, rebuilds whatever the design keeps in RAM, then
re-probes the saved samples at the run's last `now`.

| cell | open store | file table (scan `seg_loc` + `txns`) | blooms | total to ready | samples wrong |
|---|---|---|---|---|---|
| a-heed | **0 ms** | 18 ms (102 006 + 0 rows) | — | **20 ms** | 0 / 3967 |
| a-fjall | 1174 ms | 669 ms (103 683 + 0) | — | 1848 ms | 302 / 4096 (artefact, §3) |
| b-heed, blooms from sidecars | **0 ms** | 49 ms (104 038 + 721 657) | 163 ms | **212 ms** | 91 / 4096 (artefact) |
| b-heed, blooms rebuilt from files | 0 ms | 49 ms | 417 ms | 466 ms | 91 / 4096 (artefact) |
| b-fjall, blooms from sidecars | 2587 ms | 1262 ms (80 724 + 607 783) | 156 ms | 4005 ms | 76 / 4096 (artefact) |
| b-fjall, blooms rebuilt from files | 819 ms | 915 ms | 376 ms | 2110 ms | 76 / 4096 (artefact) |

The engine, not the design, dominates this table. LMDB reopens in 0 ms and scans
825 695 rows in 49 ms (**0.06 µs/row**); fjall pays 0.8–2.6 s of journal recovery
and scans at 1.8 µs/row, 30× slower. Extrapolated to the shape D10 names (50 k
msg/s, 1 h window → ~18 M `txns` rows for option (b)), option (b)'s file-table
scan before the node can answer a probe is **~1.2 s on LMDB and ~33 s on fjall**.
The laptop memo's "13–25 s" was a fjall number; on LMDB the problem mostly
disappears. Option (a) has nothing of its own to rebuild.

---

## 7. Cut short, honestly

- **Ten minutes at a six-minute window is not a soak.** Every cell spends 72 %
  of its life filling the window and only 2.8 minutes in the pruning regime,
  which is the regime a deployment lives in. Option (a)'s numbers are therefore
  mostly growth-phase numbers: its store size, its RSS and its durable-point
  cost had not stopped rising, and its *rate* in the steady regime is the
  14–19 k msg/s of minutes 8–9, not the 37.6 k average.
- **Nothing ran at the 1 h window D10 names.** Ten times this index for option
  (a); the extrapolations in §1 are arithmetic, not measurements.
- **No cell reached the offered 50 000 msg/s over a full run** (§2 says what
  each one spent its time on). The harness's single thread is not the broker's
  apply thread; the absolute rates are a comparison between the two designs on
  one machine, not a broker throughput prediction.
- **a-heed ran with a harness that the other three cells did not have**: the
  heed empty-key fix (§4, option (a) on heed could not run at all without it),
  the sample selection fix (§3) and a per-minute progress line. None of them
  touches the probe, the ack, the store writes or the file layout; the
  progress line costs one directory walk a minute.
- **redb was not measured here**, as in the first campaign: S1 dropped it for
  this write pattern.
- **The `--ack-rate 0` and `--cache-mb 0` variants of the laptop campaign were
  not repeated on the VM** — no budget. The laptop's finding (option (b) does
  42 286 msg/s with the ack probes switched off, against 25 253 with them) is
  what §2's decomposition confirms on the VM, but the VM has no direct
  measurement of it.
- **Duplicate injection reaches ~40 % of its nominal 1 %** (the oracle often
  has no sample at the exact age asked for: `oracle misses` in the logs). The
  effective duplicate rate is ~0.4–0.8 % of messages.
- **MSRV.** `cargo +1.88 check --release` still passes with these changes
  (`results/msrv-1.88-resume.log`), so C-3 holds; the VM built with rustc
  1.98.1 and the laptop with 1.94.0, and the sources are byte-identical on both
  (md5 of `main.rs`, `dedup.rs`, `heed_eng.rs` checked after the rsync).
- **`kernel bytes written` is `/proc/self/io write_bytes`.** For an mmap store
  some writeback is done by kernel flusher threads and may not be attributed to
  the process, so a-heed's 94.8 GiB is a floor, not a ceiling.

---

## 8. Refutation re-checks (2026-09-18, `refutation-vm.sh`)

Ten minutes of VM time, five 120 s cells plus two rebuilds, run 06:29:57–06:40:01
UTC on the same host as §1–§7 (rustc 1.98.1, release, engine **heed**), after
checking that the S4 job that held the box until 06:25 had finished and leaving
nothing behind (`pgrep -af s2-dedup` → none). Load identical to §1 (50 000 msg/s
offered, 10×10 per entry, 96 B, 4096 partitions, 2 MiB files, `--fsync-mode
data` ×8, campaign windows: dedup 360 s, txns 432 s, retention 72 s, duplicate
ages 0–420 s). Logs: `results/vm/v-*.log`, driver log
`results/vm/campaign-refutation.log`.

**What 120 s at a 360 s window can and cannot say.** It is the growth phase: no
cell reaches the txns prune (t = 432 s), the store never exceeds ~1 GB, and the
oracle can only hand out ack samples older than `retention + 5 s = 77 s`, so the
ack load is **15–145 calls/s instead of the campaign's 1055–1825/s**. These
cells are evidence about footprint per message, store ops per message, probe
latency, durable points and restart. **They are not evidence about sustained
rate**, and the 49 930 msg/s of the option (b) cell must not be read as "option
(b) now holds 50 k": it held it with almost no acks to answer.

### 8.1 The three questions and the five cells

| | `v-a-dms250` | `v-a-dms1000` | `v-a-dms4000` | `v-alean-dms1000` | `v-b-ack10` |
|---|---|---|---|---|---|
| option | a | a | a | **a-lean** | b, `--ack-batch 10` |
| durable point cadence | 250 ms | 1000 ms | 4000 ms | 1000 ms | 1000 ms |
| achieved rate (light ack load) | 46 275 | 49 715 | 49 557 | 49 664 | 49 930 |
| probe p50 / p99 | 0.010 / 0.021 ms | **0.010 / 0.018** | 0.010 / 0.021 | 0.011 / 0.019 | 0.063 / **0.431** |
| ack per hash p50 | 0.002 ms | 0.002 | 0.002 | 0.002 | **0.047** |
| ack per call p50 / p99 | 0.002 / 0.003 (1 hash) | 0.002 / 0.003 | 0.002 / 0.005 | 0.002 / 0.003 | **0.479 / 0.703** (10 hashes) |
| store at the end | 906.1 MiB | 975.3 | 975.6 | **677.1** | **136.2** |
| store B / message | 171.0 | 171.4 | 171.2 | **119.0** | **23.8** |
| files B / message | 100.7 | 100.6 | 100.6 | 100.6 | 116.7 |
| total disk B / message | 271.7 | 272.0 | 271.8 | **219.6** | **141.9** |
| dedup logical B / message | 79.9 | 79.9 | 79.9 | **59.1** | **21.4** |
| store ops / message | 2.48 | 2.48 | 2.48 | **1.58** | **0.58** |
| live dedup records | 5.55 M | 5.96 M | 5.97 M | 5.96 M | **599 160** `txns` |
| RSS max | 913 MiB | 985 | 987 | 689 | **221** |
| kernel bytes written | 16 413 MiB | 13 559 | **7 774** | 13 850 | **4 787** |
| durable points / mean | 189 / 384.6 ms | 83 / 443.6 | 26 / **635.1** | 79 / 519.4 | 100 / 196.5 |
| commit per entry, mean | 0.468 ms | 0.461 | 0.484 | 0.511 | **0.267** |
| prune step per entry, mean | 0.001 ms | 0.001 | 0.002 | 0.035 | 0.034 |
| rebuild to ready / wrong | — | — | — | 36 ms / **0 of 4096** | 203 ms / **0 of 1793** |
| exact both directions | yes | yes | yes | yes | yes |

### 8.2 The durable point tracks neither the store alone nor the bytes dirtied

The three `v-a-dms*` cells differ only in `--durable-ms`. A durable point is
`segs.sync_dirty()` over every dirty bucket file plus a durable store commit
(`mdb_env_sync`, heed is opened `EnvFlags::NO_SYNC`).

| cadence | points in 120 s | mean per point | **sync time per second of wall** | kernel bytes | rate |
|---|---|---|---|---|---|
| 250 ms | 189 | 384.6 ms | **0.605 s/s** | 16 413 MiB | 46 275 |
| 1000 ms | 83 | 443.6 ms | 0.307 s/s | 13 559 MiB | 49 715 |
| 4000 ms | 26 | 635.1 ms | **0.137 s/s** | 7 774 MiB | 49 557 |

- **Not proportional to the bytes dirtied since the last point.** 16× the
  interval costs 1.65× the point. The review's reading of `NO_SYNC` predicted
  ~4× per step of cadence; it is not what the disk does.
- **Not independent of the store either.** Same 1/s cadence, same load: 443.6 ms
  at a store under 975 MiB here, **1227 ms** at 3133 MiB in the 600 s cell of
  §1. It grows with the store, sublinearly.
- **Bounding the point is a real lever**, which is what matters for §11.4:
  0.137 s/s at 4 s against 0.605 s/s at 250 ms, and 2.1× fewer kernel bytes. The
  250 ms cell is the only one of the three that lost throughput (46 275) — it
  spent 60 % of the wall clock inside the durable point.
- §11.4 says "cost is proportional to what changed (I8)". On LMDB it is not.
  WP-1.4 and the flatness test of §13.6 should be written against the measured
  shape, not that sentence.

### 8.3 `a-lean`: the expiry index is 31 % of the store and none of the writes

`--option a-lean` keeps `(pid, hash) → [(offset, created_at)]` exactly as option
(a) has it — probe and ack resolution are byte-identical — and replaces the
`(created_at, pid, hash) → offset` secondary index with ONE sequential row per
Append, `(pid, base_offset) → [end][created][hashes]`, pruned by 006's rotating
per-partition walk. Against `v-a-dms1000`, same cadence, same load:

| | plain (a) | a-lean | |
|---|---|---|---|
| store at the end | 975.3 MiB | 677.1 MiB | **−31 %** |
| store B / message | 171.4 | 119.0 | −31 % |
| dedup logical B / message | 79.9 | 59.1 | −26 % |
| store ops / message | 2.48 | 1.58 | **−36 %** |
| RSS max | 985 MiB | 689 MiB | −30 % |
| kernel bytes written | 13 559 MiB | 13 850 MiB | **+2 %** |
| durable point, mean | 443.6 ms | 519.4 ms | +17 % (79 points vs 83) |
| probe p50 / p99 | 0.010 / 0.018 ms | 0.011 / 0.019 ms | equal |
| ack per hash p50 | 0.002 ms | 0.002 ms | equal |
| achieved rate | 49 715 | 49 664 | equal |
| prune step per entry, mean | 0.001 ms | 0.035 ms | worse (and neither cell pruned: growth phase) |
| exact / restart | yes / — | yes / **0 wrong of 4096** | |

The footprint claim is confirmed and the write-amplification claim is not: the
secondary index is time-prefixed, so its inserts land at one edge of the key
space and dirty almost no extra pages, while the random `(pid, hash)` row that
both variants must write dirties a 4 KiB LMDB page per message. **`a-lean` is
worth taking for the 31 % of store and the 36 % of store ops; it is not the
answer to the 94.8 GiB of §1.** Its prune is the open question: it walks per
partition (0.035 ms per entry even with nothing to drop) where plain (a) walks
one time-ordered index, and no VM cell has run it past `t = txns_window`.

### 8.4 Option (b) with 005's batching, and its first clean restart on this host

`v-b-ack10` resolves ack-by-hash the way `log_ack_by_hash_v1` does: the whole
`p_hashes` array of one ack in one pass — one bloom candidate union, one ordered
scan of the partition's `txns` rows, one read of each frame those rows point at,
every input hash matched while the frame is in hand.

- **per call 0.479 ms p50 / 0.703 p99 for ten hashes; per hash 0.047 ms.** The
  campaign's b-heed paid 0.423 ms *per hash*. Against option (a) on the same
  host the same ten hashes cost ~0.020 ms, so the gap is ~24× per hash and ~30×
  per call — large, and an order of magnitude smaller than the 200× §2 of this
  file reported with the unbatched workload.
- **1 012 714 frame reads (196.9 MiB)** for 17 930 resolved hashes, against
  74 008 955 (14 353 MiB) for 633 278 in the 600 s cell.
- **restart: `wrong=0` of 1793 samples**, with the corrected selection of §3 —
  the first valid restart-exactness number option (b) has on this host. Ready to
  probe in 203 ms (file table 47 ms for 599 160 `txns` + 104 k `seg_loc` rows,
  blooms from sidecars 156 ms).
- Its probe is the price: **p50 0.063 ms, p99 0.431 ms** against 0.010 / 0.018
  for option (a) in the cell beside it. 2 273 029 bloom tests, 11 466 hits,
  1225 of them false.

### 8.5 Cut short, honestly

- **120 s is the growth phase.** No cell pruned; every footprint figure is a
  rising number sampled at t = 120 s, and the store sizes here (677–975 MiB) are
  a third of the 600 s cells'.
- **The ack load is not the campaign's.** 15 calls/s in the (b) cell, 145/s in
  the (a) cells, against 1055–1825/s in §1. Rate comparisons between §8 and §1
  are meaningless; latency, bytes per message and ops per message are not.
- **Duplicate injection barely runs at these windows**: ~8.5 k duplicates per
  cell against 51 k oracle misses, because the ring has to hold samples up to
  420 s old and the run is 120 s. The exactness claims of §8 are weaker than
  §5's; they are a smoke test, not the PASS/FAIL.
- **The 3600 s window of D10 and `schema.sql:66` is still unmeasured**, and so
  is steady state. The memo's recommendation 5 is the run that would settle it.
- **`--ack-batch` uses filler hashes** for the siblings of the leased batch (the
  oracle indexes samples by age, not by partition). Real siblings sit in the same
  frames, so the batched cost measured here is an upper bound.
