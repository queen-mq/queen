# S2 decision memo — which dedup design the RSM carries (D10)

WP-0.4 of PLAN_RAFT.md. Options, as D10 states them: **(a)** a store index per
message hash; **(b)** the hash lists already inside each `Append` + per-file
bloom filters + a bounded recent cache.

**Revision 2, 2026-09-18**, after an adversarial review of revision 1 found its
recommendation unsound. Revision 1 recommended option (b) *plus* "a bounded
exact index for ack-by-hash, sized by the ackable span". §1 shows that index is
option (a)'s index — same keys, same rows, same write pattern — so revision 1
recommended (a) *underneath* (b) and priced neither. It also measured option
(b)'s ack path with a workload no client generates (one hash per call, where
005 resolves a whole ack in one pass); correcting that recovers 54 % of option
(b)'s throughput and dissolves revision 1's third argument. Every finding and
its resolution is in §8. Sources: `RESULTS-laptop.md` and `RESULTS-vm.md`
(§1–§7 the 2026-09-17/18 campaigns, run by `run-laptop.sh` and
`s2-vm-resume.sh`; §8 of each the refutation cells of 2026-09-18, run by
`refutation-laptop.sh` and `refutation-vm.sh`). Every command line is in those
four scripts; every raw log is under `results/`.

**Recommendation in one line: keep D10 at option (a), in its lean encoding
(−31 % store, −36 % store ops, same latency, measured), with the durable point
bounded per §11.4 — because the ack-by-hash question 005 asks IS option (a)'s
index, so (a) answers both questions with one structure while (b) needs that
same structure added to its own.**

Nothing here is ratified; D10 is Alice's at G0. D10 currently reads "ratified:
option (a) … if the S2 memo lands on (b) with evidence, re-decide before
WP-1.2". This memo does not land on (b).

---

## 1. The question that decides, and why revision 1 got it wrong

Both designs answer 003's probe well enough to ship. The design question is
005's, and revision 1 misread it.

`server/sql/procedures/005_log_ack.sql`, `log_ack_by_hash_v1` (header ≈440–476,
body: `v_lo` at ≈564, the `occ` CTE at ≈598), resolves each input hash to two
facts:

```
v_lo := GREATEST(v_c.committed + 1, v_txns_start);
occ(h, voff)  -- every hash occurrence of the partition's log_txns rows,
              -- bounded ABOVE by base_offset <= batch_end under a lease,
              -- and below only by the purge watermark txns_start
eff   = MIN(voff) FILTER (voff >= v_lo AND voff <= batch_end)
below = bool_or(voff <= v_c.committed)
```

`below` is what produces `noopHashes` / `staleHashes`; a hash with neither `eff`
nor `below` goes into `unresolvedHashes`, which since 2026-07-30 the broker
turns into an explicit per-item rejection (the fix for the silent redelivery
livelock). So:

- **the span is `[txns_start, batch_end]`, not "the ackable span".** The
  below-cursor half is `[txns_start, committed]` — the whole txns window below
  the cursor. An index that only covered `[committed+1, batch_end]` would answer
  "no occurrence" for a hash whose only occurrence is below the cursor and turn
  a `noop` into an `unresolved`. That is client-visible and a G-1 parity break.
  The harness says the same in its own words (`src/dedup.rs` ≈9–13) and prunes
  option (a) at exactly that watermark (`src/main.rs`, `dd.prune(…, now_us -
  txns_us, …)`), which is also what D10 specifies.
- **a smaller row does not exist.** `eff` is a MIN over a range whose bounds
  (`committed`, `batch_end`) differ per call, and `below` needs the occurrences
  under the cursor, so the value has to be the occurrence list. That is
  `StoreIndex`: `(pid, hash) → [(offset, created_at)]`, option (a), row for row.
  5591 rows of the 17.6 M in the VM's a-heed cell already carry more than one
  occurrence (an out-of-window re-push, which 003 accepts), so a (min, max) pair
  is not a substitute either.

**Consequence:** "option (b) + an exact ack index" is option (a) plus per-file
blooms, bloom sidecars, a file table, 16 B/message of hashes inside the frames
and hash-only compaction. It is strictly more machinery than (a) alone, it was
never measured as a composite, and no one should ratify it. Revision 1's item 3
is withdrawn.

---

## 2. The PASS/FAIL (exactness), now complete

WP-0.4 asks for exactness with duplicates injected at random ages, including the
case where retention has deleted the segment whose hashes are still inside the
txns window.

| | (a) store index | (a-lean) | (b) hash lists + blooms |
|---|---|---|---|
| duplicates inside the window, detected / missed (VM campaign, 4 cells + 5 refutation cells) | 232 252 + 24 606 / **0** | 8 462 / **0** | 118 031 + 8 511 / **0** |
| wrong original offset (003 returns the MIN occurrence) | **0** | **0** | **0** |
| duplicates outside the window falsely reported | **0** | **0** | **0** |
| fresh hashes falsely reported | **0** | **0** | **0** |
| ack-by-hash below the cursor: resolved / unresolved | 2 484 880 / **0** | 17 441 / **0** | 1 146 084 / **0** |
| of those, the segment was already deleted by retention | > 99.99 % | 100 % | 100 % |
| **exact after a restart** | **0 wrong of 3967** (VM, campaign windows) · 0 of 3813 (laptop) | **0 of 4096** (VM) · 0 of 3803 (laptop) | **0 of 1793** (VM) · **0 of 4001 and 0 of 3872** (laptop) |

The gap revision 1 had to leave open — option (b) had no restart-exactness
number taken with the corrected sample selection — is closed: three cells,
`wrong=0` in all three (`results/refutation/r-b-ack1`, `r-b-ack10`,
`results/vm/v-b-ack10`). The campaign's `wrong=76..91` for option (b) was the
same harness artefact `RESULTS-vm.md` §3 proves for option (a).

**Neither design is disqualified on correctness.** Both are exact in both
directions on both hosts, including the retention case, and both come back exact
after a restart. The decision is cost.

---

## 3. The numbers that decide

Every figure is labelled with the window and the achieved rate it was taken at,
because both matter and revision 1's table hid them.

### 3.1 VM, 600 s cells, 360 s dedup window / 432 s txns window, 50 000 msg/s offered

The campaign of `RESULTS-vm.md` §1. Its ack workload asked one hash per call
(§3.3 corrects for that); its option (a) cells are the only ones that ran long
enough to reach the prune.

| at the rate each achieved → | (a) a-heed **37 565** | (a) a-fjall **39 855** | (b) b-heed **24 036** | (b) b-fjall **21 294** |
|---|---|---|---|---|
| dedup probe p50 / **p99** | 0.013 / **0.024 ms** | 0.021 / 0.084 | 0.070 / **0.575** | 0.074 / 0.767 |
| ack-by-hash p50 / p99, ONE hash per call | 0.002 / 0.003 | 0.029 / 0.167 | 0.423 / 0.767 | 0.591 / 1.055 |
| store on disk | 3133 MiB | 1570 MiB | **177 MiB** | **147 MiB** |
| live dedup records | 17.6 M rows | 19.2 M | **722 k** locators | **608 k** |
| **total disk per message** (store + files + sidecars) | 161.8 B | 84.8 B | **49.5 B** | **50.7 B** |
| store per message / files per message | 145.8 / 16.0 B | 68.8 / 16.0 | 12.9 / 35.5 | 12.0 / 37.7 |
| store ops/s | 113 078 | 118 273 | **17 679** | **15 754** |
| RSS max | 3156 MiB | 1370 MiB | **290 MiB** | 986 MiB |
| kernel bytes written | 94 785 MiB | 14 364 MiB | 19 943 MiB | **2 907 MiB** |
| durable point, mean (1/s cadence) | 1227 ms | 101 ms | 185 ms | **78 ms** |
| ready to answer a probe after a restart | **20 ms** | 1848 ms | 212 ms | 4005 ms |

Revision 1 quoted "11–18× less store on disk" for option (b) and the p50 of the
probe. Both are corrected here: the store-only ratio is real but it leaves out
the disk where option (b) actually keeps its hashes (16 B/message inside the
frames, which is why its segment files are 2.2× bigger per message). **On total
disk per message option (b) is 3.3× cheaper on heed and 1.7× on fjall, not
11–18×.** And on the statistic WP-0.4 names — probe p99 — option (a) is **24×
better** (0.024 vs 0.575 ms), which revision 1's table did not show.

### 3.2 VM, 120 s refutation cells, same windows, heed (`RESULTS-vm.md` §8)

Ten minutes of VM time, five cells. 120 s at a 360 s window is the **growth
phase**: no cell reaches the prune, and the oracle can only offer ack samples
older than `retention + 5 s = 77 s`, so the ack load is 15–145 calls/s instead
of the campaign's 1055–1825/s. These cells are evidence about **footprint, probe
latency, durable points and restart**, and are NOT evidence about sustained rate.

| | (a) `v-a-dms1000` | (a-lean) `v-alean-dms1000` | (b) `v-b-ack10` |
|---|---|---|---|
| achieved rate (offered 50 000, light ack load) | 49 715 | 49 664 | 49 930 |
| probe p50 / **p99** | 0.010 / **0.018 ms** | 0.011 / **0.019** | 0.063 / **0.431** |
| ack per hash p50 (call of 1 / of 10 hashes) | 0.002 (1) | 0.002 (1) | 0.047 (10), call 0.479 |
| store at the end | 975.3 MiB | **677.1** | **136.2** |
| store B / message | 171.4 | **119.0** | **23.8** |
| files B / message | 100.6 | 100.6 | 116.7 |
| **total disk B / message** | 272.0 | **219.6** | **141.9** |
| dedup logical B / message | 79.9 | **59.1** | **21.4** |
| store ops per message | 2.48 | **1.58** | **0.58** |
| RSS max | 985 MiB | 689 | **221** |
| kernel bytes written | 13 559 MiB | 13 850 | **4 787** |
| rebuild to ready / samples wrong | not re-run¹ | **36 ms / 0 of 4096** | 203 ms / **0 of 1793** |

¹ the ten minutes went to the two cells that needed a restart check: a-lean,
whose expiry mechanism is new, and option (b), which had none. Plain (a)'s is
in §3.1 (20 ms, 0 wrong of 3967, at the campaign's windows).

### 3.3 What the two harness fixes changed

**005 resolves a whole ack in one pass.** The campaign charged every hash its
own bloom union, its own `txns` scan and its own frame reads. `--ack-batch 10`
does what the SQL does. Laptop, heed, 120 s, 60 s window, with a real ack load:

| | `r-b-ack1` | `r-b-ack10` |
|---|---|---|
| hashes resolved in the run | 154 114 | **334 150** (2.2× more) |
| cost per hash, p50 | 0.415 ms | **0.060 ms** (6.9× cheaper) |
| cost per ack call, p50 / p99 | 0.415 / 2.111 | 0.607 / 5.119 |
| frame reads | 8 789 867 | **4 107 845** |
| achieved rate | 28 886 msg/s | **44 410 msg/s** (+54 %) |

So revision 1's "option (b)'s ack-by-hash is what caps the node" was an artefact
of the workload: with 005's batching, option (b) does 2.2× the resolutions at
54 % more throughput. What survives is the ratio, not the cap: the same ten
hashes cost **0.607 ms on (b) and 0.011 ms on (a-lean)** (laptop) and
**0.479 ms vs ~0.020 ms** (VM) — 30–55×.

**Option (a) without its expiry index (`--option a-lean`).** The
`(created_at, pid, hash) → offset` secondary index is replaced by one sequential
`txns` row per Append, `(pid, base_offset) → [end][created][hashes]`, pruned by
006's rotating per-partition walk. Probe and ack are byte-identical. Measured
(VM / laptop): store per message **171.4 → 119.0 B** and **112.1 → 75.6 B**
(−31/−33 %), store ops per message **2.48 → 1.58** and **3.35 → 2.10** (−36 %),
RSS **985 → 689 MiB** and **642 → 433** (−30/−33 %), rate unchanged, exactness
and restart unchanged. **It does not touch the write amplification**: kernel
bytes were 13 559 → 13 850 MiB on the VM (+2 %). The expiry index is
time-prefixed, so its inserts land at one edge of the key space; what dirties a
4 KiB LMDB page per message is the random `(pid, hash)` row itself, which both
variants must write. The review's premise that the secondary index was the
source of the 94.8 GiB is refuted by measurement; its conclusion that a leaner
(a) exists is upheld.

### 3.4 The durable point is a cadence, not a property of option (a)

Revision 1 called 1227 ms mean `mdb_env_sync` "option (a) makes the durable point
unaffordable on LMDB". The review answered that heed is opened `NO_SYNC`, so a
sync flushes what was dirtied since the last one and the cost should track the
write rate, not the store. Three cells, identical except `--durable-ms`
(VM, 120 s, campaign windows, option (a) on heed):

| cadence | durable points | mean per point | sync time per second of wall | kernel bytes | achieved rate |
|---|---|---|---|---|---|
| 250 ms | 189 | 384.6 ms | **0.605 s/s** | 16 413 MiB | 46 275 |
| 1000 ms | 83 | 443.6 ms | 0.307 s/s | 13 559 MiB | 49 715 |
| 4000 ms | 26 | 635.1 ms | **0.137 s/s** | 7 774 MiB | 49 557 |

**Neither story is right.** The cost per point grows only 1.65× while the
interval grows 16×, so it is not proportional to the bytes dirtied since the
last sync — but it is not fixed either: at the same 1/s cadence the 120 s cell
(store ≤975 MiB) paid 444 ms and the 600 s cell (store 3133 MiB) paid 1227 ms,
so it does grow with the store, sublinearly. The operational conclusion is the
one that matters and it is the opposite of revision 1's: **lengthening the
durable point is a real lever** — 4 s costs 4.4× less wall time per second than
250 ms and 2.1× fewer kernel bytes — and §11.4 already has the knobs
(`QUEEN_RAFT_DURABLE_EVERY_MS` 1000, `_BYTES` 256 MiB). **"Option (a) re-opens
D9" is withdrawn.** What remains true, and unmeasured, is that a 1 h window puts
ten times this store under the same `mdb_env_sync`.

One correction for §11.4 while it is open: it states "cost is proportional to
what changed (I8)", and on LMDB it is not — 16× the bytes per point cost 1.65×
the time. Whatever the flatness test of §13.6 asserts about durable points has
to be written against the measured shape, not that sentence.

---

## 4. Reading of the two, after the refutation

**Option (a) is the shape 005 asks for.** One structure answers both questions:
the probe is a point get (10–13 µs for ten hashes), the ack is 2 µs, nothing is
resident, nothing is rebuilt after a restart (20–36 ms to ready), and its probe
p99 — WP-0.4's criterion — is 18–24 µs against option (b)'s 431–575 µs. Its cost
is `rate × txns_window` rows on every voter: at the **product default**
(`server/sql/schema.sql:66`, `dedup_window_seconds DEFAULT 3600`) and 50 000
msg/s that is **180 M rows and 10.6 GB of logical dedup data per voter** in the
lean encoding (14.4 GB plain). The 600 s cell put 1712 MiB of dedup logical
bytes into a 3133 MiB LMDB store (which also carries the segment rows), so the
number to plan against is tens of GB per voter. Nobody has run that window.

**Option (a) does not violate G-3 or I8**, and revision 1 said it did. `rate ×
window` is a constant multiple of the write rate, independent of retained
volume, and its prune is O(pruned), not O(stored). The case against it is
absolute footprint and LMDB's per-message page dirtying (4.31 KiB written per
message in the 600 s cell), not an invariant.

**Option (b) is cheaper on disk and RAM and pays for it on every read.** Per
message it costs **3.3× less total disk** than plain (a) at the campaign's
window (49.5 vs 161.8 B) and **1.5× less** than a-lean at 120 s (141.9 vs
219.6 B); on the store alone 5× (a-lean, 120 s) to 11× (plain (a), 600 s); and
3–11× less RSS (221 vs 689 MiB at 120 s, 290 vs 3156 MiB at 600 s). Its store
does not grow with the window — one locator row per Append — while its blooms
(2.9–3.0 B/message resident) and its file table (one row per Append) do. Against
that: probe p99 24× worse on the VM and 27× on the laptop, ack-by-hash 24× worse
per hash and 30–55× per call even when batched the way 005 batches it, a restart
that has to scan the file table (49 ms for 826 k rows on LMDB, ~1.2 s projected
at the spec's ~18 M rows; 1262 ms and ~33 s on fjall), and §11.7 hash-only
compaction that is not optional for it.

**Option (b) has an unresolved design problem that no measurement fixes.**
§6.1 lists `dedup` among the **replicated** keyspaces; §6.2 lists only "dedup
blooms" as node-local. The harness's `txns` locator packs `(bucket, file_id,
offset, len)` into that value (`src/dedup.rs` `v_txns`) — a position exactly as
D8 defines one — and hash-only compaction rewrites those rows locally when it
moves a file. As implemented it breaks D8 and I7 outright. The fix is a split
(replicated `(pid, base_offset) → (end, created, count)`, node-local locator
beside `seg_loc`), which is small but has consequences nobody has costed: the
locators must ride the snapshot MANIFEST (§11.6), be rebuilt at recovery
(§11.5), and a node that has just installed a snapshot cannot answer a dedup
probe until they are. Option (a) stores no positions at all.

---

## 5. Recommendation for D10

1. **Keep option (a).** It is what the SQL's two questions want, it wins the
   criterion WP-0.4 names (probe p99, 24× on the VM), it wins the ack path
   (30–55× per call, batched), it holds nothing in RAM and it is ready 5–10×
   faster after a restart. The
   evidence that was supposed to overturn it rested on an index that is option
   (a) (§1) and on an ack workload that is not 005's (§3.3).
2. **Adopt the lean encoding** (`--option a-lean`, measured in §3.2/§3.3): no
   `(created_at, pid, hash)` secondary index; expiry through one sequential
   `txns` row per Append, `(pid, base_offset) → [end][created][hashes]`, pruned
   by the per-partition walk 006 already specifies. −31 % store, −36 % store
   ops, −30 % RSS, no change to either latency path, exact in both directions,
   restart clean. Its 16 B/message of hashes live in the store, not in the
   frames, so option (a)'s "no hash list in the segment file" property holds and
   §11.7 hash-only compaction stays unnecessary.
3. **Bound the durable point** (§11.4). At a 1 s cadence an (a)-sized LMDB
   spends 0.31 s of every second inside the durable point at a ≤1 GB store and
   0.55 s/s at 3.1 GB (269 points × 1227 ms in 600 s); at a 4 s cadence, 0.14
   s/s. The cadence, not the design, is what made revision 1 call it an
   outage. WP-1.4
   should make it a knob with a measured default, and should not put an
   unbounded `mdb_env_sync` on the apply thread.
4. **Do not adopt option (b), and do not discard it.** Its numbers are now
   honest and its restart exactness is proved. It is the fallback if — and only
   if — the measurement nobody has run says option (a)'s footprint is
   unaffordable. Before it could be adopted it needs the D8/I7 split of §4, and
   its ack path must be implemented the way 005 resolves, in one pass.
5. **Run the window D10 names before WP-1.2 freezes the keyspaces.** One VM
   campaign at `dedup_window = txns_window = 3600 s`, ≥ 2× the window, at an
   offered rate both designs sustain (~20 k msg/s), cells `a-lean`, `a` and
   `b`, reporting exactly what WP-0.4 names — probe p99, ack p99, RAM, disk,
   kernel bytes, rebuild-to-ready, exactness both directions — plus the store
   size and durable-point cost at steady state. This memo's extrapolations to
   1 h are arithmetic; the decision they support is reversible, and this is the
   run that would reverse it.
6. **The recent cache (revision 1's item 2) is option (b)'s problem, and the
   case for dropping it is weaker than revision 1 said.** On the probe the
   64 MiB cache bought nothing (mean 0.043 vs 0.041 ms) and cost the median;
   but the only end-to-end pair, `b-short` vs `b-short-nocache`, went 25 253 →
   17 488 msg/s (−31 %), and no VM cell ever ran with `--cache-mb 0`. If option
   (b) is ever revived, that pair has to be re-run rate-matched before the
   cache is dropped.

---

## 6. Residual risks

1. **Nobody has run the window D10 names, and it is the product default.** Every
   number here is a 60 s (laptop) or 360 s (VM) window; `schema.sql:66` says
   3600 s. Option (a)'s 180 M rows per voter and option (b)'s ten-times blooms
   and file table are arithmetic, not measurements. This is recommendation 5.
2. **No cell held 50 000 msg/s under a real ack load for a full run.** The
   campaign's option (a) held it 3.5 minutes; the refutation's 120 s cells hold
   it but with 15–145 ack calls/s. The harness is one thread doing planning,
   applying, fsyncing and pruning in series; §7.1 puts the durable point and
   file maintenance on the apply thread *between* entries. The comparison
   between designs is credible; the absolute rates are not a broker prediction.
3. **Ten minutes is not a soak, and two minutes is not even a cycle.** The
   refutation cells never reach the prune, so their footprint numbers are
   growth-phase. Option (a)'s rate in the pruning regime, measured once, was
   14–19 k msg/s in minutes 8–9 of the 600 s cell, while it was paying for
   inserts and deletes at once.
4. **The batched ack uses filler hashes.** A batched call carries the sample's
   hash plus nine random fillers standing in for the siblings of the same leased
   batch. Real siblings sit in the same frames, so they would widen the bloom
   candidate union less: the batched cost measured is an upper bound.
5. **A design decision is still hiding in the ack-by-hash workload.** The
   harness probes below the cursor on purpose — 005's worst branch. If O16 or
   WP-2.x narrows what the broker must answer there, option (b)'s remaining cost
   gap narrows with it.
6. **`--audit-misses` is what makes these exactness claims believable**, and it
   is a harness-only path. WP-1.x needs an equivalent (the differential fuzzer
   against the postgres oracle, §13.4).
7. **Crash behaviour of the dedup structures is untouched.** S1 measured the
   store's; nobody has killed a node mid-prune and checked that the file table,
   blooms and `txns` rows come back consistent with the segment files (I11).

---

## 7. Deferred (what these runs cannot show)

- **The 1 h window and steady state** (recommendation 5) — the only regime in
  which G-3 can be checked, and the only evidence that could re-open this
  decision.
- **a-lean in the pruning regime on the VM.** Its prune is measured on the
  laptop (0.088 ms per entry mean against plain (a)'s 0.045 at the same load —
  a-lean walks per partition, plain (a) walks one time-ordered index) and on the
  VM only in the growth phase. The trade is fewer rows written against a more
  scattered walk, and only a VM cell past `t = txns_window` settles it.
- **Option (b) with the D8/I7 split**, and with its ack path implemented the way
  005 resolves. Both are design changes, not tuning.
- **`--cache-mb 0` on the VM**, rate-matched (recommendation 6).
- **redb**, dropped by S1 for this write pattern; if a future design needs a
  small, low-traffic index, it has the trivial crash story for it.
- **Three brokers sharing one disk** (§13.6) and the interaction between the
  dedup structures and a snapshot install.

---

## 8. Refutations (2026-09-18) and how each was resolved

Two reviewers, both returning `refuted / blocker`. Nothing was dismissed; the
verdict changed. **Upheld** = the finding was right and the memo now says so;
**upheld and measured** = a new cell settles it; **refuted by measurement** =
the finding's mechanism was tested and is not what happens.

| # | finding | resolution |
|---|---|---|
| R1 | The recommended design was never measured: §4 recommended (b) minus the cache plus a new exact ack index, a composite in no cell. | **Upheld.** The recommendation is withdrawn. §5 recommends a design that was measured in five cells on two hosts. |
| R2 | Item 3's sizing premise is wrong against 005: the below-cursor span is `[txns_start, committed]`, so the "ack index" is option (a)'s index, row for row, and once it exists (b)'s machinery is overhead. | **Upheld, and it is the finding that decides.** §1 works it out against 005 line by line. Both reviewers found this independently. |
| R3 | A smaller ack row cannot exist: 005 needs `eff` (a MIN over a per-call range) and `below`, so the value must be the occurrence list. | **Upheld.** §1, second bullet; the 5591 multi-occurrence rows in a-heed show a (min, max) pair would not do. |
| R4 | WP-0.4's mandatory criterion — probe p99 at 50 k msg/s with a 1 h window — was never run, and 1 h is the product default (`schema.sql:66`), not a stress target. | **Upheld, not closed.** No 1 h cell exists and one does not fit in this task's VM budget. It is recommendation 5, risk 1, and the memo no longer flips a ratified decision on the extrapolation. |
| R5 | "The only one whose cost does not follow the window" is contradicted by the spike's own sweep: every (b) path that touches a file is linear in the window. | **Upheld.** The one-line recommendation no longer makes that claim. §4 says what is true: (b)'s *store* is per Append; its blooms, its file table and its frame scans are linear in the window, so it is a constant-factor win, not a different shape. |
| R6 | The ack measurement does not model 005: one hash per call, its own scan and frame reads, where 005 resolves the whole array in one pass. | **Upheld and measured.** `--ack-batch` added; `Dedup::resolve_batch` with a real one-pass implementation for (b). Laptop: 2.2× the resolutions at −6.9× per-hash cost and **+54 % throughput** (§3.3). Revision 1's argument (3) is withdrawn. |
| R7 | Option (a) was convicted on one wasteful encoding D10 does not mandate; a lean (a) without the secondary index was never run. | **Upheld and measured** (the encoding), **refuted by measurement** (the mechanism). `--option a-lean`: −31 % store, −36 % store ops, −30 % RSS, latency unchanged. But kernel bytes were unchanged (+2 %), so the secondary index was not the source of the write amplification: the random `(pid, hash)` row is (§3.3). |
| R8 | The 1227 ms durable point is mis-attributed and has no sensitivity run; `NO_SYNC` means it tracks bytes dirtied, not store size. | **Upheld (no sensitivity run) and refuted by measurement (the mechanism).** Three cadences, §3.4: cost per point grows 1.65× for a 16× interval, so it is not bytes-dirtied; it does grow with the store (444 ms at ≤975 MiB vs 1227 ms at 3133 MiB), so it is not purely rate. The conclusion changes anyway: a longer cadence is a 4.4× lever, §11.4 allows it, and "option (a) re-opens D9" is withdrawn. |
| R9 | The recommended option had no valid restart-exactness number, and the old data dirs are gone. | **Upheld and measured.** Three new option (b) cells with the corrected selection: `wrong=0` of 4001, of 3872 (laptop) and of 1793 (VM). §2. |
| R10 | The decision table dropped probe p99, the statistic WP-0.4 names. | **Upheld.** p99 is in every table in §3, and every figure carries its window and achieved rate. |
| R11 | "11–18× less store on disk" omits the disk where (b) puts its hashes. | **Upheld.** §3.1 now reports total disk per message: **3.3×** on heed, 1.7× on fjall, against 11–18× store-only. |
| R12 | System-level, every cell went the other way: (a) achieved 1.6–2.0× the rate of (b) in every cell. | **Upheld, and partly explained.** §3.3: 54 % of that gap was the unbatched ack workload. The rest stands, and the recommendation follows it rather than an extrapolation. |
| R13 | Item 2 (drop the recent cache) was recommended against the only end-to-end number that exists (−31 % rate), and was never run on the VM. | **Upheld.** Recommendation 6 keeps the cache question open and rate-matched; it is moot while (b) is not the design. |
| R14 | Option (b) puts node-local positions in a replicated keyspace (D8, I7, I1), and hash-only compaction rewrites them locally. | **Upheld, unresolved.** §4, last paragraph: as implemented it breaks D8/I7; the split is small but its snapshot and recovery consequences are uncosted. A reason not to adopt (b) on this evidence. |
| R15 | G-3 and I8 are misapplied: `rate × window` satisfies both as written; the real objection is absolute footprint plus an engine interaction. | **Upheld.** §4, second paragraph, says so in those words. |

Two further requests from the reviewers are **not** done, and are named as such:
one VM campaign at the 3600 s product default (R4 — recommendation 5), and a
measured composite of "(b) + an exact ack index" (R1 — not run, because §1 shows
the composite is (a) plus (b), so the honest comparison is a-lean against b,
which is what §3.2 measures).

### What changed in the harness for this revision

`src/dedup.rs`, `src/main.rs`, and nothing else: `Dedup::resolve_batch` (default
= the per-hash loop, which is what option (a) genuinely costs; a one-pass
implementation for option (b)), `--ack-batch N`, `--option a-lean`
(`StoreIndex::lean` + `prune_lean`), and the `ack_call_*` fields in the RESULT
line. `--ack-batch 1` reproduces the campaign's workload exactly, and every
earlier cell's parameters still mean what they meant.
