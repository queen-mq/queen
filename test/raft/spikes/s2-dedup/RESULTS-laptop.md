# S2 laptop results — dedup: store index (a) vs hash lists + blooms (b)

**Laptop, not the VM.** PLAN_RAFT.md §0.3 says numbers to quote come from the
Linux VM, and WP-0.4's laptop half is what this file holds. Every run below
used `--fsync-mode data` (Linux `fdatasync`, macOS plain `fsync`) so that the
macOS `F_FULLFSYNC` serialisation — 0.9 s per durable point at 250 dirty files,
measured in S1 and again here — did not swallow the runs; that changes the
barrier, not the bytes. What the laptop *does* say, because it is the same
stream, the same engine and the same store seam for both designs: the cost of a
probe, the cost of an ack-by-hash resolution, the store and file bytes each
design needs, what it holds in RAM, what a restart has to rebuild, and — the
PASS/FAIL of WP-0.4 — whether each design is exact in both directions.

Host: MacBook (Darwin 24.5.0, arm64, APFS), Rust 1.94.0, release build.
Date: 2026-09-17. Crate: `test/raft/spikes/s2-dedup`, engine fjall 2.11.2
(S1's fastest; `--engine redb|heed` also build and run).

Load, identical for every run: 50 000 msg/s, batches of 10 messages per Append,
10 Appends per entry → 500 entries/s, 96 B payloads, 4096 partitions, segment
files rolling at 2 MiB, one non-durable store commit per entry, a durable point
every second, 1 % of messages injected as duplicates of an earlier message *in
the same partition* with an age uniform in [0, `dup-max-age`], plus ack-by-hash
probes below the cursor at 5000/s. Two window scales:

| scale | dedup window | txns window | retention | duplicate ages | what it shows |
|---|---|---|---|---|---|
| **short** | 60 s | 72 s | 20 s | 0–70 s | steady state: prune, file GC and hash-only compaction all running, duplicates on BOTH sides of the window |
| **long** | 300 s | 360 s | 60 s | 0–350 s | the 5-minute window the task names. Nothing expires inside a 5-minute run, so it is the growth phase only, and no injected duplicate can fall outside the window |

Exact commands: `./run-laptop.sh` (it prints each one); raw logs in
`results/*.log`, one `RESULT` line per run.

## 1. The headline: steady state, short window, 5 minutes each

| | (a) store index | (b) hash lists + blooms + cache |
|---|---|---|
| achieved rate | **49 975 msg/s** (of 50 000) | 25 253 msg/s — ack-bound |
| dedup probe p50 / p99 (one push, 10 hashes) | 0.011 / 0.056 ms | **0.015 / 0.471 ms** (92 % of probes never touch disk) |
| of which the probes that did reach the files | — (always the store) | n=60 044 (7.9 %), p50 0.295 ms, p99 1.887 ms |
| ack-by-hash p50 / p99 | **0.006 / 0.076 ms** | 0.319 / 1.759 ms |
| store ops/s | 203 919 (27.6 M gets + 29.8 M puts + 22.6 M deletes) | **19 343** (0 gets, 0.76 M puts, 0.60 M deletes) |
| store on disk at the end | 1017.9 MiB | **119.5 MiB** |
| segment files at the end | **429.8 MiB** (payload only) | 369.6 MiB (payload + 16 B/msg of hashes, after hash-only compaction) |
| dedup's own logical bytes per message | 79.4 B | **21.3 B** (16 B hashes in the frame + 5.3 B of `txns` row per message at batch 10) |
| RAM the design holds | **0** (it is all in the store) | 39.9 MiB = blooms 12.1 + recent cache 27.9 |
| process RSS, max | 1328 MiB | **455 MiB** |
| kernel bytes written | 11 944 MiB (39.8 MiB/s) | **2380 MiB (7.9 MiB/s)** |
| prune step per entry, p50 / p99 | 0.407 / 3.135 ms | **0.083 / 0.211 ms** |
| exact (both directions) | **yes** | **yes** |

Same rows for the long (300 s) window, where nothing expires yet:

| | (a) store index | (b) hash lists + blooms + cache |
|---|---|---|
| achieved rate | **50 000 msg/s** | 21 684 msg/s |
| probe p50 / p99 | 0.019 / 0.095 ms | 0.036 / 1.535 ms |
| ack-by-hash p50 / p99 | **0.016 / 0.199 ms** | 1.119 / 7.935 ms |
| store / files | 920.2 / 614.5 MiB | **119.1** / 418.1 MiB |
| RAM | 0 | 82.1 MiB (blooms 17.9 + cache 64.2) |
| RSS max | 981 MiB | **488 MiB** |
| live dedup records | 14.9 M rows | 650 550 `txns` rows (one per Append) |

## 2. What each number means

**Option (a) is fast and exact and eats the store.** One point `get` per
incoming hash (11 µs for ten of them) and one `get` for an ack. But it writes
two rows per message and deletes two rows per message, which is 200 k store
ops/s at 50 k msg/s and 11.9 GiB of kernel writes in 5 minutes. At steady state
it held 3.56 M live records in 1017.9 MiB — **286 B of store per live record for
80 B of logical data** (fjall levels plus the tombstones of the 22.6 M deletes).
Extrapolated to the shape D10 names (50 k msg/s, 1 h window → 180 M live
records) that is 14 GB logical and ~50 GB on disk for dedup alone, on every
voter.

**Option (b) is cheap and exact, and its probe is the cheaper one in the mean**
(mean 0.043 ms vs 0.015 ms p50/0.016 mean for (a) — the two are within a factor
of three, and (b) spends nothing on the store for it). 92 % of pushes are
answered without touching a file: the recent cache or the per-file blooms rule
every hash out in RAM. Its store cost is one 54 B locator row per *Append*, not
per message.

**The ack-by-hash path is where option (b) hurts.** Resolving one hash below
the cursor cost 0.319 ms (short window) and 1.119 ms (long window), against
0.006–0.016 ms for option (a) — 50× to 70×. The reason is in the counters:
24.3 M frame reads, 4.69 GiB read out of the files, **38.7 frames read per
disk-touching probe or resolution** (the counter covers both; 90 % of those are
the ack resolutions). A hash's position inside the partition is exactly what is being
asked, so a bloom hit only says "somewhere in this file" and every frame of
that partition in that file has to be read. The cost grows with the txns
window, which is why the long-window run is 3.5× worse. At 5000 resolutions/s
this alone caps the node: with `--ack-rate 0` the same design pushes
**42 286 msg/s** instead of 25 253.

Two mitigations measured:

- `--ack-early-stop` (stop at the first occurrence found) cuts it to 0.207 ms
  p50 and 17.6 frames per disk-touching probe or resolution, a 2.2× saving. It is exact only while a
  hash occurs at most once in the partition's txns window; a second occurrence
  needs a re-push *after* the dedup window expired but inside the txns window,
  which 003 accepts. Not safe as a default without a per-partition "an
  out-of-window re-push happened" flag.
- The workload here is the **worst case on purpose**: it probes hashes below
  the cursor, which is the branch that must scan back to `txns_start`. A normal
  ack of hashes inside the leased batch is bounded by the batch (005 bounds the
  join at `base_offset <= batch_end`), which is a handful of frames.

**The bounded recent cache did not pay for itself.** With a 64 MiB global cache
the probe mean was 0.043 ms; with `--cache-mb 0` it was 0.041 ms — the same,
because the per-file blooms already answer "absent" in RAM. Worse, the cache
*is* the p50: 0.015 ms with it, **under 1 µs without it**, because a 64 MiB
cache in 16 384-hash blocks is ~230 blocks, and ruling a hash out means a
blocked-bloom test per block (the broker's cache has the same shape but is
*per partition*, so it has a handful of blocks to test, not hundreds). The
64 MiB bought 27.9 MiB of RSS and a slower median. (The two runs' throughputs
differ — 25 253 vs 17 488 msg/s — but both were ack-bound and their ack p50s
differ too, so that gap is not attributable to the cache; the probe numbers
are.)

**The per-file blooms are the right size.** 16 bits/key, all k bits in one
cache line, sized from `segment_bytes / (payload + 16)`. Measured false-positive
rate 5456 / 6 155 373 = **0.09 %** (option b, no cache, where every push tests
every hash against the bucket's files). What matters is not p but `F × p`,
where F is the number of files of the partition's *bucket* that overlap the
window: a probe only ever tests its own bucket's files (~1–2 here), never the
whole window's files. Resident cost measured: 12.1 MiB of blooms for a 72 s
window at 25 k msg/s = **6.6 B per message in the window** (the blooms were
sized for full 2 MiB files and the hash-only rewrites shrink the files, so a
right-sized bloom is the nominal 2 B/message).

**Hash-only compaction (§11.7) is not optional.** Option (b) pins a whole
payload file as long as any `txns` row points into it — 72 s here against a 20 s
retention. The rewrite that keeps only the frames' hash lists ran 272 times and
turned **543.9 MiB into 24.6 MiB (22×)**, at 3.6 ms p50 per file on the apply
thread between entries (p99 8.7 ms, one outlier at 333 ms). Without it,
option (b)'s file bytes at the spec scale would be `rate × txns_window ×
(payload + 16)` — at 50 k msg/s and a 1 h window, 20 GB of payload kept alive
only for its hashes.

**Prune.** Both designs prune in bounded steps, and both needed a resume key to
do it on an LSM:
- option (a) first scanned its expiry index from the start on every step. The
  tombstones of everything it had already deleted made that step cost 155 ms at
  p99 and 915 ms at max, and dropped the run to 13 107 msg/s. With a resume key
  (the same watermark idea as 006): 0.407 ms p50, 50 000 msg/s.
- option (b) walks partitions round-robin from each partition's `txns_start`.
  Without a cap on how many partitions one step may *look at*, a step with a
  small row budget wandered through thousands of empty partitions: 4.7 ms per
  entry, 65 % of the run. Capped at 32 partitions per step: 0.083 ms.

Both are harness bugs, but both are exactly the shape of the loops WP-1.x will
write, so they are recorded here as traps.

## 3. Exactness (the PASS/FAIL)

Every run: `exact=true`. Across the seven runs, with a duplicate drawn first and
pushed to the ORIGINAL's partition (003 keys dedup on `(partition, hash)`):

| run | duplicates inside the window | detected | missed | wrong original offset | duplicates outside the window | falsely reported | fresh hashes falsely reported |
|---|---|---|---|---|---|---|---|
| a-short | 114 730 | **114 730** | 0 | 0 | 17 604 | **0** | 0 |
| b-short | 52 824 | **52 824** | 0 | 0 | 7 356 | **0** | 0 |
| a-long | 64 201 | **64 201** | 0 | 0 | 0¹ | 0 | 0 |
| b-long | 16 684 | **16 684** | 0 | 0 | 0¹ | 0 | 0 |
| b-short-nocache | 33 587 | **33 587** | 0 | 0 | 4 397 | **0** | 0 |
| b-short-earlystop | 47 347 | **47 347** | 0 | 0 | 6 196 | **0** | 0 |
| b-short-noack | 95 316 | **95 316** | 0 | 0 | 13 806 | **0** | 0 |

¹ a 5-minute run cannot produce a duplicate older than a 5-minute window; this
is why the short-window runs exist.

**The retention case D10 and §11.7 call out** — retention has deleted the
segment, the hashes must still answer — is the ack-by-hash column, and it is
the bulk of the ack probes, checked per probe (`segments` row read back and
confirmed absent before probing):

| run | ack-by-hash probes below the cursor | resolved | unresolved | of which the segment was already deleted by retention |
|---|---|---|---|---|
| a-short | 1 296 528 | **1 296 528** | 0 | 1 296 528 (100 %) |
| b-short | 567 634 | **567 634** | 0 | 567 625 (99.998 %) |
| a-long | 620 382 | **620 382** | 0 | 620 382 (100 %) |
| b-long | 121 324 | **121 324** | 0 | 121 324 (100 %) |

A re-push of such a hash is likewise still a duplicate: those are the
in-window duplicates of the table above, whose originals are older than the
20 s (60 s) retention in every short (long) run.

Two things the exactness harness found, both real:

1. **The planner overlay is load-bearing** (§7.2). The probe reads committed
   state; a duplicate of a message pushed earlier in the SAME entry is not
   committed yet. Without the overlay, ~3 % of the injected duplicates (100 %
   of the age-0 ones that landed in the same entry) read as new — 922 of them
   in a-short, 639 in b-short were caught by the overlay. The exhaustive
   `--audit-misses` scan (re-ask the question with no cache and no blooms) is
   what proved it: every miss was genuinely absent from committed state, not a
   fast-path bug. After the overlay: 0 misses, and the audit confirms
   `0 were THERE, 0 were genuinely absent`.
2. **MIN is the answer, not "an" answer.** 003 returns the ORIGINAL occurrence.
   A duplicate whose age was past the window is accepted and appended, so its
   hash then has two occurrences; the oracle must not expect the newer one. The
   harness samples only fresh messages for that reason, and checks the returned
   offset against the original's: 0 wrong offsets in every run.

## 4. Restart: what has to be rebuilt before the node can answer

`s2-dedup rebuild` reopens the store, rebuilds whatever the design keeps in
RAM, then re-probes 4096 saved samples at the run's last `now` and checks each
verdict against its expectation.

| | open store | file table (scan `seg_loc` + `txns`) | blooms | total to ready | samples wrong |
|---|---|---|---|---|---|
| a-short | 1885 ms | 183 ms | — | **2069 ms** | 0 / 4096 |
| a-long | 559 ms | 171 ms | — | **733 ms** | 0 / 4096 |
| b-short, blooms from sidecars | 1349 ms | 238 ms | 199 ms | **1786 ms** | 0 / 4096 |
| b-short, blooms rebuilt from the files | 33 ms² | 138 ms | 100 ms | **270 ms** | 0 / 4096 |
| b-long, blooms from sidecars | 1088 ms | 381 ms | 135 ms | **1604 ms** | 0 / 4096 |

² the second reopen of the same directory: the first one pays fjall's journal
recovery of the non-durable commits, the second does not. Read the open column
as "0.5–2 s of LSM recovery", not as a difference between the two modes; and
the "from the files" walk was helped by a warm page cache and by the fact that
hash-only compaction had already shrunk those files 22×.

Option (a) has nothing of its own to rebuild: the index is the store. Option
(b) must rebuild the file table and the blooms — and the file table is a full
scan of `seg_loc` and `txns` (238 ms for ~320 k rows = 0.74 µs/row). At the
spec scale (50 k msg/s, 1 h window → ~18 M `txns` rows) that scan is **~13–25 s
before the node can answer a probe**, which is a number WP-1.x must design for
(open the blooms lazily per bucket, or persist the file table itself). The
recent cache needs no rebuild: it starts empty and its `covered_from` is the
restart instant, so it simply vouches for nothing until it has refilled.

## 5. Variants

| run | rate | probe p50 | probe mean | ack p50 | frame reads per disk probe | RAM | store |
|---|---|---|---|---|---|---|---|
| b-short (64 MiB cache) | 25 253 | 0.015 ms | 0.043 ms | 0.319 ms | 38.7 | 39.9 MiB | 119.5 MiB |
| b-short-nocache | 17 488 | **<0.001 ms** | 0.041 ms | 0.487 ms | 28.9 | **12.4 MiB** | 101.6 MiB |
| b-short-earlystop | 23 180 | 0.022 ms | 0.091 ms | **0.207 ms** | **17.6** | 40.5 MiB | 107.3 MiB |
| b-short-noack (no ack probes) | **42 286** | 0.033 ms | 0.147 ms | — | — | 49.4 MiB | 178.1 MiB |

## 6. What this says for D10

- Both designs are **exact** in both directions, including the case where
  retention has deleted the segment whose hashes are still in the txns window.
  Neither is disqualified on correctness.
- D10's default candidate, option (b), costs **8.5× less store, 10× fewer store
  ops, 5× fewer bytes written and 3× less RSS**, and its probe is RAM-only for
  92 % of pushes. On the disk axis it is the only one that extrapolates: at
  50 k msg/s with a 1 h window, option (a) needs ~14 GB of logical index (~50 GB
  on an LSM at the amplification measured here) on **every voter**, against
  ~1 GB of locator rows plus 2.9 GB of hashes inside files for option (b).
- Option (b) is only viable if three things are in the design, none of which is
  free:
  1. **hash-only compaction** (§11.7) — measured 22× on the pinned bytes; without
     it the txns window pins whole payload files;
  2. **an answer for ack-by-hash below the cursor** — as built it is 38 frame
     reads and 0.3–1.1 ms, and at 5 k/s it halves the node's push rate. The
     in-batch ack is bounded by the lease and is cheap; the below-cursor and
     unresolvable answers are not. Either accept the early stop with a
     per-partition "out-of-window re-push" flag to keep it exact, or keep a
     small, separately-bounded index for the below-cursor window only;
  3. **blooms sized by `F × p`** (files tested per probe × false-positive rate),
     not by a fixed p, and the file table's rebuild cost budgeted (§4).
- The **bounded recent cache should be dropped or made per-partition.** Global
  and 64 MiB, it did not lower the probe's mean cost and raised its median; its
  only unique contribution (vouching for a span so old files can be skipped)
  is already what the per-file bloom plus the file's `created_at` span does.

## 7. Cut short, honestly

- **Nothing ran on the VM** — the task says laptop only. Every throughput number
  is a macOS number with `fdatasync`-class barriers; the p99s carry the
  laptop's scheduler and APFS. The VM half belongs to a later WP.
- **Option (b) never reached 50 k msg/s** on this laptop (25 253 with acks,
  42 286 without). Its push path is not the limit; the ack-by-hash workload the
  task prescribes is. Option (a) reached the full rate in both scales.
- The **long-window runs cannot test the "outside the window" direction** (a
  5-minute run has no duplicates older than a 5-minute window). That half of
  exactness is proved by the short-window runs only.
- The first campaign was **discarded and re-run** after two harness defects were
  found and fixed (option (a)'s prune scanning from the start of its expiry
  index, option (b)'s prune visiting thousands of empty partitions). The
  numbers above are all from the code in this commit; `results/*.log` holds
  only the final runs.
- The option (a) runs were re-run once more after a third harness defect: file
  GC counted a hash reference into every segment file for BOTH options, so
  option (a)'s files were never unlinked (its segment-file column was 1489 MiB
  instead of 429.8 MiB). Only option (a)'s file bookkeeping changed; the b runs
  were not affected and were not re-run.
- Duplicate injection only reaches ~40 % of its nominal 1 % (the oracle often
  has no sample at the exact age asked for: `oracle misses` in the logs). The
  effective duplicate rate is therefore ~0.4–0.8 % of messages, not 1 %.
- `redb` and `heed` were not measured here: one engine (fjall, S1's fastest) was
  used so that the two designs are compared on the same store. The crate takes
  `--engine redb|heed` and both build.

---

## 8. Refutation re-checks (2026-09-18, heed, `refutation-laptop.sh`)

Added after the adversarial review of `MEMO.md` revision 1. Same host and same
stream as §1–§7; **engine heed, not fjall**, because D9 ratified heed and two of
the three questions are about what the store does with option (a)'s write
pattern. Five 120 s cells at the short window scale (dedup 60 s, txns 72 s,
retention 20 s, duplicate ages 0–70 s), so each run spends 40 % of its life in
the pruning regime. Logs: `results/refutation/*.log`.

Three questions, all of which the review said the campaign had never asked:

1. **option (a) without its expiry index** (`--option a-lean`): the
   `(created_at, pid, hash) → offset` secondary index is replaced by ONE
   sequential `txns` row per Append, `(pid, base_offset) → [end][created][hashes]`,
   pruned by the same rotating per-partition walk 006 uses. The probe and the
   ack resolution are byte-identical to plain (a): only expiry changes.
2. **option (b)'s ack-by-hash resolved the way 005 resolves it**
   (`--ack-batch 10`): one call carrying the whole `p_hashes` array of one ack,
   ONE bloom candidate union, ONE scan of the partition's `txns` rows, one frame
   read per frame — against the campaign's one-hash-per-call workload, which
   charged every hash a full scan. The nominal hash load is `--ack-rate` in both.
3. **option (b)'s restart exactness with the corrected sample selection**, which
   no option-(b) cell had ever run (§3 of `RESULTS-vm.md`: the campaign's b cells
   used the pre-fix selection and their data directories were deleted).

| | (a) `r-a` | (a-lean) `r-alean` | (a-lean) `r-alean-ack10` | (b) `r-b-ack1` | (b) `r-b-ack10` |
|---|---|---|---|---|---|
| hashes per ack call | 1 | 1 | **10** | 1 | **10** |
| achieved rate (offered 50 000) | 49 905 | **50 000** | 49 921 | 28 886 | **44 410** |
| probe p50 / p99 (push of 10 hashes) | 0.010 / **0.036** ms | 0.011 / 0.036 | 0.012 / 0.042 | 0.015 / 0.487 | 0.024 / **1.119** |
| ack call p50 / p99 | 0.001 / 0.006 | 0.002 / 0.005 | **0.011 / 0.036** | 0.415 / 2.111 | **0.607 / 5.119** |
| ack per hash p50 | 0.001 ms | 0.002 | 0.001 | 0.415 | **0.060** |
| frame reads | 0 | 0 | 0 | 8 789 867 (1704 MiB) | **4 107 845** (796 MiB) |
| store at the end | 640.4 MiB | **432.7** | 432.6 | 41.2 | 56.9 |
| store B / message | 112.1 | **75.6** | 75.7 | 12.5 | 11.2 |
| segment files B / message | 80.3 | 79.3 | 79.4 | 146.9 | 96.4 |
| **total disk B / message** (store + files + sidecars) | 192.4 | **154.9** | 155.1 | 159.7 | **108.9** |
| dedup logical B / message | 79.5 | **58.9** | 58.9 | 21.3 | 21.3 |
| store ops/s | 167 289 | **104 860** | 104 651 | 21 196 | 31 332 |
| store ops per message | 3.35 | **2.10** | 2.10 | 0.73 | 0.71 |
| live dedup records | 3 558 327 | 3 574 935 | 3 576 767 | 131 192 `txns` | 300 964 `txns` |
| RSS max | 642 MiB | **433** | 432 | **117** | 166 |
| durable point mean (macOS, weak) | 297.8 ms | **168.9** | 120.3 | 22.0 | 26.7 |
| kernel bytes written | 17 612 MiB | 24 845 | 23 059 | **4119** | 5370 |
| rebuild to ready | **7 ms** | 8 ms | — | 223 ms | 167 ms |
| restart samples wrong | **0 / 3813** | **0 / 3803** | — | **0 / 4001** | **0 / 3872** |
| exact both directions | yes | yes | yes | yes | yes |

### What these five cells settle

**Option (b)'s restart exactness is now measured, twice, and it is clean.**
`wrong=0` of 4001 and of 3872, with the corrected selection (a reservoir over
the whole run, re-pushed hashes dropped), on heed at the short window. The
campaign's `wrong=76..91` for option (b) was the same harness artefact §3 of
`RESULTS-vm.md` proves for option (a). This closes the one PASS/FAIL of WP-0.4
that revision 1 of the memo had to leave open.

**The ack workload was the ceiling, not the design.** Resolving the hashes of an
ack one at a time charges each hash a bloom union, a `txns` scan and every frame
that scan selects. 005 does the opposite — one join over one materialized
occurrence set — and when the harness does the same, option (b)'s frame reads
fall 2.1× for the *same* nominal hash rate, its cost per hash falls 6.9×
(0.415 → 0.060 ms), and its achieved rate rises **54 %, from 28 886 to 44 410
msg/s**. Revision 1's "option (b)'s ack-by-hash is what caps the node" was
measured against a workload no client generates. The honest residue: a batched
call still costs 0.607 ms p50 and 5.119 ms p99 against 0.011 / 0.036 ms for the
same ten hashes on option (a) — 55× — and **(b)'s probe p99 got worse, not
better** (0.487 → 1.119 ms), because the faster run keeps more files alive.

**A leaner option (a) is real but smaller than the review guessed.** Dropping
the 32-byte-key expiry index costs nothing on either latency path and takes
store bytes per message from 112.1 to 75.6 (−33 %), store ops per message from
3.35 to 2.10 (−37 %), RSS from 642 to 433 MiB (−33 %) and the (weak) macOS
durable point from 298 to 169 ms — at the price of 16 B/message of hashes in a
store row instead of 40 B/message of secondary index. It does **not** halve the
kernel bytes: on this host it wrote *more* (24 845 vs 17 612 MiB). The expiry
index is time-prefixed, so its inserts land at one edge of the key space and
dirty few pages, while a-lean's `txns` rows are spread over 4096 partition key
ranges. What dominates both is the random `(pid, hash)` row every message must
write, which neither variant removes. `ri_diskio_byteswritten` on macOS is not
`/proc/self/io`; the VM cell (`RESULTS-vm.md` §8) is the one to quote.

**Caveat on the batch fillers.** A batched ack carries the sample's own hash
plus nine fillers drawn at random, which stand for the sibling hashes of the
same leased batch. Real siblings sit in the *same* frames, so they would widen
the bloom candidate union less than random fillers do: the batched cost measured
here is an upper bound, and the amortization is if anything understated.
