# S2 — dedup (PLAN_RAFT.md WP-0.4, decision D10)

Which dedup design can the RSM carry: a store index per message hash, or the
hash lists already inside the Append records plus per-file bloom filters and a
bounded recent cache?

Both are implemented behind one trait and measured on the same stream, on top
of the S1 store seam. The SQL is the specification, not this harness:

| question | SQL | what the harness asks |
|---|---|---|
| dedup probe | 003 `log_push_one_v1` | for each incoming hash: is there an occurrence in THIS partition with `created_at >= now - dedup_window`, and at which ORIGINAL (MIN) offset? A duplicate writes nothing; the survivors are repacked and appended. |
| ack by hash | 005 `log_ack_by_hash_v1` | for one hash: the MIN offset inside the ackable span, and whether ANY occurrence sits at or below the cursor (the `noopHashes` / `staleHashes` answer). No time filter — hence D10's rule that the hash records outlive the segments retention deletes. |
| purge | 006 `log_txns_purge_step_v1` | bounded steps that drop everything older than the txns window `max(dedup_window, completed_retention, 900 s)`, per partition watermark. |

## The two options

**(a) `--option a`, `StoreIndex`** — one store row per `(partition, hash)`
whose value is the occurrence list `[(offset, created_at)]`, plus an expiry
index `(created_at, partition, hash) -> offset`. Probe = one point `get` per
incoming hash (the same `get` the write then extends, so a duplicate costs
nothing extra). Ack by hash = one `get`. The segment frames carry NO hash list
in this option: the store is the authority, so option (a) saves 16 B/message in
the files.

**(b) `--option b`, `HashLists`** — the hash list stays in the segment frame
(`len|xxh3|flags|pid|base|count|created_at|hashes|blob`); the store keeps one
`txns` locator row per Append (38 B value, 16 B key), which outlives the
`segments` row retention deletes. Probe = the recent cache, then the blooms of
the files of that partition's bucket that overlap the window, then the hash
lists of that partition's frames inside a file whose bloom hit.

- the recent cache is a global temporal ring of sorted 16-byte blocks fronted
  by blocked blooms, the shape of the broker's own cache (server/src/dedup.rs).
  It answers "absent" authoritatively only for `[covered_from, now]`, so a
  hash it rules out still has to be looked for in older files, and a hash it
  flags falls through to the exact path (which is where the original offset
  comes from anyway).
- one bloom per file, 16 bits/key by default, all k probe bits in one cache
  line. Blooms are written next to their file when it is sealed, so a restart
  does not have to re-read every hash list.
- `§11.7` hash-only compaction: when retention has deleted every `segments` row
  of a sealed file but `txns` rows still point into it, the file is rewritten
  keeping only the frames' hash lists (write temp, fsync, rename), and the
  `txns` rows get the new positions. Without it, option (b) pins whole payload
  files for the txns window.

## The planner overlay

The probe reads COMMITTED state. A duplicate of a message pushed earlier in the
SAME entry is not committed yet, so the driver keeps the overlay of §7.2 —
`(pid, hash) -> offset` for the current cycle — and consults it first. It is
identical for both options, so it lives in the driver, not in the designs.
Without it, ~3 % of the injected duplicates at age 0 read as new; the
`--audit-misses` path (an exhaustive scan that re-asks the question without
cache or blooms) is what found that.

## Running

```
cargo build --release
./target/release/s2-dedup run --option a|b --dir <data> [flags]
./target/release/s2-dedup rebuild --option a|b --dir <data> --mode blooms|files
./run-laptop.sh          # the campaign behind RESULTS-laptop.md
./rerun-a.sh             # the option (a) runs, repeated after the file-GC fix
./s2-vm-resume.sh        # on the VM: the a-heed cell + the sample-selection cells
./repro-samples.sh       # the laptop half of RESULTS-vm.md sec 3
```

Flags that matter: `--rate` (msg/s), `--batch` (messages per Append),
`--entry-appends` (Appends per entry), `--window-s`, `--txns-s`,
`--retention-s`, `--dup-pct`, `--dup-max-age-s`, `--ack-rate`, `--ack-batch`,
`--cache-mb`, `--bloom-bits`, `--segment-bytes`, `--hash-compact`,
`--verify-frames`, `--ack-early-stop`, `--durable-ms`,
`--engine redb|fjall|heed`.

Added 2026-09-18 with the refutation of `MEMO.md` revision 1
(`refutation-laptop.sh`, `refutation-vm.sh`):

- **`--option a-lean`** — option (a) with the `(created_at, pid, hash) -> offset`
  expiry index replaced by ONE sequential row per Append in `txns`,
  `(pid, base_offset) -> [end][created][hashes]`, pruned by the same rotating
  per-partition walk 006 specifies. The probe and the ack resolution are
  byte-identical to `--option a`; only expiry changes. Measured: -31 % store,
  -36 % store ops, -30 % RSS, same latencies, same exactness.
- **`--ack-batch N`** (default 1) — how many hashes one ack-by-hash resolution
  carries. 005 resolves the WHOLE `p_hashes` array of one ack in one pass over
  one partition's rows ("ONE join, ONE materialized resolved set"); `N = 1`
  reproduces the 2026-09-17/18 campaign's workload, which charged every hash a
  full scan. The nominal hash rate stays `--ack-rate`: the driver makes
  `--ack-rate / N` calls per second. `Dedup::resolve_batch` is a per-hash loop by
  default (which is what option (a) genuinely costs) and a real one-pass
  implementation for option (b).

`--legacy-samples true` restores the pre-2026-09-18 selection of the samples
`rebuild` re-probes (the first 4096 ack probes, and no filter for hashes the run
itself re-pushed). It exists so the defect that selection caused can be shown and
re-shown; see RESULTS-vm.md sec 3.

`run` exits 1 when exactness failed (a duplicate inside the window missed, one
outside it reported, a wrong original offset, or an ack-by-hash that did not
resolve); `rebuild` exits 1 when a sample answers differently after a restart.

## What the restart check asks (and what it does not)

`rebuild` re-probes the samples the run saved, at the run's last `now`, and
compares each verdict with what the sample says. Two rules keep the
comparison honest, both added on 2026-09-18 after the first VM campaign:

- the samples are a **reservoir over the whole run**, not the first 4096 ack
  probes. With "the first 4096" every saved sample was created in the first
  seconds of the run, so by the end all of them were long past the dedup window
  and the check only ever asked the `expect no answer` direction.
- a sample whose hash the run itself **re-pushed out of window** is dropped.
  003 accepts such a push and appends it, so the hash then has a second,
  younger occurrence and the probe rightly answers it; the sample, written from
  the first occurrence, no longer says what the answer must be. `run` prints how
  many samples this drops, and `rebuild` reports, for every mismatch, whether a
  younger occurrence of that hash is still in the store.

## Exactness oracle

1 % of the messages are duplicates. The duplicate is drawn FIRST, from a
per-second ring of samples, and the push then goes to the ORIGINAL's partition
— 003 keys dedup on `(partition, hash)`, so a repeated hash elsewhere is not a
duplicate. Ages are uniform in `[0, --dup-max-age-s]`, deliberately wider than
the window, so both directions are tested: every duplicate inside the window
must be detected with the original's offset, none outside it may be reported.
A sample is consumed when it is injected, so an injected hash has exactly one
prior occurrence and its expectation is unambiguous.

Ack-by-hash probes pick samples aged between `retention_s` and 80 % of the
txns window: their payload segment has been deleted by retention while their
hashes are still inside the txns window — the case D10 and §11.7 call out. The
harness checks that the `segments` row really is gone before probing.

## Reuse of S1

`hist.rs` and `sys.rs` are S1's files, included with `#[path]` — not copied.
`src/engines/` IS a copy of S1's (2026-09-17) with S2's table list and one
added method, `Engine::range` (an ordered scan that yields keys and values; S1
only needed `prefix_count`). S1's sources are untouched so that a parallel S1
run cannot be disturbed. `src/seg.rs` is derived from S1's with 16-byte hashes,
`read_hashes` (header + hash list, no payload) and `rewrite_hash_only`.
