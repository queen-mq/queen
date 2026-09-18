# S1 — store engine spike (PLAN_RAFT.md WP-0.3)

Which embedded ordered store can carry the RSM of PLAN_RAFT.md: redb, fjall or
heed (LMDB)? This crate replays a **synthetic apply stream** shaped like §6.1
and §7.1 against all three behind one trait, and measures what §15 asks for.

It is standalone on purpose: its own `Cargo.toml` with an empty `[workspace]`
table, `rust-version = "1.88"` (C-3), never added to `server/Cargo.toml`.

```
cargo build --release          # macOS and Linux
./target/release/s1-store run    --engine redb  --dir /var/tmp/s1 --rate 20000 --duration 60
./target/release/s1-store run    --engine redb  --dir /var/tmp/s1 --shape ratified \
    --store-commit-ms 4 --store-commit-entries 256          # §11.3 + §6.1 + D10 as ratified at G0
./target/release/s1-store verify --engine redb  --dir /var/tmp/s1
./target/release/s1-store iter   --engine heed  --dir /var/tmp/s1-iter --iter-keys 10000000
./target/release/s1-store reads  --engine heed  --dir /var/tmp/s1 --read-threads 8 --heed-no-tls 1
./target/release/s1-store info
```

**Two store shapes.** `--shape legacy` (the default) is what the 2026-09-17
campaign measured: one store write transaction per applied entry, `segments`
rows in the store, no dedup keyspace. `--shape ratified` is the store the G0
amendments of 2026-09-18 describe: the write transaction is committed every
`--store-commit-ms` (4) / `--store-commit-entries` (256) per §11.3, `segments`
is NOT in the store per §6.1, and a `dedup` keyspace carries D10 option (a) —
one row per message, key `(pid, 8 random bytes)`. The default is `legacy` only
so the 2026-09-17 cells stay reproducible; **new measurements should use
`ratified`**.

## What one applied entry does

One entry = one planning cycle's effects in ONE write transaction, non-durable,
with a durable point (§11.4) every `--durable-ms` (1000) or `--durable-bytes`
(256 MiB). Per entry, at `--batch` (10) messages per push:

| keyspace | write |
|---|---|
| `segments` | 1 row per push batch, key `(pid, base_offset)` 16 B, value 40 B — **`--shape legacy` only**: the §6.1 G0 amendment takes this keyspace out of the store |
| `dedup` | **`--shape ratified` only**: 1 row per MESSAGE, key `(pid, 8 uniformly random bytes)` = D10 option (a)'s `(pid, hash)`, value `(offset, created_at)` |
| `seg_loc` | the node-local position of the same batch (§6.2, D8) |
| `partitions` | the partition row |
| `pending` | one row per subscribed group (an `Append` is O(groups)) |
| `cursors` | a claim (pop) and a commit (ack) |
| `request_ids`, `request_expiry` | the command's request id (D6) + expiry deletes as the window slides |
| `counters` | the O(1) stats of D16, hot in RAM, written out |
| `kv`, `kv_expiry` | put / delete / CAS mix at `--kv-rate` (up to 40k ops/s), plus byte-ordered prefix lists |
| `timers`, `timers_due` | a schedule and a fire at `--timer-rate` |

Payload bytes never enter the store: they are appended to 256 per-bucket
append-only segment files (D9, §11.2), framed
`len | xxh3 | pid | base_offset | count | created_at | hashes | blob`, fsynced
only at the durable point.

## Measurements printed by `run`

throughput and pacing lag · non-durable commit p50/p99/p99.9 · durable point
p50/p99 with its segment-fsync share · KV read and prefix-list latency · write
amplification (both from the kernel's per-process write counter and from
allocated bytes on disk) · RSS over time · file growth · consistent export cost
for a snapshot and whether the engine can do it incrementally · reclamation
after deletes (chunked deletes, engine maintenance, segment files unlinked) ·
ordered iteration · reopen time. The last line is a machine-readable `RESULT`
line for the scripts.

## Scripts

| script | what it does |
|---|---|
| `run-matrix.sh [duration]` | engines × rates (`ENGINES`, `RATES`), one table at the end |
| `kill-loop.sh [runs]` | N runs of start → load → `kill -9` at a random moment during non-durable commits → reopen → `verify`; counts PASS/FAIL and how often the engine reopened past its last durable commit |
| `flaky.sh [runs]` | the same verdict after **dropped unflushed writes**: loop device + `dm-flakey drop_writes` + ext4. **Linux, root**; cleans up its own loop/dm device and mountpoint on exit |
| `vm-refute.sh` | the 2026-09-18 refutation round: the ratified-shape matrix, the MDB_NOMETASYNC cell and the threaded read benchmark, sized to fit ~8 minutes of VM wall clock. **Linux** |
| `vm-campaign.sh [short\|full] [steps]` | the whole VM campaign in one driver: matrix → kill loop → flaky loop → soak, picking the two fastest engines for the crash steps. `short` is what fits a 45-minute budget (what ran on 2026-09-17), `full` is the 2 h / 100-run / 20-run version WP-0.3 asks for. **Linux, root** |

`verify` is the PASS/FAIL of WP-0.3: it reopens the store, reads the applied
index it reports, and checks that every segment the state references is really
on disk with a matching checksum (I11), that no entry is torn across tables,
and whether the applied index is ahead of the last durable index.

## Flags worth knowing

| flag | default | why |
|---|---|---|
| `--rate` | 20000 | messages/s; entries/s = rate / batch |
| `--duration` | 60 | seconds |
| `--segment-bytes` | 67108864 | roll size; short runs need a small value (e.g. 262144) or no file ever rolls and reclamation cannot be observed |
| `--fsync-threads` | 1 | the durable point fsyncs one file per bucket TOUCHED; with 256 buckets that is the dominant cost, so this measures what parallelising it would buy |
| `--fsync-mode` | full | `full` = F_FULLFSYNC on macOS / fsync on Linux; `data` = fsync on macOS (does NOT flush the drive cache) / fdatasync on Linux. `data` is for reading laptop numbers, never a durability mode to ship |
| `--cache-mb` | 256 | store cache (redb page cache, fjall block cache); heed uses the OS page cache, so its RSS is not comparable |
| `--map-size-gb` | 32 | LMDB map size (fixed at open) |
| `--shape` | legacy | `legacy` = the pre-G0 store, `ratified` = §11.3 + §6.1 + D10 as amended at G0 |
| `--store-commit-ms`, `--store-commit-entries` | 0, 1 | §11.3's commit cadence (the plan says 4 / 256). `0` / `1` keeps the old per-entry write transaction |
| `--segments-in-store`, `--dedup-rows`, `--dedup-window-s` | per `--shape`, 0 | the three shape switches on their own; the window prunes dedup rows older than N seconds |
| `--heed-flags` | nosync | `nosync` (MDB_NOSYNC), `nometasync` (MDB_NOMETASYNC: data pages flushed per commit, meta page deferred — `lmdb.h` calls this unconditional integrity), `both`, `none` |
| `--heed-no-tls`, `--heed-max-readers` | off, LMDB default (126) | open with MDB_NOTLS (`read_txn_without_tls`), the only mode where `RoTxn` is `Send`; and the reader-slot table size |
| `--read-threads`, `--readers` (`reads` only) | 1, 0 | concurrent reader threads; `--readers N` holds N simultaneous read txns to find the slot ceiling |
| `--self-kill-after-ms`, `--kill-point` | 0, random | raise SIGKILL at `append` / `precommit` / `postcommit` of an entry |
| `--verify-all`, `--verify-limit` | off, 200000 | how much of `seg_loc` `verify` checks |
| `--sample-ms` | 1000 | RSS sampling interval |
| `--growth-every-s` | 0 (off) | print a `GROWTH` line (RSS, store bytes, segment bytes, file count, entries) every N seconds |
| `--delete-at-s`, `--delete-pct` | 0 (off), 30 | one mid-run delete phase: drop the oldest N% of the rows written so far, unlink the sealed files below them, run the engine's maintenance, print a `MIDDELETE` line, and carry on loading |

## Results

- `MEMO.md` — **the decision memo of WP-0.3**: every measured quantity per
  engine, the PASS/FAIL of the mandatory criterion, deps/MSRV/license/
  maintenance, the recommendation for D9 and the residual risks.
- `RESULTS-laptop.md` — 60 s smoke per engine on the MacBook (smoke only: macOS
  serialises `F_FULLFSYNC`, so durable points there are not a Linux number).
- `DEPS.md` — build deps, MSRV, license, maintenance activity per engine.
- `RESULTS-vm.md` — the Linux VM campaign (WP-0.3 part 2, 2026-09-17): the
  9-cell matrix, 45 kill runs, 20 dropped-write runs and a 10-minute soak, with
  what was cut and what is deferred. Raw logs in `results-vm/`. **Its §1 is the
  pre-G0 store shape**; see the warning at the top of that file.
- `RESULTS-refutation.md` — the 2026-09-18 round, after two adversarial reviews
  of `MEMO.md` came back `refuted`: the ratified store shape for all three
  engines, a 2×2 that isolates why redb's ceiling moved, MDB_NOMETASYNC, and the
  D15 read path from 1/4/8 threads in both LMDB reader modes. Every finding and
  its resolution is in `MEMO.md` §6.
