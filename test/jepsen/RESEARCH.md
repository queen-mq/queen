# Jepsen for Queen: research and plan (2026-09-24)

Research by a sub-agent, 2026-09-24 morning (read-only; nothing run). Paths cite the code as it was then
(raft before the kafka-inproc/single-binary merges); re-verify before relying on a line number.
Since then: the payload-free rehydrate (7538bba0) and the forward gate (44adbbc6) are committed; the
Kafka liveness view moved to GET /api/v1/raft/liveness; kafka-inproc is merged into raft; Kafka offsets can
be native positions (QUEEN_KAFKA_OFFSET_STORE). The Jepsen cluster is 5 nodes, not 3.

This was read-only research: I touched no VM, changed nothing in either repo, and ran nothing, so none of the predicted failures below has been observed yet. The public repos (jepsen 0.3.15-SNAPSHOT, redpanda, bufstream, nats, lazyfs) are cloned under `/private/tmp/claude-502/-Users-alice-Work-queen/e6e72943-7078-4ffb-8317-d552a7de6a9e/scratchpad/jepsen/`.

Path prefixes used below:
- **S** = `/Users/alice/Work/queen/server/src` (branch `raft`, including the 2 uncommitted files)
- **K** = `/Users/alice/Work/queen-kafka-inproc/protocols/queen-kafka/src` (kafka-inproc at `ce4c5e29`)
- **KS** = `/Users/alice/Work/queen-kafka-inproc/server/src`

## Bottom line
- **Kafka workload:** runs against queen-kafka today with configuration only, in `--no-txn` mode (assign or subscribe). Under faults it will fail because of the facade, not raft. `--txn` cannot run at all in cluster mode.
- **Recommendation:** start with the native suite, beginning with the same Kafka checker pointed at Queen's own push + fetch. Both already return per-partition offsets.
- **Likely findings, from reading the code:**
  - KV reads skip the read barrier.
  - A node can apply entries before its own fsync, and the store's durable point records them anyway. This is the first thing to test with lazyfs.
  - Clock skew can starve pops.
  - The openraft pin predates the fix for the #2080 liveness bug.
- **Cluster size:** 3 nodes is fine. Ubuntu 24.04 needs four workarounds (section 4).

## 1. Kafka workload (fast path)

**Where it lives:**
- [`jepsen.tests.kafka`](https://github.com/jepsen-io/jepsen/blob/main/jepsen/src/jepsen/tests/kafka.clj) holds the generator and checker (2,585 lines).
- The client is in [jepsen-io/redpanda](https://github.com/jepsen-io/redpanda) (`workload/queue.clj`, `client.clj`, Java kafka-clients 3.8.0).
- [Bufstream](https://github.com/jepsen-io/bufstream) reuses that client through `io.jepsen/redpanda 0.1.3` and overrides only `:debug-topic-partitions`. Copy that wrapper pattern for Queen.
- Prior analyses: [Redpanda 21.10.1](https://jepsen.io/analyses/redpanda-21.10.1), [Bufstream 0.1.0](https://jepsen.io/analyses/bufstream-0.1.0).

**What each operation calls**

| op | Java call | Kafka APIs |
|---|---|---|
| `:send [k v]` | `send(ProducerRecord("t"+k/2, k%2, null, v))`, then reads `.offset()`; first use calls `createTopics(2, RF 3)` | ApiVersions, Metadata, InitProducerId, Produce acks=all, CreateTopics |
| `:poll` | `poll(100 ms)`; `commitSync` only when subscribed and not transactional | Fetch (read_committed). Also FindCoordinator, OffsetFetch and ListOffsets(earliest) even with assign, because `group.id` is always set |
| `:assign` (optional seek to beginning) | `assign`, `seekToBeginning` | ListOffsets |
| `:subscribe` | `subscribe(topics)` | JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit |
| `:txn` (only with `--txn`) | init, begin, `sendOffsetsToTransaction`, commit, abort | FindCoordinator(txn), InitProducerId(txn id), AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn |

- **Transactions off works:** `--no-txn` makes every op a single send or poll (`kafka.clj:2569`).
- **The `-s` preset:** acks=all, idempotence, 1000 retries, read_committed, earliest, manual commits, assign.
- **Final phase:** a fresh client assigns every key, seeks to the beginning, and polls until it has caught up.

**What the checker flags** (`analysis` at `kafka.clj:2207`, allowed set at `:2433`)

| anomaly | meaning | invalid? |
|---|---|---|
| `lost-write` | acknowledged send below the highest offset read for its key, never read | always |
| `unseen` | acknowledged send never read by the end | always |
| `duplicate` | same value at more than one offset | always |
| `inconsistent-offsets` | one offset read with different values (across clients or nodes) | always |
| `G1a` | a failed send was read | always |
| `precommitted-read` | a transaction reads its own sends | always |
| `nonmonotonic-send`, `int-*-poll`, `int-nonmonotonic-send` | order broken inside one process or one op | always |
| `poll-skip`, `nonmonotonic-poll` | a process's consecutive polls skip or go backwards | with assign only |
| `G0`, `int-send-skip`, `G1c` (with ww-deps) | Kafka transaction interleaving | allowed |

**queen-kafka against that list**
- **Works as is** (expected to pass with no faults):
  - All 33 API keys are in one static table with no cluster filtering (`K/versions.rs:344-526`).
  - The raft planner assigns gap-free offsets inside the replicated entry, and the answer comes after commit and local apply (`KS/rsm/planner/push.rs:97,152-166`).
  - CreateTopics accepts 2 partitions and RF 3; num_partitions is a floor and RF is reported as 1 (`K/handlers/create_topics.rs:30-62`).
  - Consumer groups work in cluster mode. Fetch never returns uncommitted data.
- **Will fail under faults:**
  - **Duplicates or false `G1a`.** The idempotence window lives in each node's memory, and no `transactionId` is sent (`K/idempotent.rs:51-80`, `K/queen.rs:327-335`).
    - After a kill or a leader move, a retried batch is either appended again (`duplicate`) or answered OUT_OF_ORDER_SEQUENCE.
    - The Jepsen client treats OUT_OF_ORDER_SEQUENCE as a definite failure (`queue.clj:524`), so if the first attempt had committed, the checker reports a false `G1a`.
  - **Non-monotonic polls.** A node that is behind answers OFFSET_OUT_OF_RANGE for offsets above its own high watermark (`KS/rsm/facade/real/phase2/reads.rs:324-327`). The consumer resets to earliest, which shows up as `nonmonotonic-poll`.
  - **Timeout mismatch.** The facade gives up after 10 s (`K/queen.rs:1030`) while the push deadline is 30 s (`S/handlers/raft.rs:131`). A push can commit after the facade answered REQUEST_TIMED_OUT, and the retry then duplicates it.
  - **Stalls.** Partition leadership follows a registry with a 10 s TTL, so each node loss stalls some partitions for 10–16 s.
- **Cannot run:** `--txn`. Cluster mode refuses it with error 53 at FindCoordinator (`K/txn.rs:84-97`).
- **Smallest facade changes:**
  1. Durable idempotence through Queen's dedup, about 2–3 days:
     - Use a per-record `transactionId` of `qk:&lt;pid&gt;:&lt;epoch&gt;:&lt;partition&gt;:&lt;seq&gt;`. The facade's own comment names this fix (`K/idempotent.rs:74-80`), and dedup already returns the original offset (`S/rsm/entry.rs:233-245`).
     - Accept a non-zero sequence when there is no local state.
     - Add a per-(producer, partition) in-flight lock.
  2. Take the existing read barrier (`S/rsm/facade/real/phase2.rs:148-154`) when a fetch is above the local high watermark, or answer empty. About 0.5–1 day.
  3. Make the facade's 10 s budget match the 30 s push deadline. About 0.5 day.
  4. For subscribe only: read barrier on OffsetFetch. About 0.5–1 day.
  - Real Kafka-style transactions on raft would take 3–6 weeks.
- **Interim run:** `--no-idempotence --retries 0` tests offsets and fetch while keeping the idempotence gap out of the picture.
- **Environment:**
  - `QUEEN_KAFKA_EMBEDDED=true`
  - `QUEEN_KAFKA_NODE_ID=i`
  - `QUEEN_KAFKA_ADVERTISED_ADDR=&lt;private ip&gt;:9092`
  - `QUEEN_TOKEN=x`
  - `QUEEN_KAFKA_DEFAULT_PARTITIONS=2`
  - `QUEEN_KAFKA_CLUSTER_TTL_MS=3000`, `…_HEARTBEAT_MS=1000`
  - `QUEEN_KAFKA_GROUP_JOIN_DELAY_MS=0`
  - Never set `QUEEN_URL`.

## 2. Native suite (Clojure client over HTTP)

**Facts the design relies on:**
- Push answers per item `{status: queued|duplicate|error, offset}`. A duplicate carries the original offset (`S/rsm/facade/real.rs:1816-1821,1990-2020`).
- Pop messages carry `offset`, `leaseId`, `deliveryAttempt` and `partitionId` (`S/rsm/facade/real.rs:2630-2652`).
- Fetch: `POST /api/v1/fetch {entries:[{queue,partition,offset}], maxWaitMs}` returns records with offsets, `highWatermark` and `logStartOffset`, and takes no lease.
- A push to several partitions is **not** atomic. Only `/transaction` is one raft entry (`S/rsm/planner/txn.rs:1-13`).
- An ack checks the lease **only when `leaseId` is sent** (`S/rsm/planner/ack.rs:97-105`).
- `requiredLeases` is only a hint (`S/rsm/facade/real.rs:1222-1230`).

**How the client labels failures**

| label | responses |
|---|---|
| `:fail` (definitely not applied) | 400 rejected, 413, 429 overloaded, 507 storage_full, push item `error`, ack `success:false`, transaction `success:false`, KV `applied:false` or `kv_precondition`, 503 `no_leader` (offload off only) |
| `:info` (outcome unknown) | 503 `timeout` and `retry`, 500, KV `kv_timeout`/`kv_retry`, socket timeout or reset |

References: `S/rsm/facade/mod.rs:176-229`, `S/handlers/raft.rs:49-97`. Set the socket timeout below 30 s, or lower `POP_DEFAULT_TIMEOUT_MS`.

**Queue setup:** configure every test queue explicitly with `/configure`, because its defaults are wrong for Jepsen: ttl 3600, retryLimit 3, DLQ on, lease 300 s (`S/rsm/facade/real/phase2.rs:999-1023`). Use:
- `ttl: 0`, `retentionEnabled: false`
- `deadLetterQueue: false` with a high `retryLimit`
- `dedupWindowSeconds` longer than the test
- a short lease per workload
- `subscriptionMode=all` on every pop

**Workloads**

| workload | operations | checker | what it proves |
|---|---|---|---|
| **W1 log** | `:send` = one-item push with `transactionId "p&lt;proc&gt;-&lt;v&gt;"`, recorded as `[:send k [offset v]]`; `:poll` = fetch from a position the client keeps; `:txn` = write-only `/transaction` with several pushes | `jepsen.tests.kafka/checker` with assign; wrap it so G0 and G1c are **not** allowed (a transaction is one entry); final polls from every node | acknowledged pushes survive; per-partition order; one value per offset on every node; atomic multi-partition transactions |
| **W2 lease queue** | pop (named group, `leaseSeconds` short) → record → batch ack **with** `leaseId`; the op is `:ok` only if the ack succeeded | total-queue (lost, unexpected) plus a custom checker (below) | at-least-once delivery, ack durability, lease exclusivity, per-partition FIFO |
| **W3 KV register** | independent keys; read = GET; write = put `forever:true`; cas = GET version, then put `expect:&lt;version&gt;` | `independent` + Knossos cas-register; one run with all clients on the leader, one across all nodes | linearizable KV. **Expected to fail across nodes:** no barrier in `kv_read` (`S/rsm/facade/real/kv.rs:156-181`), while `RAFT_STATUS.md:323` requires one |
| **W3b / W3c** | `incr`; putIfAbsent (`expect:0`) | `jepsen.checker/counter`; one winner per key | no lost or doubled increments; CAS arbitration |
| **W4 Elle** | KV batches `[getMany + puts]` | Elle rw-register at strict-serializable | atomic multi-key writes. Readers see live, half-applied entries (`S/rsm/store/heed_store.rs:66-72`), so expect G-single |
| **W5 txn pipeline** | pop from `in` with lease L, then one `/transaction`: ack in `in` (with leaseId) + push `out` d(m) with a random id + `kv incr c:k` | custom checker (below) | exactly-once processing; push + ack + KV all-or-nothing |
| **W6 dedup** | reuse `transactionId`s; on `:info`, retry the same id on another node | at most one message per id per partition, and the duplicate reports the original offset | dedup survives failover |

**Custom checkers:**
- **W2** flags three things:
  - a message acked successfully twice;
  - a message redelivered after a successful ack;
  - per partition, a successful ack out of offset order, or a pop that starts past last-acked + 1.
- **W5** checks five things:
  - no input is lost;
  - no input is committed twice (two `out` messages for one m);
  - no effects from a failed transaction appear (phantom);
  - each counter equals the number of `out` messages for its key;
  - the counter checker's bounds hold.

## 3. Nemeses

| fault | how | expected | risk |
|---|---|---|---|
| **kill -9 + restart** | `db/Kill`, plus a supervisor that restarts only on exit 75 (the `qc.sh` loop; exit at `S/rsm/replicator/raft/mod.rs:509`) | no acknowledged loss; new leader in 3–4 s | leader restart with uncommitted entries (fix still uncommitted in `log_store.rs`); exit-75 path; kill during snapshot build or install never tested; a poisoned node keeps serving 500s with `/health` 503; shrink `QUEEN_RAFT_PURGE_HOLD_S` and `QUEEN_RAFT_LOG_KEEP` to force snapshots |
| **pause** | SIGSTOP/SIGCONT the `queen` pid | election, then the old leader steps down | stale reads from a paused leader (KV and fetch have no barrier) |
| **partition** | iptables on INPUT | minority answers 503 or times out (`:info`); no split brain | see below |
| **clock** | bump ±11 ms–288 s, strobe; needs gcc and NTP off | elections unaffected (monotonic clock); cluster time only moves forward (`S/rsm/planner/mod.rs:768-779`) | see below |
| **lazyfs** | mount `QUEEN_RAFT_DIR` on lazyfs; call `lose-unfsynced-writes!` in `db/kill!`, as the [NATS test](https://github.com/jepsen-io/nats) does | quorum fsync means no acknowledged loss from one power loss | see below |
| **NATS composites** | `pause-kill` / `part-kill` (NATS `nemesis.clj:376,418`): keep a minority behind, power-fail one node, form a new majority | the sharpest test of "acknowledged only after quorum fsync" | same as lazyfs |
| **file corruption** | combined `:file-corruption` (bitflip, truncate) on one node | detect the damage, refuse or refetch | a corrupted node winning an election (NATS #7556 pattern) |
| **membership** | none | not supported | `change_membership` exists (`S/rsm/replicator/raft/mod.rs:1443-1467`) but nothing calls it and there is no endpoint; the batcher tripwire would stop writes while `/health` stays green |

**Partition details:**
- Targets: `:one`, `:primaries` (needs the DB to implement `db/Primary` via `/health` `raft.role`), and custom grudges for bridge (cut n1–n3) and leader-deaf (`{leader #{f1 f2}}`).
- On 3 nodes, `:majorities-ring` turns into a total partition over TCP, because every pair loses one direction.
- The openraft #2080 fix ([PR #2089](https://github.com/databendlabs/openraft/pull/2089), 542b9046, 2026-09-18) is **not** in Queen's pin 54094270 (`server/Cargo.toml:167`). The spike reproduced the mechanism and saw client writes hang (`test/raft/spikes/s3-consensus/MEMO.md:61`). Leader-deaf is expected to hit it; the 3-node bridge grudge alone should not, since the leader plus the bridge node still form a majority. Bump the pin before running partition tests.

**Clock details:**
- A fast leader expires leases early. Redelivery is fine, but acks under the old lease must fail.
- The dedup and request-id windows shrink, so duplicates can get through.
- Pops starve when a node's wall clock is behind cluster time by more than the pop timeout (`S/rsm/facade/real.rs:2155-2164` against `S/rsm/planner/pop.rs:212-215`).

**lazyfs details:**
- **Main target:**
  - The durability mark is raised before the fsync (`S/rsm/replicator/raft/log_store.rs:185-199,1370-1378`).
  - Durable points then record the applied index anyway (`S/rsm/apply.rs:3627-3642,3718-3733`).
  - At boot, the tail check uses the maximum across all queue logs (`S/rsm/qlog/set.rs:505-526`), so a hole in one queue log goes undetected.
  - Pop and fetch then skip the missing records silently (`S/rsm/facade/real.rs:2584-2591`).
- **Other risks:**
  - `committed.json` is never fsynced, so a restarted node can serve reads that go backwards.
  - lazyfs does not lose directory operations.
  - LMDB's `lock.mdb` is a shared writable mmap, so smoke-test it on lazyfs first.

## 4. Infrastructure
- **Control node:** a 4th VM in the same VPC, 8 vCPU / 32 GB (the Jepsen test projects use `-Xmx24g`).
  - Jepsen 0.3.14, [released 2026-09-22](https://clojars.org/jepsen).
  - JDK 21 or newer (compiled with `--release 21`).
  - Leiningen 2.11 or newer; 2.13.0 is current.
  - Also `libjna-java`, `gnuplot`, `graphviz`.
- **DB nodes:** 3 fresh Ubuntu 24.04 VMs, 4 vCPU / 8 GB each, root SSH with an ed25519 key (Jepsen's default sshj client).
  - Use new VMs rather than the benchmark boxes. The old 02/03 nodes hijacked votes on port 7400 with the shared token, so use a random `QUEEN_RAFT_TOKEN` per run.
  - Name the nodes by private IP, or map n1–n3 to private IPs in `/etc/hosts` on the control node. Jepsen resolves names on the control node when building iptables rules, and on DigitalOcean eth0 is the public interface.
- **Three nodes vs five:** three is fine and is what you ship. Drop `:minority-third` (it selects 0 nodes when n=3); `:majority` behaves like `:one`. Five nodes are only needed for the true #2080 bridge topology.
- **DB automation (`jepsen.queen.db`, modelled on [db/kafka.clj](https://github.com/jepsen-io/redpanda/blob/main/src/jepsen/redpanda/db/kafka.clj)):**
  - **setup:** teardown first (kill `queen`, unmount, wipe), `c/upload` the binary given by `--bin`, write `run.sh` (the `qc.sh` environment plus the exit-75 loop, with `ulimit -n`), optionally mount lazyfs, synchronize, start all three together (empty directories, each node calls `initialize`), wait until `/health` is 200 and exactly one node reports `raft.role=leader`, then configure the queues.
  - **kill:** `grepkill` the wrapper and the binary, then lose un-fsynced writes if lazyfs is on.
  - **pause:** SIGSTOP the binary only.
  - **primaries:** read `/health`. Do not use `/status` (it says "segments-rust") or `/api/v1/raft/status`, which hard-codes node 1.
  - **logs:** `queen.log` (including restart lines), `raft/*.json`, `lazyfs.log`; add `log-file-pattern` for panics and `NA-QLOG-I1`.
  - `jepsen.db.watchdog` restarts on any death, which would hide crashes. The `run.sh` loop is better.
- **What breaks on Ubuntu 24.04:**
  1. `jepsen.lazyfs/install!` hardcodes `libfuse3-4` (`lazyfs.clj:75`, a Debian 13 package). Noble ships `libfuse3-3` ([fuse3 3.14](https://packages.ubuntu.com/noble/fuse3)), so write our own installer and reuse `mount!` and `lose-unfsynced-writes!`.
  2. `jepsen.os.ubuntu` says "tested against 18.04" and does not install build-essential, which the clock nemesis needs to compile its helpers. Use `jepsen.os.debian/os`; its packages all exist on noble.
  3. `maybe-disable-ntp!` probes `service timedated`, which probably does not match the Ubuntu unit name, so timesyncd could keep correcting the skew. Disable it explicitly: `timedatectl set-ntp false` and `systemctl disable --now systemd-timesyncd`.
  4. `net-dev` picks eth0, so for netem use `(net/iptables-with-dev "eth1")`.
  5. Disable unattended-upgrades; it holds the dpkg lock on first boot.
  6. The nft-backed iptables works, but keep Docker and ufw off the nodes, because Jepsen's `iptables -F` wipes their rules.
  7. Set the lazyfs `:cache-size` well above 0.5 GB. When the cache is full, lazyfs writes straight through (`lazyfs.cpp:1051,1170`), which silently makes the fault toothless.

## 5. Plan and effort

| phase | work | effort | what it proves |
|---|---|---|---|
| **P0** | VMs, Jepsen project, `jepsen.queen.db`, no-fault smoke | 1 day + 2–3 VM hours | the harness controls Queen |
| **P1** | W1 log workload with kill, pause, partition (`:one`, `:primaries`), clock; offload on and off; lanes 1 and 16 | 1.5 days + 1 night | log order and durability, same data on every node |
| **PK** | Bufstream-style wrapper around the Redpanda client, no-txn, assign then subscribe | 0.5–1 day, +3–4 days of facade fixes to pass | Kafka conformance |
| **P2** | W2 with its custom checker | 2 days | leases, acks, FIFO |
| **P3** | W3, W3b/c, W4 | 2 days | KV linearizability and atomicity |
| **P4** | W5, W6 | 2 days | exactly-once, dedup |
| **P5** | lazyfs with own installer, NATS composites, file corruption, bridge and leader-deaf (after the openraft bump), forced snapshots and exit 75, full test matrix, soaks | 3 days + 2–3 nights | durability claims and the RAFT_STATUS G4 gate (`RAFT_STATUS.md:69`) |

**First concrete step:** create the control VM and 3 fresh noble nodes in the VPC, then get `lein run test --workload log --nemesis none --time-limit 60` green against the new `jepsen.queen.db`.

**Order: native first, then the Kafka workload.**
- The native path tests the raft core directly, with the same Kafka checker, through a client of about 200 lines.
- The Kafka path's first failures are already known facade gaps (idempotence, out-of-range, timeouts), and the facade code documents them itself.
- `--txn` is impossible in cluster mode, and the Kafka workload never exercises leases, KV transactions or dedup.
- Still, run the Kafka workload once with no faults as soon as P0 is done: the DB automation is shared and it costs almost nothing.

**Complements: deterministic simulation and Antithesis.** openraft already runs its own deterministic simulation plus a Docker Jepsen suite (partitions, process, pause, membership, clock, packet; no disk faults), and a copy at your exact pin is in `~/.cargo/git/checkouts/openraft-c9fbe90779948994/5409427/jepsen`, so its partition and clock modules can be reused. [turmoil](https://github.com/tokio-rs/turmoil) 0.7 (with an unstable simulated filesystem) and [madsim](https://github.com/madsim-rs/madsim) 0.2.25 would need Queen's OS threads, blocking I/O and C LMDB mmap abstracted away first. A realistic target is the replicator, batcher and planner running over a simulated transport, not the whole binary. [Antithesis](https://antithesis.com/docs/environment/fault_injection/) replays network, node, clock and thread-pause faults deterministically, and Jepsen's `io.jepsen/antithesis` runs the same tests inside it (the NATS analysis did this). Its fault docs list no disk or power-loss faults, so lazyfs stays Jepsen's job.

## Sources
- Jepsen: [repo](https://github.com/jepsen-io/jepsen), [lazyfs.clj](https://github.com/jepsen-io/jepsen/blob/main/jepsen/src/jepsen/lazyfs.clj), [combined.clj](https://github.com/jepsen-io/jepsen/blob/main/jepsen/src/jepsen/nemesis/combined.clj)
- Test suites: [NATS](https://github.com/jepsen-io/nats), [Bufstream](https://github.com/jepsen-io/bufstream), [Redpanda](https://github.com/jepsen-io/redpanda)
- Analyses: [NATS 2.12.1](https://jepsen.io/analyses/nats-2.12.1), [Bufstream 0.1.0](https://jepsen.io/analyses/bufstream-0.1.0), [Redpanda 21.10.1](https://jepsen.io/analyses/redpanda-21.10.1)
- [lazyfs](https://github.com/dsrhaslab/lazyfs)
- openraft: [#2080](https://github.com/databendlabs/openraft/issues/2080), [Jepsen suite](https://github.com/databendlabs/openraft/tree/main/jepsen)
- [Ubuntu ntpdate package](https://packages.ubuntu.com/noble/ntpdate), [Leiningen](https://leiningen.org)
