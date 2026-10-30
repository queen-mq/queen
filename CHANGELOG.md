# Changelog

Release history for the Queen MQ server and client SDKs. Full release notes live on
[GitHub Releases](https://github.com/queen-mq/queen/releases).

## 2.7.0 - unreleased

**Before upgrading: what 2.7.0 refuses, trims or removes that 2.6 kept.** Most of what 2.7.0 adds
is a limit on something a client could grow without end; each is in the entries below and on
[Limits](https://queenmq.com/reference/limits/). Four can change what an application sees. Dead
letters are removed oldest first past 100,000 rows, 1 GiB or 14 days in a queue
(`QUEEN_DLQ_MAX_ROWS_PER_QUEUE`, `QUEEN_DLQ_MAX_BYTES_PER_QUEUE`, `QUEEN_DLQ_MAX_AGE_S`; `0` turns
one off). A pop answers at most 10,000 messages and 64 MiB, and a push of more than 10,000 messages
is refused `too_many_items`. A name whose store keys would pass LMDB's 511 bytes is refused
`name_too_long`, where 2.6 stopped every node. And a node near its memory limit answers 503
`memory_pressure`: page reads past 80 %, writes that grow the stored state past 90 %. Stream runners
use claims once every node runs 2.7.0 and they restart (the last entry says why). Why: a broker that
runs out of memory or stops on one bad key loses every client, so it now refuses the request first.

**Streams: a window on a quiet partition closes, also after a restart and with several runners
(issue #153).** A stream's idle flush used to know only the partitions its own process had read in
the last five minutes, and it wrote with no lease. A window open when a runner restarted never
closed (its result was never sent, and its state kept the partition from being cleaned up), and
two runners could send one window twice or delete a window the other had just extended. The broker
gains `POST /streams/v1/claim`, which leases idle partitions of a stream's source queue with an
empty batch: only when nobody holds them and the stream's group has read all of them. A cycle that
acks that empty batch (`count: 0`) closes the windows under the lease, and is refused whole when
the lease is gone, like every cycle. `POST /streams/v1/state/partitions` lists the partitions
holding a query's state, so a starting runner finds the windows nobody tracks. The Rust, Go,
JavaScript and Python clients use both: each keeps when every open window ends, claims the partition
then, closes on the broker's clock, and on an older broker falls back to the old flush; `stop()`
waits for a close in flight. In Go, JavaScript and Python a processing-time session now closes on
its messages' `createdAt`, as in Rust, not on the process clock, so a backlog no longer splits it.
On a cluster, a claim that reaches a follower is forwarded to the leader, where the leases live.
Also fixed: a cycle naming a lease on a partition its group never read, or one that is gone, skipped
the ack and committed its state and pushes with no lease; it is now refused (`bad_lease`). Why: a
session per visit or per trace ends with silence, so closing it cannot wait for the next message.

**Broker: no client request can stop the cluster.** A store key longer than LMDB accepts (511
bytes) fails in apply on every node, and a restart replays the entry and fails again, until an
operator skips it by hand. Several client names reached apply in longer keys than any receiver
checked: a Streams state key over 485 bytes, an `ephemeral/configure` queue name, the consumer
group of a seek, timers sent inside a transaction (their due index is 8 bytes longer than the key
that was checked), quota and tenant names, and names whose `0x00` bytes are stored twice. The
planner now measures every key each command makes apply write, derived keys and escaping included,
and refuses a long one as `name_too_long` (4xx) before anything is written; the routes answer the
same first. An admin command that carried one effect per row (DLQ purge, seek, group delete, a
streams reset, a queue or tenant delete) is sent in chunks of at most 4,096 effects, and one over
`QUEEN_RAFT_ENTRY_MAX_BYTES` is refused. Why: one request with a long key stopped every node.

**Broker: one request can no longer use gigabytes of a node's memory.** A pop takes at most
`QUEEN_POP_MAX_BATCH` (10,000) messages and stops at `QUEEN_POP_MAX_BYTES` (64 MiB, always at
least one message); a fetch without `maxBytes` totals at most `QUEEN_FETCH_MAX_BYTES` (50 MiB); a
push carries at most `QUEEN_PUSH_MAX_ITEMS` (10,000) messages. The message, dead-letter and trace
pages write payloads as stored, with no JSON tree, up to `QUEEN_READ_PAGE_MAX_BYTES` (16 MiB) a
page; a trace's data is at most `QUEEN_TRACE_MAX_BYTES` (1 MiB). A zstd payload is decompressed
with a limit, and a binary push is admitted at its inflated size. `/depth` and
`/consumer-groups/lagging` list at most `?limit` (1,000, up to 10,000) partitions, `/status`
counts dead letters without reading their payloads, `/messages` refuses an `offset` past 10,000.
Why: a read built many times its size in memory, and a few at once took a node down.

**Broker: a KV read no longer builds a JSON tree of each value it answers.** Reads are answered
on the apply path, so they see the state at their entry; each stored value was parsed into a tree
there and printed again by the route. The KV routes and the in-process Kafka facade now put each
value's stored text into the answer, checked without building a tree; the answer is the same, byte
for byte. Why: a client reading large values made apply build them, and every write waited.

**Broker: a memory guard and connection limits.** Each node reads its cgroup memory limit and its
anonymous memory every 250 ms. Past `QUEEN_MEMGUARD_SOFT_PCT` (80 %) heavy reads answer 503
`memory_pressure` with a `Retry-After`; past `QUEEN_MEMGUARD_HARD_PCT` (90 %) writes that grow the
stored state do too. Acks, deletes, pops, renewals, health and Raft are never refused. At most
`QUEEN_HEAVY_READS_MAX` (32) heavy reads run at once. The HTTP port holds at most
`QUEEN_MAX_CONNECTIONS` (20,000) connections and closes one that sends no request headers within
`QUEEN_HTTP_HEADER_READ_TIMEOUT_MS` (120 s); at most `QUEEN_MAX_PARKED_POPS` (50,000) long polls
wait at once. Why: a broker that runs out of memory loses every client, so it now refuses work
first.

**Broker: state that clients grow has a bound.** Dead letters: at most
`QUEEN_DLQ_MAX_ROWS_PER_QUEUE` (100,000) rows, `QUEEN_DLQ_MAX_BYTES_PER_QUEUE` (1 GiB) and
`QUEEN_DLQ_MAX_AGE_S` (14 days) per queue, oldest removed first. `dedupWindowSeconds` is at most
`QUEEN_MAX_DEDUP_WINDOW_S` (7 days). The KV expiry sweep keeps going while it has expired rows
(it removed at most 512 a second). A Streams cycle carries at most `QUEEN_STREAMS_MAX_STATE_OPS`
(10,000) state ops; `QUEEN_STREAMS_STATE_TTL_S` removes state rows not written for that long (off
by default). KV operations inside a transaction (a `once()` gate, a `kv` rider) take the tenant's
KV quota as `POST /api/v1/kv` does, and their own rate, `QUEEN_KV_TXN_WRITE_RATE` (10,000 a second,
burst `QUEEN_KV_TXN_WRITE_BURST` 20,000); the Kafka facade's own commit row takes neither. The Raft log cache counts each entry's real size, and the
retention scan finishes its rounds while partitions grow fast: a round is paced from the larger of
the previous round and what it has visited so far. At 1,000 new partitions a second for ten minutes,
one node held 63,000 to 69,000 partitions and deleted every one within 60 seconds of the end, where
2.6.0 kept 594,000 of 595,000 unless `QUEEN_RAFT_RETENTION_SCAN_PER_S=40000` was set. Why: every row lives in memory on every node and is reloaded at
start, so state that only grows ends in a node that cannot start.

**Kafka facade: bounded requests.** Before a client authenticates its frames are at most 512 KiB,
and a frame or an idle connection times out. A produce batch's record and header counts are
checked against its bytes before they are decoded; fetches are clamped to
`QUEEN_KAFKA_FETCH_MAX_BYTES` (50 MiB) and pass stored payloads through; a metadata answer lists at
most `QUEEN_KAFKA_METADATA_MAX_PARTITIONS` (100,000) partitions and a request auto-creates at most
16 topics; offset requests are checked against their limits before anything is built; a group
holds at most `QUEEN_KAFKA_GROUP_MAX_MEMBERS` (1,000) members; token checks are cached. Why: a
client could make the broker reserve memory from counts it declared.

**Kafka facade: one transactional id never waits for another.** The transaction coordinator held
one lock over every transactional id across its KV and commit calls, so one slow commit stalled
every transactional producer, and the timeout sweep inside the 100 ms tick stalled group session
expiry with it. Each id now has its own lock, the sweep runs on its own task and skips an id that
is in a call, and an id idle for 10 minutes leaves memory (its row brings it back). The row keeps
the producer's transaction timeout, and expires 7 days after its last write, as Kafka's
`transactional.id.expiration.ms`. Why: a client minting transactional ids grew memory and KV rows
without end, and one slow commit held back every other producer.

**Ephemeral queues, proxy and PG connectors: bounded.** An implicit inbox is removed after
`QUEEN_EPHEMERAL_IMPLICIT_TTL_S` (600 s) idle even when it still holds messages, at most
`QUEEN_EPHEMERAL_MAX_QUEUES` (100,000) queues exist, a pop answers at most
`QUEEN_EPHEMERAL_POP_MAX_BYTES` (16 MiB), and messages and queues are charged their overhead.
Long-poll wake gates are freed again when idle. The proxy reads no request body before the caller
authenticates: a KV batch, read up to 16 MiB to tell the Kafka facade's own batches apart, was read
before authentication, so a caller with no credential made a node hold 16 MiB per open request. It
is now read after, and only its names are scanned; a KV batch with no credential gets 401 where it
got 403 `feature_gated`. Requests with no credential, or with one refused 401, share a
per-address limit (`QUEEN_EDGE_ANON_RPS_PER_IP`, 20 a second); any `Authorization` header used to
skip it. A PG source snapshot reads chunks of at most
`QUEEN_PG_SNAPSHOT_CHUNK_MAX_BYTES` (64 MiB) and at most 10,000 rows; at most `QUEEN_CONNECTORS_MAX`
(256) connectors exist. Why: each of these grew with what clients sent.

**Broker and dashboard: network, disk and Raft traffic.** System charts what each node moves per
second: its network in and out (Linux), its disk reads and writes, and the bytes of its Raft port
by channel: replication, forwarding, snapshots, the standby link and control. The Raft table gains
a Traffic column, each node's last minute, with the channels on hover. The same counters are on
`/metrics/prometheus`: `queen_raft_traffic_bytes_total{channel,direction}`,
`queen_network_bytes_total{direction}` and `queen_process_disk_bytes_total{direction}`. Why: a
follower falling behind, a snapshot sent for minutes or a disk writing twice as much shows there
before it shows as latency.

**Dashboard: the Overview flags the cell, not only the queues.** For an operator, the verdict and
the Open issues list now include what is wrong under the queues, each row linked to System: a
member being sent a snapshot, a follower behind or silent, voters unreachable or quorum lost, and a
node's disk, memory or CPU past its line. They are the lines that already coloured the top bar,
which now also shows the bytes on the Raft port's wire beside Raft, and per node in its panel.

**Dashboard: dark cards stay near-black, the logo is smaller.** The contrast pass keeps its
brighter text and 11px floor, but the dark theme's cards, inputs and borders are back to their
earlier values; the Queen wordmark is 34px, centred in its row.

**Dashboard: a list beside every page, and one place for each queue.** The rail is a fixed column
of icons, without Consumer groups. Each page lists what it holds in a second column, searched from
its top, and picking a row opens that row without changing the list. A queue's page has tabs:
Overview, Messages, Consumer groups, Supervisors, Dead letter, Timers and Alerts, the last showing
what needs you on that queue now and the lines it is judged by. The Messages, Dead letter and
Timers pages are now those tabs, and the queue list filters by namespace. The Overview lists a
summary card, open issues, the cell, throughput, backlog, consumer groups and dead letters. System
lists Overview, Nodes, Raft, Storage, CPU and memory, Disk and Network, picks a node on the views
that chart one, and shows the broker version. Selects and checkboxes are the dashboard's own in
both themes, the side column is a sheet on a phone, and the rail and top bar are soft greys in dark
and near the page's white in light. Why: a list that stays put is faster to scan, and everything
about a queue was spread over five pages.

**Broker: large stream states and fetches no longer use many times their size in memory.**
`POST /streams/v1/state/get` and `POST /api/v1/fetch` parsed every stored state and payload into a
JSON tree and copied the tree two more times before printing it: a read held 19 to 25 times what it
returned. `POST /streams/v1/cycle` held about 10 times its body. Window states of tens of megabytes,
read on every cycle and every idle flush, took a leader past its memory limit, and then each new
leader. These routes now check each stored document with the same JSON rules and write it into the
answer as stored, so a read holds about its own size. The answers are the same JSON. A fetched
payload keeps the producer's key order, spacing and number digits, as a pop always returned it, and
a cycle stores its state and sink payloads as the SDK sent them.

**Broker: a stream state value above 16 MiB is refused.** A Streams cycle with a state value larger
than `QUEEN_STREAMS_MAX_STATE_BYTES` (16 MiB by default) answers 413 `state_too_large` and stores
nothing, its pushes and ack included. Why: a window's state is read and written whole on every cycle
of its partition. A delete is never refused.

**Broker: a long poll of a partition by name wakes when the partition has work again.** After a
pop that named the partition (`.partition(...)` in the SDKs), a long poll of that partition that
was already waiting was not woken by a push, by the end of a `failed` ack's `retryDelay`, or by the
end of a `delayedProcessing` or `windowBuffer` hold: it answered empty at its timeout, and the next
pop got the message. A retry could so wait the SDKs' 30 s poll timeout instead of `retryDelay` (1 s
by default). On 2.6.0 such a long poll with a 3 s timeout answered empty after 2.7 s; it now gets
the failed message after about `retryDelay`. Long polls of the whole queue, and those after a pop of
the whole queue, were not affected. The push case is older than 2.6.0: 2.5.0 has it too.

**Docs: rebuilt from the little book of QueenMQ.** The site now follows the book, chapter by
chapter: introduction, writing applications, patterns, integrations, managing clusters and
architecture, with every example in JavaScript, Python, Go, Rust, PHP and C++, cut from programs
that run against a broker and check their results (`examples/tutorials/run.sh`). The book is a free
PDF at [queenmq.com/book](https://queenmq.com/book/). Every page of the old tree redirects to the
page that now answers its question.

**JavaScript and Python clients: a consumer waits out a broker that cannot answer, instead of
stopping.** While a cluster elects a leader, pops are answered 503 (`timeout`, "the request deadline
elapsed", or `no_leader`) for seconds to minutes. `consume()` retried only a 429 and a refused
connection: any other error stopped it, and a process that stayed up consumed nothing until it was
restarted. A pop answered 408, 429 or any 5xx, a timeout, or a dropped connection now waits and
polls again, for as long as it takes: the broker's `Retry-After` when it sent one, otherwise 1 s
doubling up to 30 s, ±20 %. Each failed pop logs a warning (`pop-failed-retrying`) and the first
answered one logs `pop-recovered`. A 4xx and a 503 `raft_phase1_unsupported` still stop the
consumer. The Go, Rust and PHP consumers already polled again after a 503.

**Go client: a guarded step no longer rolls back while its lock is held, after a slow renew.** The
background renew gives up after `RenewEvery` (a third of the lifetime by default). A renew slower
than that could still land on the broker: the lock's token moved and the handle kept the old one,
so a step with `Guard(lock)` rolled back, `Success: false` with no error, until the next renew
answered. The step now renews with the token it has, which the broker carries through for the same
owner, and is sent again with the current token.

Supervisor fixes made after supervisor 0.8.1, which PHP client 2.5.0 pins: they ship with the next
supervisor release.

**Clients 2.7.0.** The JavaScript, Python, Rust, Go, PHP and C++ clients are 2.7.0 and talk to
any 2.x broker. Stream claims need a 2.7.0 broker: on an older one a runner falls back to 2.6's idle
flush of recently read partitions.

**Laravel and supervisor: a SIGTERM during a broker call reaches the workers sooner.** A SIGTERM
that arrived while the master waited on the broker, for a heartbeat, a depth read or a status
publish, reached the workers only after the rest of that pass, `http_timeout` per URL for each
call, so a node drain during a broker outage could outlast `terminationGracePeriodSeconds`. The
Rust master now runs those calls on a thread that a stop abandons, so its workers get SIGTERM at
once; a request that panics on that thread is not taken for a stop. The PHP master signals its
workers from its signal handler, which PHP runs once the call in progress returns: at most one
`http_timeout` later.

**Laravel and supervisor: a pool is refilled at once after a deploy, and a clean exit is replaced
while a probe runs.** A worker that exited counted as a scale-up step, so an `auto` pool that lost
most of its workers at once, as at `queue:restart`, came back one `balance_max_shift` per
`balance_cooldown`; and while a crash held the restart circuit, no worker was replaced, so workers
that stopped at `--max-time` left the pool shrinking to the probe. In both engines a worker that
left as expected (at `queue:restart`, its memory limit or Laravel's job timeout, or after it ran
`stable_after`) is now replaced outside the budget and whatever the circuit says, up to the size
the pool had. Any other exit, even with status 0, waits for both as before: Laravel's worker exits
0 when it loses its database, every second through an outage. A scale-down that stops the probe
keeps the circuit's failure count.

**Laravel and supervisor: a master that is PID 1 reaps the orphans it inherits.** In a container
with no init process, as a Kubernetes pod is, a process a job started and left behind, or the lease
helper of a killed worker, reparents to the master, and its zombie kept a pid until the container
had none left to start a worker with. On Linux, both engines now reap every zombie child they do
not track when they are PID 1, and the Rust master also when it is a child subreaper; their own
workers and fork servers keep their exit statuses.

**Laravel and supervisor: a status that cannot be written no longer drains every worker.** One
failed write of `status.json`, on a full or failing disk, stopped the master and drained every
worker. The workers now keep running and the write is retried every pass, while the probes see the
status grow stale; the master stops once the writes have failed for `heartbeat_timeout`. A failure
of the state directory's ownership or generation still stops it at once.

**Laravel and supervisor: a fork server that failed to boot is not booted again at every exit.**
After `queue:restart` each worker that stopped for it asked for a new fork server, and one that
failed to boot was booted again each time, blocking the master for up to a minute per boot. After a
failed boot the next one now waits 60 seconds, doubling to 15 minutes; the requests meanwhile share
one boot, and workers are spawned in the meantime. With prefork on and the exit markers disabled,
the Rust master's warning now says that `queue:restart` goes unseen.

**Supervisor: a SIGTERM during start-up, and a closed stderr, no longer cost the workers.** The
signal handler was installed after the configuration export, which runs Artisan and may take a
minute; a master that is PID 1 ignores a signal it does not handle, so a SIGTERM during the export
was lost and the workers it then started were killed at the platform's deadline. The handler now
comes first, and a stop during the export ends the master before it starts a worker. And the
master logged with `eprintln!`, which panics when stderr is a pipe whose reader is gone; the panic
ended the master and, through `PR_SET_PDEATHSIG`, every worker. A line it cannot write is now
dropped.

**Supervisor: a bad control request is discarded instead of stopping the master.** A control
document that was expired, malformed or dated in the future by a clock step made the Rust master
drain every worker and exit. It is now removed with one log line and supervision goes on, as in the
PHP engine; one that cannot be removed still stops the master, as a fault of the state directory.

**The cluster version stays 7.** Nothing in 2.7.0 raises it: its log entries and stored rows are
those of 2.6.0, so a cluster rolls from 2.6 to 2.7.0 one node at a time, and back. During the roll
a stream runner whose claim reaches a 2.6 node turns claims off and keeps 2.6's idle flush until it
restarts: restart stream runners once every node runs 2.7.0.

## queenctl 2.6.1 and Go client 2.6.1 - 2026-10-24

**queenctl: binaries for 2.6, on a release of their own.** queenctl 2.6.1 is built against Go
client 2.6.1. Its archives for Linux and macOS (x86_64 and arm64) and Windows (x86_64) are on the
GitHub release [queenctl 2.6.1](https://github.com/queen-mq/queen/releases/tag/clients/client-cli/v2.6.1);
the last ones were attached to the 2.3.0 broker release. Each CLI version now has its own release
on its own tag, `clients/client-cli/vX.Y.Z`, so a CLI release no longer creates a `vX.Y.Z` tag that
belongs to the broker, or renames a broker release. With Go,
`go install github.com/smartpricing/queen/clients/client-cli/v2/cmd/queenctl@latest` installs the
same version: the module keeps its `smartpricing` path, and a path under `github.com/queen-mq/queen`
resolves to the whole repository, which the Go proxy refuses as too large. `queenctl docs` opens the
pages of the rebuilt site.

**Go client: `QueueConfig` sets the dedup window, the retry delay and the max wait.**
`QueueConfig{DedupWindowSeconds: 60}` now sends `dedupWindowSeconds`, as the JS, Python, PHP and
Rust clients already could; until now a Go caller had to spell it with `Option()`.
`RetryDelayMillis` sends `retryDelay` (milliseconds, enforced from broker 2.6.0) and
`MaxWaitTimeSeconds` sends `maxWaitTimeSeconds`. A zero is omitted like every other field, so
turning deduplication off or redelivering at once still goes through
`Option("dedupWindowSeconds", 0)` and `Option("retryDelay", 0)`. queenctl 2.6.1 accepts the three
keys in an `apply` manifest's `config:` block; 2.3.0 refused them as unknown fields.

## 2.6.0 - 2026-10-22

**The Kafka facade is a translation layer onto Queen, rewritten from scratch.** The first facade
kept its own registry, fences, producer windows, group coordinator, transaction stages and config
mirror beside the broker. 2.6's maps each Kafka request onto what Queen already does, inside the
broker process: a produce is a push, a fetch reads at an offset without a lease, a committed offset
is the Queen consumer group's position. In a cluster the raft leader leads every partition and
coordinates every group and transaction, and the other nodes point clients at it. A group's
membership lives in the leader's memory: after a leader change the members rejoin and resume from
their committed offsets, and a group with no members leaves that memory ten minutes after its last
member did. An idempotent producer's records carry the transaction id `k<producer id>.<epoch>.<sequence>`,
so a batch sent again inside the topic's dedup window writes nothing: topics the facade creates get a
window of 15 minutes (`QUEEN_KAFKA_DEDUP_WINDOW_SECONDS`). A transaction is held on the leader and
committed as one Queen transaction, its pushes and its offsets together. CreateTopics applies
`retention.ms` and `cleanup.policy` to the topic's queue, and refuses a value it cannot apply with
`INVALID_CONFIG`. The leader writes each group's members to the key-value store every 15 seconds and
at once when they change, which is how the broker's group listing and the console see Kafka groups.
Measured against 2.5.0's facade on three 24-vCPU nodes (2026-10-20): both carry 500,000 msg/s of
1 KB records (2.6 end-to-end p50/p99 10.1/15.0 ms); at 1,000,000 msg/s 2.6 pushes 30% less, because
it takes produce on the leader only, where 2.5.0's followers relayed theirs. Not offered in 2.6:
AlterConfigs, IncrementalAlterConfigs, OffsetDelete, DescribeLogDirs, the ACL calls and a TLS
listener (terminate TLS in front of the Kafka port). The standalone facade binary is gone, and so are
the settings only the first facade read: `QUEEN_KAFKA_PIPELINE`, `QUEEN_KAFKA_RELAY`,
`QUEEN_KAFKA_COALESCE`, `QUEEN_KAFKA_OFFSET_STORE`, `QUEEN_KAFKA_NODE_ID`, `QUEEN_KAFKA_CLUSTER`,
`QUEEN_KAFKA_CLUSTER_HEARTBEAT_MS`, `QUEEN_KAFKA_CLUSTER_TTL_MS`, `QUEEN_KAFKA_MAX_PARTITIONS`,
`QUEEN_KAFKA_MAX_PRODUCER_STATES`, `QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS`,
`QUEEN_KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS`, the `QUEEN_KAFKA_TXN_MAX_*` limits,
`QUEEN_KAFKA_TLS_KEY` and `QUEEN_KAFKA_FORWARD_SNI_HOST`. New: `QUEEN_KAFKA_AUTO_CREATE`,
`QUEEN_KAFKA_DEDUP_WINDOW_SECONDS`, `QUEEN_KAFKA_MAX_REQUEST_BYTES` and `QUEEN_KAFKA_PEERS`.
Topics and their records are queues and do not move; during the upgrade Kafka clients reconnect,
groups rejoin and transactions open on the old facade abort.

**A member that comes back recovers from the leader's log or from a delta, not from the whole
state.** The leader keeps its log for a member that is away (`QUEEN_RAFT_MEMBER_HOLD_S`, 6 hours by
default, ended sooner by `QUEEN_RAFT_MEMBER_HOLD_DISK_PCT`, the disk's low mark), so a member that
restarts, or is moved by a node upgrade, catches up from the log. When the log no longer reaches it,
the leader first asks where the member's log stands (`POST /raft/v1/snapshot/basis`) and, with the
same queue-log layout, sends the store and only the records above that point, which the member
appends byte for byte. A snapshot carries the sealed files' indexes, so the receiver no longer reads
every file to rebuild them, and a boot maps the indexes without reading them and checks them in the
background. The leader builds nothing for a member that is down, refuses at once a member that is
still starting instead of waiting on it, and backs off its reconnects (50 ms doubling to 1 s) instead
of opening about 4,000 a second. On three GCP nodes holding 521 GB each (2026-10-20), a member away
for five minutes came back with a 1.14 GB delta, appended in 1.3 s, and was healthy 28 s after it
started, where 2.5.0 sent it the whole state (566 GB, 15 minutes). On stage under a Kafka load
(2026-10-21), a killed leader's cluster was whole again in 47 s (141 s on 2.5.1), and none of six
restart and kill tests needed a snapshot.

**A restart refills the dedup rows from a sidecar per file, not from the files.** On those nodes
nine tenths of a restart went into reading the queue logs for the hash lists behind the dedup rows:
310 GB read in 300 s for lists under a gigabyte. Every sealed file now gets a sidecar, `r<id>.qhsh`
(each message record's offset, partition, base, count, stamp and hashes, with checksums), built off
the log writer's path and written in steps it waits for; a boot reads a sidecar that is whole and
matches its file, and the file otherwise. Paired cold restarts of 254 GB nodes (2026-10-21): the
refill took 20 s reading 0.7 to 1.4 GB with sidecars, against 171 to 201 s reading 185 to 217 GB
without, and the node was healthy after 30 to 33 s against 186 to 240 s. The sidecars cost about
16 bytes a message on disk (147 to 163 in the 3,000,000 msg/s benchmark), and latencies are 2.5.0's.
`QUEEN_QLOG_HASH_SIDECARS=0` turns them off. The first boot of 2.6.0 still reads every sealed file
once, and writes their sidecars.

**Raft: three fixes found while testing the recovery above.** A node binds its raft port at the start
of its boot and listens only once its raft core exists, and a port another process already holds
fails the open instead of leaving the node up and deaf. A member's 404 to the snapshot question is
read from the answer's status: a member that refuses it is a node before 2.6 and gets the whole state.
A replication stream that asks for entries the leader purged since it chose them now sends a heartbeat
instead of entries that do not follow their `prev_log_id`; only a member back with an empty data
directory could start that race, and in a release build the member refused the request.

**Empty partitions can go within the hour: `PARTITION_CLEANUP_SECONDS`.** `PARTITION_CLEANUP_DAYS`
counts whole days, at least one, so a workload that opens a partition per entity kept a day of empty
ones in memory and in every retention round. `PARTITION_CLEANUP_SECONDS`, when set, replaces it (at
least 60), and whatever the setting a partition is never deleted inside its queue's dedup window, so
a repeated push is still recognized. The cleanup's check for a stream query's state seeks each query
instead of reading all of them, which a short age makes matter. On a production cluster with one
partition per trace, 3600 took the partitions from 520,857 to 151,120 within 40 s of the first 2.6
leader.

**A queue log no longer fills its disk with raft entries nothing needs.** The queue logs are the
write-ahead log: each entry is written to the logs of the queues it touches. A sealed file was
rewritten once half of its message bytes were dead, and its entries did not count. In production a
stream query wrote its whole state, up to 15 MB, on every commit (2026-10-22): one shared log
(`QUEEN_QLOG_SHARDS`) grew about 40 GB an hour, most of each file was those entries, and the few
messages beside them, of a queue kept 2 days, held every file for days. The entries now count as
dead: a file reclaim may touch is below the point recovery and the cluster need, and a rewrite
keeps only messages. And while the last reclaim pass left work, a pass gets
`QUEEN_QLOG_RECLAIM_BOOST` (4) times its budget (256 files, 200 ms, one rewrite), rewrites
included; passes go back to the small budget once one finds nothing left. Judging and copying stay
off the log's write lock, so appends do not wait for them.

**A queue's status can be read without its partitions.** `GET /api/v1/status/queues/:queue` with
`includePartitions=false` answers the queue's totals, its partition count and consumed total
included, without per-partition rows or the dead-letter scan; the default answer is unchanged. The
console's queue page asks for it on large queues and draws aggregate seeds, so a queue with 100,000
partitions no longer sends 44 MB to every refresh (835 bytes with the summary). From
queen-mq/queen#141 by albertovincenzi.

**Every SDK reads a partition by offset and lists the partitions written since a time.** Two queue
methods wrap `POST /api/v1/fetch` and `POST /api/v1/partitions/changed`, which only the Kafka facade,
the S3 sink and hand-written HTTP calls used until now. `fetch(partitions)` (Go `Fetch`) reads every
record of the named partitions, from the oldest one still kept or from given offsets, up to where each
partition ended when the call first read it: 1000 partitions per request, asked again until each one is
read. Each partition answers its records with their offsets, transaction ids, payloads and push times,
its high watermark, its log start, and the offset to start from next time. `changedSince(since)`
(`changed_since` in Python, Rust and C++, `ChangedSince` in Go) lists the partitions written at or after
a time, every page of them, in creation order, with the answer's `safeTime` to pass as the next
`since`. Neither takes a lease, moves a cursor or needs an ack. A loop that reads a partition from
offset 0 by hand reads nothing once retention has moved the partition's start, because the broker
answers an offset below `logStartOffset` with `OFFSET_OUT_OF_RANGE` and no records; `fetch` goes on
from `logStartOffset`. The methods ship in the 2.6.0 clients (all six, PHP included). Any 2.x broker
serves both routes; one before 2.3.0 ignores the byte ceiling of a request.

**Server: the consumer-group listing reports a Kafka group by its members.**
`GET /api/v1/consumer-groups` gives a group the Kafka facade's rows name `kind: "kafka"`, and every
row three new fields, null on a native group: `liveMembers`, `lastSeenAt` and `inactiveSeconds` (0
while the queue is read for the group, else the seconds since it was last seen read). With presence, a
Kafka group's `state` is `Stable` or `Lagging` (past 300 s of lag) while members read the queue and
`Empty` when none does; a group whose members died is `Empty` three intervals after its last
presence write, judged on the broker's clock. A Kafka group without presence (a facade before
2.6) is reported as before. The group detail and the lagging list carry the same `kind`,
and `GET /api/v1/resources/queues` marks a queue the facade created with `kafkaTopic: true`. The
broker reads the facade's rows when it answers: no replicated row or command changes, and a native
group's row only gains the three null fields.

**Server: `retryDelay` is enforced.** After a `failed` ack that leaves retries, the message comes
back once the queue's `retryDelay` (milliseconds) has passed, and its partition waits with it for
that consumer group, so nothing behind the message overtakes it; a pop that names the partition
waits too. Every version up to 2.5.0 stored the option, echoed it, and redelivered at once. A queue
created by a push, or by `/configure` without the option, stores 1000: after the upgrade a failed
message comes back about a second later there, and `retryDelay: 0` keeps the old behaviour. A
`retry` ack, an expired lease and a dead-lettered message do not wait, and a `failed` ack inside a
transaction waits from the commit. The wait lives in the leader's memory: after a leader change the
message comes back at once. The console's queue editor now offers the option beside the retry
limit, and the queue page no longer marks it as not enforced.

**Server: an ephemeral pop no longer sleeps through a push that lands as it parks.** A waiting
`/api/v1/ephemeral/pop` looked at its partitions and only then registered for a wake, so a push
landing between the two woke nothing and the pop answered at its own timeout, with the message
there all along. On 2.5.0, with 16 requesters and a new inbox per request, about one reply in 950
came back at the pop's 2 s timeout; with one inbox per requester, one in 28,000. The pop now
registers before it looks. Durable pops wait in the consumption engine and were not affected.

**Console: a queue read by Kafka consumers shows its reads, and a chart missing a node says so.** The
records the in-process Kafka facade serves to Fetch requests were counted nowhere, so a topic read
only by Kafka consumer groups showed pushes and no consumption on the Overview, and Workload showed
nothing for Kafka topics at all. Each node now counts the records its facade's fetches return, per
queue and minute, beside the pops it already counted: `/api/v1/analytics/queue-ops` answers them as
`fetchMessages` and `fetchPerSecond`, `/api/v1/analytics/workload` as `window.fetchMessages` and
`series.fetch`, and the console charts a "Fetch (Kafka)" line on the Overview and a "Fetched (Kafka)"
figure in Workload. Each record a fetch returns also counts as a pop, with its lag, and a commit that
moves a group past N records counts N acks and adds N to the group's consumed total, so two Kafka
groups reading one topic count its records twice, as two native groups' pops do. Dashboard rows are kept by the node that
served the traffic and gathered from the others on every read; a node that did not answer within the
gather's 1.5 s, as during a rolling restart, used to leave its rows out without a word, and on stage
(2026-10-19) the Overview read about 1 push a second instead of 1,500 while the leader restarted.
Gathered answers now name the nodes that did not answer in `nodesMissing`, the node logs it at most
once a minute per peer, and the console shows a "Partial" chip naming them.

**Console: a Kafka group is a reader while it has members, and inactive after a grace.** A Kafka
group with live members counts as its topic's reader and is judged by its lag like any group. One
with no member is still judged by its lag for 15 minutes (a restart, a rebalance, a crash not yet
called), then shown as inactive on Consumer groups, with when it was last seen, and no longer
reported as an issue; Settings moves the grace (**Inactive after**) for the tenant or one queue.
A queue is flagged "No reader" only when no consumer group is on it and it is not a topic the Kafka
facade created: on a queue with groups, each group answers for itself. "No reader is expected"
still silences a queue that holds messages nobody is meant to read.

**Console: Workload opens on a broker with no queues.** A tenant without queues is `"tenant": {}` in
the workload answer, and the page stopped on "Interface error: Cannot read properties of undefined
(reading 'groups')" on every fresh broker. It now shows zeros until the first queue exists.
Reported in queen-mq/queen#140.

**Console: the time filters offer 12 hours, and sign-in recovers from a Google error.** From
queen-mq/queen#139 by albertovincenzi. A Kafka record is shown decoded in the message drawer, and Kafka groups with no
members leave the Overview's lag panel and Workload's lag figures.

**The cluster version stays 7.** A cluster rolls from 2.5.0 to 2.6.0 one node at a time, and back
on the same data, with one condition for the way back: a node holding a snapshot it has not swapped
in yet (a `snapshot.pending` or `snapshot.swapping` that names a `basis`) boots 2.6.0 once more first,
because 2.5.0 would take that delta for the whole state. 2.5.0 ignores the sidecars and
`raft/member_holds.json` that 2.6.0 writes. Tested on three GCP VMs under load, with deduplication
on, and the full Jepsen campaign ran on 2.6.0's code: 257 tests, all valid
(`benchmark-queen/2026-10-21-jepsen-26`).

## 2.5.0 - 2026-10-18

**The Kafka facade carries 1,000,000 msg/s on the OpenMessaging Benchmark, with the stock Java
clients.** Measured on 2026-10-17 on three GCP brokers (c4-standard-24-lssd) with the benchmark's
own `driver-kafka` and its `kafka-throughput.yaml` producer settings: 1 KB messages on one topic of
100 partitions, 4 producers and 4 consumers. At 500,000 msg/s the end-to-end p50/p99/p99.9 went from
46/625/792 ms on 2.4.0 to 5/20/58; at 1,000,000 msg/s the facade carries the load (1,001,000 msg/s,
14/87/151 ms), where 2.4.0 fell behind. Four idempotent producers writing 200,000 msg/s through the
facade lost and reordered nothing when the raft leader was killed with `kill -9` or stopped in the
middle of the run; the kill wrote one in-flight batch twice, the bound the facade's idempotence
documents (its sequence windows live in the leader's memory). The runs and their method are in
`benchmark-queen/2026-10-16-omb/RESULTS.md`.

**The cluster version stays 7.** Nothing in 2.5.0 raises it: a cluster rolls from 2.4.0 to 2.5.0 one
node at a time, and back, as a node on 2.4.0 starts on and serves a directory 2.5.0 wrote. Tested on
three droplets under load, with deduplication on (`benchmark-queen/2026-10-17-jepsen-25`).

**Server: openraft 0.10.0-alpha.37 from crates.io.** The consensus library was a git revision, alpha.34
with the `LogEntryDiscarded` fix; it is now the published 0.10.0-alpha.37, with exact pins on its
macros and runtime crates. Among its 58 further commits, leader I/O waits for the previous flush,
heartbeats stop once a quorum is lost, replication backs off differently, and linearizable reads,
which Queen's reads use, get a fix. From queen-mq/queen#138 by gerald (getong).

**Server: a `windowBuffer` hold ends one window after the oldest waiting message.** A queue's
`windowBuffer` held a partition until it had gone that many seconds without a write, so a partition
written more often than once a window delivered nothing for as long as the writes went on. A hold
now ends `windowBuffer` seconds after the oldest message waiting for the group, the first after its
cursor, whatever came since, and the group gets what has accumulated: a burst shorter than the
window is still one batch, and under steady writes no message is held longer than the window. The
end is read from that message's stored timestamp, so a new leader ends the hold at the same
instant.

**Kafka facade: a parked fetch answers when its topic is written elsewhere on the node.** A Kafka
client keeps one fetch in flight per broker and leaves out of it the partitions it still holds
records of, so a partition it drained while that fetch was parked on quiet ones waited for the
fetch to come back, up to `fetch.max.wait.ms` (500 ms in Java). A parked fetch that has read
nothing now answers, empty, once it has waited `QUEEN_FETCH_TOPIC_WAKE_MS` (5 ms) and another
partition of its topic was written on the node; an idle topic still parks to the client's wait, and
a fetch holding fewer than its `min_bytes` keeps waiting for them. With the
OpenMessaging Benchmark's stock Java consumer (1 KB, 100 partitions, 500,000 msg/s, three GCP
nodes) the end-to-end p50/p99/p99.9 went from 46/625/792 ms to 9/32/55 ms.

**Kafka facade: produce is pipelined on the leader and relayed from followers.** A facade served a
connection one push at a time: a request waited for the push before it to commit on two disks
before its own began, so most of a produce's time was queueing (p50 7.4 ms against 2.1 ms for the
push itself). On the node that leads, a connection's next push is now sent as soon as the one before
it is in the raft log, up to `QUEEN_KAFKA_PIPELINE` (8) in flight, and the answers keep request
order; the log keeps the order they were sent in and commits a prefix, so a later push never lands
without the ones before it. A pipelined push is never retried behind the client's back once it is in
the log: a push that fails ends the connection, and the client sends again, in order, what it was not
answered. On a follower, a client's produce goes as it came to the facade on the leader, over one
connection per client connection (`QUEEN_KAFKA_RELAY`), instead of through the broker's forwarding,
whose streams do not keep one connection's order. Every other request, and every connection
elsewhere, is served as before.

**Kafka facade: a topic lookup no longer queues behind the others.** The facade's catalog kept its
list of queues behind a fair async mutex, so under a high produce rate every request's topic lookup
waited its turn: at about 1,000,000 msg/s on 100 partitions it took 7.4 ms a request on the leader,
and the facade fell behind its clients. The list is now behind a plain mutex, never held across an
await.

**Kafka facade: the facade's report in `/status` times every produce stage.** `producePlans` (the
topic lookup), `produceCheck`, `produceDecode`, `produceEnvelope`, `producePush` and `produceRender`
show where a slow produce spends its time, beside the `producePushes`, `produceRequests` and
`produceRecords` counters.

## 2.4.0 - 2026-10-16

**41% fewer bytes written a message, and end to end about three times faster at 3M msg/s.**
Measured on 2026-10-15 on three GCP brokers (c4-standard-24-lssd, four local NVMe disks in a RAID 0
with XFS) at 3,000,000 msg/s on 1,000 partitions, with 256-byte payloads, the 2.3.0 wire forms and
deduplication off, the settings of the benchmark pages: every broker wrote 146 to 148 bytes a
message against 248 for 2.3.0 (440 MB/s a broker, what Kafka 4.3.1 writes at that load). End to end
p50 / p99 was 6.6-6.7 / 14-17 ms against 19-21 / 46-61, push 3.2 / 6.7-7.0 against 8.3-9.2 / 22-24,
for 4.7 GB less memory a broker and 2.2 cores more in all; the queue log's `fdatasync` went from
about 3 ms to 0.5. On 1,000 partitions the highest rate carried went from 3,500,000 to 5,000,000
msg/s (39 / 87 ms there). At 3,000,000 msg/s on 10 to 10 million partitions the p99 is 15 to 25 ms
where 2.3.0 read 42 to 76 (one run on 1,000 partitions read 65, four more of that point 15 to 20).
The runs, their method and what is not explained are in
`benchmark-queen/2026-10-15-bytes-per-msg/README.md`.

**The upgrade is one way: cluster version 7.** Once every node runs 2.4.0 the cluster moves from
version 6 to 7 by itself, and from then on 2.3.0 and older refuse to start on its data. While one
node still runs 2.3.0 the version stays at 6, every node writes what 2.3.0 reads, and a node can go
back; `QUEEN_RAFT_CLUSTER_VERSION_MS=0` on every node keeps the version where it is. Rehearsed on
three droplets from 2.3.0 under load, with deduplication on (`benchmark-queen/2026-10-15-jepsen-24`).
A 2.3.0 client talks to a 2.4.0 broker as to 2.3.0.

**Server: a push's outcome is written as runs.** A push's recorded outcome, in its raft entry, in the
queue log's entry record and in its `request_ids` row, held one 25-byte verdict a message. From
cluster version 7 it is runs: the messages a push created in a partition are one 29-byte run (the
partition, the first offset, the timestamp, the count), consecutive duplicates another, a refusal
one each. The answer, a retry's answer and a transaction's outcome read the same verdicts as
before. A one-message push keeps the old form, which is smaller for it.

**Server: the store checkpoint keeps a push's index row without its hashes.** A push's index row
holds the 16-byte hash of each `transactionId`, and the checkpoint wrote them every time it wrote
the row: a 100-message row, about 1.6 KB, took a 4 KiB page of its own. From cluster version 7 the
checkpoint writes the row without them, and a boot puts them back from the queue log, which keeps
the push's message record for as long as the row exists; the boot logs `txns rows refilled from
the queue logs` with the count. On one node at 300,000 msg/s the checkpoint's index table went from
616 to 11 MB. `QUEEN_RAFT_CHECKPOINT_TXNS=whole` writes them as before.

**Server: an entry record leaves out the hashes its message records hold.** The queue log's entry
record repeated the hash list of every append whose message record it is written with, 16 bytes a
message. From cluster version 7 it leaves them out, and a read puts them back from those records and
checks the result against the entry record's checksum. `QUEEN_QLOG_ENTRY_HASHES=keep` writes them as
before.

**Server: an encryption key no longer turns off the framed forms on every queue.** A broker that
ran with `QUEEN_ENCRYPTION_KEY` answered every `frames=true` pop in JSON, and compressed every
framed push and binary transaction again, on the queues that do not encrypt too. A framed group is
now filed as the client compressed it unless its queue encrypts, and a framed pop answers JSON only
when a record it would send holds an encrypted message. A queue whose encryption was switched off
keeps its older messages encrypted, so its answers stay JSON, decrypted, until those are consumed.
With a key, a framed pop reads the flags of each record's messages, one decompression per record;
without one nothing changes.

## PHP client 2.5.0 - 2026-10-14

**Laravel: a job whose timeout its lease cannot cover fails alone.** Without lease renewal, a job
whose own `$timeout` is 0 or at least `retry_after` was refused at every delivery: it never ran,
never reached `tries` and held its partition, and since PHP client 2.4.2 every supervised worker
that popped it exited, so its pool crash-looped. The job now fails at once, without running, with
its failed-job row, its `failed()` call and its dead-letter entry, and the exception names the
setting to change; the worker goes on. A `$timeout` that is a numeric string, as `env()` returns it,
counts as the number, as Laravel's worker reads it; it was refused even with renewal on.

**Laravel: a failure whose message is not UTF-8 still reaches the dead-letter queue.** A failed
job's error went into the dead-letter ACK as it was, and a message that is not UTF-8, as a database
error quoting a latin-1 value is, could not be encoded: no ACK was sent, `failed()` and the batch
and chain callbacks were skipped, and the job failed a second time as `MaxAttemptsExceededException`
once its lease expired. The text is now made valid UTF-8 and bounded to 8 KiB.

**Laravel: the dead-letter ACK in Laravel's timeout handler can never prevent the kill.** With
`failOnTimeout`, or on the last attempt, Laravel's handler fails the job before it kills the
worker, and that ACK used the ordinary client: up to 90 seconds on one URL, and an exception that
landed in the job's code. The handler now makes one bounded attempt of 2 seconds that never throws;
if it fails, the lease expires and the broker delivers the job again, past its tries.

**Laravel: a supervised worker reports a queue whose pops fail, whatever its other queues do.** A
pop that worked on any queue, an empty one included, cleared the worker's failure state, so with
`--queue=high,low` a `low` whose every pop failed was never reported. Each queue's failure is now
kept apart, in the same file. A pop that failed because the broker reset the connection ("reset by
peer", "Broken pipe") no longer makes Laravel stop the worker with status 0 as if it had lost its
database, which restarted it without backoff and removed its failure file.

**Laravel: workers no longer inherit the supervisor lock, and the fork server releases the boot's
cache stores.** The lock file was open in every process the master started, so after the master was
killed where PID 1 survives (systemd, supervisord, a shell entrypoint), orphan workers held it and
the next master refused to start. And the fork server kept the cache store connections the boot had
opened, a memcached socket say, which every forked worker then shared. Both are closed now.

**Laravel: the shutdown hand-back fails over, and a batch whose lease cannot be tracked is handed
back.** With several broker URLs, the client that hands a worker's prefetched jobs back at shutdown
tried one URL only, so with that node down the jobs waited for their lease and lost an attempt; it
now tries the others within the same 2-second bound. A popped batch whose lease tracking failed is
handed back at once, as the pop-ahead path already did.

**Laravel: a refused lease renewal budget says what it is made of.** A renewal may try every broker
URL, so with three URLs, the default `lease_renewal_timeout` and a `retry_after` of 30 seconds the
budget is 44 seconds and the supervisor refuses to start. The refusal now gives the budget, its
parts and the settings that shrink it. The dashboard's tuning advice no longer uses a null array
offset, which PHP 8.5 deprecates.

**PHP client: a request waits out a leader election.** A cluster that elects a leader answers 503
for a few seconds; each URL got one try with no delay, so a request failed and `consume()` ended. A
request now waits at the pace of `Retry-After`, for at most 10 seconds or its timeout, after a 503
that says it did not run (`no_leader`, `standby`); a read, or a push whose every message has a
`transactionId`, also after `retry`. A pop whose 503 says it may have run is never sent again.
`consume()` backs off after a 5xx and goes on polling, and reports an ACK that failed. Its automatic
lease renewal (`renewLease()`) waits at most the renewal interval instead of the request timeout,
encodes the lease id, and reports a renewal that failed or renewed nothing.

**PHP client: failover tries every backend, and a redirect or an empty answer is no success.** With
every URL marked unhealthy, as an election leaves them, a request went to the first URL only; it now
tries each. A 3xx read as a success with an empty result, so a push behind a redirecting proxy
reported its messages stored; it is now an error, never followed or retried. An empty body where
the broker always sends one (push, ACK, transaction, a pop with status 200) is an error too, after
which `consume()` backs off and polls again, as after a network error. A body
that cannot be encoded as JSON fails at once, with no request sent and no backend marked
unhealthy; it marked every URL unhealthy and slept through the retries. Asynchronous requests now
report the health of their backend, so a `consume()` with several workers leaves a dead node.

## 2.3.0 - 2026-10-14

**Three brokers, a million messages a second: end to end p99 from 100-160 ms to about 20.** Measured on
2026-10-12 and 13 with the fixes below and this release's wire forms (binary push, compressed groups,
framed pops, counted acks), 100 messages a request, a fresh cluster for every point, figures at
seconds 30-60 of the load. With deduplication off and nothing deleted, end to end p50 / p99 is the
same from 100 to 100,000 partitions: 4 / 6 ms at 100k msg/s, 5 / 9 at 500k, 10 / 17 at 1M, 17-20 /
35-40 at 1.5M, 27-31 / 56-62 at 2M. Ten million partitions read 10 / 20 ms at 1M msg/s and 39 / 69 at
2M; before these fixes they did not carry 1.5M. 2.5M msg/s is pushed and not carried. With
deduplication and retention on (60 s windows) 1.5M msg/s reads 19-28 / 36-51 ms from 100 to 1M
partitions; it is not carried on 10 partitions (the planner, with deduplication at 150,000 msg/s a
partition) nor on 10M (memory: more than the 32 GB of these brokers). 2M msg/s with retention holds on
1,000 partitions and rises slowly there. The runs, their method and what is not solved are in
`benchmark-queen/2026-10-12-three-node-new-forms/NIGHT.md`.
Kafka 4.3.1 ran on the same machines on 2026-10-13, worst load process on both sides: on 100
partitions it reads 8 / 11 ms at 1M msg/s against Queen's 10 / 19 and 8 / 43 at 2M against 31 / 63,
and carries 4M (13 / 243), which Queen does not; at 2M msg/s on 10,000 partitions it reads 64 / 134
on 30 broker cores against Queen's 31 / 63 on 20, and it does not carry 100,000 partitions, where
Queen reads 30 / 59. The benchmark pages of the site carry these figures.

**The upgrade rolls both ways.** 2.3.0 changes nothing in the replicated state: the cluster version
stays 6, and a cluster rolls from 2.2.0 to 2.3.0 and back one node at a time (a standby of several
raft groups has a rule of its own, below). The JavaScript, Python and Rust clients are 2.3.0, the Go and C++ clients
are tagged 2.3.0, and the PHP client is 2.5.0; `queen-protocol` stays 1.4.0. A 2.1.0 client and a
2.3.0 broker, or the reverse with the new options off, talk as before.

**Server: a binary push body, and a client's compressed batch stored as it is.** `POST /api/v1/push`
reads a body that starts with `QPB1` as length-prefixed groups (queue, partition, payloads, a
`transactionId` each when the client sends them) instead of a JSON document: same admission, same
commands, same verdict for every message. A payload is still one JSON document, and is checked.
The body may be one zstd frame, and its answer may be the counts alone (`count`, `queued`,
`duplicate`, `error`). In its framed form (flag 8) each partition's batch is the frames the queue
log stores, with message ids the client minted, compressed by the client as one zstd frame: the
broker inflates and checks every frame and, when it has nothing to add (no producer identity, no
payload encryption, no `transactionId` twice in the request), files the client's bytes as the
stored record and ships them to its followers, so no node compresses them again. A node that does
not lead hands those bytes to the leader with the forwarded request (`QUEEN_PUSH_FORWARD_STORED`,
on; an older leader ignores them). A JSON push is read as before, with one clock read a request
and borrowed strings. The layouts are in the HTTP reference.

**Server: an ack answered with counts, a pop answered with the stored records.** An ack's body was
parsed twice, the first time whole to read one key; it is parsed once, and its result is looked up
by index (it was a scan of the batch for every item). `"answer": "counts"` in the body answers
`count`, `success`, `failed` and the first `error` instead of one object per message. `frames=true`
on the pop routes answers `application/x-queen-pop`: the records as the queue log holds them, zstd
frames included, with the claims' offsets, so the broker inflates and renders nothing and the
client unpacks. It falls back to the JSON answer when the broker runs with payload encryption,
when a claim is conflated, or when a record is not readable as stored. Without the parameter a
pop is the JSON one.

**Server: a binary transaction body.** `POST /api/v1/transaction` reads a body that starts with
`QTB1`: the JSON body without its pushes, then the pushes as framed groups, stored as the client
compressed them when a group is a partition's whole append. Acks, required leases, KV, timers and
the lease fence are those of the JSON body; results come back with the head's operations first,
then the pushes, then the riders, and each SDK hands them back in the caller's order.

**What the three forms measure against 2.2.0.** Same three machines, one queue of 200 partitions,
a 60 s deduplication window, 2026-10-12 (`benchmark-queen/2026-10-12-three-node-new-forms/`,
before the fixes below): at 1M msg/s the push goes from 8.1 / 39.9 ms to 5.2 / 8.7 and the end to
end from 27.1 / 85.0 to 11.5 / 18.4; the three brokers use 13.4 cores for 16.4 and the load
machines 8.3 for 14.4; offered more than it can take, the cluster pushes 2.45M msg/s for 1.57M.
Transactions of 10 messages at 12,000 a second commit in 3.6 / 8.6 ms with the binary body for
5.3 / 12.4 on 2.2.0, and the ceiling moves from 11,320 to 12,890 a second; of 100 messages at
3,000 a second, 4.1 / 7.0 ms for 6.1 / 19.2. The ceilings are still the planner's: one cycle at a
time for pushes, one serial phase for transactions that ack.

**Clients: binary push, push compression and framed pop in all six SDKs and the Laravel driver.**
Three options, off by default everywhere, so a client that does not set them sends and reads JSON
byte for byte as before: `binaryPush`, `pushCompression`, `framedPop` in JavaScript and PHP,
`binary_push`, `push_compression`, `framed_pop` in Python, Rust and C++, `BinaryPush`,
`PushCompression`, `FramedPop` in Go, and three keys of the same names in `config/queen.php`. With
binary push a push goes out as the binary body; with push compression too, as framed groups, and a
transaction that pushes as the binary transaction body; with framed pop the client asks for the
stored records and unpacks them into the messages a JSON pop returns. A push or a transaction that
carries a trace id stays JSON. zstd comes from Node.js itself, from Python 3.14's standard library
or the new `zstd` extra before it, from a dependency of the Go module and of the Rust crate, from
libzstd in C++ (`-DQUEEN_CLIENT_ZSTD`, `-lzstd`), and from the zstd extension in PHP, which the
client asks for at construction when an option needs it. Binary push and push compression need a
2.3.0 broker on every node: an older broker answers the binary bodies with 400 and no client falls
back. Framed pop is safe against any broker.

**Server: the deduplication filter of a busy partition takes 4 bytes a hash.** A push whose
`transactionId` the filter cannot rule out is looked up in the partition's rows, and for a
partition at thousands of messages a second one lookup scanned about 1,400 rows of 100 hashes: at
1.85M msg/s on 200 partitions 0.34 to 0.40% of the messages paid one and the planner's lanes spent
62% of their time there. A filter generation of 8,192 hashes or more now has 32 bits a hash instead
of 16; smaller ones, which a quiet partition has, keep 16. The lookups fell to 0.026% of the
messages and the most that queue pushes rose from 1.85M to 2.45M msg/s. The filter grew from 0.56
to 1.23 GB in that run, so its default cap (`QUEEN_RAFT_DEDUP_FRONT_MB`) goes from 2 to 4 GiB.

**Server: the durable point every 250 ms, and the checkpoint written in steps.** The store's
checkpoint was written once a second and synced in one go, and the raft log's own sync waited
behind it. `QUEEN_RAFT_DURABLE_EVERY_MS` is now 250 by default, each node of a cluster writes its
checkpoint at its own place in that period (`QUEEN_RAFT_DURABLE_PHASE_MS`; `off` is the 2.2.0
behaviour), and on Linux the checkpoint is handed to the disk 8 MB at a time before its sync
(`QUEEN_RAFT_CHECKPOINT_FLUSH_STEP_MB`; `0` hands it over in one go). No answer ever waited for
the durable point, and none does now: it bounds what a restart replays. The 2.2.0 figures above
ran with its 1 s default, so how much of the p99 these settings alone move was not measured on
three machines.

**A command's recorded outcome leaves when its window ends, not ten seconds' worth at a time.**
Every command's outcome is kept `QUEEN_RAFT_REQUEST_ID_WINDOW_S` (60 s) to answer a retry. The step
that retires them ran on a 10 s tick, so ten seconds of outcomes left together, in steps of 4,096
rows one after the other, and apply did nothing else for 50 to 150 ms: from second 70 of every load
the p99 doubled or tripled. The leader now looks every `QUEEN_RAFT_REQUEST_EXPIRE_FAST_MS` (10; `0`
leaves it to the tick) whether 256 outcomes have left the window, and retires what has in its next
cycle; fewer wait for the tick, so an idle cluster writes no entry for them. The step and its limit
are unchanged. 1,000 partitions, last 30 s of a 90 s load, end to end p50 / p99: 11.8 / 29.9 → 10.6
/ 16.3 ms at 1M msg/s, 17.4 / 78.8 → 17.4 / 32.8 at 1.5M, 29.4 / 127 → 30.0 / 60.4 at 2M.

**Ten million partitions: the leader's idle scan works in turns.** The scan that unloads idle consumer
state walked 625,000 partitions under one lock once a second, on the thread that serves pops: with
10M partitions 7% of the messages were up to 80 ms late at any load. It now looks at 1,024 partitions
a turn, a turn every 5 ms, and a consumer's state for a partition is held by a pointer, so a table
that grows no longer moves 430 bytes for each (100 to 160 ms while 10M partitions were first read).
10M partitions, end to end p50 / p99: 6.8 / 73.7 → 4.0 / 6.9 ms at 100k msg/s, 13.4 / 130 → 11.1 /
17.2 at 1M.

**A pop's checkpoint is taken at once.** A pop or an ack is answered once the checkpoint that holds
its change has committed, and checkpoints were taken every `QUEEN_CONSUME_CHECKPOINT_MS` (5): an
answer waited half a tick on average before its checkpoint even started. An answer that waits now
has the checkpoint taken at once when none is in flight (`QUEEN_CONSUME_CHECKPOINT_EAGER`, on; `0`
waits for the tick). What an answer waits for is unchanged. At 100k msg/s: 6.5 / 10.2 → 3.9 / 6.8
ms, for more log entries at low load (3.1 → 4.4 broker cores there).

**Retention no longer stops apply.** Three causes, each measured:
- A round over a small store was one slice: every partition's watermark reached the planner together
  once an interval and went into one entry (1,000 partitions at 1M msg/s: 50,000 rows retired in one
  go, apply stalled 44-59 ms every 5 s). The round is now spread over its interval in 100 steps:
  12.9 / 76.8 → 11.8 / 19.5 ms with deduplication and retention on.
- The dashboard's node-local retention history was written by the apply thread: for every watermark
  a JSON record, the file opened, three writes, closed, and the file rewritten with two fsyncs every
  8,192 records, up to 37 ms at a time and several times in a row. Apply now keeps a step in memory,
  where the dashboard reads it at once, and the metrics collector writes the file every
  `METRICS_FLUSH_MS` (60 s) and at shutdown. After a crash the dashboard's history misses up to that
  last minute; no queue state is involved. A million partitions at 1.5M msg/s: 23 / 120 → 21 / 39 ms;
  100,000 partitions at 2M msg/s, not carried before (2 s and rising), reads 63 / 106.
- Not solved: with retention on and 10,000 to 100,000 partitions the store's checkpoint cannot keep
  its 250 ms pace (a page written per changed row), which lifts the p99 there to about 50 ms at
  1.5M msg/s against 36-40 at 1,000 and at 1M partitions.

**Two stalls at high partition counts and loads.** The queue log's cross-file directory is merged
about every 40 s on every node, and its files were synced in one go: with 100,000 partitions at 1.5M
msg/s the raft log's sync waited behind it (the leader's longest commit 15 → 64 ms, one 10 s window
at 138 ms). They now reach the disk 8 MB at a time while they are written. And the planner kept a
queue's partition names in one hash table, which hashes every name again when it doubles: 36 ms at
458,752 names and 89 ms at 917,504, on the thread every push waits for (a million partitions at 2M
msg/s: one window at 151 ms against 55-65). The names sit in 256 tables once a queue has 4,096 of
them, and lookups that are dropped are freed off that thread.

**Kafka facade: a fetch is read to what its response can carry.** Kafka's `max_bytes` is for the whole
response, and each call the facade makes to the broker now carries it as the call's own ceiling (the
route's top-level `maxBytes`), counted as Queen stores the records: a key and a value are kept in
base64, so the client's number is asked for a third larger. The entries are read in order while any
of it is left, and the rest answer their bounds and no records. Before, every partition of a wide
assignment was read to its own budget, so a consumer behind on a thousand partitions had a gigabyte
read for the 32 MiB a response carries, on every fetch; and asked for the client's number as it
stood, a fetch came back a quarter short. A fetch from an old offset now reads the queue log with the
log's lock released, as a pop does (the appends to that log waited behind a read from the disk), and
once a record waits for the disk the kernel is asked to read ahead (`QUEEN_QLOG_READ_AHEAD_MB`, 1;
`0` is off). Not measured yet.

**Proxy: a plan counts requests and uncompressed bytes, no longer messages.** A plan limited what a
tenant sends with three caps counted in messages: a rate (`max_msgs_per_sec`, `msgs_burst`), a size
for one payload (`max_payload_bytes`) and a number of items for one push (`max_batch_items`). The
proxy could answer them only by parsing the body as JSON, and the binary push and transaction bodies
the broker now reads (`QPB1`, `QTB1`) were forwarded without any of them. One rate replaces the
three: `max_bytes_per_sec`, with `bytes_burst`, counted in uncompressed bytes. A push, a
transaction, an ephemeral push and a streams cycle that emits are weighed on the way in, and a pop's
answer leaves the same bucket once the broker has answered. The proxy reads the weight off the
body's headers and inflates nothing: a body compressed as a whole carries its `raw_len`, each group
of a framed push or of a binary transaction is one zstd frame that declares its content size, and
the broker refuses a body that holds more than it declares. A binary body whose headers do not say
what it weighs answers 400 `invalid_request` at the proxy, and the broker now refuses a binary push
with flags 1 and 8 together (a framed body compressed again as a whole), which hid the groups and
their sizes. No seeded plan has a byte rate: it is unlimited until `max_bytes_per_sec` is set in a
cluster's overrides, so set it before a cell that enforces its limits takes this build. An override
that still names one of the four retired keys is accepted and changes nothing, so a control plane
that sends them keeps provisioning. `QUEEN_PROXY_MAX_BATCH_ITEMS` is no longer read, and the limits
log line says `kind=bytes` where it said `kind=msgs`. Still read from a JSON body only: the queues
and partitions a push names (the plan's `max_queues` and `max_partitions_per_queue`), and the
messages the meter books.

**Server: a standby cluster can run several raft groups.** A standby replayed one log, so a link
needed `QUEEN_RAFT_GROUPS=1` on both clusters and a standby started with more refused to boot.
Each raft group of a standby now replays the source's group of the same number: it reads the nodes
`QUEEN_LINK_SOURCE` names on their raft port plus the group's number, takes its seed from that
group's snapshot into its own directory, and keeps its own position, id and lag. Nothing is
configured per group. Where a tenant lives is each cluster's configuration and not replicated
state, so every call of a standby's group now states its group, the number of groups and its tenant
pins in a header (`x-queen-link-placement`), and a source group answers `409` to a standby whose
`QUEEN_RAFT_GROUPS` or `QUEEN_TENANT_GROUPS` differ from its own: the standby shows `waiting` with
the reason and replays nothing, and a refused seed stops the boot. That also closes a hole of
2.1.0 to 2.2.0, where a source of several groups answered a standby of one group, which replayed
group 0 alone and said nothing: such a standby is now refused, and a standby of several groups
refuses a source up to 2.2.0, which does not state its placement. A link between clusters of one
group works as before, in both directions between this build and 2.2.0. On a node of several
groups `GET /api/v1/system/link` and `/health`'s `raft.link` list every group under `groups`, with
a `role` on top that is `mixed` when the groups differ, and `POST /api/v1/system/link/promote`
promotes every group; a group that could not be promoted is named in the answer with its status,
and the call is repeated. A promotion cuts each group at the entry its own replay had reached: no
command spans two groups, so every tenant gets a state the source had for it, and tenants in
different groups can be cut at slightly different moments. The Jepsen failover tests ran one
group. See [standby cluster](https://queenmq.com/manage/standby/).

**Server: a node added to a cluster of several raft groups can become a voter in every group.**
With `QUEEN_RAFT_GROUPS` above 1 each group prefers one member as its leader, and a group's leader
handed leadership to that member as soon as it answered and was nearly caught up, without asking
whether it was a voter. A node being added is a learner first. In the group that preferred it, the
leader handed leadership to a learner, which cannot be elected, and from then on no longer heard
its learners: `POST /api/v1/system/raft/membership/promote` answered `409 learner_behind` ("it has
not answered recently") for that group, for good, while the other groups promoted the node. And
when the node had become a voter in time, the leader could hand it leadership between the two
steps of the promotion, which then stayed unfinished (`409 in_flight`) until an operator sent the
voters again. A group's leader now hands leadership only to a member that is a voter and only
while no membership change is in flight, so the hand-over happens after the promotion. Found by a
new test that adds two nodes to a seeded standby of two groups.

## 2.2.0 - 2026-10-10

**Server: a queue's memory no longer grows with the messages it keeps.** Every push left a row in
the memory of every node (about 200 bytes: where the append begins and ends, when it was written,
the hash of each `transactionId`), and the row stayed until retention removed the message. A queue
with retention off, or with days of it, held a row for its whole history in RAM, on each node, and a
restart loaded them all again. A row now leaves the store when the queue's dedup window has passed
(`dedupWindowSeconds`, 3,600 s by default, and never less than `QUEEN_RAFT_TXN_WINDOW_MIN_S`,
900 s), whatever the retention: `retentionSeconds` and `completedRetentionSeconds` say how long a
message stays on disk, and no longer how long its row stays in memory (until 2.1.0 a longer
`completedRetentionSeconds` was also the least time a row was kept). Messages older than that are
read from the queue log, which always held them: a consumer far behind, a group that subscribes at a
past instant, a lookup by timestamp, and retention itself. Memory now follows the push rate times
the window, not the size of the queue. On one 16-core VM, 20,000 single pushes a second for five
minutes with retention off and a 60-second window: 2.1.0 ended with 5.95 million rows and 1.8 GB,
and still held 1.4 GB after the pushes stopped; this build stayed at 1.2 million rows while they ran
and held no row and 0.3 GB 75 seconds after. Consumers at the tail are served from memory as before:
at 300,000 messages a second the push p50 was 8.5 to 9.4 ms on this build and 8.8 to 9.0 ms on
2.1.0, over five runs on ten cores of that VM.
The backlog of that run, 5.9 million messages with no row left, was read back by 16 consumers at
about 500,000 messages a second, against about 520,000 on 2.1.0, which read it from memory. While
it was being read, the traffic at the tail kept its 300,000 messages a second, with a push p50 of
9 to 10 ms on both builds. From the disk it is the same on both: with 32 GB of
backlog (48.75 million messages) and the page cache dropped, the consumers read it back at about
360,000 messages a second on this build and 375,000 on 2.1.0, and the broker's own memory stayed
at 0.6 GB against 2.2 GB. A walk over old messages asks the kernel for a chunk of records at
once as soon as one read has waited for the disk, so the reads overlap. A queue of 99 GB (149
million messages, retention off) left the broker with 0.4 GB of memory once its rows had gone, and
the node started on it in 3.5 seconds with a cold page cache. The runs are in
`benchmark-queen/2026-10-09-rows-window/`.
On three 16-core machines, at 1,000,000 messages a second in and out of one queue of 500,000
partitions for five minutes, this build and 2.1.0 both carried the rate with nothing shed and no
error. With retention off, where 2.1.0 removes nothing, its leader's memory grew from 2.3 to
11.1 GB and was still growing, and this build's stayed under 7.1 GB and fell to 5.1 GB, for eight
more points of the apply thread on each node (55% against 47% on the leader) and 4 to 7% on the
p99s. With consumed messages removed after a minute on both, the two builds could not be told
apart (`benchmark-queen/2026-10-10-three-node-1m/`).
The change needs cluster version 6, which the leader raises by itself once every node runs this
release; from then on 2.1.0 and older refuse to start on that data, and
`QUEEN_RAFT_CLUSTER_VERSION_MS=0` keeps the way back open while the release bakes. Until the
version is 6, rows stay as long as their messages, as before. `QUEEN_RAFT_ROWS_WINDOW=0` does the
same on purpose. New metric: `queen_consume_cold_claims_total`, the claims served from the queue
log.

**Server: reading an old message no longer asks every log file.** A read by offset asked the
sealed log files one after the other, oldest first, so its cost grew with what the queue retained.
It now asks the newest first, where consumers usually are, and for the rest reads a directory of
the sealed files: small sorted files beside the log (`dirx-*.qdx`) that say which file holds which
offsets of which partition. They are built in the background once 16 sealed files have none,
merged as they pile up, checked when the log is opened, and built again if lost; a node without
them answers the same, more slowly. `QUEEN_QLOG_DIRX=0` turns the building off. New metric:
`queen_raft_qlog_dirx`, the directory files and the sealed files none of them covers yet.

**Server: retention keeps up with a busy partition.** One retention round moved a partition by at
most 1,000 pushes (`RETENTION_BATCH_SIZE`), and a round starts every `RETENTION_INTERVAL` (5 s):
a partition written faster than 200 pushes a second fell behind retention for good, and its rows
piled up in memory. A partition that has more due is now judged again 40 ms later, until it has
caught up.

**Server: an ack repeated long after its message was pushed.** A `completed` ack of a message that
was already acked is still a no-op. The broker recognises it while the message's row exists (the
dedup window), and after that while the message is among the last 65,536 its group consumed from
that partition; an older one is answered as not found. On a queue without retention, 2.1.0
recognised it for as long as the queue existed, and on a queue with retention for at least as long
as it kept the message.

**Server: a quiet queue no longer makes a log file every ten minutes.** A log file is sealed when
it is full (64 MiB), and a quiet one by age, so that retention can free what expired in it: every
`QUEEN_QLOG_SEAL_AGE_S` (600 s) the file of any queue that had a message in it was sealed, whether
or not retention had anything to free. A queue that got a message now and then made 144 files a
day, each with its index, and a queue without retention kept them all. An aged file is now sealed
only when that is worth a file: retention found a message in it that has expired or whose queue
is gone, or it holds an eighth of a full file. A queue without retention gets a new file every
64 MiB and no other. On a VM, 500 queues with a message each every 5 seconds for five minutes,
retention off and the seal age at a twentieth of its default (what 100 minutes do at 600 s):
2.1.0 ended with 3,630 files and 6,780 memory maps, this build with the 501 it started with and
537 maps. With a retention of 20 seconds on the same queues, files are sealed as their messages
expire and freed as they die: between 130 and 260 of them at any time, and none 45 seconds after
the last message.

**Server: the kernel's limit on memory maps cannot stop a node.** Every sealed log file kept its
index memory-mapped, twice (the log's own map and retention's), and the kernel allows a process
65,530 maps unless `vm.max_map_count` is raised: about 32,700 sealed files. Past that the next
file could not be sealed (see the entry below for what that did), and with twice as many files
no start succeeded, since a start maps every index. Reached by size, the first is 2 TB of log on
a node; reached by quiet queues at 144 files a day each, it is 227 days of one queue, or ten days
of 23. Three changes. An index that fits one page (about 80 records) is read into memory
and takes no map. Retention judges a file from the log's own index, so a file has one map: 65,530
of them is 4 TB of log at 64 MiB a file. And an index the kernel refuses to map is held in memory
instead, with a warning, so the limit costs memory and never the node. New series
`queen_raft_qlog_index_maps` (`mapped`, `limit`, `refused`), and a warning in the log past 70% of
the limit. Every release since 2.0.0 has the limit; a node on one of them is as close to it as
twice its `queen_raft_log_storage{kind="files"}`.

**Server: a log file that could not be sealed keeps its messages.** Sealing a file writes its
index and opens it. When that failed (the process out of memory maps or file descriptors, a write
error) the file was left marked as sealed with no index in memory: its messages, and those
written to it afterwards, could not be found until a restart. The next attempt then wrote the
index again from what memory still held, a part of the file or nothing, and a restart trusted
that index, so those messages stayed on disk and out of reach. A seal now changes nothing until
the index is written and open: a failed one leaves the file as it was, and the next one seals it
whole. Every release since 2.0.0 has this.

**Server: an entry written after a log cut can be read.** A log file is created for a sequence
number: by a roll, for the group about to be written, and by the idle pass, which seals a quiet
file and creates the next one for the writer's next number. When a follower cuts its log back (a
tail a new leader overruled), the entries it takes next carry lower numbers than that, in two
ways. The cut can come after the seal, and the empty file kept the number it was created for. Or
it can come before: the idle pass still counted the writer's next number from the tail the cut
had removed, and a file sealed in the moment before the new leader's entries arrived was created
too high. Either way an entry went into a file whose header said it held nothing that low, a
search by number skipped the file, and at its next start the node refused:
`raft log entries N..N+1 are not all in the queue logs`. A cut now lowers the first number of an
empty file and the writer's own count, and a file begins where its first record does, whatever
created it. Every release since 2.0.0 has this; it needs a log sealed around the moment a node's
entries are overruled, which the default seal age of 600 seconds makes rare. Found by Jepsen on
logs sealed after 10 seconds: the first way by four tests that were not valid, the second in the
node logs of a test that was, because the other four nodes had carried it.

**Server: a node starts after an empty queue log was removed half-way.** A queue's log whose files
have all been reclaimed is closed and its directory deleted. The deletion was done in place, and
could stop between the last file and the directory: a crash, or a filesystem that keeps a deleted
file while it is open and then refuses to remove the directory (FUSE, NFS). The directory that
was left, with no file in it, is what a log that lost its records looks like, and the next start
refused it: `it ends at seq 0, below seq N, which this node fsync'd and applied`. The directory is
now renamed to `q<id>.dead` in one step and deleted after that is durable; a start sweeps such
directories, and a snapshot leaves them out. Every release since 2.0.0 has this too. Found by
Jepsen under power loss, where it took every node of a cluster down one after the other.

**Server: `/health` no longer says healthy without a majority.** A node answered `200 healthy` as
long as it knew a leader and its apply was not behind. A leader that had lost every follower, and
a follower cut off from its leader, kept that leader's name and said healthy for good; a voter
that came back with an empty disk said it from its first answer. On Kubernetes, where `/health` is
the readiness probe, the Service kept sending clients to a node that could not serve them, and a
rolling update moved on to the next pod before the one it had restarted was back in the cluster.
`/health` now also needs the leader the node follows (itself, when it leads) to have heard from a
majority of the voters within `QUEEN_RAFT_READY_QUORUM_MS` (5000 ms; `0` turns the check off), and
the body says how long ago that was, in `raft.quorumAckMs`. A node that has applied nothing while
the cluster is more than `QUEEN_RAFT_READY_LAG_ENTRIES` ahead answers `503 settling`: its catch-up
is now measured against the commit point the leader's appends carry when the leader does not
answer its question. An election stays `200`: the figure is the silence of a majority, not the
absence of a leader. Do not point a liveness probe at `/health`: a lost majority would restart
every pod.

**Dashboard: moving between views keeps what you were looking at.** The selected queue, the time
window, the filters and the page of a list are part of the address: they survive a reload, a
shared link and the browser's Back, and they follow you from a queue to its messages, its
consumer groups and its traces. A queue's pages share a breadcrumb and tabs. The sidebar is
grouped by what you do (Messaging, Workers, Analysis), with KV and Locks under Messaging. A
filtered list says how many of the total it shows. Who is signed in, and the way out, are in a
menu at the right of the top bar; an operator reaches Users from Members.

**Dashboard: the lines it judges by can be moved.** What turns a queue or a consumer group amber
or red was fixed in the code: a group 60 seconds behind, five minutes of backlog, 1% of acks
failing, a node at 80% of its memory. A new Settings page states those lines and lets a tenant
move them, for all its queues or for one queue (also from the queue's own page), and mark a queue
that no consumer group is meant to read. The moved lines are one JSON document in the tenant's KV
(`queen.console` / `settings`), written with a version check, so two people cannot overwrite each
other. A blank line keeps the built-in value; saving needs the right to configure a queue.

**Dashboard: the two Performance pages are gone.** Analytics and Queue operations showed, with
their own charts, what Workload, a queue's own page and System already hold: Workload goes from
a namespace or a task down to one queue (flow, lag, acks, dead letters, retention). A link to
`/analytics` or `/operations` lands on the queue it named, or on Workload, with the same time
window.

**Dashboard: a supervisor card charts all its queues.** The activity chart of a card opened on the
first queue the application reports. It now opens on the sum of all of them, each counted once
whatever the number of replicas, and the list of queues, now to the left of the chart, filters it
to one.

**Kubernetes: an operator runs a cluster from one object.** `deploy/operator` is an operator and
its `QueenCluster` definition (`queenmq.com/v1alpha1`). It creates the StatefulSet, the Services
and the disruption budget of the Kubernetes page and does what a StatefulSet cannot. It restarts
the pods one at a time with the leader last, each behind a write that commits. It adds and removes
voters through the membership API, a node leaving the membership before its pod stops. It replaces
a pod whose volume is lost (the annotation `queenmq.com/replace-pod`), and it grows the volumes. It
restarts nothing while a voter is behind and does not catch up, and says so. It uses the broker's
HTTP API as it is and keeps no state outside the object. It takes no backups, it does not take
over a cluster applied by hand, and its API may still change. These steps ran on k3s against 2.0.3
on 2026-10-08, under a write load of 6,100 pushes of which none was lost; on this release its unit
tests ran, and those runs were not repeated. The image is `ghcr.io/queen-mq/queen-operator`, built
by the `Operator` workflow. Docs: `operate/operator`.

**Terraform: a provider for queues and the S3 sink.** `deploy/terraform-provider-queen` is a
provider for Terraform and OpenTofu with two resources. `queen_queue` declares a queue's options:
one you leave out stays as the broker has it, and destroying the resource forgets the queue and
deletes it only with `delete_on_destroy`. `queen_s3_sink` declares a cluster's S3 sink through
the control plane, with a `secret_key` that is write-only and never enters the state. Its
acceptance tests pass against this release with Terraform 1.15.8 and OpenTofu 1.13.1. It is not in
a registry yet: build it and point the CLI at the build with `dev_overrides`. Docs:
`operate/terraform`.

## PHP client 2.4.3 - 2026-10-10

**Laravel: the PHP client pins supervisor 0.8.1.** `queen:supervisor-install` installs 0.8.1, with
the fixes of the Rust master below. A deployment that pins the SHA-256 of the manifest, as
`--manifest-sha256` or `supervisor_binary.manifest_sha256`, takes the one of the 0.8.1 release
manifest.

**Laravel: a worker that has not consumed counts from the start of its first failed pop.** A
supervised worker whose pops fail records since when, and `queen:supervisor status` reports its pool
after 60 seconds (PHP client 2.4.2). The time was taken when the pop failed, not when it began. A
broker that keeps its connections but stops answering fails a pop only when the request times out,
35 seconds for each broker URL the client tries, about 105 seconds with three, so a hung broker
showed as `pool_not_consuming` about 165 seconds in. The worker now records the start of the pop
that failed: the pool shows as not consuming once that first pop gives up. A pop that works still
clears it, and the next failure starts a new count from its own pop.

## Supervisor 0.8.1 - 2026-10-10

**Supervisor: strays are stopped, prefork is tried again at `queue:restart`, a dead server's
workers are reaped, and only the probe closes a circuit.** The Rust master has the same four
changes as the PHP engine of PHP client 2.4.1: a late fork reply is read once per loop whether or not a worker
of that server is polled; a `queue:restart` announced by a spawned worker starts a fork server
when prefork has none; a forked worker whose server died is reaped by the master when the master
is the nearest subreaper (PID 1 in a container), with its real status, instead of staying a zombie
that kept its pool slot; and a sibling that reached `stable_after` while the pool's circuit was in
backoff no longer closes it, which only the probe does, as in the PHP engine. The master also no
longer lets a pool that balances inherit its own `QUEEN_LARAVEL_BLOCK_FOR`. The PHP client
pins this release from 2.4.3; the worker-invocation record pins its behaviour.

## PHP client 2.4.2 - 2026-10-10

**Laravel: a supervised worker that cannot consume leaves, or says so.** Laravel's worker catches
what a pop throws, reports it, sleeps a second and pops again, for as long as it lives, so a worker
whose every pop failed stayed alive and counted as capacity: with the PHP client 2.3.1, forked
workers that popped from a default connection that only dispatched threw on every pop for half an
hour while `queen:supervisor status` said ready, at full capacity, with closed circuits. A worker
started by either engine, spawned or forked, now tells two cases apart. When its loop works
another connection than its pool's, or a pop on the pool's connection throws a `LogicException`
(`InvalidArgumentException` included), no retry can help: it throws `WorkerCannotConsume` from its
next loop, which reaches the application's exception handler, and exits 1, which both engines
count as a crash, so the pool's restarts back off and its circuit opens. Any other failure, a
broker that is down, slow or refusing, leaves the worker running, since restarting every worker of
every pool would turn a short outage into a restart storm: the worker writes `<pid>.pop-failures`
in the state directory's private `exits` directory, saying since when its pops fail, and the first
pop that works, empty or not, removes it. A healthy worker writes nothing. `SupervisorState`
reads the file for the workers a pool lists: `readiness()` reports `pool_not_consuming` when every
running worker of a pool has failed every pop for 60 seconds, and `capacityHealth()` reports
`pool_worker_not_consuming` when any has, so `queen:supervisor status --check` and
`--check-capacity` fail; a shorter outage reports nothing, and the issues clear on their own.
`status --json` lists those workers on each entry of `pool_status`, in `not_consuming` with the
longest in `not_consuming_seconds`, however short their failure, for monitors with thresholds of
their own. It works with the supervisor 0.8.0: both engines already pass the directory to their
workers and empty it for every generation. The issue codes `status --json` reports are now listed
in the supervisor guide as a stable contract.

What an application sees change: a supervised worker may now exit 1 with `WorkerCannotConsume`
where it used to loop on the same exception; workers write `exits/<pid>.pop-failures` in the state
directory while their pops fail; and `readiness()` and `capacityHealth()`, with `status --check`
and `--check-capacity`, report two new codes, `pool_not_consuming` and
`pool_worker_not_consuming`. A readiness probe that passed during a broker outage now fails once
the outage lasts 60 seconds; the Kubernetes guide shows how to leave these codes out of a probe
that must not stop a rollout.

**Laravel: a delivery that carries no Laravel job goes to the dead-letter queue at once.** A
message whose data is not a JSON object, or names no `job` to call, made `pop()` throw, and the
delivery was left to its lease. A lease expiry never charges the queue's retry limit, which only a
failed ACK does (checked on the 2.0.4 broker with `retryLimit` 3 and `dlqAfterMaxRetries`: six
expiries, six deliveries, no dead letter), so it came back at every expiry, forever, and the
messages behind it on its partition never ran; on a queue with one partition, none did. The worker
now files it into the dead-letter queue at its first delivery, as it does a failed job, reports a
`NotALaravelJobException` to the application's exception handler with the delivery's transaction ID,
queue, partition, delivery attempt and first 120 bytes, the same text as the dead-letter error, and
pops again. No `failed_jobs` row is written: it was never a Laravel job. Since the PHP client 1.0.
A Laravel job that fails for the last time also goes to the dead-letter queue, as before, beside
its `failed_jobs` row, so a count of dead letters includes ordinary failures: the error text
"carries no Laravel job" tells the deliveries of this paragraph from them.

**Laravel: a job whose timeout its lease cannot cover ends a supervised worker.** A job with a
`timeout` at or above `retry_after`, without lease renewal, made `pop()` throw a
`RuntimeException` and was left to its lease, silently, at every delivery. Only a deploy fixes the
job class or the connection, so the exception is now an `UnsafeJobTimeoutException`, a
`LogicException` with the same message, and a worker started by a supervisor leaves at its next
loop: its pool's restarts back off and the crash shows in `queen:supervisor status`. The delivery
waits, to lease expiry, for a worker that runs the fixed code.

**Laravel: a refused supervisor installation says why, and root can install it for the user that
runs it.** The launcher starts the Rust supervisor only when the user it runs as owns each
directory and file of the installation. Otherwise it stopped with exit code 70 and "The Queen
supervisor installation base must be a real, owned directory without group/world write access.",
which named neither the condition that failed nor the way out, since PHP client 1.3.0. An image
that ran `queen:supervisor-install` as root and the supervisor as `www-data` stopped there, and so
did the Dockerfile of the Kubernetes guide. The message now goes on with the path and the
condition: missing, a symbolic link, not a directory, a mode that grants group or world write
access, or another owner than the effective user, with both users by uid and name and the ways
out. The binary, the receipt and the installer's own refusals do the same; the first sentence and
the exit code are unchanged. `queen:supervisor-install --owner=<user>`, a name or a numeric uid,
lets root install and verify as itself and then give the installation to that user, with the
modes a normal install gives. It changes the owners from the files up to the base with `lchown()`,
below the pinned base, so no other user can swap an entry before it changes. It is refused without
root, for a user that does not exist, and on a thread-safe PHP, whose `chdir()` pins the base by
name only. In a multi-stage build, `COPY --chown` gives the copied installation to the runtime
user, as the Kubernetes guide now shows.

**Laravel: a refused state says whose it is and whom to run as.** A Kubernetes exec probe runs as
the container's user. In a container that starts as root and runs the supervisor as `www-data`,
`queen:supervisor status --check` refused the state with "Queen supervisor state ancestor
[/run/queen-supervisor] must be owned by root or the current user." and nothing else: the message
did not say that the directory is `www-data`'s, that the probe ran as root, or that root is refused
by design. The probe failed and Kubernetes restarted a healthy pod every few minutes. Since PHP
client 1.3.0, where the supervisor arrived. Every refusal of a path that another user owns, root
included, now goes on to say who owns it and who is asking, by uid and by name when the user
database has one, and how to run as the owner, such as
`su -s /bin/sh www-data -c "php artisan queen:supervisor status --check"` or
`securityContext.runAsUser: 33` on the container. The first sentence of each message, the checks
themselves and the exit codes are unchanged: a probe must still run as the supervisor's user, and
the Kubernetes guide now shows the probes wrapped in `su` for a container that starts as root.

**Laravel job metrics: a timed-out attempt and a job that gives up with `fail()` count as failed.**
A job that outlived its timeout was not recorded at all: Laravel's timeout handler raises
`JobTimedOut`, then `WorkerStopping`, then kills the worker, and the recorder listened only to
`JobProcessed` and `JobExceptionOccurred`. The Jobs page therefore missed those attempts, and a
class's longest runs were missing from its runtime and from the `max_ms` that the tuning advice
compares with `shutdown_grace`. Such an attempt is now a failed one, with its runtime, written at
that `WorkerStopping` before the worker dies. A job that calls `$this->fail()` returns normally, and
Laravel raises `JobProcessed` for it: it counted as processed, and now counts as a failed attempt.
Since PHP client 1.7.0, where job metrics arrived. A forked worker also no longer writes counts it
inherited from the fork server, under the server's key, if it stops before its first loop; Laravel's
first loop already started them over, so no released version wrote them.

## PHP client 2.4.1 - 2026-10-09

**Laravel prefork: a forked worker works the connection it was given.** The supervisor sends the
fork server the arguments after `queue:work`, the connection first, and the fork server bound them
as a command line of their own: Symfony took the connection for the command's name, `connection`
stayed empty, and the worker fell back to `queue.default`. A forked pool therefore popped from the
application's default connection, with the pool's `--queue`. A default connection that only
dispatches threw on every pop, every 2 s per worker, and the pool worked no job; a default that is
a Queen connection worked, with the default's settings instead of the pool's. Spawned workers
(`prefork` off) were never affected, and `ps` showed `queue:work <connection>` all along, because
the process title is set by hand. Since the PHP client 1.7.0, where prefork arrived. The fork
server now binds the arguments after the command's name. The tests that let this through ran a
stand-in for `queue:work` and, when they ran the real one, an application whose default connection
was the pool's. They now run Laravel's own `queue:work` from an application whose default
connection is none of the pools', both as a spawned worker and as a forked one, and the fork
server, the PHP engine and the Rust master are each checked against one record of what a pool's
worker receives (`tests/Fixtures/Supervisor/worker-invocation.json`), from `config/queen.php` to
the worker's arguments and environment. That record also showed the PHP engine giving a spawned
worker of a balancing pool an empty `QUEEN_LARAVEL_BLOCK_FOR`, which the connector ignored; it is
no longer set, as for a forked worker.

**Laravel prefork: the fork server accepts a `log` service that is not Laravel's.** Before each
fork it forgot the log channels through `getChannels()`, which an application's own PSR-3 logger
bound as `log` does not have: the server failed as it started, and the supervisor spawned the
workers instead, without prefork. It now forgets channels only from a `LogManager`.

**Laravel: a pool's settings reach the worked connection alone.** A supervisor starts a worker
with its pool's consumer group, `retry_after` and `block_for` in the environment, and the
connector applied them to every Queen connection the worker resolved. A job on connection A that
dispatched onto connection B built B with A's group and lease: `size()` answered the wrong group's
depth, and a B with an explicit lease renewal interval, sized for its longer lease, failed its
timing check and the dispatch threw. The supervisor also names the worked connection, and the
settings now apply to that one; another connection keeps its own configuration.

**Laravel: a pool works its connection's queue and consumer group.** A pool that named only its
connection took `consumer_group` and the default queue from `config/queen.php`, not from that
connection, so under the supervisor its workers consumed another group and another queue than
`queue:work <connection>` run by hand. The connection's own settings are the defaults now, and the
worker-invocation record pins a pool that names only its connection.

**Laravel prefork: a worker forked after `queue:restart` leaves at once.** The master replaces the
fork server when a worker leaves with the restart marker, and a scale-up in between forked from
the old server. Laravel's worker reads the restart signal once, as its baseline, so such a worker
never stopped and ran the old code until it was removed. The fork server remembers the signal at
boot, and a child forked after it changed leaves with the restart marker: the master replaces the
server, and the replacement runs the code on disk. Since the PHP client 1.7.0.

**Laravel prefork: the fork server lets go of the queue connections the boot resolved, and a
child reports its exception.** The queue manager keeps every connection it resolved. One a
provider or a health check resolved at boot was shared by every forked worker, with the server's
environment instead of the pool's settings and with one socket for all; the server now forgets
them before each fork and warns at boot which ones it found. And a forked worker's exception went
to stderr alone, where a spawned worker's reaches the application's exception handler through the
console kernel: the child reports it the same way and exits 1.

**Laravel: a connection's own `url` is not replaced by the default `urls`.** `QUEEN_URLS`, a
cluster, is the default of every Queen connection, and it replaced the single `url` of a
connection on another broker, in the connector and in the supervisor's export alike.

**Laravel: failed jobs of other drivers stay Laravel's.** With `sync_failed_jobs` on, the default,
every failed job on redis, database or sync took the cache lock, waited up to
`failed_jobs_lock_wait` under contention and threw on a store without locks; and `queue:forget`,
`queue:flush` and `queue:prune` resolved every record's connection, so a record of a connection
since removed made them throw. A failure on another driver goes to Laravel's provider now, and such
records are removed without resolving their connection.

**Laravel: `QUEEN_SUPERVISOR_REMOTE_STATUS=1` and `QUEEN_DASHBOARD_ENABLED=1` turn their features
on,** as `=1` turns prefork, coordination and metrics on. Queue names given as an array are trimmed
like the comma-separated form.

**Laravel PHP engine: SIGHUP drains, prefork is tried again at `queue:restart`, strays are
stopped, a dead server's workers are reaped.** The engine died hard on SIGHUP, with its workers
and its lock behind it; it drains, as the Rust master does. A fork server that missed its boot
timeout, or failed a fork, left the engine spawning until it was restarted; a `queue:restart`
announced by any worker asks for a new server. The late reply to a fork request the engine gave up
on stopped its stray only when another forked worker was polled; every server's events are read
once per loop. A forked worker whose server died was taken for exited 0 once gone; where the
engine is the nearest subreaper its real status is read, instead of a zombie. A request line over
the limit no longer ends the server, and every worker with it. A document without `quiet` runs
quiet, as the Rust master reads it.

## 2.1.0 - 2026-10-09

**Server: a standby cluster.** A second cluster can now replay the first one's log and take over
when the first is lost. The standby's leader reads the source's committed entries over the raft
port and proposes each one into its own log, so the standby holds the source's messages, cursors,
leases, KV, timers and dedup window, a moment behind. It answers reads, refuses
writes with `503` and `"code": "standby"` on every node, and becomes an ordinary cluster with
`POST /api/v1/system/link/promote`. An empty standby follows a young source from the start of its
log (`QUEEN_LINK_STANDBY`); a source with a history seeds the standby's first node with its
snapshot (`QUEEN_LINK_SEED`). Replication is asynchronous: a promotion after a crash loses at most
what the standby had not read yet, and a planned switch loses nothing. On one laptop, with both
clusters and the load generator sharing a disk, a standby stayed within 0.25 s of a source taking
200,000 messages a second. The source keeps its log for the standby on every node and across
restarts, and gives it up when its own disk fills (`QUEEN_LINK_HOLD_S`,
`QUEEN_LINK_HOLD_DISK_PCT`). `GET /api/v1/system/link`, the `link` block of `/health` and the
`queen_link_*` series say where a standby is. The source needs `QUEEN_LINK_TOKEN`, the standby
`QUEEN_LINK_SOURCE` and `QUEEN_LINK_SOURCE_TOKEN`; a cluster with none of them behaves as before.
See [Standby cluster](https://queenmq.com/manage/standby/).

**Server: the last node of a cluster to stop no longer hangs.** A node that led a cluster whose
other nodes were already down, with an entry in its log that could no longer commit, waited for
that entry for ever while it shut down, and used a whole core doing it. A stopping node now leaves
once every caller of its entries in flight has an answer; an entry that timed out had already
answered `retry`.

**Server: a leader that no majority has acknowledged for two seconds no longer stops for good.**
A leader cut off for two or three seconds and not replaced, or one whose majority needs a
follower with a slow disk, had its next write refused by raft, took the refusal for a lost
leadership and stopped planning. It still led, so nothing started it again: every request ran
into its deadline until the leadership changed or the node restarted. Every release since 2.0.0
has it. Such a leader now keeps its entries and logs them, in order, when a majority answers
again, and a leader that finds itself stopped while it leads starts again after 5 s. Found by
Jepsen on a five-node cluster with two slow disks. With the leader of five nodes cut off for
2.4 s, the build before the fix took no write afterwards in 11 of the 12 trials where the leader
kept its leadership, and this one took the next write in all 10. The log lines that say when it
happens are on [Monitoring](https://queenmq.com/manage/upgrades-monitoring/).

**Server: locks and semaphores.** A lock is a lease: one holder at a time, for a lifetime the holder
declares and renews, with a token that fences a holder that outlived it. A semaphore is the same
lease with up to 1,024 permits. One route, `POST /api/v1/locks`, carries `acquire`, `renew`,
`release` and `get`, up to 64 locks a call, and answers a verdict per operation with HTTP 200
(`acquired: false, reason: "held"` is not an error). A permit is one KV row in the namespace
`queen-locks` (key `<name>#<slot>`, the owner as its value, the lifetime as its TTL), so the route
adds nothing to the log's format: an acquire is a `putIfAbsent`, a renew a `put` with `expect`, a
release a `delete` with `expect`. The token is the row's version and changes at every renew.
An `owner` makes a call safe to send again: the same owner is answered the permit it has. Locks
count against the tenant's KV quota and KV write rate, need a read-write token, and behind the
proxy are part of the KV plan and never quota-blocked. New metrics: `queen_locks_ops_total` and
`queen_locks_op_duration_milliseconds`. A lock expires; a holder that crashed keeps it until its
lifetime ends, a waiter polls, and there are no read/write locks.

**Server: KV `check`, a precondition that writes nothing.** `{"op":"check","ns","key","expect"}` asserts
that a key is at a version, or absent with `expect: 0`. With `required: true` it gates a KV call or
a transaction on a key the call does not write. It is what guards a transaction with a lock: every
acquire and renew answers a `guard`, a `check` of the permit's row at its token, and a transaction
that carries it commits only while the lock is still the caller's.

**Server: KV versions are a fencing token.** On one key, a later write always has a higher version than
every earlier one, across a delete and a re-create, an expiry, a restart and a change of leader.
The planner always assigned them that way; it is now the documented contract, with a test, and the
docs no longer say not to rely on their order.

**Clients: `lock`, `semaphore`, `check` and `guard`, in all six SDKs.** `queen.lock(name)` and
`queen.semaphore(name, limit)`, each with a lifetime, return a handle that acquires (with an
optional wait), keeps the current token, renews and releases; `transaction().guard(lock)` puts the guard in the
commit and reads the token when the commit is sent. The JavaScript, Python, Go and Rust handles
renew in the background every third of the lifetime and signal a lost lock. The PHP and C++
handles have no background renewal: `keepAlive()` (`keep_alive()` in C++) renews at a checkpoint
in the work loop. A guarded commit that lost only to its own handle's renewal is sent again with
the new token. `kv.check` is on the KV client and the transaction's KV builder of each SDK. In the
JavaScript, Python, Go, Rust and C++ clients 2.1.0 and the PHP client 2.4.0. The Rust client
requires `queen-protocol` 1.4.0, the wire-types crate that carries the lock types.

**Dashboard: a Locks page.** Every held lock and semaphore permit with its holder, since when it
is held, its last renewal and when it expires. It reads the permits as KV rows, so a viewer can
open it, and it writes nothing: the drawer shows the guard a transaction carries and the call
that releases the lease period on screen. `get` answers the same `since` for each holder: a
renewal does not move it.

**Dashboard: supervisors grouped by application.** Instances that publish under one group share
one card: workers against the target, pools to check, the instances underneath, and the last hour
of traffic or backlog of a reported queue (#110). The card is built from the dashboard's own
header, stats and tables, and a healthy application is a grey dot, as on the Overview.

**Dashboard: the theme switch is an icon menu.** System, Light and Dark sit behind one icon in the
header instead of a select (#108).

**Server: a KV call that mixed a read with writes that all lost no longer hangs.** A batch on
`POST /api/v1/kv` such as a `putIfAbsent` that lost beside a `get` wrote nothing, so it had no log
position, and its read waited for one until the statement timeout (30 s) and answered 503. Such a
call now takes a position of its own and answers at once.

**Server: the upgrade is one-way.** A node forwards a `check` to the leader only when every member can
read it. Once every member runs this release the leader raises the cluster version to 5, and
from then on 2.0.4 or older refuses to start on the data directory. Until then a call that carries a `check` answers 503
(`kv_check_needs_cluster_version_5`); acquire, renew and release need no new format and work as
soon as the node you call runs this release. To keep the way back open while the release bakes,
set `QUEEN_RAFT_CLUSTER_VERSION_MS=0`.

## 2.0.4 - 2026-10-08

**Dashboard, sign-in page and docs: the logo is a yellow sunflower.** A new drawing replaces the
red one of 2.0.3: seven yellow petals round the q of "queen", with "message queue" under the name.
The dashboard shows the q and its petals beside the word "queen", the sign-in page and the README
show the whole logo, and the tab icon is the q with its petals. The picture of the bee and the
sunflower on the documentation site takes the same yellow.

**Dashboard: the logo's yellow is the accent.** Primary buttons are yellow with black labels, and
the page you are on, selected segments and focus rings take the same colour. On light surfaces,
text and rings in that colour are a darker gold, so they can be read. The sign-in page and the
documentation site follow. Status colours keep their meanings: amber is attention, coral is failure.

**Dashboard: System, Light and Dark.** The theme toggle in the top bar is now a choice of three.
System follows the device's colour scheme as it changes, and is what a browser with no stored choice
gets. Light and Dark stay fixed on that device until System is chosen again, and a choice made
before 2.0.4 is kept.

**Dashboard: supervisor cards share one grid.** The Supervisors page gave each publication group
its own two-column grid, so two groups with one instance each took two rows and left the second
column empty. The instances now share one grid, each card under its group's name: two to a row on
a wide screen, one on a narrow one.

The broker's engine, its API and the clients are as in 2.0.3.

## 2.0.3 - 2026-10-08

**Server: a pop that nobody receives no longer counts a delivery attempt.** A pop's answer waits for
the checkpoint that holds its lease. When that takes longer than the margin the broker keeps before
the request's deadline, or the client leaves, nobody receives the answer, and the broker hands the
leases back at once. It left the delivery attempt counted all the same, so the next consumer got
the message as a redelivery. With `deliveryAttempt` 2 on its first real delivery, a Laravel job
with `tries = 1` failed with "attempted too many times" without ever running. In a 23-hour run of
4.1 million jobs at 50 jobs/s, 4 jobs failed that way, each about 0.4 s after its push, while every
message went out to a worker once; checkpoint writes reached 0.45 s against a 250 ms margin. Handing
back an unreceived claim now also takes back its attempt, on the leader's engine and through the
`Nack` a follower sends. A lease that really expired still counts.

**Clients: a consumer can report to the dashboard, off by default.** Every SDK's consumer builder
takes a supervision option that names a group for the dashboard: `.supervision({ group })` in
JavaScript, `Supervision(&queen.SupervisionConfig{...})` in Go, `supervision(...)` in the Python,
Rust, C++ and PHP clients. Each consume call then writes an observation to the broker's
`queen-supervisor` KV namespace every 10 seconds, and a last one when it stops: its execution model,
its live loops against the configured concurrency, the handlers running, the handler calls completed
and failed, and the age of the oldest handler still running. No payload and no error text is
published. Publishing is best effort and bounded, and it needs a credential that can write KV. It only
observes: it restarts nothing, scales nothing and changes no ack or lease, and with the option off
there is no timer and no request. The PHP client publishes at its cooperative checkpoints, so a long
synchronous handler leaves its last observation stale. In the JavaScript client 2.0.4, the Python, Go,
Rust and C++ clients 2.0.3 and the PHP client 2.3.1.

**CLI: `queenctl tail --supervision-group`.** `tail` reports the same way when the flag names a
group, one identity for each invocation.

**Dashboard: the Supervisors page shows the consumers that report.** Beside the process supervisors
it lists each reporting consumer with its execution model (async tasks, goroutines, threads or
cooperative loops), its live loops against the configured concurrency, its busy handlers, its
completed and failed handler calls, its last completion and the age of its oldest handler in flight.
A completed handler does not prove its ack succeeded, and a fresh heartbeat does not prove progress; a
stale or inconsistent report stays unconfirmed. Process budgets and readiness stay with the process
supervisors.

**Dashboard, sign-in page and docs: a new logo.** The q that is a sunflower replaces the sunflower and
the bee: as a badge beside the word "queen" in the dashboard, as the wordmark on the sign-in page and
in the README, and as the tab icon. The sign-in page now takes the dashboard's light or dark scheme.

## JS client 2.0.3 - 2026-10-07

**JS client: stopping a consumer no longer strands its messages.** Aborting the `signal` passed to
`consume()` was checked only between polls. A long poll open at the abort stayed open for up to its
timeout, and the broker could still hand it a message. `.each()` then dropped that message without
settling it, so its partition stayed blocked until the lease expired, on every rolling restart. The
abort now closes the poll in flight, and the broker hands nothing to a poll whose caller is gone. A
pop answer already arriving is read to the end, since the broker leased its messages when it sent
it. Under `.each()`, messages popped but not yet handed to the handler, and those popped beyond
`.limit()`, go back with a `retry` ack: the lease is released and no retry is charged. An aborted
request is not a backend failure: it is not retried, does not fail over to another node and does not
mark one unhealthy. A wait between attempts (429 backoff, retry after a 5xx or a network error) ends
at once instead of running out. A `wait(false)` consumer stopped during a pop now resolves instead of
rejecting. Two cases still fall back to the lease: an answer lost before its headers arrive, and a
`retry` ack that cannot be delivered. Measured on a three-node 2.0.1 cluster, a consumer stopped as a
message arrived: 40 of 40 messages waited out the lease before, none after.

**JS client: the request timeout covers the response body.** A JSON response was read after the
request's `try` block had ended, so the timeout was already cleared: a body that stalled midway hung
with no timeout.

## 2.0.2 - 2026-10-07

**C++ client: `commit_on_delivery()` commits a pop at delivery.** The new
`QueueBuilder::commit_on_delivery()` makes `pop()` and `pop_result()` send the broker's
`autoAck=true`: the broker moves the consumer group's cursor past the messages as it hands them
out, with no lease and nothing to ack, so the delivery is at-most-once. `auto_ack()` still decides
only the ack after a `consume()` handler and has no effect on `pop()`. `consume()` always leases
its messages, and with `commit_on_delivery()` it throws `std::invalid_argument` before any request.
On the ephemeral pop, `EphemeralPopOptions::commit_on_delivery` is the new name of `auto_ack`, which
still works and is deprecated.

**C++ client: a handler that throws no longer loses a message.** With `auto_ack(false)` and
`each()`, a handler exception was dropped without a log line and the loop went on with the rest of
the pop: a handler that then acked a later message of the same partition moved the cursor past the
failed one, and the failed message was lost. After a failure, the later messages of the same
partition in that pop are no longer handled; they come back with the failed one. The other
partitions of a multi-partition pop are still handled. The consumer still sends no nack under
`auto_ack(false)`, by design: the failure is now logged, and the message comes back when its lease
expires. With `auto_ack(true)` the nack now carries the exception's message as its error, capped at
4096 bytes and with invalid UTF-8 replaced, so the text cannot stop the nack. A handler that throws
something other than a `std::exception` is handled the same way; before, it ended the worker.

**C++ client: `renew_lease()` renews.** The consume loop accepted the setting and never renewed.
While the handler runs, it now renews the batch's lease every `interval_millis`, one request per
lease, and stops after the ack or nack, as the JS, Go and Rust clients do.

**C++ client: `consume()` throws a pop's 4xx and waits out a 5xx.** A 4xx on a pop other than a
403 or a 429, such as a 400, ended the worker inside a pool task whose result nobody read, so
`consume()` returned as if it had finished. It now throws that error once every worker has
stopped, as it does for a 403. A 5xx that outlasted the retries ended the worker the same way; the
loop now waits a second and polls again, as after a network fault, and a connection timeout now
counts as a network fault. `pop()` still logs a failure and returns an empty result.

**C++ client: `ack()` and `renew()` report what the broker refused.** `ack()` returned
`success: true` for any HTTP 200 and `renew()` for any answer, but the broker answers 200 with
`success: false` when it settled or extended nothing, for example under an expired or released
lease. Both now read the body: `success` is false with an `error`, and `ack()` keeps the broker's
answer in `result`.

**Server: the pop parameter `commitOnDelivery`.** `commitOnDelivery=true` is the new name of a
pop's `autoAck=true`: the broker moves the group's cursor past the messages as it hands them out,
with no lease and nothing to ack, so delivery is at-most-once. Every SDK also has an `autoAck()` on
`consume()`, which is client-side: the pop stays leased and the SDK acks after the handler, so
delivery stays at-least-once. The broker option now has a name of its own. `autoAck` is still
accepted as a deprecated alias, so the released Go and Rust SDKs and the CLI keep working: a pop
that sends either name set to true commits at delivery, and a pop that sends neither is leased as
before. The queue, partition, discovery and ephemeral pops read both names, conflation refuses
both with a 400 that names both, and the OpenAPI document marks `autoAck` deprecated. The docs
name only `commitOnDelivery`. A broker up to 2.0.1 reads only `autoAck`: it ignores
`commitOnDelivery` and leases the batch.

**Server: a page of message history reads only the appends that hold it.** A historical
`GET /api/v1/messages` read each partition backwards from its live tail, 256 offsets at a time, and
decoded every payload before it applied `to`, so a small page of old messages could read a large
newer suffix, and a smaller `limit` did not bound the work. It now seeks the queue log's indexes,
active and sealed, by timestamp and offset, ranks the candidates from their metadata, and reads
only the appends that contain the page. The response fields, the status filters and the minute
rounding of `to` are unchanged, and so is the storage format. Listing a tenant's partitions and
cursors still costs what it did, so deep offsets and selective status filters can still be slow.

**Dashboard: the Overview lists the queues that need you, and there is a Supervisors page.** Under
the verdict, the open issues are a list you can search and filter (needs attention, a lag of 5
minutes or more, no reader, pending increased, all queues), ten to a page. Selecting one opens a
drawer beside the page with its pending and in-flight counts, the change between two readings, the
groups behind and the next thing to check. The new Supervisors page shows the worker status that
Laravel supervisors publish to the broker, and reads it again every 30 seconds like the other
pages.

**Python client: `admin.move_message_to_dlq()` and `admin.clear_queue()` are deprecated and
raise.** They sent `POST /api/v1/messages/:partitionId/:transactionId/dlq` and
`DELETE /api/v1/queues/:name/clear`, routes the 2.x broker does not have, so every call failed
with a 404 `no_such_route`. Both now raise `NotImplementedError` before any request and name the
way that works. For a dead letter, ack the message with the `dlq` status,
`await queen.ack(message, 'dlq', {'group': group})`, while its consumer group holds the lease. To
skip what is queued, seek the consumer group to the end,
`await queen.admin.seek_consumer_group(group, queue, {'toEnd': True})`; to drop the queue with its
messages and configuration, `await queen.queue(name).delete()`.

**Python client: `renew()` and the batch `ack()` read the broker's verdict.** The broker answers
HTTP 200 whether or not it extended a lease or took an ack, and says which in the body. `renew()`
reported `success: True` for every 200, and a batch `ack()` did the same for a refused ack, for
example under an expired lease. `renew()` now reports `success: False`, with the broker's error,
when it extended nothing: the lease expired, was released by an ack or nack, or does not exist. It
also returns `renewed`, the count the broker extended. A batch `ack()` reports `success: False`
when the broker refused any item, with each item's verdict in `results`.

**Python client: `commit_on_delivery()` commits a pop at delivery.** The broker can move a
consumer group's cursor past the messages as it hands them out, with no lease and nothing to ack,
but this client could not ask for it: `auto_ack(True)` on a pop never reached the broker.
`queen.queue(q).group(g).commit_on_delivery().pop()`, and `pop_result()`, now send `autoAck=true`,
the parameter every 2.x broker reads; the messages come back with no `leaseId`. This is
at-most-once: a crash after the pop loses the messages. `consume()` always leases, so it raises
`ValueError` before any request when the builder has `commit_on_delivery()`. `auto_ack()` stays the
ack `consume()` sends after the handler and still never reaches the broker. On ephemeral queues,
`queen.ephemeral.pop()` takes `commit_on_delivery=True`; its `auto_ack` argument, which meant the
same, still works and is deprecated. `POP_DEFAULTS` now says what a pop does: `wait` is `True`, as
every pop has long-polled unless `.wait(False)`, and `auto_ack` is never sent. No other behaviour
changes.

**Python client: `pop()` raises the broker's 400 refusal of a conflating pop.** `pop()` returns
an empty list when a pop fails, and did so for the broker's 400 refusal of a conflating pop too:
one without a consumer group, or one with `commit_on_delivery()`. No retry can make either
succeed, and an empty list reads as an empty queue, so a consumer that asked for last-value
delivery never learned that it was not getting it. `pop()` and `pop_result()` now raise that 400,
as the JavaScript client does. Every other failure still returns an empty list.

**Python client: a nack in `each()` mode skips only its own partition.** A multi-partition pop
claims several partitions under one lease, and a nack releases only the failed message's
partition. The loop dropped the whole rest of the pop after a nack, so the other partitions'
messages stayed leased and came back only when the lease expired. It now skips only the later
messages of the failed partition, which the broker redelivers, and handles the others at once.

**Python client: a handler error is never taken for a pop error.** With `auto_ack(False)`,
`consume()` sends no nack for a handler that raises, by design, and the error stops the consumer.
When the error's text said "timeout" or "connection", the loop read it as a long-poll timeout or a
network fault instead: it polled again with the message still leased, and the error was lost. Such
an error now stops the consumer like any other handler error.

**Laravel and supervisor 0.8.0: prefork per pool.** A pool's own `prefork` key wins over
`supervisor.prefork`: `false` spawns that pool's workers, `true` forks them, `null` follows the
switch. One forking pool is enough to start the fork server, and both engines spawn the workers of
a pool with `prefork` false beside it. Use it for jobs that must not run in a forked process, such
as a Kafka client the job creates; a boot that starts a thread still makes the fork server refuse to
serve, and then every pool spawns. The dashboard names the pools that differ from the switch. PHP
client 2.3.0 pins supervisor 0.8.0, which reads the new key: run `php artisan
queen:supervisor-install` after the upgrade. A fork took 0.48 ms on the Linux server, against 88 ms
for a worker that booted the benchmark application on its own
(`benchmark-queen/2026-10-05-laravel-worker-memory`).

**PHP client: `autoAck()` is the ack `consume()` sends after the handler.** The reference said
that on `pop()` it was the broker's at-most-once auto-ack, which never reached the broker. It is not
meant to: `autoAck()` has no effect on `pop()`, `popResult()` and `popDetached()`. A pop commits at
delivery only with the new `commitOnDelivery()`, below. The reference and the builder now say so,
and tests pin it. No behaviour changes.

**PHP client: `commitOnDelivery()` commits a pop at delivery.** The broker can move a consumer
group's cursor past the messages as it hands them out, with no lease and nothing to ack, but this
client could not ask for it. `$queen->queue($q)->group($g)->commitOnDelivery()->pop()`, and
`popResult()` and `popDetached()`, now send `autoAck=true`, the parameter every 2.x broker reads,
and nothing else; the messages come back with an empty `leaseId`. This is at-most-once: a crash
after the pop loses the messages. `consume()` and `getConsumer()` always lease their messages, so
they throw `LogicException` before any request when the builder has `commitOnDelivery()`.
`autoAck()` stays the ack `consume()` sends after the handler and still never reaches the broker.
The broker refuses `commitOnDelivery()` together with `conflation()` (400), and `pop()` throws
that `HttpException`. On ephemeral queues, `$queen->ephemeral()->pop()` takes
`'commitOnDelivery' => true`; its `autoAck` option, which meant the same, still works and is
deprecated, and passing both throws `InvalidArgumentException`.

**PHP client: `Admin::moveMessageToDLQ()` is deprecated and throws.** It posted to
`/api/v1/messages/:partitionId/:transactionId/dlq`, a route the 2.x broker does not have, so every
call failed with a 404 `no_such_route`. No 2.x route dead-letters a message by its address. The
method now throws `BadMethodCallException` before any request and names the way that works: ack
the message with the `dlq` status, `$queen->ack($message, 'dlq', ['group' => $group])`, while its
consumer group holds the lease.

**PHP client: `renewLease()` in batch mode counts from the pop.** With `concurrency(N)`, `consume()`
handles the batches of a poll round one after the other, but it started each batch's renewal
interval when the batch reached its handler, so the check before that handler never found it due.
The interval now runs from the pop answer, and a batch that waited behind other handlers longer
than the interval is renewed before its own handler starts. The loop still cannot renew while a
handler runs, since PHP runs the handler on the loop's only thread, so a handler that declares a
second parameter now gets a renewal call: `function (array $messages, \Closure $renew)`. `$renew()`
renews the leases in hand once `renewLease()`'s interval has passed, or on every call without an
interval, and returns whether the broker extended them; a long handler calls it between parts of
its work. A one-parameter handler is called as before. The loop's own renewal sends one request per
lease instead of one per message.

**Laravel: `queen:consume` sets and renews its lease, and stops on `--idle-timeout`.** With
`--auto-ack`, the nack of a handler that throws now carries the exception's message as its error.
Without `--auto-ack` the command still sends no nack, as the SDK consumers do: it prints the error
and `Not nacked without --auto-ack`, and the message comes back when its lease expires.
`--lease=SECONDS`, default `retry_after` (90), sets the lease of every pop; with
`lease_renewal` on in `config/queen.php` and `--auto-ack`, the command renews it while `handle()`
runs, with the renewer `queue:work` uses, checks it before the ack or nack, and neither acks nor
nacks a lease it can no longer vouch for. Without `--auto-ack` renewal stays off and the command
says so: a handler that acks by itself releases the lease, and renewing it would stop the process.
`--idle-timeout`, which did nothing, now stops the command with exit code 0 after N ms without a
message. `--limit` counts every message handed to `handle()`, failed ones too, and a pop never asks
for more than it leaves. A refused ack or nack prints one warning, and a broker the pops cannot
reach is reported at most every 30 s, then once when it answers again; the command waits 1 s
after each pop that reached no broker instead of polling in a tight loop. `HighLevelConsumer` gains
`lastPopError()`, and its `ack()` and `nack()` take an optional error. The help of `--batch` and
`--conflation` now says what they do.

**Laravel: the lease renewal helper finds the application's autoloader.** It walked up from
the package's `Queen.php` to the first `vendor/autoload.php`. When Composer links the package in
from a path repository, PHP reports that file by the link's target, outside the application, so
the helper failed to start ("Unable to locate Composer autoload.php") or loaded the package's own
development autoloader. It now asks Composer for the autoloader that loaded the package, and
walks up only when Composer registers none.

**Laravel: the supervisor checks the settings its workers run with, on every connection.** A
worker's queue connection starts from `config/queen.php` whatever its name, but the supervisor
and the dashboard read `config/queen.php` only under the connection named `queen`. A pool on a
second connection, for example one that sets only `'prefetch' => 'auto'`, was refused for a
missing `lease_renewal` that its workers did have, and the dashboard showed lease renewal off for
pools whose workers renewed. Both now start every queen connection from `config/queen.php`, as
the workers do.

**Go client: `Each()` with `AutoAck(false)` stops at the first handler error.** The loop handed
the rest of the popped batch to the handler after a message failed and kept only the last
message's error. When a later message succeeded, `Execute` returned nil, and if the handler had
acked that later message, the ack moved the cursor past the failed one, so it was never delivered
again and never reached the dead-letter queue. Now the first error stops the worker at that
message, the rest of the batch does not reach the handler, and the error comes back out of
`Execute`, as the transaction tutorial says. The loop still sends no nack under `AutoAck(false)`:
the failed message comes back when its lease expires, which spends no retry, so nack it in the
handler when it should count against the queue's `RetryLimit`.

**Go client: `Renew()` reports a lease the broker did not extend.** `POST
/api/v1/lease/:leaseId/extend` answers 200 with `success: false` and `renewed: 0` when the lease
expired, was released by an ack or nack, or never existed, and `Renew()` reported every 200 as
`Success: true`. It now reads `success` from the body and fills `Error` when it is false, as the
JavaScript and Rust clients do.

**Go client: a pop the broker refuses no longer loops.** The consume loop answered any error
other than a network error, a 403 or a 429 by popping again at once: a token the broker refused
(401) brought 14,546 pops in one second, and a conflating consumer without a group 2,937 pops in
half a second, with nothing shown unless logging was on. A 4xx now stops the worker and comes back
out of `Execute`, as a 403 does; a 5xx waits a second before the next pop, as a network error
does.

**Go client: a nack in `Each()` mode skips only its own partition.** A multi-partition pop claims
several partitions under one lease, and a nack releases only the failed message's partition. With
`AutoAck`, the loop dropped the whole rest of the pop after a nack, so the other partitions'
messages stayed leased and came back only when the lease expired: live, with a 30 s lease, partition
B was not handled before the run ended. It now skips only the later messages of the failed
partition, which the broker redelivers, and handles the others at once. With `AutoAck(false)` a
handler error still stops the consumer at that message.

**Go client: `CommitOnDelivery()` is the pop's option, and `AutoAck()` no longer affects a pop.**
Behaviour change. On a pop, the broker's `autoAck=true` moves the consumer group's cursor past the
messages as it hands them out: no lease, nothing to ack, at-most-once. `AutoAck()` sent it from
`Pop` and `PopResult`, while on `Consume` it is the ack the loop sends after the handler, which
never reaches the wire. The pop now has its own option: `CommitOnDelivery(true)` sends
`autoAck=true` from `Pop` and `PopResult`, and `AutoAck()` applies to `Consume` and `ConsumeBatch`
only. A pop after `AutoAck(true)` is now leased, so it is at-least-once (a message can come again,
none is lost) and you ack what it returns; call `CommitOnDelivery(true)` there to keep committing at
delivery. `Consume` and `ConsumeBatch` refuse a builder with `CommitOnDelivery(true)`: `Execute`
returns `ErrCommitOnDeliveryConsume` before any request. `EphemeralPopOptions` gains
`CommitOnDelivery`, and its `AutoAck` stays as a deprecated alias with the same effect.

**CLI: `queenctl pop --commit-on-delivery`.** The broker moves the group's cursor past the messages
as it hands them out: no lease, nothing to ack, at-most-once. `ephemeral pop` takes the same flag.
On both, `--auto-ack` is now a hidden, deprecated alias with the same effect, and using it prints a
deprecation line on stderr. `tail --auto-ack` keeps its name, and its help now says what it does:
the client acks each message after printing it (the help said "ack server-side"). `bench` still
drains with pops that commit on delivery. Until a client-go release has `CommitOnDelivery`, queenctl
calls `CommitOnDelivery` or `AutoAck`, whichever the client-go it is built with has, so the
`go install` build (client-go v2.0.0) and the workspace build both send `autoAck=true`.

**Rust client: a nack in `consume()` skips only its own partition.** A multi-partition pop claims
several partitions under one lease, and a nack releases only the failed message's partition. With
`auto_ack` on, the loop dropped the whole rest of the pop after a nack, so the other partitions'
messages stayed leased and came back only when the lease expired: live, with a 30 s lease,
partition B was not handled before the run ended. It now skips only the later messages of the failed
partition, which the broker redelivers, and handles the others at once. With `auto_ack(false)` the
loop still abandons the rest of the pop after a failure.

**Rust client: the consume loop logs a refused ack.** The broker refuses an ack, for example one
sent after the lease expired, with HTTP 200 and `success: false` on the item. The loop read only the
transport result, so a handler that outlived its lease had its ack refused without a trace.
`consume()` and `consume_batch()` now read the verdict and log a refused ack or nack at error level,
with the broker's reason and, for a batch, how many items it refused. The loop still carries on, and
`ConsumeSummary` still counts what the loop decided, not what the broker accepted. Under
`auto_ack(false)` the loop sends no nack for a handler that returns `Err`, by design, but it logged
"nacked" all the same, and `consume_batch()` logged nothing. Both now log the handler's error as a
warning that says the message was not nacked.

**Rust client: `commit_on_delivery()` replaces `pop_auto_ack()`.** On a pop, the broker's
`autoAck=true` moves the consumer group's cursor past the messages as it hands them out: no lease,
nothing to ack, at-most-once. The builder now has an option for it, `commit_on_delivery(true)`,
which `pop()` and `pop_result()` send; without it both stay leased, as before. `pop_auto_ack()` is
deprecated in favour of `commit_on_delivery(true).pop()` and sends the same request. `consume()` and
`consume_batch()` refuse a builder with `commit_on_delivery(true)` with `Error::Invalid` before any
request, since a consumer always leases its messages; `auto_ack()` stays the loop's ack after the
handler and never reaches the wire. The ephemeral pop builder gains `commit_on_delivery()` too, and
its `auto_ack()` is a deprecated alias with the same effect.

**JavaScript client: a nack in `each()` mode skips only its own partition.** A multi-partition
pop claims several partitions under one lease, and a nack releases only the failed message's
partition. The loop dropped the whole rest of the pop after a nack, so the other partitions'
messages stayed leased and came back only when the lease expired: live, with a 6 s lease, the two
messages of partition B waited 6 s after a message of partition A failed. It now skips only the
later messages of the failed partition, which the broker redelivers, and handles the others at
once.

**JavaScript client: the pop defaults say what `pop()` does.** `POP_DEFAULTS` and the README said
a pop returns at once and that `autoAck(true)` commits it at delivery. Neither was ever true: a pop
long-polls for `timeoutMillis` unless `.wait(false)`, and `autoAck()` never reaches the broker,
because it is the ack `consume()` sends after the handler. A pop commits at delivery only with the
new `commitOnDelivery()`, below. `POP_DEFAULTS.wait` is now `true`, the README and the builder's
comments say so, and tests pin both. No behaviour changes.

**JavaScript client: `commitOnDelivery()` commits a pop at delivery.** The broker can move a
consumer group's cursor past the messages as it hands them out, with no lease and nothing to ack,
but this client could not ask for it: `autoAck(true)` on a pop never reached the broker.
`queen.queue(q).group(g).commitOnDelivery().pop()`, and `popResult()`, now send `autoAck=true`, the
parameter every 2.x broker reads, and nothing else; the messages come back with an empty `leaseId`.
This is at-most-once: a crash after the pop loses the messages. `consume()` always leases, so it
throws before any request when the builder has `commitOnDelivery()`. `autoAck()` stays the ack
`consume()` sends after the handler and still never reaches the broker. The broker refuses
`commitOnDelivery()` together with `conflation()` (400), and `pop()` raises that 400. On ephemeral
queues, `queen.ephemeral.pop()` takes `commitOnDelivery: true`; its `autoAck` option, which meant
the same, still works and is deprecated, and passing both throws.

**JavaScript client: `admin.moveMessageToDLQ()` and `admin.clearQueue()` are deprecated and
throw.** They sent `POST /api/v1/messages/:partitionId/:transactionId/dlq` and
`DELETE /api/v1/queues/:name/clear`, routes the 2.x broker does not have, so every call failed
with a 404 `no_such_route` and the message `not found`. Both now reject before any request and
name the way that works. For a dead letter, ack the message with the `dlq` status,
`queen.ack(message, 'dlq', { group })`, while its consumer group holds the lease. To skip what is
queued, seek each consumer group to the end, `queen.admin.seekConsumerGroup(group, queue,
{ toEnd: true })`; a pop without a group reads as the group `__QUEUE_MODE__`.

## PHP client 2.2.0 - 2026-10-06

**Laravel: `prefetch` `'auto'`.** Each worker sizes its next pop from how long its jobs take, so a
batch holds about 250 ms of work: short jobs get batches of up to 16, a job of a second or more
gets one per pop. A job is timed from the moment the worker hands it to Laravel to its next pop,
its ACK included, so an empty long poll or the worker's sleep never counts as work. A queue starts
at one job per pop and doubles only after two pops in a row came back full; a short pop sets the
next one to what the queue had, an empty pop to one job, and slower jobs shrink it at once. A
worker that serves several queues sizes each on its own. It runs in the worker, so it needs no
supervisor and applies at once, and like any prefetch above 1 it needs `lease_renewal`. On the
Linux server, 32 workers ran 2,003 jobs/s of 10 ms jobs with it against 1,548 at prefetch 1, and
2,867 with `ack_async` and `pop_ahead`, as many as a fixed prefetch of 4 with a third of its pops
(`benchmark-queen/2026-10-05-laravel-auto-prefetch`). The Laravel guide now describes three
profiles, safe, balanced and fast; balanced and fast long-poll (`block_for` 1) with the pool's
`sleep` at 0, since workers asleep when a burst began left it to the few awake ones and pushed the
p99 at 500 jobs/s to 338 ms in one run of five. The dashboard shows `'auto'`, and its advice for
short jobs suggests it.

## PHP client 2.1.0, supervisor 0.7.0 - 2026-10-05

PHP client 2.1.0 pins supervisor 0.7.0: the Rust master reads `lease_service` from its
configuration and handles the `queue:restart` exit below, so 0.6.0 cannot run with it.

**Breaking, Laravel: `config/queen.php` reads 20 environment variables instead of 95.** Only the
values that differ between environments or deployments still read the environment: the broker
URLs and token, the queue, consumer group and partitions, the supervisor's read token, state
directory and its remote status, prefork and coordination switches, the dashboard's switch, path,
domain and console URL, the alert mail, the metrics switch and token, and the supervisor binary's
install path and mirror. Every other setting is a plain value in the published file, with the
default it had: set it there, or add your own `env()` call where a value must differ per
environment. `QUEEN_SUPERVISOR_LEASE_SERVICE`, which the Rust master read from its environment,
is the config key `supervisor.lease_service` (default on), so the dashboard now shows the master's
value instead of the web host's. `supervisor.remote_status.key` is no longer required: it defaults
to a slug of `APP_NAME` and `APP_ENV`. An application that publishes its own `config/queen.php`
keeps every variable that file reads; the configuration reference maps each removed variable to its
key.

**Laravel prefork: `php artisan queue:restart` runs the deployed code.** A forked worker stops at
`queue:restart` like a spawned one, but its replacement was forked from the fork server, which
still held the code it booted, so the workers kept the old code until the master restarted. A
worker that stops for the restart signal now says so (Laravel 12 gives the reason), and both
engines then start a new fork server: the workers forked from then on boot nothing and run the
code on disk. The old server stays open until the last worker it forked exits. Changes to
`config/queen.php` still need `queen:supervisor terminate`, as before.

**Laravel prefork: a fork server that is not safe to fork is not used.** `fork()` copies only the
calling thread, so a booted application that runs another thread (a gRPC or Kafka extension, an
APM agent) would give every worker a broken copy. On Linux the fork server now refuses to serve
when another thread outlives a 5-second grace (libcurl's resolver thread ends within it), names
the threads, and the master spawns its workers instead. It warns about sockets the boot left open,
which every forked worker would share, and it releases the database and Redis connections, log
channels and mailers the boot opened before the first fork, not in each child only.

## 2.0.1 - 2026-10-05

Everything in 2.0.1-beta below, and:

**Traces live on disk.** A trace was a row in every node's RAM, about one and a half times its
stored size, until `QUEEN_RAFT_TRACE_RETENTION_S` (7 days by default) expired it, so a client
that traced every message with a few KB of data grew every node by hundreds of MB an hour. Each
node now keeps its traces in a second LMDB environment, `<QUEEN_RAFT_DIR>/traces/`, whose pages
are page cache the kernel can drop: 100,000 traces of 5 KB add 56 MB to a node's process memory
instead of 810 MB. The trace routes, their fields, order and pagination are unchanged, and traces
written before the upgrade stay readable until they expire. Expiry runs in bounded steps (512
traces each, a few ms at most) instead of one step for everything past the cutoff (139,000
traces took 1.24 s on 2026-10-05). New gauges: `queen_raft_traces_map_bytes` and
`queen_raft_traces_stored`.

**The upgrade is one-way.** A cluster writes traces to disk once every member runs 2.0.1: the
leader then raises the cluster version to 4, and from then on a 2.0.1-beta or older binary
refuses to start on the data directory. On a cluster at version 4, a learner can be added only
once its node is running and answering (as `QUEEN_RAFT_JOIN` already requires).

**A trace with a very long transaction id or name no longer stops the cluster.** Its store key
went past LMDB's 511-byte limit and every node stopped applying at that entry. The request is now
refused with a 400 (`name_too_long`).

**The dashboard shows the memory a node holds.** A node's memory meter showed its resident set,
which counts the file pages the process maps: the store's LMDB file is read whole at boot, so a
node looked about 1 GB fuller than it was. The meter now shows the process's anonymous memory,
the part that can run a node out of memory, reported as `anonBytes` in `/api/v1/raft/members`;
an older broker still shows its resident set.

## 2.0.1-beta - 2026-10-05

**A new partition's first message reaches every consumer group.** On a queue read by two or more
consumer groups, a group that took a new partition into the leader's memory before the partition's
first message was written could miss that message until the partition's next message or a change
of leader. A second group loading the partition a moment later raised the shared tail and armed
only its own cursor, so the append's own wake-up found nothing left to do. A group's load now arms
every group already watching the partition whenever it finds the tail moved.

**The leader keeps only the consumer state that is in use.** The consumption engine kept every
(consumer group, partition) pair it had served in the leader's memory, about 1 KB each, until the
partition or the group was deleted, and a partition is deleted only after `PARTITION_CLEANUP_DAYS`
(30 by default). A workload that keeps opening partitions, one per conversation or entity, grew the
leader without bound. A whole-queue group's pair that holds nothing (no lease, nothing to deliver,
no hold, every change durable) and stays so for `QUEEN_CONSUME_IDLE_UNLOAD_S` (default 600; `0`
keeps every pair) now leaves memory. The next message on its partition loads it back from its
cursor row, as a new leader does. New gauges: `queen_consume_engine{kind="groups"|"parts"|"partitions"}`
and `queen_consume_parts_total{kind="loaded"|"unloaded"}`.

## 2.0.0 - 2026-10-03

**The PostgreSQL storage class is removed.** Queen 2.0 has one storage class, its own replicated
log, and there is no database to run beside it. Each node keeps its whole state in one data
directory, `QUEEN_RAFT_DIR` (default `/var/lib/queen/raft`): the queue logs that hold the payloads,
an embedded ordered store for everything the broker looks up, and, on a cluster, the raft state.
A single node answers a write once it is fsynced to its own disk. A cluster of three or five voters
(`QUEEN_RAFT_REPLICATOR=openraft`, `QUEEN_RAFT_NODE_ID`, `QUEEN_RAFT_PEERS`, `QUEEN_RAFT_TOKEN`)
has one leader order every write, answers it once a majority of the voters have it on disk, and
keeps serving through the loss of a minority. `QUEEN_STORAGE` is gone with the choice it made.
There is no in-place upgrade from 1.x: a 2.0 broker does not read a 1.x database and messages do
not carry over, so a move is a cutover. Start 2.0 beside the old deployment, re-apply the queue
configuration, move the producers, drain the old deployment and move the consumers.

**The SQS facade is removed.** It is not in the image or the binary any more, and `QUEEN_SQS_*`
is not read.

**The S3 sink runs inside the broker, on every node.** `QUEEN_S3_EMBEDDED=true` starts the
data-lake sink in the broker process, on a runtime of its own (`QUEEN_S3_THREADS`, default a
quarter of the cores, 1 or 2); there is no `queen-s3` binary, no child process, no `QUEEN_S3_BIN`
and no `/healthz` or `/metrics` listener of its own, and `QUEEN_S3_LISTEN` and
`QUEEN_S3_LOG_FORMAT` are named in a warning and ignored. Every node runs it with the same
configuration, one sink per broker tenant (below). Per queue, a lease in the key/value store
(`s3:<sink>:<queue>:lease`, TTL `QUEEN_S3_LEASE_TTL_MS`, default 30 s, refreshed every third of
it) picks the node that writes the queue, and every window intent and commit carries that lease as
a required conditional write, so a node that lost the queue cannot commit. A node claims a free
queue after waiting one second for every queue it already runs or is claiming, plus a jitter under
200 ms, at most half the lease TTL, so the least-loaded node claims first and the queues spread
over the nodes; a claim that fails for a passing reason (no leader yet) is retried within about a
second rather than a TTL. Queues are then rebalanced: each sink counts the nodes running it through
TTL'd presence rows, and a node holding more than `ceil(queues / nodes)` gives one queue back at a
time (drained and released as at a SIGTERM) to a node below its share, so pods started one after
another still end up sharing the queues, and nothing moves in a steady state. A node stopped
with SIGTERM gives its queues back at once; one that dies loses them when its leases expire, and
one that comes back under the same `QUEEN_S3_INSTANCE` (by default `node-<id>@<host>`) takes its
own back at once. Each node reads its own applied copy of the
log, followers included, and closes a window against its own `safeTime`; the lease refresh is a
log entry, so `safeTime` keeps moving on a broker nothing writes to and the last window before a
quiet spell still closes by age. The sink needs no `QUEEN_URL`
and no token, and the proxy's key/value carve-out for it is gone. Both record envelopes are the
1.5.0 formats; every key gains a `tenant=` level, the manifest a `tenant` field, the Parquet
footer a `queen.tenant` pair, and the position checkpoint each partition's id, so a 2.0 sink is
pointed at a new prefix or bucket. A record's `ts` is the stamp of the append that wrote it, and across the lake
`(partition, offset, ts)` is unique, since a partition deleted and created again starts again at
offset 0. A missing or bad value of one of the sink's variables fails the broker's boot, naming
the variable; a bucket that does not answer delays the sink and nothing else. The window buffers are the broker's memory
now, so `QUEEN_S3_MEMORY_MB` defaults to 512 instead of 1024, and an out-of-memory takes the
broker: size the container for both. On SIGTERM the sink stops reading at once, finishes the
window it is committing and gives its leases back while the broker hands off and drains, and the
broker waits for it up to `QUEEN_S3_SHUTDOWN_GRACE_MS` (30 s) counted from the signal. Its state is the `s3` block of `GET /status`, and its
`queen_s3_*` families are on `/metrics/prometheus`. `queen_s3_lag_seconds` is the node's
`safeTime` minus the stamp the queue's lake is complete through (`completeThrough` in the status),
so a queue read to its end and idle lags by about the guard and one discovery interval instead of
growing; a node exports a queue's gauges only while it runs the queue. The commit pointer's `tEnd` is now an
ISO-8601 timestamp: 1.5.0 wrote integer microseconds, which the retention hold could not read,
so `retentionSinkHold` always sat at its cap; it now follows the sink. Running it is
[deploy/s3](https://queenmq.com/integrations/s3/); what it writes is
[reference/s3](https://queenmq.com/integrations/s3/).

**Every broker tenant can have an S3 sink and a bucket of its own.** The default tenant's sink is
configured by `QUEEN_S3_*` and turned on by `QUEEN_S3_QUEUES`: without it the default tenant has no
sink, and another per-tenant `QUEEN_S3_*` variable set without it fails the boot. Every other
tenant's is set by the control plane: `PUT /api/cp/clusters/:slug/s3` with the tenant's endpoint,
region, bucket, access key and queues, any other per-tenant setting, and `secretKey`, which is
stored only sealed with the cell's `QUEEN_ENCRYPTION_KEY` (the same on every node; a cell without
one refuses the secret). `GET` answers it without the secret, `DELETE` removes it, and the
tenant's purge and delete remove it too. The routes are offered only where the node runs the sink.
Every node reads those rows every 5 seconds and runs a tenant's sink while its row is enabled and
its cluster's status, combined with its tenant's, is `active` or `push_blocked`; a write that moves
`updatedAt`, which every PUT with a secret does, rebuilds the sink once the old one has drained. The memory budget, threads, fetch
concurrency, discovery interval, guard, lease TTL, multipart threshold, checkpoint cadence and
instance name stay node-wide, in the environment, shared by every tenant's sink, and a tenant's
settings cannot name them. Keys start with the tenant, `<prefix>/tenant=<id>/queue=<name>/…`, and
the sidecars sit under `<prefix>/_queen/tenant=<id>/queue=<name>/`, so two tenants never share a
key even in one bucket and prefix. A tenant's leases and commit pointers are in its own key/value
store, where its queues' retention hold reads them. The `s3` block of `GET /status` is
`{mode, phase, threads, controlPlane, sinks}`, one entry per tenant with its `source` (`env` or
`cp`), and a control-plane tenant's `queen_s3_*` series carry a `tenant` label; the node renders
each family once, and a removed tenant's series go with it.

**`POST /api/v1/partitions/changed` answers a sound `safeTime`.** It is the greatest record stamp
the answering node has applied: every record stamped at or below it is already readable on that
node, so a time window that ends there is complete. 1.x derived it from PostgreSQL's open
transactions, with a fixed fallback floor (`safeTimeDegraded`, now always `false`), and the 2.0
betas answered the node's wall clock, which a record planned before the answer and applied after
it could fall below. It is per node, and it moves only when an entry applies, so an idle node
answers the same value until the next write. Pages run in partition creation order with and
without `since`, under an opaque cursor that means the same in both, and a complete pass reads
each of the queue's partitions once, where every page used to sort the whole queue; a cursor of
the old shapes (`n|…`, `t|…`) is `BAD_CURSOR`. Every partition carries `id`, its uuid, which is
new when a partition is deleted and created again under the same name. `since` is read to the
microsecond, and one that is not a timestamp is a `400`, as in 1.x, where the 2.0 betas read it
as absent and listed everything.

**The standalone proxy is removed.** The proxy runs inside the broker process with
`QUEEN_PROXY_EMBEDDED=true`, fronting `PORT`, or its own `QUEEN_PROXY_PORT` while `PORT` stays the
internal broker port. Its tenants, clusters, users, API-key hashes, plans and usage live in the
broker's replicated key/value store under a system tenant, so the proxy database, `PXDB_*` and the
proxy's SQL migrations are gone, along with the `queen-proxy` binary and image. A first tenant can
come from `QUEEN_PROXY_BOOTSTRAP_*` at boot, and the rest through the control plane under
`/api/cp/*`, guarded by `QUEEN_PROXY_CP_TOKEN`.

**The Kafka facade runs in-process.** `QUEEN_KAFKA_EMBEDDED=true` starts it inside the broker,
calling the broker's router without a socket; there is no child process and no `QUEEN_KAFKA_BIN`.
Committed offsets are Queen consumer-group positions by default (`QUEEN_KAFKA_OFFSET_STORE`,
`positions` or `kv`).

**Environment variables removed.** Every PostgreSQL variable (`PG_*`, `DB_POOL_SIZE`,
`PG_USE_SSL`, `PG_SSL_REJECT_UNAUTHORIZED`, `PG_SSL_ROOT_CERT`), the disk spool (`FILE_BUFFER_*`),
the broker mesh (`QUEEN_MESH_*`, `QUEEN_UDP_*`, `QUEEN_SYNC_*`), the hot-list, fusion, ack-fusion,
pop-fusion and admission knobs, `QUEEN_APPLY_SCHEMA`, `RETENTION_PARALLELISM`, the statistics
refresh intervals, `QUEEN_STORAGE`, `QUEEN_SQS_*`, `QUEEN_S3_BIN` and `PXDB_*`. Some 1.x names stay
because the 2.0 engine reads them: `RETENTION_INTERVAL`, `RETENTION_BATCH_SIZE`,
`PARTITION_CLEANUP_DAYS`, `QUEEN_PARTITION_CLEANUP_ENABLED`, `METRICS_FLUSH_MS`,
`QUEEN_SWEEPER_BACKOFF_MIN_MS`, `QUEEN_SWEEPER_BACKOFF_MAX_MS`,
`QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS`, `QUEEN_SWEEPER_MAX_ATTEMPTS`, `QUEEN_STMT_TIMEOUT_MS`,
`DEFAULT_TIMEOUT`, `POP_DEFAULT_TIMEOUT_MS` and `DEFAULT_SUBSCRIPTION_MODE`.

**Routes and answers.** `GET /api/v1/analytics/postgres-stats` and the broker-to-broker
`/internal/api/*` routes are gone, and `/metrics` no longer carries a `database` block. A push item
answers `queued`, `duplicate` or `error`: with no spool there is no `buffered`. When a node cannot
take a write the request fails with `503` and `Retry-After`, and a full disk answers `507`.
`POST /api/v1/stats/refresh` still answers `200` and does nothing, because counters are kept as
entries apply.

**Maintenance mode is removed.** `GET`/`POST /api/v1/system/maintenance`,
`GET`/`POST /api/v1/system/maintenance/pop` and `GET /api/v1/status/buffers` answer `404`. No push
or DLQ replay is refused with a maintenance `503`, and no pop answers `{"messages":[],"paused":true}`.
The embedded Rust API loses `Broker::set_push_maintenance`. The SDKs drop their maintenance calls
(`get`/`setMaintenanceMode` and `get`/`setPopMaintenanceMode` in JS and PHP, the same four in Go,
`get`/`set_maintenance_mode` and `get`/`set_pop_maintenance_mode` in Python, and `Admin::maintenance`,
`set_maintenance`, `pop_maintenance` and `set_pop_maintenance` in Rust) along with their handling
of a paused pop; `queenctl maintenance` and the dashboard's maintenance switches are gone. The kv,
timers and ephemeral kill switches and tenant quotas stay.

**The embedded Rust API boots on a data directory.** `BrokerConfig::new().raft(dir)`, with
`raft_disk_pct(high, low)` and `stmt_timeout_ms`; `pg()`, `pg_use_ssl`,
`pg_ssl_reject_unauthorized`, `pool_size`, `apply_schema`, `spool_dir`, `retention`,
`stats_refresh`, `system_metrics` and `log_reports` are removed. `StartError` has only `Config`, and
`Broker::shutdown()` returns nothing.

**One image, one binary.** `ghcr.io/queen-mq/queen` carries the broker, with the proxy, the Kafka
facade and the S3 sink linked in, the dashboard and `queenctl`; the `queen-kafka`, `queen-sqs` and
`queen-s3` binaries and the PostgreSQL client tools are gone. The dashboard is raft-only: the
Postgres stats panel, the database pool and the disk-spool cards are removed, and the replicated
log's status takes their place.

**Every client is 2.0.0.** The JavaScript, Python and Rust packages (`queen-mq` on npm, PyPI and
crates.io), the PHP client, the Go module, the C++ header and `queenctl` move to 2.0.0, with the
Queen 2 broker. Go users change an import path: a major version above 1 is part of a Go module's
path, so the client is now `github.com/smartpricing/queen/clients/client-go/v2` and the CLI
installs with `go install github.com/smartpricing/queen/clients/client-cli/v2/cmd/queenctl@latest`.
The tags keep their directory prefix (`clients/client-go/v2.0.0`, `clients/client-cli/v2.0.0`), and
the client's has to be pushed first, since the CLI requires it. `queen-protocol` stays at 1.3.0.

**JavaScript, Python, Go, C++ and PHP clients, `queenctl`: every ACK of a transaction names its own
lease.** The transaction builders sent a message's lease only in `requiredLeases`. A Queen 2 broker
fences each ACK with the `leaseId` its operation carries, and lends it the one in `requiredLeases`
only when the bundle names a single lease, so in a bundle that acked messages of two leases (two
pops, two queues, a Laravel worker handing back two batches) every ACK went unfenced: it could
complete a message that another consumer had taken after the lease expired, and commit the rest
of the bundle with it. Each ACK operation now carries `leaseId`, and the broker refuses the whole
transaction once any of those leases is no longer the caller's. `requiredLeases` is still sent,
and 1.x brokers already read the operation's lease first. `queenctl tx` sent no lease at all: it
dropped the bundle file's `requiredLeases`. It now sends each ACK's own `leaseId`, lends an ACK
without one the lease `requiredLeases` names when it names exactly one, and exits 1 before sending
anything when an ACK without a lease sits in a bundle that names several. The Rust client already
named the lease on each ACK. PHP 1.9.0 shipped without this fix, its two-batch hand-back included.

**Laravel dashboard: the Jobs page reads with a read-only token.** It read the job metrics through
`POST /api/v1/kv`, which takes read-write access, so with a `read_bearer_token` that may only read
the page showed the metrics as unavailable on any broker that checks tokens. It now reads
`POST /api/v1/resources/kv/list`, the read route the console's KV browser uses (broker 1.6.0 and
later), through the new `Admin::listKv()`. It falls back to `getPrefix` when the broker has no such
route (404) and when the credential may not read but may use the KV surface (403), such as a
proxy API key with `consume` and no `read` scope.

**The partitions sunflower names its seeds.** Each queue owns a wedge of the flower, and hovering a
seed lights its queue and shows the queue, the partition, its pending and its lag. The per-partition
figures come from a new read-only route, `GET /api/v1/resources/partitions?queue=&limit=`: the
partitions holding the most pending (default 610), each with `pending`, `processing` and
`lagSeconds`, the age of the oldest message its slowest reader has not consumed (`null` when caught
up). The Overview asks for it only while it draws one seed per partition.

**The broker in the top bar.** For operators, every page's top bar shows the cell's CPU, memory,
disk and Raft state, each figure its fullest node's, over a hairline of how full it is; a click
opens every node. Colour appears only past a line: 90% of the CPUs a node may use, 80% and 90% of
its memory limit, and the node's own disk write gate. `GET /api/v1/raft/status` and each member of
`GET /api/v1/raft/members` gain `host`, the figures behind it.

**The sidebar collapses to a rail of icons** (the button at the top left of the top bar, or `⌘\`),
remembered per browser. Members and Users share one row: an operator switches between the acting
cluster's members and every account on the cell from the Members page.

## PHP client and Laravel supervisor

**Laravel: up to 1,024 stripes per queue.** A stripe runs one job at a time, so the 64 stripes a
queue could have capped its ordinary jobs at 64 busy workers. `QUEEN_PARTITIONS` now takes 1 to
1,024 (64 by default, unchanged), and a pop still asks for at most the 64 partitions the broker
checks out per call: the broker serves the next ready ones in turn. On the Linux benchmark server,
128 workers drained 50,000 jobs at 9,257 jobs/s with 256 stripes against 4,722 with 64, 64 workers
20% faster, and the p95 at 500 jobs/s fell from 23.4 to 16.7 ms, with no measured cost at 50 jobs/s
(`benchmark-queen/2026-10-02-laravel-partition-stripes`). Event-driven scaling watches up to 1,024
stripes per connection in one fetch; the regular poll finds jobs on the others.

**Laravel: a crashed worker's prefetched jobs keep their attempt.** With `prefetch` above 1 and
lease renewal, a worker that dies without a shutdown (SIGKILL, the kernel's OOM killer, a PHP
fatal error such as `memory_limit`) no longer leaves its unstarted jobs to lease expiry, which
charged each one an attempt: with `tries = 1` they failed without running, and a job that crashed
its worker every time used up the attempts of the jobs prefetched with it. The worker now journals
the transaction its shutdown would send, and rewrites one small record of it before each ACK or
release. Whatever renews its lease sends that transaction once the worker has exited holding the
lease, while the lease is still the worker's: the Rust supervisor's master on Linux, whose journal
sits next to its lease socket, or the worker's PHP lease helper, whose journal sits in a private
temporary directory. Each unstarted job is completed and copied to its partition with the runs so
far, the job that was running counts its run, and the jobs return at once instead of after
`retry_after`. Every ACK in it names the lease, so the broker refuses the whole transaction once
the lease is no longer the worker's. Still charged one attempt: a lost node, a crash that takes
the lease helper with the worker, and a batch popped ahead whose answer the worker had not read.
The dashboard's Configuration page, `queen:supervise` and the Rust supervisor (through
`queen:supervisor-config`) now warn at start about a pool with `tries` 1 on a connection with
`prefetch` above 1 or `pop_ahead`.

**Laravel: a job handed back unstarted keeps its attempt.** A worker that stopped with a
prefetched tail (`--memory`, `--max-jobs`, a deploy, Laravel's timeout handler), or that could
not track a batch popped ahead, handed the jobs back with a `retry` ACK. The broker counts the
next pop of such a position as a redelivery, so each hand-back charged an attempt to jobs that
never ran, and with `tries = 1` they failed with `MaxAttemptsExceeded` on their next delivery.
The hand-back is now one transaction that completes each unstarted job and pushes a copy to its
partition with the runs so far, as a release does. A crash is handled by the entry above.

**Laravel: a forked child no longer kills its worker.** With the PHP lease-renewal helper, a
child forked by a job (Laravel's fork concurrency driver, `pcntl_fork()`) stopped the parent's
helper when it exited, and the parent's watchdog then SIGKILLed the parent mid-job; the child
was also fenced as soon as a subprocess of its own ended.

**PHP client: a detached request is written before the next job runs.** With `ack_async` or
`pop_ahead`, a request on a new connection (a worker's first detached request, or one after the
broker closed an idle keep-alive connection) was only started when it was sent: cURL wrote it
when the request was settled, which with `prefetch` above 1 is after the next job. A hard kill
during that job (shutdown grace exceeded, out of memory, node loss) lost the ACK, and the
acknowledged job ran again when its lease expired. `postDetached()` and `getDetached()` now
return once the whole request is written, the connection included; on the Guzzle transport
(behind a proxy, or with `QUEEN_SDK_HTTP_TRANSPORT=guzzle`) this holds for a request with a
body, the ACK. A request that cannot be written within the 5-second connect timeout throws at
once, and the Laravel queue then acknowledges synchronously. A pop sent ahead, which carries no
body, is waited for at most 250 ms, so a slow or dead backend does not hold up the next job.

**PHP client: a process forked by a job exits.** libcurl's resolver threads do not survive
`fork()`, and recent libcurl keeps them alive for a moment after each name resolution (2 seconds
on the multi handle that carries detached requests). A child forked in that window, by Laravel's
fork concurrency driver or by `pcntl_fork()` in a job, waited for them forever when its exit
freed the cURL handles it inherited, so the job hung until its timeout and left the child stuck.
The cURL transport now sets `CURLOPT_QUICK_EXIT` and shares one DNS cache between its handles,
so detached requests reuse the synchronous handle's resolution and never start a resolver
thread of their own.

**Laravel: job metrics and tag records make one bounded attempt.** They are written from every
worker's job events and on `WorkerStopping`, and used the queue's ordinary client, whose
retries held the worker on a slow or rate-limiting broker and could spend the shutdown grace
before the prefetched tail was handed back. They now use one 2-second attempt.

**Laravel dashboard: retry a failed job in one click.** The failed-job page has a *Retry now*
button. It runs `queue:retry` for that job, so the broker's dead-letter entry and the
`failed_jobs` row stay in step, and the page still shows the command for a terminal. Forgetting,
flushing and pruning stay with Laravel's commands. An exception during the retry is reported to
the application's log, and the page shows only its class: its message can quote the job's
payload. The id `all` is refused, since `queue:retry all` retries every failed job.

**Laravel dashboard: what each queue holds now.** The Workload page shows, for every supervised
queue, the jobs waiting and running and how long the oldest unfinished job has waited, from one
broker read per queue, cached for 5 seconds. `QUEEN_DASHBOARD_CONSOLE_URL` links each queue to
the Queen console, which lists the messages themselves. An invalid value is reported to the
application's log and turns the links off; it never stops the application or its workers.

**Laravel dashboard: the Configuration page is a tuning guide.** It shows every resolved setting
of the connection and the supervisor with its environment variable, each pool as the running
supervisor published it, and advice from the supervisor's state and the job metrics: job classes
whose longest run exceeds `shutdown_grace`, one worker for several queues, short jobs on
`prefetch` 1, prefork off, a lease helper per worker, polling instead of event-driven scaling,
and pools that could run a job twice. A setting that can hold a credential shows only whether it
is set. Job metrics now record each class's longest run.

**Laravel: a rejected broker URL no longer leaks its password.** The supervisor configuration
printed an invalid broker URL, credentials included, into logs and error pages; it now redacts
them.

**Supervisors (both engines).** A worker that ran at least `stable_after` and exits non-zero
(`queue:work` exits 12 at `--memory`, a job timeout kills the worker) is restarted at once: it
no longer holds its pool at a single probe for `stable_after`. On stop, the workers get SIGTERM
before the coordination leave and the remote status publish, which a slow broker could stretch
past the platform's stop deadline. The PHP engine's event-driven watcher can no longer freeze
the master loop on an answer that stalls mid-body, its fork server survives a failed
`pcntl_fork()`, and preforked workers honour `--quiet`. The Rust engine trusts the platform's
CA store (and `SSL_CERT_FILE`), so a broker behind a private CA works, and its lease service
fences a worker before it logs why, so a broken stderr pipe cannot skip the fence.

**Supervisors: a job timeout is not a crash.** Laravel SIGKILLs a worker whose job outlives its
timeout, and both engines counted that exit as a crash: with job timeouts shorter than
`stable_after`, a burst of them opened the restart circuit and left the pool at one probe worker
for every job on its queue, the healthy ones included. The worker now leaves a marker in the state directory's `exits/` before it dies, and
the master restarts it without backoff and without counting it. A worker that stops at `--memory`
after it handled a job counts as a clean exit too; one that stops before any job still backs off,
since its boot alone passes the limit. Every other non-zero exit counts as before, a `SIGKILL`
without a marker included (the OOM killer, a lease fence, `kill -9`).

## 1.6.0 - 2026-09-11

**A read-scoped credential could replay a dead letter through the proxy. It cannot now.**
`classify` in `queen_proxy` had a single arm for `/api/v1/messages/`, and it matched `DELETE`
only, so every other method on that prefix fell through to the reads block. The one route of
that family that writes, `POST /api/v1/messages/:partitionId/:transactionId/retry`, was
therefore classified `read`,
which is the class every user role holds and which an API key gets from the `read` scope alone.
Through the proxy, a Viewer session or a read-only key could push a message into a live
partition and delete a dead-letter record. Brokers reached directly were never affected: the
broker gives a `POST` its `read-write` default. Both replay routes are now `queue admin`, so
they need the Admin role or an admin-scoped key; they answer the storage and monthly push
blocks like a push, because they grow stored bytes like one; and a confirmed replay is metered
as one `push` message on a second sample with `reqs: 0`. The `/api/v1/messages` family is now
method-bounded as a whole, `GET` and `HEAD` read and everything else blocked, so the next write
added under that prefix cannot inherit a read either, and the generator that publishes the route
table refuses to build when a non-`GET` route classifies as `read` without a written reason.

**`/configure` merges instead of replacing, and a manifest says so explicitly.** An option a
body does not mention now keeps the value the queue already has; an explicit `null` restores
that one option's default; and a new top-level `"mode": "replace"` re-parses the whole
configuration from defaults, which is what every call did before. This is a behaviour change for
every partial body, which means Go, Rust, `queenctl queue configure` and any hand-written
request: `queenctl queue configure orders --lease-time 60` used to reset the dedup window,
retention and the dead-letter policy on its way past, and now changes the lease and nothing
else. JavaScript, Python and PHP merge a nine-option client-side default bag into every
`.config()` call, so those nine still travel; the other twelve now survive. `queenctl apply -f`
sends `mode: replace`, because a manifest is the whole desired configuration, and it now refuses
a manifest key it cannot bind rather than ignoring it, since under `replace` an ignored key is
an option silently reset. That refusal also covers the all-lowercase spellings (`leasetime:`)
that used to bind by accident. `queue configure` sends `--dlq` and `--encrypt` only when they
are typed, and as literal values, so `--dlq=false` finally disables dead-lettering. The two
sink-hold option refusals now answer `400` with an `invalid` key naming the option, where they
used to arrive as `500`. `GET /api/v1/resources/queues/:queue` gained an `options` object
carrying all 21 keys in the spellings `/configure` parses, so a read, an edit and a write
round-trip without a mapping table.

**Dead-letter replay runs on a move.** `queen.log_dlq_move_v1` claims the dead-letter row under
`FOR UPDATE`, pushes one frame through the ordinary allocator and deletes the row, in one
transaction. That closes four defects at once: the replayed frame carries the deterministic
transaction id `dlq:<row id>` instead of a fresh one per attempt, two concurrent callers
serialise on the lock, there is no "pushed but still dead-lettered" state because the push and
the delete commit together, and only the addressed consumer group's record is removed where the
old path deleted every group's. `POST /api/v1/dlq/:id/replay` is new: it addresses the
dead-letter row by id, which is what a console and a redrive want, and takes an optional
`{queue, partition}` naming a different destination, provisioned if it does not exist. Three
verdicts to read rather than infer. `moved` wrote the frame and removed the row; `duplicate`
wrote nothing **and kept the row**, because the dedup identity is a transaction id anyone who
can read the listing could derive, and a move that moved nothing must not destroy a record; a
second call answers `404 gone`. Push maintenance now refuses a replay with `503` rather than
diverting it, since a move cannot be spooled, and a `500` may carry `dlqRowRemoved: null` when
the broker never learned whether the statement committed. The response gained `result`,
`consumerGroup`, `dlqId` and `originalTransactionId` beside the `replayedAs` push result the
five SDK wrappers and `queenctl dlq retry` already parse, so those keep working unchanged.
Through the proxy, a destination override names both halves or neither: a half-named body is
refused `400 invalid_request` naming the half to add on a cell that enforces, because the
omitted half lives in the broker's row and no plan cap could answer for the pair that would be
created. A cell in shadow mode logs that one and forwards it, like every refusal there except
the size caps and the two push-block quotas.

**The console pushes.** One form behind three entry points: a Push button on Messages, a Push
message button on a queue's detail page with the queue fixed, and **Push a copy** in the message
drawer, pre-filled from the message being inspected. It renders the broker's own per-item
verdict rather than the HTTP status, so `duplicate`, `buffered` and a spool failure each read as
themselves, and a copy is stated as a new message rather than a retry: the original stays where
it is. A payload the broker could not decrypt refuses the copy instead of pushing the envelope.
Gated on `produce`.

**The console creates and edits queues.** Create from the Queues page, edit from a queue's
detail page. The editor prefills from the new 21-key `options` read and sends only the fields
that changed, with a cleared field going out as `null`, which is exactly the merge rule above
made visible. Typing the name of a queue that already exists repaints the form from that queue
rather than reporting a creation that never happened. `ttl`, `maxSize` and `retryDelay` are
shown read-only and labelled declared rather than enforced, because this broker stores and
echoes them and reads them nowhere. `priority` is a fourth option of exactly that kind, and it
stays in the editor, worded there as a label the engine does not act on.

**A KV browser, on two new read-only routes.** `GET /api/v1/resources/kv/namespaces` lists every
namespace of the tenant with its exact key count, and `POST /api/v1/resources/kv/list` returns
one keyset page of one namespace. They are not `getPrefix` under another name: the prefix may be
empty, the page carries the rows whose expiry has passed and whose sweep has not happened yet,
labelled `expired`, and both are classified `read` so a read-only token and a Viewer can browse.
The list is a `POST` because its cursor is a key, and a key in a query string is written to the
access log of every component in the path; for the same reason both routes refuse a query
string outright, including the selector that reads no parameters. Neither is metered as a KV
operation or gated by the `kv` plan feature, and both sit behind the KV kill switch and the
per-tenant read rate. The dashboard's page is read-only by design.

**A Timers page.** The four timer routes get a surface: a queue picker, a keyset list, an exact
count for a key prefix, a peek drawer that base64-decodes and, where the row says so,
zstd-decompresses the payload in the browser, and a cancel that renders the stored procedure's
verdict verbatim. An encrypted payload is named rather than guessed at, because the broker
encrypts at schedule and the envelope is outermost. The page has no ticker: every row is a
database read on a metered route, and a keyset page that refetched under the reader would move
rows while they are being read.

**A State group in the dashboard's navigation.** KV and Timers sit together between Routing and
Observability, because both read stored state that belongs to no queue. Dead Letter gains Replay
beside Purge, and `app/README.md` no longer advertises a pop inspector or calls the light theme retired.

## 1.5.3 - 2026-09-09

**A Workload page, and the four reads behind it.** The dashboard gains an analytics page that
answers who does the work and what it costs: deliveries over time, share, activity, backlog and
efficiency per namespace, task or queue, plus the footprint of each group in partitions, queues,
consumer groups and retained bytes. Any group drills down to its queues, any window can be
compared with the previous period, yesterday or last week, and a findings list at the foot of
the page says in words what the panels show.

Four read paths are new on the broker, all tenant scoped and classified Read by the proxy:
`GET /api/v1/analytics/workload` groups the metrics by namespace, task or queue and answers
window totals, a bucketed series and the current gauges in one payload; `dlq-signatures` folds
the error messages of the newest dead-letter rows into their shapes without reading a payload;
`partition-liveness` counts partitions against those written to in the last day; and
`/api/v1/analytics/retention` takes an optional `groupBy` that adds a per-queue split while its
existing answer stays byte for byte the same. On a broker that lacks the workload route the page
rolls the same figures up in the browser from queue-ops and the status reads, and says so.

## 1.5.2 - 2026-09-07

**One detail drawer across the dashboard.** Messages, dead letter and traces now open the same
drawer shell, so a payload, a transaction id and an error read the same wherever you land on
them. Two columns at desktop width, payload beside metadata, falling back to a single column
when the window is narrow. Each view keeps its own actions. Dashboard only: no server,
protocol or client change.

## 1.5.1 - 2026-09-07

**Timers reach their consumer when they fire.** Two gaps, each worth up to 30 seconds with
default settings, made a 2 second timer arrive after about 30 seconds on an idle broker,
through `POST /api/v1/timers` and the transaction rider alike. The sweeper's fire landed the
frame in the log but never told the pop path, so a consumer parked on the queue saw it only at
the next hot-list reseed (`QUEEN_HOTLIST_RESEED_MS`), and nothing rang the sweeper's own waker,
so a timer scheduled while the timer table had been empty waited out the idle backoff
(`QUEEN_SWEEPER_IDLE_MAX_SLEEP_MS`) before it fired, whatever its delay. The fire now announces
every fired segment exactly as a push announces its commit, and both schedule seams ring the
waker after their commit. The same announce now covers messages the spool drain replays after a
database outage, which stayed invisible for a reseed interval too. Affects 1.0.3 through 1.5.1;
the pinned partition route was never affected. Setting both knobs to 1000 was the workaround
and is no longer needed.

## 1.5.0 — 2026-09-04

**An S3 / data-lake sink connector.** `queen-s3` ships inside `ghcr.io/queen-mq/queen`
beside the two wire facades. It reads a queue through the broker's own API and writes
JSONL or Parquet under a Hive layout (`queue=…/dt=…/hour=…`), which DuckDB, Spark,
ClickHouse and Athena read with no loader in front. Each object is one closed time
window on PostgreSQL's clock, committed by a compare-and-set on a pointer in Queen's
key/value store, so a crash or a retry rewrites the same object rather than adding a
second copy of a row: exactly once, with no coordinator. Windows are per queue, not
per partition, so a queue with a million partitions is one object an hour instead of a
million objects. Inert until `QUEEN_S3_EMBEDDED=true`, where the broker supervises it
as a child process, or run it standalone as an ordinary client to scale out across
queues. What it writes is [reference/s3](https://queenmq.com/reference/s3); running it
is [deploy/s3](https://queenmq.com/deploy/s3).

**One read-only endpoint, and two queue options.**
`POST /api/v1/partitions/changed` answers which partitions of a queue have been written
since a given time, as one index scan, with the `safeTime` the sink closes windows
against. `retentionSinkHold` and `retentionSinkHoldMaxSeconds` floor a queue's retention
cutoffs at what its sink has committed, under a cap, so retention cannot outrun the lake.
Both are additive: nothing about push, pop or existing retention changes. On a Queen
Cloud cell the route carries the same authority as `fetch`, and a key/value batch touching
only the sink's reserved `s3:` prefix is reclassified with it, so a tenant over its storage
quota can still commit a window.

## 1.4.2 — 2026-09-04

**Dashboard timestamps name their clock.** Freshness stamps render in the viewer's own timezone and
say which one, with the UTC value alongside, in the dashboard and the account console alike.

**Dead letters clear in bulk.** The dead-letter view purges every record for one queue, optionally
narrowed to a consumer group. The queue is required at every layer, so an omitted filter can never
become a tenant-wide delete.

**Cell operators manage accounts.** A proxy-backed Users page creates accounts for the tenants
represented on the selected cell, renames them, shows their latest sign-in, and grants or removes
their roles on that cell's clusters. It cannot set the operator bit.

## 1.4.1 — 2026-09-02

**A Kafka topic can declare its own partition count.** Until now the width every topic was
advertised at came from one broker-wide knob: `max(live lanes, QUEEN_KAFKA_DEFAULT_PARTITIONS)`,
where the second term is a start-up setting. A create's `num_partitions` was accepted and
discarded, so a low-volume topic and a high-volume one could not differ, and changing either meant
restarting the broker for all of them. `CreateTopics` now stores the number it was asked for as
that topic's own width **floor**, and the topic is advertised at `max(live lanes, its floor)`.
Two topics on one broker can be 8 and 512 lanes wide, declared by the clients that made them.

**`CreateTopics` is the only writer.** Both alter paths carry an existing floor through untouched,
so a `retention.ms` change cannot silently narrow a topic, and there is no config key that sets
one. `CreatePartitions` still refuses, in the same words a real broker uses for the two cases a
real broker also refuses; its message now says what to do instead. Changing a declared width means
deleting the topic and creating it again. A `num_partitions` above 100,000 is refused
`INVALID_PARTITIONS` rather than clamped, so the facade never stores a number it would then
quietly answer as something else.

## 1.4.0 — 2026-08-31

**Kafka and SQS clients connect directly.** Two wire-protocol facades now ship inside
`ghcr.io/queen-mq/queen`, beside the broker binary. `queen-kafka` advertises 32 Kafka API keys,
transactions included, so an unmodified producer or consumer moves over by changing
`bootstrap.servers`; `queen-sqs` answers the SQS and SNS protocols, so an unmodified AWS SDK moves
over by changing `endpoint_url`. Both are inert until `QUEEN_KAFKA_EMBEDDED=true` or
`QUEEN_SQS_EMBEDDED=true`, where the broker spawns and supervises the facade as a child process on a
backoff — neither holds a database connection or stores anything durable of its own, so they are
Queen clients like any SDK. Kafka adds a two or three node cluster mode with its node registry in
Queen's key/value store; SQS is stateless behind an ordinary load balancer given a shared
`QUEEN_SQS_HANDLE_SECRET`. What each protocol covers, and every place it deviates, is
[reference/kafka](https://queenmq.com/reference/kafka) and
[reference/sqs](https://queenmq.com/reference/sqs); running them is
[deploy/kafka](https://queenmq.com/deploy/kafka) and [deploy/sqs](https://queenmq.com/deploy/sqs).

**Laravel queue driver, worker supervisors and a dashboard.** A native queue driver, supervisors
with prefetch and lease renewal, and a supervisor dashboard, hardened for production supervision and
qualified against a benchmark harness. The PHP client is packaged for Packagist as
`queen-mq/php-client` (registered; no version tagged yet) and mirrored to its own repository by CI.
Guides are under [use/laravel](https://queenmq.com/use/laravel).

**Proxy: `kv`, `timers` and `ephemeral` are base surfaces, not upsells.** The plan seed predated all
three families, and a plan row that does not name a feature is a plan that does not have it, so every
plan answered `403 feature_gated` on those routes from the day each shipped. Migration
`009_default_families` grants them on every existing plan and defaults new plans the same way.
`streams` and `traces` stay per-plan.

**Fixes.** A partial ack no longer erases the redelivery marker: `attempt_offset` follows the first
uncommitted frame of a live lease, so when a worker dies after acking a prefix, the tail is
recognised as a redelivery rather than as fresh work, and retry budgets and DLQ routing see the true
`deliveryAttempt`. A DLQ replay now provably mints a transaction id different from the original, so
a replay can never be mistaken for the frame it was quarantined from; it is still not idempotent, on
the terms 1.3.0 spells out. The JavaScript stream runner fails closed on a bootstrap error instead of
running a partially initialised chain.

## 1.3.0 — 2026-08-28

**Streams are tenant-scoped.** `queen_streams` was the last name- and pid-addressed surface without
tenant scoping, and on a shared cell it was unsafe: a query name was globally unique, so `reset:true`
could take over another tenant's query and hand back their uuid; the cycle resolved sink queues by
bare name, which can multi-match on `(tenant_id, name)`; and state operations ran with no ownership
check at all. `queen_streams.queries` now carries `tenant_id` with uniqueness on `(tenant_id, name)`,
the cycle gates the source partition and the query before taking the advisory lock, and all four sink
resolves are scoped. The new `queen_streams.quota` is the grant: for a non-default tenant, absence is
a denial (`403`, distinct from the `config_hash` `409`), checked on the fresh-insert path only, so
revoking a grant stops new queries without stranding a Runner that is still draining. Tenant purge
covers `queries` and `quota`, and empties `state` through the query FK in a bounded phase before any
query row is deleted.

**Cloud must-builds (proxy).** `QUEEN_PROXY_SHARED_HOSTS`: on a shared host the cluster resolves from
the credential rather than the hostname, and the whole listener answers `401` instead of `421` when
any shared host exists; host canonicalisation closes trailing-dot, case and port bypasses. Tenant
wipe lands as `queen_proxy.delete_tenant` (requires `status=deleting`, redacts the outbox) plus
`queen.delete_tenant_data_v1`, which walks every tenant-carrying table under per-table row budgets.
Proxy boot now fails fast when JWT material is supplied but unusable, and answers a precise `503` at
login when there is none at all. `PG_SSL_ROOT_CERT` and `PXDB_SSL_ROOT_CERT` take PEM content, not a
path, so a private CA can be verified without turning authentication off.

**DLQ retry reaches the SDKs.** `queenctl dlq retry <partitionId> <transactionId>` replays one
dead-lettered message, and Go's `RetryMessage` went from a stub that refused to a working call. The
route is **not idempotent** — the replay is minted with a fresh transaction id, so two replays are two
distinct messages that nothing collapses — so it is sent with the new `WithoutFailoverRetry` option
and must not be retried on failure without re-reading the DLQ first.

**`JWT_ALGORITHM` accepts the whole HMAC family.** HS384 and HS512 were implemented by the
request-time verifier but refused at boot, which made pinning either impossible and pushed operators
onto `auto`, the strictly wider posture. One `SUPPORTED_JWT_ALGORITHMS` constant now drives the boot
check and the error message it prints, with a test that fails if boot and verifier ever disagree
again.

**Version-line alignment.** Every client and SDK moves to 1.3.0 with the broker and proxy, which a
MINOR requires. Only two changed content since 1.2.0: `client-go` and `queenctl`, both above. The
rest move because a client version says which broker line it was released against.

## 1.2.0 — 2026-08-25

**Pop autopilot (server-advised pop sizing).** New SDKs omit `partitions`/`batch` and send
`autopilot=true`; the broker sizes the sweep from hot-list state (ready count, ready-age,
burst bypass under `QUEEN_POP_AUTOPILOT_BURST_CAP`). An explicit client value is a hard pin,
old clients are byte-identical, kill switch `QUEEN_POP_AUTOPILOT=on|shadow|off`, and an
`autopilot` echo + divergence log make the choices observable. Ported to all seven SDKs
(Go, JS, Python, Rust, PHP/Laravel, C++, queenctl) plus `queen-protocol`.

**Retention is now O(deletable), not O(partitions).** Per-partition watermarks
(`oldest_live_at`/`oldest_txn_at`, maintained by the push allocator and the retention steps)
turn the work list into per-queue indexed probes; a batched daily safety walk doubles as the
one-time backfill. Measured on an 827k-partition cell: 20 → 663 seg/s, cycle 172 s → 0.7 s.
Knobs: `QUEEN_RETENTION_DUE_CAP`, `QUEEN_RETENTION_SAFETY_WALK_MS`. The cycle's advisory
lock is transaction-scoped on a dedicated holder that takes no table locks, so a timed-out
or dying cycle can never leave retention deadlocked cluster-wide.

**Boot-time schema apply is safe on a busy cluster.** One statement per transaction
(dollar-quote-aware splitting), 2 s `lock_timeout` slices with jittered bounded retries
(~3 min patience, never >2 s of traffic stall), `CREATE INDEX CONCURRENTLY` support, and —
on exhaustion — a diagnostic that names the blocking sessions (robust to `pg_stat_activity`
privilege masking). Proven live: full apply in 443–724 ms against 1000+ tx/s.

**Hot-list memory is bounded.** A broker serving no pops for a queue (standby, failover
leftover) drops that ring after `QUEEN_HOTLIST_UNSERVED_TRIM_MS` and releases the pages
(`malloc_trim`); the partition intern went from four heap allocations per entry to one.
Measured: standby broker 2.1 GB → tens of MB; active plateau roughly halved.

**Observability.** Burst-resolved pop telemetry on the rates line (`ring_depth_max`,
`ring_oldest_max_ms`, `max_lane_ready`, `pop_wait_max`, ready-entry provenance).

Operational note for rolling upgrades from ≤1.2.0-beta.2 under heavy load: apply new
hot-table DDL manually first (single-statement `ALTER` with `lock_timeout`, indexes via
`CREATE INDEX CONCURRENTLY`) or boot once with `QUEEN_APPLY_SCHEMA=false`; from this
release onward the applier handles it unaided.

## 1.1.0 (2026-08-21) — conflation

**Last-value delivery, as a consumer-group policy.** `conflation=true` on any pop route makes a
pop of a partition deliver exactly one message — the newest visible one — and commit everything
behind it when the handler acks. It is for command-style queues where one partition is one
logical key and only the freshest "recompute X" is worth running: under a backlog the handler
runs once per partition instead of once per message. Nothing on disk is touched; retention still
governs storage. The guarantee it keeps is that after the last push to a partition, at least one
run of that partition's handler *starts* after that push commits — the broker never commits past
an offset it did not observe at pop time.

Conflation is a property of the **group on the queue**, not of the call: the first pop that
registers the group persists it, and from then on the stored value wins for every consumer of
that group. That is what lets `workers` conflate while `audit` on the same queue reads
everything. A consumer that declares the opposite of the stored policy is not rejected — the
stored policy is applied and the response says `"conflationConflict":true`, so the consumer keeps
running and warns once. Rejecting would take down the already-correct half of a rolling deploy.
Two combinations ARE refused, with a `400` that names the reason: `conflation=true` without a
`consumerGroup`, and `conflation=true` with `autoAck=true`.

**Degrade loudly, in every SDK.** No SDK negotiates a version with the broker, so a 1.1.0 client
against an older one would have the unknown query parameter ignored and quietly drain the whole
backlog message by message. Instead the broker echoes `"conflation":true` on every conflating
response *including empty ones*, and an SDK that asked and did not get the echo raises on its
first round trip, before a single message is handled: *"conflation was requested but this broker
did not apply it — requires broker >= 1.1.0"*. Because those keys have to reach the client, a pop
whose answer has anything to say about conflation is a `200` with a body even when it delivered
nothing, pop maintenance included. Every response that never mentions conflation is byte-identical
to 1.0.6, `204` included.

Also in this release: `subscriptionMode`, `subscriptionTimestamp` and `subscriptionCreatedAt` are
real values on `GET /api/v1/consumer-groups` instead of hard-coded `null`, next to the new
`conflation` field; `GET /api/v1/resources/queues/:queue/depth` gains `partitionsPending`,
`conflation` and `effectivePending`, where a conflating group's `pending` is log depth and
`effectivePending` is the handler runs that remain (`pending: 4000000, effectivePending: 12` is
healthy under conflation and an incident without it); and
`queen_queue_conflated_per_minute{queue}` counts the positions conflation retired without a
handler invocation. The dead `queen_queue_depth_total` and `queen_queue_depth_pending` families
were removed: they read an aggregator key no stored procedure has ever produced, so they were
never in the exposition, only in the generated reference.

The C++ SDK's push buffer catches up to the 1.0.6 contract the other five SDKs got on
2026-08-20: `max_size` is now a blocking backpressure bound (unbounded is deliberately not
expressible), a batch whose POST fails is re-queued at the front of the buffer, in order, and
retried until it lands or an explicit `flush_buffer`/`flush_all_buffers` deadline expires — at
which point `BufferFlushError` says how many messages are still buffered, none of them dropped —
and `close()` flushes under the same 30 s deadline and reports what is left. Before this, the C++
buffer grew without limit and dropped a failed batch after a log line.

**Rollout.** Default off: a group created without the flag behaves exactly as before, and there
is deliberately no `QUEEN_CONFLATION_ENABLED`. Ship the broker first (the columns are additive
and defaulted, old SDKs are unaffected), then the SDKs; no coordinated cutover and no migration
for existing groups. The broker rolls back cleanly, with one caveat worth saying out loud: a
group already registered with `conflation=true` is served full batches by an older binary, which
ignores the column. Rolling back turns conflation off, it does not turn it into an error.

## 1.0.6 (2026-08-20) — clients only

**The client-side push buffer is now bounded, and a failed flush no longer loses messages.** In
every SDK (Go, JavaScript, Rust, Python, PHP). Before this release the buffer grew without
limit — a producer filling faster than the flush pipeline drains was measured accumulating 20.9M
messages (11.7 GB of RSS) in 45 seconds and losing every one of them at process exit, with zero
client-side errors reported — and a batch whose POST failed was dropped after a log line. Now
`maxSize` (default `4 × messageCount`; unbounded is deliberately not expressible) makes the add
path wait for the flusher in each language's idiom, and a failed batch is re-queued at the front
of the buffer, in order, and retried every `retryDelay` until it lands. A broker outage shows up
as blocked producers and bounded memory instead of silent loss. The formerly inert `maxSize` and
`retryDelay` knobs now do exactly what they say; `close()` flushes under a 30 s deadline and
reports anything left unsent. Same measured workload after the fix: 881k msg/s sustained with
exact send/receive parity and 71 MB of RSS.

The server stays at 1.0.5; this release bumps only the client packages.

## 1.0.3 (2026-08-18)

**Key/value state and timers are part of the engine, not features to switch on.** There is no
`QUEEN_KV_ENABLED` and no `QUEEN_TIMERS_ENABLED` — the broker reads neither, and setting them
does nothing. The reason is the one that governs every other surface: there is no
`QUEEN_PUSH_ENABLED` either. A boot flag is the claim that a thing is optional, and a cell where
`/api/v1/kv` might or might not exist is a cell no client can be written against. From the moment
this binary lands, both surfaces are live and the sweeper is running on every cell.

**Upgrading breaks a broker started with `QUEEN_TENANCY_HEADER=1` unless you also set
`QUEEN_KV_TRUSTED_PROXY=1`.** This is the one change that needs an edit before the upgrade, and
the failure is loud: the process exits at boot with the reason in its last log line, so a
Kubernetes rollout crash-loops rather than serving. Add the variable wherever the tenancy header
is on:

```
QUEEN_TENANCY_HEADER=1
QUEEN_KV_TRUSTED_PROXY=1     # new, and now mandatory alongside the line above
```

Set it only where the claim it makes is true: **a proxy in front sets `x-queen-tenant` and strips
whatever the client sent.** The interlock is not about KV being dangerous. With the header on, the
tenant identity is opaque and validated against nothing, and any caller who can reach the broker
directly can name another tenant — KV was simply the first surface addressable purely *by name*,
which is what made it visible. The requirement existed before; it was conditional on the KV flag,
and with that flag gone it is unconditional for anyone running with the header. If you cannot make
that affirmation truthfully, the answer is to stop running with `QUEEN_TENANCY_HEADER=1`, not to
set the new variable: there is no longer a flag that could withhold the surface, and there should
not be — a fleet where the engine is missing on some cells is worse than a boot that names the
variable to set.

**Nothing ships dark any more.** The rollout plan for these surfaces used to begin by installing
the complete broker with both flags false, so the routes were not even registered, and enabling
them later cell by cell. That step no longer exists. Anything to be watched has to be watched on a
cell that already answers. The instrument for a cell in trouble is the runtime kill switch —
`POST /api/v1/system/kv-timers` with `kvEnabled`, `timersScheduleEnabled` or `timersFireEnabled`
set false — which pauses a surface that exists, answers 503 with `Retry-After` while paused, takes
effect on the next call rather than the next restart, and is expected to be flipped back. Same
class as maintenance mode. It is not a rollout gate and does not make an unvalidated tenant header
safe.

**Wire and metric consequences of the flags being gone.** A kv or timer route can no longer answer
`404 not_found` with reason `kv_not_enabled` or `timers_not_enabled`, and a transaction carrying a
`kv` or `timers` rider can no longer be refused with `400 bad_request` for reaching a cell without
the surface — a 404 from those routes now means a wrong URL or an older image. `GET`/`POST
/api/v1/system/kv-timers` no longer return `kvEnabledByConfig` and `timersEnabledByConfig`; every
other field, including the three switch states, the mirror status and the quotas, is unchanged.
The `queen_kv_*`, `queen_timers_*` and `queen_sweeper_*` metric families are now exported by a
broker that has never served a kv call, so a dashboard can be built before any traffic exists.
`queen_kv_read_rejected_total{reason="disabled"}` now means the operator's kill switch and nothing
else; refusals that used to land in `reason="pool"` because they were classified from the HTTP
status are attributed correctly.

## 1.0.2

**A new logo.** The duck gives way to a geometric mark — a ring with an exit port and the piece
that left through it — across the dashboard, the docs, the sign-in page and the README.

## 1.0.1

**The hot-list reseed asks a bounded question.** The reseed is how a broker rebuilds its
in-memory candidate ring: for a (queue, group) it enumerates the partitions that still hold
unconsumed data. It did that by walking every partition of the queue, once per ring per 30
seconds per broker, and on a production database of 51,552 partitions that had become the
single largest consumer of the whole instance — measured on one 9,563-partition queue, 49 ms
per call to return zero rows, 8.2 calls a second, 0.58 cores, more total database time than
the entire pop path at 24x the call count. The cost was never the partition scan; it was the
one `log_consumers` primary-key probe the join pays per partition, 38,292 of the query's
39,700 shared buffers.

It now walks only the partitions written in the last `QUEEN_HOTLIST_RESEED_WINDOW_MS`
(default: four reseed intervals, floored at 120s), driven by an index that already existed.
Same question, same answer, 0.375 ms. That bound is sound because a partition can only
*become* pending by being written, and every push stamps `last_write_at`; acks and retention
only ever remove pendingness.

**What a window cannot see is a cursor moving backwards**, which is why the full walk stays,
at `QUEEN_HOTLIST_RESEED_FULL_MS` (default 300s; `0` pins every reseed to the full walk and
restores the previous behaviour without a rebuild). **This is the one behavioural change to
weigh before upgrading**: the worst case for repairing a ring that lost a partition with no
write behind it moves from roughly 45 seconds to roughly 6 minutes. Nothing is lost or
duplicated by that — the reseed is a cache over PostgreSQL and errs toward over-inclusion —
but a stall that used to heal itself inside a minute can now take five.

The operations that genuinely move a cursor backwards no longer wait for it. A consumer-group
delete and a seek both force the full walk on the broker that served them and publish a
durable repair marker in their own transaction; every other broker applies it on its next
reconcile pass. Measured on a two-broker stage cluster: 18 seconds from the seek to the peer's
ring being repaired.

**A ring on a `windowBuffer` or `delayedProcessing` queue can be reclaimed again.** One
ordinary push was enough to arm an entry that could never return to idle, so the queue's state
was pinned for the lifetime of the process and burned about 18 empty claims a second forever.
No seek, no mesh and no replication were required to reach it. On those queues a partition
that becomes visible again with no new write behind it — a retry delay longer than the
visibility cut — is now recovered by the reseed floor rather than by that loop, which is the
exposure plain queues have always had.

**The reseed says what it is doing.** The periodic floor line now separates full from windowed
passes, counts the ones that failed, and reports each ring's age since its last full walk, so
"which mode is this ring in, and when was it last repaired" is answerable without arithmetic.

`queen.log_hotlist_reseed_window_v1` gains an absolute cutoff. The argument is appended after
the tenant rather than beside the window it pins, so a 1.0.1-beta.1 replica keeps resolving
its call while a rolling upgrade replaces it, and the schema is safe to apply under a running
previous release.

## 1.0.0

First stable release. The broker, the proxy, the dashboard and the operator console all carry
`1.0.0`. The prerelease numbers had drifted apart — the proxy, the dashboard and the console
never moved past `1.0.0-beta.1`, because the later betas were broker-only — and this release
puts them back on one number.

**The broker is embeddable as a Rust library.** `queen-engine` publishes the same engine the
container runs, importable as `queen::Broker`: `Broker::start` applies the schema, starts the
background machinery and hands back typed operations — push, pop with long-poll, ack, leases,
transactions, configure, delete, the DLQ, metrics. Each one invokes the handler functions the
HTTP router dispatches to, so behaviour and defaults are the broker's by construction rather
than a reimplementation's. `default-features = false` drops the `server` feature, which is
the axum serve stack, the embedded dashboard and the process tracing subscriber; with the
default features the `queen` binary is byte-identical to the pre-feature layout. The package
publishes as `queen-engine` because the bare crates.io name `queen` belongs to an unrelated
crate, while the library still imports as `queen`. Measured MSRV is 1.88. The Rust surface is
published as **beta**: the HTTP API remains the stable compatibility contract.

**`RETENTION_PARALLELISM` does something again.** It was parsed and ignored through the
log-engine port, which pinned deletion at one partition at a time — a measured ceiling of
~13.8k step rows/s against the ~14.6k that 1M msg/s produces, so the database grew without
bound at 1M and held fine at 600k. Retention phases 1-3 now fan out over that many workers,
each on its own pooled connection and its own maintenance-lane admission slot, pulling
partitions off a shared cursor rather than a static split, so one deep backlog cannot leave a
single worker running alone. Phase 4, partition cleanup, stays serial on the cycle's own
connection: it is the one step that is not per-partition, so it is the one that can hold more
than one lock at a time. The value is clamped to 16 and defaults to 1 — the historical serial
cycle — so upgrading changes nothing until it is set.

Raising `RETENTION_BATCH_SIZE` is not a substitute. The step cost is per row, and the step
takes the same `log_partitions` row lock the push allocator takes, so at 1M msg/s a batch of
8000 pushed client p99 from 0.6 s to 20 s and absorbed no more rows.

**The admission controller can be told a lane's concurrency instead of discovering it.** A
lane's cap only widens on a probe, and a probe needs a minimum number of completions inside
one tick, which a lane running ~250 ms transactions never reaches: it decays to the global
minimum and stays there. Measured, retention with 4 fan-out workers sat at cap 2 with 4
waiters for a whole run — no faster than the serial cycle it replaced. Both the binary and
the embedded boot path now state the maintenance lane's floor as `RETENTION_PARALLELISM + 1`.
Raise `QUEEN_ADMISSION_SHARE_MAINT` along with the parallelism.

## 1.0.0-beta.4

**Google sign-in through the proxy could never complete.** The token exchange died with
`bad_gateway` / "google token exchange failed", and the underlying error was a TLS one:
`peer closed connection without sending TLS close_notify`. The proxy's self-contained HTTP
client sends `Connection: close` and reads to end-of-stream, but a peer that closes the TCP
connection without a TLS close_notify surfaces through rustls as `UnexpectedEof`, and the
read loops treated any read error as a failure — so a response that had already arrived in
full was thrown away. Google's token endpoint closes exactly that way. Both read loops now
treat it as the end of the message, which also unblocks the JWKS fetch that verifies the
returned id_token; the JWKS path would have failed a step later for the same reason.

Tolerating an abrupt close is only safe if a genuinely truncated response is still
rejected, so response parsing now validates the body against `Content-Length` and returns
`truncated body` when it is short. The chunked path already required its terminating
zero-size chunk. `Transfer-Encoding: chunked` takes precedence over `Content-Length`, per
RFC 9112.


**The dashboard reported messages as pending after they had been consumed and
acknowledged.** Two independent accounting defects, both around the group-less
`__QUEUE_MODE__` cursor, both visible only to grouped ("bus mode") consumers — the data
plane was never affected, and no message was ever delivered or retained wrongly.

- **The Messages list showed `pending` forever.** `list_messages_v1` derived a frame's
  status from a join pinned to the `__QUEUE_MODE__` cursor alone. A partition consumed by
  named consumer groups has no such row, so the join produced all NULLs, neither the
  completed nor the processing branch could fire, and every frame fell through to the
  `pending` fallback. The same select was already counting named groups two lines below,
  which is why a row could render `pending` next to `1/1 groups`. The status is now derived
  the way the message-detail endpoint already derived it: named groups decide when the
  partition has any, and the group-less cursor decides only when it has none.

- **A single group-less pop could pin a queue's pending count high forever.** The stats
  refresh took the worst cursor across every consumer group without distinguishing them.
  Popping without a `consumerGroup` seeds a `__QUEUE_MODE__` cursor at the head of the
  backlog — on every partition of the queue for a wildcard pop, and even when that pop
  returns nothing — so one debug pop or load-test run left a permanent floor under
  `pending_messages` that no amount of real consumption could drain. The same precedence
  rule now applies: named groups when they exist, the group-less cursor only otherwise.
  Applied identically in the queue-detail and queue-list paths.

Retention deliberately keeps the old across-all-groups watermark: there, including the
group-less cursor is the conservative choice, because deleting a segment a consumer might
still want is not recoverable.

## 1.0.0-beta.3

**A push was rejected whenever a JSON escape appeared in `queue`, `partition` or
`transactionId`.** Those three fields deserialized into borrowed `&str`, and serde cannot
borrow a string literal that needs unescaping, so the parse failed for the whole request
body: HTTP 400, every item in the batch discarded. The trigger was any escape at all, not
one bad character. Go's `encoding/json` escapes `&`, `<` and `>` by default, so a
transaction id like `Bed&Breakfast-771` from the Go SDK was rejected; Guzzle escapes `/`
and all non-ASCII, so `2026/07/BK-11` was rejected from PHP; and `"` and a backslash are
escaped by every JSON encoder, so they failed from every client including Queen's own Rust
one. The C++ broker copied these fields into owned strings and had none of this, which made
it a regression introduced with the Rust push path.

The fields now deserialize through a `Cow` newtype that still borrows when the literal
needs no unescaping, so the push path keeps its per-item allocation count. Control
characters remain rejected, now deliberately and with an error that says so: the layer-1
dedup key and the fusion group key are both composed by joining these fields on `0x1F`, an
invariant that until now held only because every escape happened to fail.

Three defects found alongside it are fixed in the same pass:

- **Error bodies were not valid JSON.** Fourteen handlers built `{"error":"..."}` with a
  raw `format!`, and a serde error embeds the offending value in quotes, so the response to
  a malformed request could not itself be parsed. They now route through the existing
  escaping helper.
- **An over-long `transactionId` was silently truncated.** The segment frame codec
  length-prefixes it with a `u16` while computing the body length from the full size, so a
  transaction id above 65535 bytes produced a frame whose declared length disagreed with
  its contents. The limit is now enforced at the HTTP boundary and asserted in the codec.
- **Dead-lettered messages could be unreplayable.** DLQ replay rebuilds a push body with
  `serde_json` and feeds it back through this parser, so a message on a queue or partition
  whose name contained a quote or a backslash could never be replayed. Existing stuck rows
  become replayable.

**A new consumer group now starts at the tail, not at the beginning of the backlog.** The
C++ broker honored `DEFAULT_SUBSCRIPTION_MODE` and the shipped charts set it to `new`; the
Rust port dropped the variable and hardcoded the per-request default to `all`. A chart that
still said `new` was therefore serving `all`, and the first group created after traffic
resumed replayed the whole retained log. The variable is honored again and its default is
`new`. Set `DEFAULT_SUBSCRIPTION_MODE=all` to restore the previous behavior without a
rebuild.

This changes only grouped consumers on their first contact. Existing groups resume from
their stored cursor as before, and a pop with no `consumerGroup` is unaffected: the SQL
pins those to `all` regardless.

**`subscriptionMode=new-only` did the opposite of what it advertised.** The Go SDK exports
it as an alias of `new`, the CLI offers it in `--from-mode`, and the JS README shows it in
use, but the SQL compares the mode literally, so `new-only` missed the `new` branch and
replayed the entire backlog. The broker now normalizes it. Any unrecognized value still
resolves to `all`, which is the safe direction.


**Two fixes in the Rust SDK, both found by putting its test suite under audit.**

- **A dead-lettered message's reason was never readable.** `DlqMessage.error` deserialized
  the key `error`, while `queen.get_dlq_messages_v1` projects the stored reason as
  `errorMessage`. The field was therefore always `None` and the reason sat unnoticed in the
  struct's `flatten`ed `rest`. It now reads the wire key, and accepts the plain one as an
  alias. The test that was meant to cover this asserted only that the DLQ had one row.
- **A `gate` after a stateless operator acked the wrong messages.** The gate settles by
  offset commit — *n* messages ending at a transaction id — but counted the records the
  gate had allowed. A `filter` before the gate leaves fewer records than messages, so the
  commit landed short and the remainder was consumed but never acked, redelivered on lease
  expiry, filtered out again: a partition that never drains. A `flat_map` leaves more
  records than messages, and the count indexed past the end, panicking inside the spawned
  loop task while `stop()` still reported a clean shutdown. Records are now grouped back
  onto their source message, and a message is settled only when every record it produced
  was allowed. State mutations from a denied message are rolled back, which is what
  `Stream::gate` already documented.

`TxnAckOperation` gained the `error` field the broker reads as the dead-letter reason, so a
transactional failure can explain itself; `TransactionBuilder::nack` and `ack_with_reason`
expose it.

The SDK's tests also now run in CI, which they did not before: `rust-client` is in the
suite matrix, and a native job runs the client's and `queen-protocol`'s unit tests, clippy,
rustfmt and the declared MSRV.

## 1.0.0-beta.2

**One admission arbiter replaces the two Vegas limiters.** Concurrency against PostgreSQL
was governed by a pair of TCP-Vegas-style controllers, one per lane, each inferring
queueing from per-operation round-trip time. On a WAL-bound commit path most of the excess
over the observed minimum is the group-commit flush wait: intrinsic cost that admission
cannot remove. The estimator counted that floor as congestion and backed off from it, and
because its grow and shrink thresholds were absolute the dead band widened as the limit
fell, making low limits an attractor. The broker left cores idle while the connection pool
reported no waiters at all, and nothing exported the controller's inputs, so the
disagreement was invisible.

`server/src/admission.rs` replaces it with a single arbiter for every write transaction:

- **The sensor is passive.** Group commit clusters write completions in time, so grouping
  the broker's own commit completions measures the flush pipeline with no PostgreSQL-side
  telemetry. Train size is amortisation measured rather than assumed; train cadence is the
  flush rate; the gap between train starts is the flush cycle.
- **One budget, four lanes.** Push, Pop, Ack and Maint share a work-conserving budget.
  Guarantees act on the wake order when the budget is exhausted, acks first because they
  unblock lanes. The budget never rises above `DB_POOL_SIZE` minus a reserve, so admitted
  work cannot starve on the pool.
- **Slots are RAII.** Dropping a slot releases it. The previous limiter leaked its
  in-flight counter at eight call sites that took a permit and returned early, until its
  own anti-ramp guard was permanently disabled.
- **A degraded mode is detected, not guessed.** With `synchronous_commit = off` commit
  waits collapse and the trains carry no signal; the arbiter pins a static budget and says
  so in telemetry (`adm_mode`).

**The pop claim path is set-based.** `queen.log_pop_list_v1` used to call `log_pop_v1` once
per candidate partition, about six statement executions each, inside the admission permit
and the committing transaction. It now does the same work in roughly six statements total,
independent of the candidate count. Measured on the sparse-partition shape, the per-pop
cost was 3.42 ms of which 0.17 ms was data work.

**New defaults.** `QUEEN_V2_FUSION_HOLD_MS` is 3, down from 15: the hold is paid twice per
flow, on ingress and on the derived republish. `QUEEN_ADMISSION_MIN` and
`QUEEN_ADMISSION_INIT` now derive from the pool, two thirds of
`DB_POOL_SIZE - QUEEN_ADMISSION_POOL_RESERVE`, which is 96 on the defaults. They are
derived rather than fixed so a small deployment cannot admit more concurrent transactions
than it has connections for. On the 2000 ev/s / 1000-lane shape, raising the floor alone
took p50 from 1143 ms to 240 ms.

**Validated by a soak.** 600,000 msg/s for 3 h 10 m with full production semantics (leases,
explicit async acks, 60 s dedup window, retention active): **6.82 billion messages, zero
push, pop and ack errors, flat lag**, p50 120 ms and p99 297 ms, with the median steady
inside ±8% across the run.

**Known weakness, stated because this is a beta.** The shrink signal is the age of the
oldest admitted slot, which conflates statement execution with commit wait: a transaction
doing genuine heavy work looks like one waiting behind a queue. On workloads whose write
transactions routinely run long the budget hunts rather than settles. This is documented in
`admission.rs` and on [flow control](https://queenmq.com/internals/flow-control), and the
replacement signal is named there.

### Breaking

- **Prometheus metric names changed.** `queen_seg_push_vegas_limit` and
  `queen_seg_pop_vegas_limit` no longer exist. The replacements are
  `queen_admission_budget`, `queen_admission_inflight{lane}`,
  `queen_admission_waiting{lane}`, `queen_admission_trains_per_s`,
  `queen_admission_txn_per_train` and `queen_admission_cycle_ms`. **Dashboards and alerts
  referencing the old names need updating.**
- **The `rates` log line changed shape.** `vegas_push` and `vegas_pop` are replaced by
  `adm_budget`, `adm_mode` and `adm_lanes`.
- **Vegas-era variables are no longer read.** `QUEEN_SEG_{PUSH,POP}_{INIT,MIN,MAX}`,
  `QUEEN_VEGAS_ALPHA` and `QUEEN_VEGAS_BETA` are accepted by the environment and ignored;
  the broker logs a warning at boot for each one it finds set, rather than letting a
  deployment tune a control loop that is not there. The Helm chart no longer sets the eight
  libqueen-era `QUEEN_{PUSH,ACK,POP}_MAX_CONCURRENT` / `QUEEN_VEGAS_*` /
  `QUEEN_PUSH_*_BATCH` variables, none of which the Rust broker ever read.

## 1.0.0-beta.1

First beta of the 1.0 line: the Rust broker on the segment storage engine described under
[1.0.0](#100) below, plus the tenancy surface built on top of it.

- **Multi-tenancy and the gateway.** Native tenant scoping in the broker behind
  `QUEEN_TENANCY_HEADER` (off by default), and `queen-proxy`, a separate Apache-2.0 gateway
  carrying API keys, plans, quotas, rate limiting, metering and a console.
- **One webapp behind the proxy.** Auth, roles, tenant selector and the operator surface in
  a single application, re-keyed onto the documentation site's palette.
- **The migration tool is gone.** It shelled out its PostgreSQL connection parameters,
  which was a remote-code-execution path; the fix landed first and the tool was then
  removed rather than kept.
- **Tenancy correctness and cost.** Each tenant gets its own discovery wake gate, so one
  tenant's pushes stop waking every other tenant's parked consumers. Confirmed
  partition-to-tenant ownership is cached, dropping a round trip per ack.
- **Hot-list lease revisit is bounded**, which removes a stall that could hold a single
  partition indefinitely.

## 1.0.0

**A Rust broker on a new storage engine.** The 0.x line was a C++ implementation
(libqueen, uWebSockets, libpq) storing one row per message in `queen.messages`. 1.0.0
replaces both halves: the broker is a single stateless Rust binary (`queen-seg`, axum and
tokio over a pooled `libpq`), and the storage engine is a log of compressed segments.

### The storage engine

A message's position is now a single monotone per-partition offset. A segment is one row
holding many length-prefixed frames, packed and zstd-compressed, so a push writes segments
rather than rows. Consumption state is one cursor per (partition, consumer group) in
`queen.log_consumers`: there is no per-message delivery state anywhere in the schema, which
is what makes tens of thousands of partitions cheap.

- **Acknowledgement is an offset commit.** Acking a message commits the cursor past it and
  implicitly completes everything before it in that partition for that group. Acks still
  arrive addressed by `transactionId`; the broker resolves them through a 16-byte-per-frame
  hash sidecar (`queen.log_txns`).
- **Two honesty guarantees, both contract-tested.** An explicit `failed`, `dlq` or `retry`
  is never skipped by a later `completed` ack in the same call: the cursor clamps at the
  lowest signal. An ack that lands below the cursor or outside the hash window is reported
  as a no-op rather than silently succeeding.
- **Deduplication is exact and enforced in SQL.** The probe of the transaction-id sidecar
  happens under the partition row lock, before an offset is allocated, so a duplicate writes
  nothing. The broker-side cache can only narrow that probe, never change its verdict.
- **Commits are amortised.** A fusion layer groups pending writes by partition and bundles
  disjoint partitions into one transaction, so N segments cost one commit and one fsync.
- **Pop candidates come from memory.** An in-process hot-list ring replaces the SQL
  candidate scan on the wildcard pop path, with a deferred-visibility wheel for leases and
  timed retries.

### Elsewhere

- **Multi-tenancy.** Native tenant scoping in the broker behind `QUEEN_TENANCY_HEADER`
  (off by default), plus `queen-proxy`, a separate Apache-2.0 gateway with API keys, plans,
  quotas, rate limiting, metering and a console. Documented under
  [Self-hosting](https://queenmq.com/selfhost).
- **Multi-broker coordination is framed TCP**, not UDP. The old `QUEEN_UDP_*` variables are
  accepted as aliases for `QUEEN_MESH_*`. Everything the mesh carries is a best-effort hint;
  PostgreSQL remains the only source of truth. **The mesh port must be firewalled.**
- **The dashboard is compiled into the binary** (`rust_embed`). There is no
  `QUEEN_STATIC_DIR` and no on-disk assets at runtime.
- **The dashboard works broker-direct.** With auth off (the default) the broker answers
  `GET /auth/me` itself with a standalone operator identity, so the full dashboard runs
  with no proxy: every view live, session controls hidden. With `JWT_ENABLED=true` the
  broker serves an explanation page at `/auth/login` instead; a dashboard with logins and
  roles remains `queen-proxy`'s. Documented at
  [queenmq.com/selfhost/dashboard](https://queenmq.com/selfhost/dashboard).
- **New documentation.** [queenmq.com](https://queenmq.com) is rewritten from the current
  source. Its route table, environment-variable reference, metric list, proxy route classes,
  OpenAPI documents and benchmark figures are generated from the code and the archived
  benchmark artifacts, and CI fails when any of them falls behind.

### Breaking

- **The migration tool is gone.** `POST /api/v1/migration/*` and its handler were removed.
  Back up with `pg_dump` over the `queen` and `queen_streams` schemas.
- **The retired engine's objects are dropped at boot.** Applying the 1.0.0 schema removes
  the previous engine's tables and procedures. This is not a data migration: messages stored
  by a 0.x broker do not carry over. Drain a queue before upgrading, or start fresh.
- **Prometheus names moved.** The in-process counters are `queen_process_*`; the
  `queen_cluster_*` namespace now means database-backed lifetime totals, identical on every
  instance. Per-queue series sum across tenants, so the endpoint is not a per-tenant surface.
- **`QUEEN_STATIC_DIR` no longer exists**, and several 0.x tuning variables
  (`NUM_WORKERS`, `QUEEN_*_SLOTS`, `SIDECAR_*`, `RESPONSE_BATCH_*`) have no equivalent: the
  Rust broker's concurrency is adaptive and sized from the connection pool.

### Compatibility

The HTTP message plane keeps the 0.16.0 contract: push, pop, ack, transaction and lease
extension are unchanged on the wire, and an existing SDK keeps working against a 1.0.0
broker for those calls. The SDKs are version-aligned at 1.0.0. Details, including which
client methods target routes that no longer exist, are in
[the compatibility reference](https://queenmq.com/reference/compatibility).

Measured behaviour, with the configuration of every run attached, is at
[queenmq.com/benchmarks](https://queenmq.com/benchmarks).

## Release History

> Every row below **0.16.0 and including it** describes the retired C++ implementation and
> its row-based storage engine. Those measurements and architecture notes do not describe
> 1.0.0.

**JS clients from version 0.12.0 can be run inside a browser**

| Server Version | Description                                                                                                                     | Compatible Clients                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **1.0.0**      | **Rust broker on a segment-based log engine.** Offsets instead of row cursors, acknowledgement as an offset commit, exact windowed deduplication enforced before offset allocation, amortised commits via request fusion, an in-memory pop candidate ring, framed-TCP mesh, native multi-tenancy and a separate multi-tenant gateway. See the section above. | SDKs are version-aligned at 1.0.0; the message plane keeps the 0.16.0 wire contract |
| **0.16.0**     | **Push-serialization architecture + SIMD JSON.** New function-split libqueen engine cluster (3 shared engines — push/ack, pop, rest — decoupled from `NUM_WORKERS`, sized by per-function connection slots). Per-partition push serialization (in-memory in-flight gate + `pg_advisory_xact_lock` + `clock_timestamp()`) makes `messages.created_at` **commit-ordered**, eliminating the cursor-skip where a message could commit behind an already-advanced pop cursor under high concurrent push. Data-path concurrency (push/pop/ack) is now **static** (Vegas retained for auxiliary lanes), and `partition_lookup` maintenance is coalesced Nagle-style. Hot-path JSON is parsed and assembled with **simdjson** (nlohmann/json fallback), cutting broker CPU on push result fan-out. Balanced ~110–120k msg/s push & pop concurrently, ~190k push-only, on a 32-core host. Validated by a 24-hour soak: **10.4 billion messages, ~119k msg/s balanced, zero loss, flat ~400 MB broker**. [See soak →](https://queenmq.com/benchmarks-0.16-soak.html) | All ≥0.14.0 clients work unchanged — HTTP contract is identical; 0.16.0 SDKs are a version-aligned release |
| **0.15.5**     | Resolves **#30** (proxy compatible with Traefik forward-auth middleware), **#31** (write-only access role), **#32** (hardened Node base image). Robustness: malformed payloads can no longer crash a worker. Invalid UTF‑8 bytes and unpaired UTF‑16 surrogates — in a request body or in DB‑returned data — are serialized leniently and rejected with a clean **400** instead of throwing out of the event loop. Previously‑unguarded admin/metrics routes wrapped in error handling. | All ≥0.14.0 clients work unchanged |
| **0.15.0**     | Cross-language streaming SDK: fluent `Stream` builder + `.gate()` rate limiter + tumbling/sliding/session/cron windows + event-time + watermarks shipping in `queen-mq` (JS), `queen-mq` (Python), and `client-go` — all backed by the same `/streams/v1/*` endpoints and three new stored procedures (`streams_register_query_v1`, `streams_cycle_v1`, `streams_state_get_v1`). Identical SHA-256 `config_hash` across runtimes so a query registered by one client can be resumed by a worker written in another. UUIDv7 stamping in `streams_cycle_v1` push items to preserve FIFO order in batched sink emits. | All ≥0.14.0 clients work unchanged — upgrade clients to 0.15.0 to use the streaming SDK |
| **0.14.3**     | Improved frontend. | All ≥0.14.0 clients work unchanged |
| **0.14.1**     | Updated frontend: new metrics views and embedded developer guide; Google OAuth on the proxy; Prometheus metrics route (`/metrics`); significantly optimized lease renewal (reduced lock contention and DB round-trips); delete partition and delete messages API. | All ≥0.14.0 clients work unchanged |
| **0.14.0**     | Major release: new dynamic libqueen loop; rewritten `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and `stats` stored procedures; `maxPartitions` on all clients (JS, Python, Go, Laravel, C++); new frontend. Benchmarked on real hardware: **104k msg/s** push (batch=100), **165k msg/s** fan-out across 10 consumer groups, pop throughput **+80–90%** vs 0.12 under partition contention, 52 MB server RSS at peak, zero message loss across 1.6B events. [See benchmarks →](https://queenmq.com/benchmarks.html#version) | All ≥0.13.x clients work unchanged — upgrade clients to gain `maxPartitions` support |
| **0.13.0**     | Major release: new libqueen with adaptive batch/concurrency/scheduling engine (S1 ~2x, S3 ~3x push throughput), new `push_messages_v2` stored procedure (temp-table + batched-insert pipeline), new Vue 3 dashboard, and server-stamped `producerSub` from the JWT on every message (closes #23) | All ≥0.12.x work unchanged — 0.13.0 pop responses add a new `producerSub` field that older clients silently ignore. Upgrade to 0.13.0 clients only if you want typed access to `producerSub` (Go struct field, Python TypedDict hint) |
| **0.12.19**    | Fix bug that on seek or cg delete do not deleted the watermark                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.18**    | Improved charts and filters                                                                                                     | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.17**    | Improved stats                                                                                                                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.13**    | Added watermark tracking for efficient wildcard POP discovery. x20 faster pop on high partition count queues                    | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.12**    | Built-in database migration (pg_dump \| pg_restore, no temp file, selective table groups, row count validation)                  | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.12.10**    | Fixed JWKS fetch over HTTPS (cpp-httplib TLS support)                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.9**     | Fixed server crash (SIGSEGV) on lease renewal, added EdDSA/JWKS auth, fixed examples                                            | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.8**     | Added single partition move to now to frontend                                                                                  | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.7**     | Optimized cg metadata creation for new consumer groups                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.6**     | Improved slow cg discovery when there are tons of partitions                                                                    | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.5**     | Fixed cg lag calculation for "new" cg at first message                                                                          | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use            |
| **0.12.4**     | Fixed window buffer debounce behavior                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.3**     | Added JWT authentication                                                                                                        | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.12.x**     | New frontend and docs                                                                                                           | JS ≥0.7.4, Python ≥0.7.4, 0.12.0 if needs to use proxy auth |
| **0.11.x**     | Libqueen 0.11.0; added stats tables and optimized analytics procedures, added DB statement timeout and stats reconcile interval | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.10.x**     | Total rewrite of the engine with libuv and stored procedures, removed streaming engine                                          | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.8.0**      | Added Shared Cache with UDP sync for clustered deployment                                                                       | JS ≥0.7.4, Python ≥0.7.4                                    |
| **0.7.5**      | First stable release                                                                                                            | JS ≥0.7.4, Python ≥0.7.4                                    |

**[Full Release Notes →](https://github.com/queen-mq/queen/releases)**

---

## Latest bug fixing and improvements

- Server 0.16.0: **Push-serialization architecture (commit-ordered `created_at`).** Under high concurrent push to the same partitions, `created_at` (transaction-*start* time) could be assigned out of commit order, letting the wildcard pop cursor `(created_at, id)` advance past a not-yet-committed message — silent loss. The push path now serializes **per partition** (an in-memory "≤1 in-flight push transaction per partition" gate plus a Postgres `pg_advisory_xact_lock` in a dedicated two-int keyspace) and stamps `created_at = clock_timestamp()` under the lock, so per-partition `created_at` is monotonic in commit order. POP and ACK are unchanged. Deterministic repro + proof in `benchmark-queen/2026-06-06-engine-scaling/cursor-repro.sh`.
- Server 0.16.0: **Function-split engine cluster.** libqueen now runs as 3 process-global engines (push/ack, pop, rest) shared by all HTTP workers via `QueenCluster`, replacing the previous one-engine-per-worker model. `NUM_WORKERS` sizes only HTTP I/O; DB concurrency is sized by per-function slots (`QUEEN_PUSH_SLOTS` / `QUEEN_POP_SLOTS` / `QUEEN_REST_SLOTS`). The push engine forms disjoint-partition batches that run concurrently (`QUEEN_PUSH_MAX_PARTITIONS_PER_BATCH`).
- Server 0.16.0: **SIMD JSON on the hot path.** The engine parses Postgres stored-procedure results and assembles batch payloads with **simdjson** (on-demand + DOM parsers), keeping the previous nlohmann/json path as a fallback. Lower CPU and tail latency on push/ack/pop result demultiplexing at 100k+ msg/s.
- Server 0.16.0: **Static data-path concurrency.** push/pop/ack default to static limits (24/16/16). Vegas (RTT-adaptive) mis-reads the hot path — it under-shoots push (high per-commit RTT) and collapses pop (long-poll parking read as PG queuing) — so it is retained only for the auxiliary lanes. Override per lane with `QUEEN_<TYPE>_CONCURRENCY_MODE`.
- Server 0.16.0: **`partition_lookup` coalescing.** Post-push lookup maintenance is batched Nagle-style (at most one flush in flight) instead of one call per push batch, clearing a backlog that appeared under sustained high push.
- Docs 0.16.0: **Architecture docs refreshed.** The developer guide and website now describe the new engine topology, push serialization, and concurrency model (`developer/02-architecture.md`, `04-libqueen.md`, `05-database-schema.md`, `docs/architecture.html`, `server/README.md`, `server/ENV_VARIABLES.md`).
- Proxy 0.15.5: **Traefik forward-auth compatibility (#30).** The Queen proxy can now be used as a Traefik external/forward-auth middleware, not only behind its own login flow.
- Server 0.15.5: **Write-only role (#31).** A new write-only access level lets producers push without being able to read or consume.
- Build 0.15.5: **Hardened Node base image (#32).** The proxy and dashboard images now build on a hardened, minimal Node base.
- Server 0.15.5: **Malformed‑payload hardening.** A request body — or DB‑returned content — containing invalid UTF‑8 or an unpaired UTF‑16 surrogate no longer throws out of the worker event loop. JSON is now serialized with a lenient error handler on every HTTP response and on the libqueen result/callback path, so such input is rejected with a clean **400** instead of crashing the worker (previously a `json.exception.type_error.316` could take a worker down and loop on retry).
- Server 0.15.5: **Defensive error handling on admin/metrics routes.** The shared‑state, partition‑seek, migration‑reset, and `/metrics/prometheus` handlers (including the deferred callback that runs on the event loop) are wrapped in try/catch, so an unexpected exception returns a 500 instead of killing a worker.
- Clients 0.15.0: **Streaming SDK on every runtime.** Ships a fluent `Stream` builder + composable operators (`.map`, `.filter`, `.flat_map`, `.key_by`, `.window_tumbling`, `.window_sliding`, `.window_session`, `.window_cron`, `.reduce`, `.aggregate`, `.gate`, `.to`, `.foreach`) and helper factories (`token_bucket_gate`, `sliding_window_gate`) in JS, Python, and Go. All three packages export the SDK from the same package as the broker client (`queen-mq` on npm/PyPI; `client-go/streams` subpackage in Go) — one install, one import.
- Clients 0.15.0: **Exactly-once cycles via `/streams/v1/cycle`.** State mutations + sink emissions + source acks commit in a single PostgreSQL transaction. On commit failure the entire cycle rolls back; Queen redelivers via the existing lease/retry path.
- Clients 0.15.0: **`.gate()` rate limiter with FIFO preservation.** New per-message ALLOW/DENY operator with persistent per-key state, a partial-ack on deny, and `release_lease=false` so the un-acked tail of the batch is redelivered in original order when the lease expires — no deferred queue, no reordering. The `tokenBucketGate` and `slidingWindowGate` helpers cover all four canonical rate-limit shapes (req/s, msg/s, cost-weighted, sliding-window quota) on every language.
- Clients 0.15.0: **Tumbling, sliding, session, and cron windows.** Per-window `gracePeriod`, `idleFlushMs`, optional `eventTime` extractor with per-partition watermarks (stored under the reserved `__wm__` state key), `allowedLateness`, and `onLate: 'drop' \| 'include'`. The runner emits closed windows on idle partitions via a per-window flush timer.
- Server 0.15.0: **Three new streaming stored procedures.** `streams_register_query_v1`, `streams_cycle_v1`, and `streams_state_get_v1` route through libqueen's existing async pipeline — same uvloop, same connection pool, same metrics attribution. Streaming cycles increment `record_ack_request` / `record_ack_messages` / `record_push_messages_with_queue` so the dashboard's per-queue Ack/s and Push/s charts include streaming throughput.
- Server 0.15.0: **UUIDv7 message IDs in streaming push.** `/streams/v1/cycle` stamps every sink push item with a UUIDv7 server-side, matching the `/api/v1/push` route. Time-ordered IDs preserve partition FIFO order even when batched inserts share a `created_at` timestamp.
- Tests 0.15.0: **75 Python streams tests, 33 Go subtests, 45 JS unit tests pass live.** Plus 11 examples per language ported 1:1 from the JS reference, including a "rate-limiter all canonical models" stress test (100 tenants × 10k messages, 4 runners, ~360 msg/sec aggregate sustained).
- Docs 0.15.0: Added [`use-cases.html`](https://queenmq.com/use-cases.html) landing page and [`use-case-rate-limiter.html`](https://queenmq.com/use-case-rate-limiter.html) with verified end-to-end snippets in JS, Python, and Go.
- Server/App 0.14.3: **Improved frontend.** Further refinements to the dashboard UI and user experience.
- Server/App 0.14.1: **Updated frontend.** New metrics views and an embedded developer guide surfaced directly in the dashboard.
- Proxy 0.14.1: **Google OAuth support.** The proxy now supports Google as an OAuth provider for end-to-end authentication without a custom identity server.
- Server 0.14.1: **Prometheus metrics route.** A `/metrics` endpoint exposes standard Prometheus-compatible metrics for scraping.
- Server 0.14.1: **Significantly optimized lease renewal.** Reduced lock contention and database round-trips on the hot lease-renewal path, lowering tail latency under high consumer concurrency.
- Server/App 0.14.1: **Delete partition and delete messages.** New API and dashboard actions to delete individual partitions or bulk-delete messages from a queue.
- Server and clients 0.14.0: **New dynamic libqueen loop.** Full rewrite of the core scheduling engine — adaptive concurrency controller (TCP-Vegas-style) now drives push, pop, ack, and stats independently. Active DB connections stay at ~2.5 even with a pool of 50 under 104k msg/s peak load. Largely eliminates the PG deadlock mode that appeared under heavy fan-out at high partition counts on 0.12 (occasional deadlocks still observed at 10 001 partitions, all absorbed by file-buffer failover — see [benchmarks](https://queenmq.com/benchmarks.html)).
- Server 0.14.0: **Rewritten stored procedures.** `push_messages_v3`, `pop_unified_batch_v4`, `ack_messages_v2`, and stats procedures redesigned around the new loop. PG memory usage 30–70% lower for equivalent workloads vs 0.12. Pop throughput +80–90% under partition contention.
- Clients 0.14.0: **`maxPartitions` on all clients.** JS, Python, Go, Laravel, and C++ clients expose `maxPartitions` on queue creation and configuration.
- Server 0.14.0: **New frontend.** Redesigned dashboard for the new stats model.
- Server 0.13.0: **New libqueen with adaptive engine.** Per-worker push/ack drain factored into three independently-tuned concerns — batching, concurrency, scheduling — glued by an event-driven orchestrator. Fixes two long-standing bottlenecks: per-commit overhead amortization on small-batch workloads, and the single-slot-per-drain cap on high-fanout workloads. Perf harness numbers: S1 ~6.2k → ~13k pg_ins/s, S3 ~4.7k → ~20k pg_ins/s, PG pinned instead of idle. Design notes in `cdocs/LIBQUEEN_IMPROVEMENTS.md`.
- Server 0.13.0: **New push stored procedure.** `queen.push_messages_v2` rewritten around a temp-table + batched-insert pipeline that feeds cleanly into the adaptive engine. HTTP contract (queued/duplicate/failed) unchanged.
- Server 0.13.0: **New Vue 3 dashboard.** Reworked queues, analytics, DLQ management, and maintenance-mode views. Served by the same C++ acceptor at `/`.
- Server 0.13.0: Added server-stamped `producerSub` to close the impersonation vector from GitHub issue #23. When JWT auth is enabled the server stamps the validated `sub` claim on every pushed message; clients cannot set this field and it is exposed on pop responses and admin message APIs. Schema migration is additive and metadata-only (no table rewrite), safe on tables with millions of rows.
- Clients 0.13.0: All clients (JS, Python, Go, Laravel, C++) expose `producerSub` on popped messages; Go adds a typed `Message.ProducerSub` field.
- Server 0.12.19: Fix bug where seek or cg delete did not delete the watermark.
- Server 0.12.13: Added watermark tracking for efficient wildcard POP discovery — x20 faster pop on high partition count queues.
- Server 0.12.12: Added built-in database migration — stream pg_dump | pg_restore directly from the dashboard, no temp file, selective table groups, row count validation.
- Clients 0.12.2: Added custom `headers` option to JS, Python, and Go clients for API gateway authentication.
- Server 0.12.9: Fixed server crash (SIGSEGV) on lease renewal; added native EdDSA and JWKS JWT authentication (auto-discovery via `JWT_JWKS_URL`).
- Server 0.12.3: Added JWT authentication.
