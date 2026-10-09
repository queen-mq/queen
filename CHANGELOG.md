# Changelog

Release history for the Queen MQ server and client SDKs. Full release notes live on
[GitHub Releases](https://github.com/queen-mq/queen/releases).

## 2.2.0

**Server: a queue's memory no longer grows with the messages it keeps.** Every push left a row in
the memory of every node (about 200 bytes: where the append begins and ends, when it was written,
the hash of each `transactionId`), and the row stayed until retention removed the message. A queue
with retention off, or with days of it, held a row for its whole history in RAM, on each node, and
a restart loaded them all again. A row now leaves the store when the queue's dedup window has
passed (`dedupWindowSeconds`, 3,600 s by default, or `completedRetentionSeconds` if longer, and
never less than `QUEEN_RAFT_TXN_WINDOW_MIN_S`, 900 s), whatever the retention. Messages older than
that are read from the queue log, which always held them: a consumer far behind, a group that
subscribes at a past instant, a lookup by timestamp, and retention itself. Memory now follows the
push rate times the window, not the size of the queue. On one 16-core VM, 20,000 single pushes a
second for five minutes with retention off and a 60-second window: 2.1.0 ended with 5.95 million
rows and 1.8 GB, and still held 1.4 GB after the pushes stopped; this build stayed at 1.2 million
rows while they ran and held no row and 0.3 GB 75 seconds after. Consumers at the tail are served
from memory as before: at 300,000 messages a second the push p50 was 6.4 to 6.7 ms on both builds.
The backlog of that run, 5.9 million messages with no row left, was read back by 16 consumers at
about 500,000 messages a second, against about 590,000 on 2.1.0, which read it from memory. While
it was being read, the traffic at the tail kept its 300,000 messages a second, with a push p50 of
about 21 ms for those seconds on both builds. From the disk it is the same on both: with 32 GB of
backlog (48.75 million messages) and the page cache dropped, the consumers read it back at about
365,000 messages a second on this build and 375,000 on 2.1.0, and the broker's own memory stayed
at 0.5 GB against 2.2 GB. A walk over old messages asks the kernel for a chunk of records at
once as soon as one read has waited for the disk, so the reads overlap.
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
recognised it for as long as the queue existed.

**Server: small log files take no memory map.** Every sealed log file kept its index
memory-mapped, and the kernel allows a process 65,530 maps unless `vm.max_map_count` is raised. A
quiet queue's log is sealed ten minutes after its oldest message for as long as the queue gets
one now and then, so a node with many quiet queues holds far more small files than it may hold
maps, and at the limit it could not seal its next file. An index that fits one page (about 80
records) is now read into memory instead of mapped. For the files that are mapped there is a
series, `queen_raft_qlog_index_maps` (mapped, and the limit), and a warning in the log past 70%
of the limit.

**Server: an entry written after a log cut can be read.** A log file is created for a sequence
number: by a roll, for the group about to be written, and by the idle pass, which seals a quiet
file and creates the next one for the writer's next number. When a follower then cut its log back
below that number (a tail a new leader overruled), the empty file kept it, the next entry the log
took carried a lower one, and a search by number skipped the file. The entry was on disk and the
node refused to start: `raft log entries N..N+1 are not all in the queue logs`. A cut now lowers
the first number of an empty file to the cut. Every release since 2.0.0 has this; it needs a quiet
log sealed while the node holds entries that are later overruled, which the default seal age of
600 seconds makes rare. Found by Jepsen on logs sealed after 10 seconds.

**Server: a node starts after an empty queue log was removed half-way.** A queue's log whose files
have all been reclaimed is closed and its directory deleted. The deletion was done in place, and
could stop between the last file and the directory: a crash, or a filesystem that keeps a deleted
file while it is open and then refuses to remove the directory (FUSE, NFS). The directory that
was left, with no file in it, is what a log that lost its records looks like, and the next start
refused it: `it ends at seq 0, below seq N, which this node fsync'd and applied`. The directory is
now renamed to `q<id>.dead` in one step and deleted after that is durable; a start sweeps such
directories, and a snapshot leaves them out. Every release since 2.0.0 has this too. Found by
Jepsen under power loss, where it took every node of a cluster down one after the other.

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
See [Standby cluster](https://queenmq.com/operate/standby/).

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
happens are on [Monitoring](https://queenmq.com/operate/monitoring/).

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
[deploy/s3](https://queenmq.com/guides/s3/); what it writes is
[reference/s3](https://queenmq.com/guides/s3/).

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
