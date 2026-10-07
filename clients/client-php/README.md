# Queen for PHP

**A Laravel queue backend, a worker supervisor, and a standalone PHP client for
[Queen MQ](https://queenmq.com).**

Your jobs stay ordinary Laravel jobs. What changes underneath them is the backlog — Redis becomes
the Queen broker and its own replicated log — and the control plane, where Horizon's PHP master
becomes a Rust one.

```bash
composer require queen-mq/php-client
```

[Documentation](https://queenmq.com/guides/laravel/) ·
[Migrate from Horizon](https://queenmq.com/guides/laravel/migrate-from-horizon/) ·
[PHP client reference](https://github.com/queen-mq/php-client/blob/master/REFERENCE.md) ·
PHP 8.3 / 8.4 · Apache-2.0

```text
   Horizon                              Queen
   ───────                              ─────
   dispatch() ──► Redis                 dispatch() ──► Queen
                    │                                     │
   horizon master ──┤  49.1 MiB PHP     queen-supervisor ─┤  7.0 MiB Rust
                    │                                     │
   horizon:work ────┘                   queue:work queen ─┘
```

Resident memory of the master process through a 45-minute soak on a Linux server, from the
[Laravel benchmark](https://queenmq.com/benchmarks/laravel/#the-soak). It is a control-plane
number, not a whole-stack claim: Queen still runs a broker, with prefork a fork server holds the
booted Laravel beside the Rust master, and Horizon runs `horizon:supervisor` beside its own. The PHP
engine's master held 58.5 MiB.

On one 16-vCPU Linux server, with a broker and a Redis that both fsync every write:

- 32 workers completed 2,753 jobs/s of 10 ms jobs against Horizon's 1,124.
- 64 forked workers and their supervisor used 225 MiB against Horizon's 1,967 MiB, about 27 MiB
  less per worker.

Diagnostic results; every lane and its limits are on the
[benchmark page](https://queenmq.com/benchmarks/laravel/).

---

## Why move off Horizon

**One ordered lane per entity, not per shard.** Redis gives you a queue. Queen gives you a FIFO
partition per ordering key — `customer:4471`, `account:9`, `device:aa:bb` — created by the first
push that names it. One customer's jobs never queue behind another customer's.

**The backlog lives on disk, in the broker's replicated log.** The broker answers a push once it is
fsynced — on a three- or five-node cluster, once a majority of the nodes have it on disk. One binary
and one data directory per node, and no external database to run next to it.

**A control plane that is not a Laravel application.** The Rust supervisor loads Artisan once to
resolve configuration, then leaves only Rust and your ordinary `queue:work` processes resident:
7.0 MiB against 49.1 MiB for Horizon's master through a 45-minute soak.

**Smaller workers.** With prefork, Laravel boots once and every worker is forked from it, sharing the
framework and the opcache: a forked worker kept 1.7 MiB of private memory against 28.9 MiB for a
Horizon worker. The memory a job allocates stays the worker's own, so the saving is about 27 MiB
per worker, not a ratio. The Rust master also
renews the workers' leases itself, so prefetching workers need no helper process.

**More jobs per worker.** The acknowledgement of a job and the pop for the next batch can travel
while a job runs, so a worker does not wait for the broker's fsync between jobs.

**Built for Kubernetes.** Coordinated replicas split every pool's target, and a Prometheus endpoint
gives HPA or KEDA the backlog to scale pods on.

**Metrics without a snapshot command.** Per-job-class throughput and runtime, monitored tags and
long-wait alerts are recorded by every worker into the broker, across every host.

**Stay on Horizon** if you need silenced jobs, job lists, batches, Slack or SMS notification routes,
or per-supervisor controls. The honest, itemized comparison is
[Queen or Horizon](https://queenmq.com/guides/laravel/queen-vs-horizon/).

> **Preview.** The queue driver is usable on its own. The supervisor and dashboard are preview
> features and Unix-only. Read
> [Production checks](https://queenmq.com/guides/laravel/supervisors/#before-production) before
> replacing Horizon.

---

## Install

Package discovery registers the service provider, the `Queen` facade and a `queen` queue
connection. You do not have to touch `config/queue.php`.

```bash
composer require queen-mq/php-client
php artisan vendor:publish --tag=queen-config
```

```dotenv
QUEUE_CONNECTION=queen
QUEEN_URL=http://127.0.0.1:6632
QUEEN_QUEUE=default
QUEEN_CONSUMER_GROUP=laravel
```

Nothing changes at the call site.

```php
GenerateInvoice::dispatch($invoiceId);
```

```bash
php artisan queue:work queen --queue=default --timeout=60 --tries=3
```

That is the whole integration. Dispatch, middleware, `--tries`, backoff, `failed_jobs` and the
`JobFailed` event all behave as they do today. Keep your current systemd, Kubernetes or Supervisor
unit — Queen's own supervisor is optional and comes later on this page.

**The one rule that matters:** `retry_after` in `config/queen.php` (90 seconds by default) is the
Queen lease. It must be longer than the worker timeout and longer than your slowest job. A job that outlives its lease gets redelivered
while it is still running.

Add `QUEEN_BEARER_TOKEN` when the broker requires it. Give each application and environment its own
`QUEEN_CONSUMER_GROUP`: two applications sharing one group share one cursor and split the work.

---

## Migrate from Horizon

Redis jobs, Horizon history, metrics and tags do not move. Queen workers cannot drain a Redis
backlog and Horizon workers cannot drain a Queen one, so every safe migration gives each backend an
explicit ownership window.

### Translate the pool configuration

Same concepts, snake_case names, independent implementations.

| `config/horizon.php` | `config/queen.php` | |
| --- | --- | --- |
| `connection: redis` | `connection: queen` | |
| `queue` | `queues` | always an array |
| `balance: auto` | `balance: auto` | dynamic total and per-queue allocation |
| `balance: simple` | `balance: simple` | fixed `processes`, evenly spread |
| `balance: false` | `balance: off` | ordered queue list on every worker |
| `autoScalingStrategy` | `strategy` | `size` or `time` |
| `minProcesses` | `min_processes_per_queue` | Horizon's minimum is per queue; `min_processes` bounds the pool |
| `maxProcesses` | `max_processes` | |
| `balanceMaxShift` | `balance_max_shift` | add `fast_scale_up` to close half the gap per cycle |
| `waits` | `waits` | schedule `queen:check-waits` every minute |
| `tags()`, monitored tags | same `tags()`, Tags page | |
| `horizon:snapshot` metrics | `job_metrics` | live, nothing to schedule |
| `balanceCooldown` | `balance_cooldown` | add `event_driven` to grow as soon as jobs arrive |
| `maxJobs` / `maxTime` | `max_jobs` / `max_time` | worker recycle limits |
| `timeout` `tries` `memory` `sleep` `rest` `force` | same names | |
| `nice` | — | keep OS priority outside Queen |
| array `backoff` | — | the supervisor takes one integer |

Matching names are not matching algorithms. For strict priority such as `high,default`, use
`balance=off` with `prefetch=1`; `auto` allocates by measured pressure, not by queue order.

### Canary beside Horizon

Leave the default connection on Redis while you prove the path.

```php
RebuildSearchIndex::dispatch($tenantId)
    ->onConnection('queen')
    ->onQueue('queen-canary');
```

```bash
php artisan queue:work queen --queue=queen-canary --timeout=60 --tries=3
```

Throughput is not the gate. Verify attempts and backoff, a deliberate failure through Laravel, a
worker killed inside user code, failed-job synchronization, deployment drain, and the broker being
unreachable — with your own jobs.

### Cut over

**Drain, then switch** — simplest ownership boundary, costs a dispatch pause. Stop producers, let
Horizon empty every Redis queue, confirm no reserved job remains, terminate Horizon, deploy
`QUEUE_CONNECTION=queen`, resume.

**Route new, drain old** — no pause. Ship code that sends new jobs to `queen`, run both sets of
workers side by side, watch Redis to zero, then `php artisan horizon:terminate` and remove the
routing flag.

Full runbook, including rollback: [Migrate from Horizon](https://queenmq.com/guides/laravel/migrate-from-horizon/).

---

## Ordering per entity

By default jobs spread deterministically over 64 partitions, so they run concurrently without
creating a partition per job. When a business entity needs its own ordered lane, say so:

```php
use Queen\Laravel\Contracts\QueenPartitionable;

final class RebuildCustomer implements QueenPartitionable
{
    public function __construct(public string $customerId) {}

    public function queenPartition(): string
    {
        return 'customer:' . $this->customerId;
    }
}
```

Every `RebuildCustomer` for one customer now runs in dispatch order, and customers never block each
other. Horizon, on Redis, has no equivalent.

---

## Throughput profile

The defaults keep Laravel's ordinary one-job-at-a-time reserve/delete boundary. They are the right
starting point for a migration. These are keys of `config/queen.php`; the same keys on the `queen`
connection in `config/queue.php` win over them.

| Key | Default | |
| --- | --- | --- |
| `prefetch` | `1` | jobs claimed per broker request, or `'auto'` to size each pop from the jobs' runtime |
| `ack_batch` | `1` | successful jobs committed together |
| `autopilot` | `false` | lets the broker size the pop sweep width instead of `partitions` |
| `block_for` | `0` | long-poll seconds; `0` polls without blocking |
| `bulk_batch` | `100` | bound for `Queue::bulk()`, not for `dispatch()` |
| `lease_renewal` | `false` | keeps the lease alive under a running job |
| `ack_async` | `false` | sends each ACK without waiting; the answer is read after the next job |
| `pop_ahead` | `false` | pops the next batch while the last job of a full batch runs |

Raising prefetch trades round trips for a wider redelivery window: a crash can redeliver the
unflushed batch, and a paused worker can sit on prefetched jobs until the lease expires. So the
connector **rejects `prefetch` above 1 unless `lease_renewal` is `true`**, however the worker was
started. When a worker crashes holding a batch, its lease renewer (the Rust master or the PHP
helper) hands back the jobs it had not started, without an extra attempt; a lost node still charges
one, so keep `tries` at 2 or more with `prefetch` above 1 or `pop_ahead` on.

`autopilot` is off here even though the SDK client enables pop autopilot by default: the
queue driver keeps sending the fixed `partitions` width, at most 64, so an upgrade changes
nothing on its own. Turn it on to let the broker size the sweep width per `(queue, group)` from ready-partition
pressure and ready age. The pop batch stays pinned to `prefetch`. It needs a broker on 1.2 or
later; an older one ignores the parameter and applies its own default width.

```php
// config/queen.php
'prefetch' => 16,
'ack_batch' => 16,
'lease_renewal' => true,
'lease_renewal_interval' => 30,
```

Renewal keeps the lease alive under the active job and fences the worker if it cannot. Under the
Rust supervisor on Linux the master renews the leases of all its workers; elsewhere each worker
starts one small PHP helper, on Unix CLI PHP. Delivery stays at least once either way — handlers
still need idempotency keys. `process_limit` still counts a slot for the helper, which a worker
starts when the master refuses it.
[The safe delivery profile](https://queenmq.com/guides/laravel/#safe).

Requests go over the client's own kept-alive cURL handles, not Guzzle: with 32 workers and empty
jobs on the Linux server, 6,030 jobs/s against 5,441 and 1.03 against 1.41 ms of application CPU
per job.
`QUEEN_SDK_HTTP_TRANSPORT=guzzle` switches back; an HTTP proxy variable does so too.

`ack_async` and `pop_ahead` take the broker's round trip off the worker's path. A failed
asynchronous ACK is reported one job later and the job is delivered again; `pop_ahead` needs
`lease_renewal`, and `ack_async` needs `ack_batch` 1. With both, 32 workers on the Linux server
went from 1,879 to 2,794 jobs/s of 10 ms jobs.
[The fast profile](https://queenmq.com/guides/laravel/#fast).

Keep `prefetch=1` for long jobs, strict per-job acknowledgement, or comma-separated priority queues.

---

## Supervisor

The optional replacement for Horizon's master. Two engines, one configuration, one control
protocol: PHP is the readable reference, Rust is the one you deploy.

```bash
# PHP engine
php artisan queen:supervise

# Rust engine: an explicit, version-pinned deploy step. Composer never downloads it.
php artisan queen:supervisor-install
vendor/bin/queen-supervisor --php php --artisan artisan
```

```php
'supervisor' => [
    'poll_interval'   => 3,
    'shutdown_grace'  => 75, // must exceed every worker timeout
    'state_directory' => storage_path('queen-supervisor'),
    'supervisors' => [
        'jobs' => [
            'connection'            => 'queen',
            'consumer_group'        => 'laravel',
            'queues'                => ['high', 'default'],
            'balance'               => 'auto',   // auto | simple | off
            'strategy'              => 'time',   // time | size
            'min_processes'         => 1,
            'max_processes'         => 20,
            'min_processes_per_queue' => 1,     // every queue stays warm
            'fast_scale_up'         => true,     // close half the gap per cycle
            'target_clear_seconds'  => 60,
            'balance_cooldown'      => 3,
            'balance_max_shift'     => 2,
            'timeout'               => 60,
        ],
    ],
],
```

`strategy=size` sizes the pool from queue depth and `target_jobs_per_process`. `strategy=time`
multiplies depth by observed job runtime to hit `target_clear_seconds`. Both engines cap restart
backoff, open a circuit after five consecutive crashes, and allow one probe after the cooldown.

Each pool reads these keys; the defaults are the ones of the `default` pool shipped in
`config/queen.php`.

| Pool key | Default | |
| --- | --- | --- |
| `balance` | `auto` | `auto`, `simple` or `off` |
| `strategy` | `size` | `size` reads depth; `time` multiplies depth by observed runtime |
| `min_processes` | `1` | floor per pool |
| `max_processes` | `10` | ceiling per pool |
| `target_jobs_per_process` | `10` | jobs per process, `size` strategy |
| `target_clear_seconds` | `60` | drain target, `time` strategy |
| `default_runtime_seconds` | `1` | assumed runtime until samples exist |
| `balance_cooldown` | `3` | seconds between scaling decisions |
| `balance_max_shift` | `1` | processes added or removed per decision |
| `min_processes_per_queue` | `0` | `auto` only: workers every queue keeps without backlog |
| `fast_scale_up` | `false` | close half of the gap to the target per decision |
| `scale_down_delay` | `10` | idle seconds before shrinking |
| `restart_backoff` | `1` | first restart delay |
| `restart_backoff_max` | `30` | backoff ceiling |
| `stable_after` | `60` | seconds before a restarted worker counts as stable |

Two switches of `supervisor` apply to the whole master: `event_driven` (`false`) wakes on new jobs
through a read-only long poll instead of the next poll, and `lease_service` (`true`; Rust engine on
Linux) renews the workers' leases in the master, while `false` keeps one helper per worker. Up to
2.0.0 the Rust master read `QUEEN_SUPERVISOR_LEASE_SERVICE` from its environment; supervisor
0.7.0, pinned by 2.1.0, reads only the key.

Control is engine-independent, through the local state directory:

```bash
php artisan queen:supervisor status --check   # live, plus minimum serving capacity
php artisan queen:supervisor pause
php artisan queen:supervisor continue
php artisan queen:supervisor terminate
php artisan queen:supervisor-config --pretty  # resolved config, credentials redacted
```

> **Several replicas need coordination.** Each master sizes its pools from the whole backlog, so two
> uncoordinated replicas on two hosts both scale to maximum. Set `QUEEN_SUPERVISOR_COORDINATION=true`
> on every replica: they register in the broker's key/value store and each runs an even share of
> every autoscaling pool's target, with `min_processes` and `max_processes` applied per replica.
> Replicas coordinate when their broker, consumer group and queue set match; fixed pools are not
> split. Without coordination, run one replica with a `Recreate` strategy.

**Prefork workers.** `QUEEN_SUPERVISOR_PREFORK=true` boots Laravel once in a fork server and forks
every worker from it, with the same arguments and environment a spawned worker gets. The server
opens no connection before forking, purges database and Redis connections in each child, and
SIGKILLs its workers if the master dies. A failed fork falls back to spawning. A pool's own
`'prefork' => false` spawns that pool's workers while the others fork (2.3.0). Needs `ext-pcntl`
and `ext-posix`. Enable `opcache.enable_cli` with prefork, where the fork server's opcache is shared
by every worker; without prefork, each worker keeps its own copy and opcache costs memory.

- `php artisan queue:restart` makes the master start a new fork server (Laravel 12), so the workers
  forked after a deploy run the new code; `config/queen.php` changes still need
  `queen:supervisor terminate`.
- On Linux the fork server refuses to serve when the booted application runs another thread (a gRPC
  or Kafka extension, an APM agent), and the master spawns workers. It warns about sockets the boot
  left open, which every forked worker would share.
- [When not to use prefork](https://queenmq.com/guides/laravel/supervisors/#when-not-to-use-prefork).

**Monitoring.** The dashboard's Jobs and Tags pages, `queen:check-waits` with the
`LongWaitDetected` event and mail, and a Prometheus endpoint at `/queen/metrics`
(`QUEEN_METRICS_ENABLED`, `QUEEN_METRICS_TOKEN`) are described in
[Monitoring and alerts](https://queenmq.com/guides/laravel/monitoring/).
> [Several replicas](https://queenmq.com/guides/laravel/supervisors/#several-replicas).

Requires Unix with `pcntl` and `posix`. Windows is rejected explicitly rather than left to fail;
WSL runs the Linux artifact. Installer verification, air-gapped installs, Sigstore pinning and the
endpoint-failover read token are covered in
[Worker supervisors](https://queenmq.com/guides/laravel/supervisors/).

### Dashboard

A server-rendered local panel at `/queen`, disabled by default, showing supervisor health, pools,
restart state, sampled depth and failed-job metadata. Each section is its own page (`/queen`,
`/queen/workload`, `/queen/supervisors`, `/queen/failed-jobs`, `/queen/configuration`). Failed jobs
are paged newest first with a keyset cursor, never an `OFFSET` or `COUNT(*)`, so a table with
millions of rows costs the same per page. The Workload page charts jobs completed, failed and
dispatched over the last hour, 6 hours, day or week from the broker's own per-queue counters
(`GET /api/v1/analytics/queue-ops`), so it adds no write to the job path.

```dotenv
QUEEN_DASHBOARD_ENABLED=true
```

The panel refreshes in place with a small packaged script (header and main region only, with a
**Pause auto-refresh** control) and falls back to a `<noscript>` meta refresh without JavaScript.
If the web server answers every `*.css` or `*.js` from `public/` without reaching PHP (a common
static-asset rule), publish the assets. The panel uses each copy only while it matches the package,
and falls back to its own routes otherwise:

```bash
php artisan vendor:publish --tag=queen-assets --force
```

Each failed job opens in a drawer (or as a page at `/queen/failed-jobs/{id}`) that shows why it
failed: the job class, maximum tries, the exception message and the stack trace (paths relative to
the application root), each with a Copy button, but never the payload.

In production it is **deny-by-default even when enabled** until the application defines the ability:

```php
Gate::define('viewQueenDashboard', fn ($user) => $user?->canOperateQueues() === true);
```

Controls are POST-only, CSRF-protected and carry the exact supervisor `instance_id`, so a stale page
cannot command a replaced master. Without remote status, the panel reads one local state directory.
Global backlog analytics and DLQ operations live in the Queen broker dashboard.
[Dashboard reference](https://queenmq.com/guides/laravel/dashboard/).

**Supervisor on another host.** When the dashboard is served by other processes than the supervisor
— Kubernetes web pods and a separate worker pod, for instance — either engine can also publish its
status to the broker's key/value store:

```dotenv
QUEEN_SUPERVISOR_REMOTE_STATUS=true
```

Set it on every supervisor host and on the web hosts. The status goes under
`supervisor.remote_status.key`, one per application and environment, which defaults to a slug of
`APP_NAME` and `APP_ENV` (such as `orders-production`): hosts that share both values agree on it.
Each supervisor instance publishes into its
own slot under the key, so the dashboard lists every host or pod: a live local supervisor first,
then each published one with its host name, and totals over the live ones. Published instances are
**read-only**: pause, continue and terminate stay with `php artisan queen:supervisor` on their own
host. Liveness comes from the published heartbeat alone. When two running masters autoscale the
same queue and consumer group without coordinating, the dashboard warns. The
document is split across `<key>/<instance_id>/head` and `<key>/<instance_id>/chunk/NNNN` in the
`queen-supervisor` namespace, written in one transaction, so it never depends on the key/value value
ceiling. A `<key>/head` document from an earlier release is still read. Publishing is best effort
and budgeted into the heartbeat; a broker outage shows the supervisor as stale and never stops
supervision. The Rust engine publishes the same format from supervisor 0.3.0 (this package pins
0.6.0); 0.2.0 wrote the single `<key>/head` slot.

| Key of `supervisor.remote_status` | Default | |
| --- | --- | --- |
| `enabled` (`QUEEN_SUPERVISOR_REMOTE_STATUS`) | `false` | publish the status document |
| `key` | slug of `APP_NAME` and `APP_ENV` | shared by every supervisor and web host of the application |
| `connection` | `queen` | Queen connection whose broker and credentials are used |
| `namespace` | `queen-supervisor` | key/value namespace |
| `interval` | `poll_interval` | seconds between publishes; a state change publishes at once |
| `ttl` | `2 × heartbeat_timeout`, min 300 | expiry of the published copy |

---

## What Laravel keeps

| Laravel surface | With Queen |
| --- | --- |
| `ShouldQueue`, `dispatch()`, middleware, timeout, `--tries`, backoff | unchanged |
| Delayed dispatch and backoff | Queen timers; release is atomic with the ack |
| `failed_jobs` | authoritative; Queen keeps a DLQ snapshot in sync |
| `queue:retry` / `forget` / `flush` / `prune-failed` | remove the Queen snapshot after a safe handoff |
| Delivery | at-least-once; a lease expiry or crash can redeliver |
| `Queue::size()` / `pendingSize()` / `reservedSize()` | consumer-group depth from the broker |
| `queue:clear` | **not supported** — no atomic clear across ready jobs, live leases and timers |

Retry a Laravel failed job with `queue:retry`, never with the generic Queen `Admin::retryMessage()`:
only the Artisan command moves the payload attempts and both failure indexes together.

---

## Without Laravel

The same package is a plain PHP 8.3 client. No framework, no service container.

```php
use Queen\Queen;

$queen = new Queen('http://localhost:6632');

$queen->queue('orders')->partition('customer-123')->push([
    ['data' => ['orderId' => 1, 'amount' => 100]],
])->execute();

// Without ->each(), the handler is given the whole claimed batch.
$queen->queue('orders')->group('processors')
    ->consume(function (array $messages) {
        foreach ($messages as $message) {
            processOrder($message['data']);
        }
    })
    ->execute();
```

Beyond push and pop, the broker gives every client an atomic transaction that bundles the
acknowledgement with what it causes — the idempotency idiom for a redelivered job:

```php
$result = $queen->transaction()
    ->ack($message)
    ->kv('saga')->putIfAbsent($orderId, ['step' => 'reserved'], [
        'ttlSeconds' => 86400,
        'required'   => true,
    ])
    ->queue('payments')->push([['data' => $charge]])
    ->timers('payments.timeout')->schedule($orderId, 900_000, ['orderId' => $orderId])
    ->commit();

if (($result['reason'] ?? null) === 'kv_precondition') {
    // Somebody already did this one. Nothing was pushed, nothing was acked.
}
```

A lost precondition is the expected outcome of a legitimate redelivery, so `commit()` returns that
verdict instead of throwing. It belongs in an `if`, not in a `catch`, and not in your error metrics.

The rest of the surface — buffered push, multi-partition pop, pop autopilot, the pop that
commits at delivery (`commitOnDelivery()`), conflation, the `KafkaConsumer`-style consumer,
key/value state, timers, the DLQ, the admin API, tracing and wildcard consumption — is documented
with every option in the [PHP client reference](REFERENCE.md).

---

## Configuration

Every key is in the published `config/queen.php`, and
[Map the configuration](https://queenmq.com/guides/laravel/migrate-from-horizon/#map-the-configuration) maps Horizon's settings onto them.
Since 2.1.0 the file reads 20 environment variables, the values that differ per environment or
deployment; every other setting is a plain value in the file, and you can add your own `env()`
where you need one. The
[configuration reference](https://queenmq.com/guides/laravel/configuration/#upgrading-from-200)
maps each variable that 2.0.0 read to its key. The settings worth knowing on day one:

| Setting | What it controls |
| --- | --- |
| `QUEEN_URL` / `QUEEN_URLS` | one endpoint, or a comma-separated list for failover |
| `QUEEN_BEARER_TOKEN` | broker authentication |
| `QUEEN_CONSUMER_GROUP` | the cursor identity; give each application its own |
| `retry_after` | lease seconds; must exceed the worker timeout |
| `QUEEN_PARTITIONS` | default fan-out for jobs without `QueenPartitionable` (64, up to 1024; one worker per stripe at a time) |
| `sync_failed_jobs` | keep `true` so Laravel commands clean the Queen DLQ too |

Behind the Queen proxy, HTTP 429 is retried transparently with jitter and a cap; HTTP 403 is
terminal. Both carry a machine-readable `ErrorCode` on `Queen\Exceptions\HttpException`.

---

## Contributing

The package is developed in the [Queen monorepo](https://github.com/queen-mq/queen) under
`clients/client-php` and mirrored here on every push to `master`. Open issues and pull requests
against the monorepo.

```bash
composer install
vendor/bin/phpunit
```

Apache-2.0. See [LICENSE.md](LICENSE.md).
