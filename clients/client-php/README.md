# Queen for PHP

**A Laravel queue backend, a worker supervisor, and a standalone PHP client for
[Queen MQ](https://queenmq.com).**

Your jobs stay ordinary Laravel jobs. What changes underneath them is the backlog — Redis becomes
Queen on PostgreSQL — and the control plane, where Horizon's PHP master becomes a Rust one.

```bash
composer require queen-mq/php-client
```

[Documentation](https://queenmq.com/use/laravel/) ·
[Migrate from Horizon](https://queenmq.com/use/laravel/migrate-from-horizon) ·
[SDK reference](https://queenmq.com/reference/sdk/php/) ·
PHP 8.3 / 8.4 · Apache-2.0

```text
   Horizon                              Queen
   ───────                              ─────
   dispatch() ──► Redis                 dispatch() ──► Queen ──► PostgreSQL
                    │                                     │
   horizon master ──┤  65.0 MiB PHP     queen-supervisor ─┤  2.9 MiB Rust
                    │                                     │
   horizon:work ────┘                   queue:work queen ─┘
```

Median proportional set size of the orchestrator alone, from the
[supervisor benchmark](https://queenmq.com/benchmarks/laravel-supervisors). It is a control-plane
number, not a whole-stack claim: Queen still runs a broker and PostgreSQL. The PHP reference master
measures 35.1 MiB.

---

## Why move off Horizon

**One ordered lane per entity, not per shard.** Redis gives you a queue. Queen gives you a FIFO
partition per ordering key — `customer:4471`, `account:9`, `device:aa:bb` — created by the first
push that names it. One customer's jobs never queue behind another customer's.

**The backlog lives in the database you already back up.** Queue state is PostgreSQL rows, inside
your transactions, your replicas, your PITR window. No second durability story for Redis.

**A control plane that is not a Laravel application.** The Rust supervisor loads Artisan once to
resolve configuration, then leaves only Rust and your ordinary `queue:work` processes resident.

**Stay on Horizon** if you need job tags, silencing, retained throughput graphs, long-wait
notifications, or supervisor masters on more than one host. Queen does not have those yet. The
honest, itemized comparison is [Queen or Horizon](https://queenmq.com/use/laravel/queen-vs-horizon).

> **Preview.** The queue driver is usable on its own. The supervisor and dashboard are preview
> features, Unix-only, and one master per application and consumer group. Read
> [Production checks](https://queenmq.com/use/laravel/supervisors#production-checks) before
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
QUEEN_RETRY_AFTER=90
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

**The one rule that matters:** `QUEEN_RETRY_AFTER` is the Queen lease. It must be longer than the
worker timeout and longer than your slowest job. A job that outlives its lease gets redelivered
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
| `minProcesses` / `maxProcesses` | `min_processes` / `max_processes` | |
| `balanceMaxShift` | `balance_max_shift` | |
| `balanceCooldown` | `balance_cooldown` | |
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

Full runbook, including rollback: [Migrate from Horizon](https://queenmq.com/use/laravel/migrate-from-horizon).

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
starting point for a migration.

| Variable | Default | |
| --- | --- | --- |
| `QUEEN_PREFETCH` | `1` | jobs claimed per broker request |
| `QUEEN_ACK_BATCH` | `1` | successful jobs committed together |
| `QUEEN_AUTOPILOT` | `false` | lets the broker size the pop sweep width instead of `QUEEN_PARTITIONS` |
| `QUEEN_BLOCK_FOR` | `0` | long-poll seconds; `0` polls without blocking |
| `QUEEN_BULK_BATCH` | `100` | bound for `Queue::bulk()`, not for `dispatch()` |
| `QUEEN_LEASE_RENEWAL` | `false` | keeps the lease alive under a running job |

Raising prefetch trades round trips for a wider redelivery window: a crash can redeliver the
unflushed batch, and a paused worker can sit on prefetched jobs until the lease expires. So the
connector **rejects `QUEEN_PREFETCH > 1` unless `QUEEN_LEASE_RENEWAL=true`**, however the worker was
started.

`QUEEN_AUTOPILOT` is off here even though the SDK client enables pop autopilot by default: the
queue driver keeps sending the fixed `QUEEN_PARTITIONS` width, so an upgrade changes nothing on its
own. Turn it on to let the broker size the sweep width per `(queue, group)` from ready-partition
pressure and ready age. The pop batch stays pinned to `QUEEN_PREFETCH`. It needs a broker on 1.2 or
later; an older one ignores the parameter and applies its own default width.

```dotenv
QUEEN_PREFETCH=16
QUEEN_ACK_BATCH=16
QUEEN_LEASE_RENEWAL=true
QUEEN_LEASE_RENEWAL_INTERVAL=30
```

Renewal starts one small PHP helper per worker, on Unix CLI PHP, that renews the lease under the
active job and fences the worker if it cannot. Delivery stays at least once either way — handlers
still need idempotency keys. Budget one extra process per renewed worker.
[The safe delivery profile](https://queenmq.com/use/laravel/#start-with-the-safe-delivery-profile).

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

Each pool reads these; the defaults are the ones shipped in `config/queen.php`.

| Variable | Default | |
| --- | --- | --- |
| `QUEEN_SUPERVISOR_BALANCE` | `auto` | `auto`, `simple` or `off` |
| `QUEEN_SUPERVISOR_STRATEGY` | `size` | `size` reads depth; `time` multiplies depth by observed runtime |
| `QUEEN_SUPERVISOR_MIN_PROCESSES` | `1` | floor per pool |
| `QUEEN_SUPERVISOR_MAX_PROCESSES` | `10` | ceiling per pool |
| `QUEEN_SUPERVISOR_TARGET_JOBS` | `10` | jobs per process, `size` strategy |
| `QUEEN_SUPERVISOR_TARGET_CLEAR_SECONDS` | `60` | drain target, `time` strategy |
| `QUEEN_SUPERVISOR_DEFAULT_RUNTIME_SECONDS` | `1` | assumed runtime until samples exist |
| `QUEEN_SUPERVISOR_BALANCE_COOLDOWN` | `3` | seconds between scaling decisions |
| `QUEEN_SUPERVISOR_BALANCE_MAX_SHIFT` | `1` | processes added or removed per decision |
| `QUEEN_SUPERVISOR_SCALE_DOWN_DELAY` | `10` | idle seconds before shrinking |
| `QUEEN_SUPERVISOR_RESTART_BACKOFF` | `1` | first restart delay |
| `QUEEN_SUPERVISOR_RESTART_BACKOFF_MAX` | `30` | backoff ceiling |
| `QUEEN_SUPERVISOR_STABLE_AFTER` | `60` | seconds before a restarted worker counts as stable |

Control is engine-independent, through the local state directory:

```bash
php artisan queen:supervisor status --check   # live, plus minimum serving capacity
php artisan queen:supervisor pause
php artisan queen:supervisor continue
php artisan queen:supervisor terminate
php artisan queen:supervisor-config --pretty  # resolved config, credentials redacted
```

> **One master per application and consumer group.** The state lock stops a second local owner, but
> there is no distributed leader lease yet: two replicas on two hosts both see the whole backlog and
> both scale to maximum. In Kubernetes use one replica with a `Recreate` strategy. Workers can be as
> many as you like — the restriction is on the supervising master.

Requires Unix with `pcntl` and `posix`. Windows is rejected explicitly rather than left to fail;
WSL runs the Linux artifact. Installer verification, air-gapped installs, Sigstore pinning and the
endpoint-failover read token are covered in
[Worker supervisors](https://queenmq.com/use/laravel/supervisors).

### Dashboard

A server-rendered local panel at `/queen`, disabled by default, showing supervisor health, pools,
restart state, sampled depth and bounded failed-job metadata.

```dotenv
QUEEN_DASHBOARD_ENABLED=true
```

In production it is **deny-by-default even when enabled** until the application defines the ability:

```php
Gate::define('viewQueenDashboard', fn ($user) => $user?->canOperateQueues() === true);
```

Controls are POST-only, CSRF-protected and carry the exact supervisor `instance_id`, so a stale page
cannot command a replaced master. The panel reads one local state directory; it is not a multi-host
aggregator. Global backlog analytics and DLQ operations live in the Queen broker dashboard.
[Dashboard reference](https://queenmq.com/use/laravel/dashboard).

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

The rest of the surface — buffered push, multi-partition pop, pop autopilot, conflation, the
`KafkaConsumer`-style consumer, key/value state, timers, the DLQ, the admin API, tracing, wildcard
consumption and the `queen:consume` Artisan command — is documented with every option in the
[PHP SDK reference](https://queenmq.com/reference/sdk/php/).

---

## Configuration

Every key is in the published `config/queen.php` and documented in
[Config keys and env vars](https://queenmq.com/reference/sdk/php/#config-keys-and-env-vars). The
ones worth knowing on day one:

| Variable | What it controls |
| --- | --- |
| `QUEEN_URL` / `QUEEN_URLS` | one endpoint, or a comma-separated list for failover |
| `QUEEN_BEARER_TOKEN` | broker authentication |
| `QUEEN_CONSUMER_GROUP` | the cursor identity; give each application its own |
| `QUEEN_RETRY_AFTER` | lease seconds; must exceed the worker timeout |
| `QUEEN_PARTITIONS` | default fan-out for jobs without `QueenPartitionable` (64) |
| `QUEEN_SYNC_FAILED_JOBS` | keep `true` so Laravel commands clean the Queen DLQ too |

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
