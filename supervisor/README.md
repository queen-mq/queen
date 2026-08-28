# Queen Supervisor

`queen-supervisor` is the low-memory Rust control plane for Laravel workers
backed by Queen. It is a separate binary from the broker: it starts local
`php artisan queue:work` children, reads group-specific queue depth from Queen,
scales the pools and performs graceful process-group shutdown.

The Laravel package also includes a PHP engine with the same resolved
configuration and control files:

```bash
# Reference engine; one resident Laravel/PHP master.
php artisan queen:supervise

# Low-memory engine; PHP is used once to resolve Laravel configuration.
queen-supervisor --php php --artisan artisan
```

Both engines supervise ordinary Laravel workers. The Queen broker never spawns
or manages PHP processes.

## Configuration and scaling

Configuration lives under `queen.supervisor` in `config/queen.php`. Both engines
use the same resolved v2 contract; inspect its exact, versioned document with:

```bash
php artisan queen:supervisor-config --pretty
```

This inspection form redacts bearer tokens and header values. The PHP engine
resolves the contract in memory; the Rust engine requests its credential-bearing
`--for-engine` form internally.
The one-shot export is capped at 1 MiB and 60 seconds; a Laravel bootstrap that
hangs or emits an unbounded document fails startup instead of leaving a
resident supervisor stuck forever.

Each named supervisor declares its connection, consumer group, ordered queue
list and process limits. The balancing modes are:

- `auto`: dynamically sizes the total pool and assigns workers to queues by
  their pressure.
- `simple`: keeps `processes` workers and spreads them evenly across queues.
- `off`: preserves Laravel queue priority by starting every worker with the
  declared comma-separated queue list. The supervisor forces non-blocking
  reserves for these workers so an empty first queue cannot hide later queues.

The `size` strategy treats pressure as `effectivePending` and targets roughly
`target_jobs_per_process` queued jobs per worker. The `time` strategy estimates
work as `effectivePending × observed runtime` and targets clearance within
`target_clear_seconds`. Workers publish best-effort duration telemetry in the
private state directory; `default_runtime_seconds` is used until samples exist.
Both strategies remain bounded by `min_processes`, `max_processes`,
`balance_cooldown` and `balance_max_shift`.
`scale_down_delay` suppresses short-lived downscales, while
`restart_backoff`, `restart_backoff_max` and `stable_after` bound crash loops.
Scale-up remains immediate; downscale is applied only after the lower target
has stayed valid for the entire delay. Both engines apply capped exponential
backoff to unexpected short-lived exits. The Rust engine additionally opens a
circuit after five consecutive failures and, after its cooldown, admits one
probe until that worker becomes stable. The PHP engine has no open/one-probe
circuit: from the fifth failure onward it keeps using `restart_backoff_max`.
Scale-down sends `SIGTERM` and moves workers to a separately tracked draining
set, so the control loop remains responsive while an in-flight job finishes;
the shared grace deadline still ends with a forced kill when needed. Draining
workers continue to consume `process_limit` capacity, preventing repeated
rebalances from exceeding the configured global safety limit. The Rust engine
targets the worker process group for termination; pause/continue target only
the Artisan leader so job subprocesses never receive Laravel control signals.
The PHP engine targets the Artisan worker process itself.

Runtime files are matched on supervisor, connection and consumer group. The
`time` strategy combines each matching worker's runtime EWMA using a capped
sample weight, so a stale or unrelated pool cannot skew another pool's target.

For broker failover, set a comma-separated endpoint list instead of one URL:

```env
QUEEN_URLS=https://queen-a.internal:6632,https://queen-b.internal:6632
QUEEN_SUPERVISOR_READ_BEARER_TOKEN=read-only-token
```

Both v2 engines read each pool's Queen connection directly from the resolved
`connections` entries and build isolated depth clients from its `urls`,
`bearer_token` and `headers`; they do not reuse a worker's live Laravel client.
`read_bearer_token` can replace the worker token in those entries (and removes
an inherited `Authorization` header), while the other validated custom headers
remain available to depth reads. The read token therefore needs only access to
the queue-depth endpoint and workers keep their normal credentials. A named
connection missing from the v2 contract fails startup. The Rust engine tries
its resolved endpoints in order; both engines move network/5xx failures to
another configured endpoint. Both poll queues in waves of at most 16 concurrent
requests. Multiple endpoints do not make the supervisor active-active: the
single-master rule below still applies.

## Local control

The engine holding `state_directory/supervisor.lock` publishes status and reads
commands from that same private directory. The Artisan control command works
with either engine:

```bash
php artisan queen:supervisor status
php artisan queen:supervisor status --json
php artisan queen:supervisor status --check # suitable for a process health check
php artisan queen:supervisor pause
php artisan queen:supervisor continue
php artisan queen:supervisor terminate
```

Pause sends `SIGUSR2`, continue sends `SIGCONT`, and terminate initiates a
graceful `SIGTERM` shutdown. Remaining workers are killed after
`shutdown_grace`. The Artisan status command also checks the shared lock and
adds `live`; a leftover `running`/`paused` document is reported as `stale` when
no engine owns it.
Status and commands carry a per-start `instance_id`; a delayed command aimed at
a replaced supervisor is discarded. Status includes both an ISO-8601 UTC
`updated_at` value and its `updated_at_epoch` counterpart, plus the number of
workers currently draining.

## Production topology

> **Run exactly one supervisor replica for an application/consumer group.**

The lock is local filesystem exclusion, not distributed leadership. Two
supervisors on different hosts read the same global backlog and can each scale
to the configured maximum. Queen does not yet expose a fenced leader lease for
this control plane, and its expiring KV keys are not a safe substitute.

Worker processes may be numerous; the restriction applies to the supervising
master. In Kubernetes use one replica, no HPA and a `Recreate` deployment
strategy. Mounting `state_directory` from a `ReadWriteOncePod` volume adds
cluster-level single-pod exclusion, but is still defense in depth rather than a
fenced Queen leadership lease. With systemd, run one foreground master with
`Restart=always`; this also makes `queen:supervisor terminate` reload Laravel
configuration under a fresh master. Set the platform stop deadline longer
than `shutdown_grace`, which itself must be longer than every worker timeout.

A minimal systemd service for the Rust engine is:

```ini
[Service]
Type=simple
User=app
Group=app
WorkingDirectory=/srv/app
UMask=0077
ExecStart=/usr/local/bin/queen-supervisor --php /usr/bin/php --artisan artisan
Restart=always
RestartSec=2
TimeoutStopSec=90
```

A complete editable unit is shipped as
[`dist/queen-supervisor.service.example`](dist/queen-supervisor.service.example);
adjust its user and absolute application paths before installing it.

For the PHP engine, replace `ExecStart` with
`/usr/bin/php artisan queen:supervise`. A Kubernetes deployment should pair
`replicas: 1` and `strategy: {type: Recreate}` with a
`terminationGracePeriodSeconds` value greater than `shutdown_grace`.

Both engines currently target Unix. Laravel workers require PHP `pcntl` for
pause, continue and graceful signal handling. The Rust engine also creates a
process group per worker so a forced shutdown cannot leave descendants behind.

## Secrets and state

The ordinary config command redacts bearer tokens and header values. Its
`--for-engine` form contains them. Prefer the one-shot Artisan pipe used by
`--php`/`--artisan`; if a credential-bearing `--config` file is required, write
it with mode `0600` in a non-web-accessible directory and remove it after use:

```bash
umask 077
php artisan queen:supervisor-config --for-engine > /run/queen-supervisor.json
queen-supervisor --config /run/queen-supervisor.json
```

On Unix, every `--config` input must be an owner-owned regular, non-symlink
file with mode `0600` or stricter. These properties are checked on the opened
file descriptor before any JSON is parsed.

Keep `state_directory` writable only by the application user. It contains the
PID lock, local control requests, status, worker PIDs and runtime telemetry; it
must never be served by the web application or included in public artifacts.
The Rust engine requires an absolute, non-root path. It creates a missing final
directory with mode `0700`; a pre-existing directory must already be owned by
the supervisor user with mode `0700` and is never silently re-permissioned.

## Build

Build with Rust 1.88 or newer from this repository, then run the resulting
binary from the Laravel application directory:

```bash
cargo build --release --manifest-path supervisor/Cargo.toml
cd /srv/app
/path/to/queen/supervisor/target/release/queen-supervisor --php php --artisan artisan
```

Tags named `supervisor/vX.Y.Z` publish static Linux `amd64` and `arm64`
tarballs plus SHA-256 files. The release workflow refuses a tag whose version
does not match `supervisor/Cargo.toml` and smoke-tests `--version` and `--help`
before publishing.
