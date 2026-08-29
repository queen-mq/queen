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
Startup and recovery establish `processes` in `simple` mode, or at least
`min_processes` otherwise, without spreading baseline capacity across several
cooldown windows. `balance_max_shift` limits subsequent elastic changes.
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
rebalances from exceeding the configured global safety limit. Both engines
send graceful termination only to the Artisan leader, allowing an active
lease-renewal helper to protect the in-flight job; a forced kill targets the
whole worker process group in the Rust engine. The PHP engine targets the
Artisan worker process itself.

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

Pause gracefully drains the current worker generation and starts no replacement
until `continue`; this prevents a prefetched, leased tail from becoming stale
during an unbounded operator pause. Continue reconciles fresh workers, while
terminate initiates a graceful `SIGTERM` shutdown. Remaining workers are killed
after `shutdown_grace`. The Artisan status command also checks the shared lock and
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
WorkingDirectory=/srv/queen-app/current
UMask=0077
ExecStart=/srv/queen-app/current/vendor/bin/queen-supervisor --php /usr/bin/php --artisan artisan
Restart=always
RestartSec=2
KillMode=mixed
SendSIGKILL=yes
TimeoutStopSec=90
```

`KillMode=mixed` sends the initial `SIGTERM` only to the supervisor master, so
the master can drain each worker without prematurely killing its lease-renewal
helper; systemd still applies the final `SIGKILL` to the whole cgroup after the
deadline. Keep `SendSIGKILL=yes`, and set `TimeoutStopSec` strictly above the
configured `shutdown_grace`.

A complete editable unit is shipped as
[`dist/queen-supervisor.service.example`](dist/queen-supervisor.service.example);
adjust its user and absolute application paths before installing it.

For the PHP engine, replace `ExecStart` with
`/usr/bin/php artisan queen:supervise`. A Kubernetes deployment should pair
`replicas: 1` and `strategy: {type: Recreate}` with a
`terminationGracePeriodSeconds` value greater than `shutdown_grace`.

Both engines currently target Unix. Laravel workers require PHP `pcntl` for
pause, continue and graceful signal handling; the native installer/launcher
also requires PHP `posix` to enforce file ownership. The Rust engine creates a
process group per worker so a forced shutdown cannot leave descendants behind.
If `queen-supervisor` is the container entrypoint, use a real init process
(Docker `--init`, Compose `init: true`, tini or an equivalent reaper). This is
required when lease renewal is enabled because its helper deliberately leaves
the worker group during graceful drain and may be orphaned briefly after a
forced kill.

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
Both engines require an absolute, non-root path. They create a missing final
directory with mode `0700`; a pre-existing directory must already be owned by
the supervisor user with mode `0700` and is never silently re-permissioned.
Every parent must already exist and be owned by root or the supervisor's
effective UID. A group/world-writable parent is rejected unless it is sticky
and owned by root or that UID (root-owned `/tmp` is supported); its child must
also be root- or effective-UID-owned. The state leaf cannot be a symlink.
Trusted symlink ancestors are checked before they are resolved and then
canonicalized once. Acquisition pins the leaf device/inode, and all subsequent
control/status operations fail closed if that path is renamed or replaced.
The PHP engine, local CLI control state and Laravel dashboard require
`ext-posix` to enforce the same ownership policy. A typical Laravel `storage`
directory using mode `0775` is therefore not a safe parent: configure a private
`0700` parent or another trusted absolute state path.

## Build

Build with Rust 1.88 or newer from this repository, then run the resulting
binary from the Laravel application directory:

```bash
cargo build --release --manifest-path supervisor/Cargo.toml
cd /srv/app
/path/to/queen/supervisor/target/release/queen-supervisor --php php --artisan artisan
```

The native crate intentionally lives at repository root under `supervisor/`:
it has a Rust toolchain, lockfile and release lifecycle independent from
Composer, and is not PHP-autoloadable source. Distribution is nevertheless
coordinated with the Laravel package. Composer installs the launcher and
`queen:supervisor-install`; that installer selects a native binary from the
manifest for the exact `SupervisorBinary::VERSION`. CI refuses a release unless
that version, the tag, `Cargo.toml` and `Cargo.lock` all agree.

The installer pins its verified installation-base inode before it creates the
lock, temporary files or published files; its version smoke and the Composer
launcher additionally pin the target directory. The intermediate version
directory must be a real owner-owned directory without shared write access;
the installer rechecks its device/inode and the target's around publication.
They compare device/inode, ownership and modes and execute through a relative path. This closes
ancestor-rename substitution between verification and exec on Unix; processes
already running under the same effective UID remain trusted.
The install base cannot be a filesystem root or contain parent traversal. Its
immediate parent must already exist as a real directory and is pinned before
Queen creates the final base component. Point a custom install path at the real
target when the application's `storage/` directory is itself a symlink.

Tags named `supervisor/vX.Y.Z` publish these precompiled archives:

| Operating system | amd64 / x86-64 | arm64 / Apple Silicon |
| --- | --- | --- |
| Linux | `x86_64-unknown-linux-musl` | `aarch64-unknown-linux-musl` |
| macOS | `x86_64-apple-darwin` | `aarch64-apple-darwin` |
| Windows | Not supported | Not supported |

Windows is deliberately absent rather than redirected to an incompatible
archive: the current implementation depends on Unix signals, advisory locks,
file ownership/modes and process groups. It needs native Windows process and
locking backends before a Windows asset can be published safely.

Each release includes `queen-supervisor-manifest.json`, a canonical JSON v1
document containing the version, target, HTTPS URL and SHA-256 digest of every
archive. It has no generated timestamp, so identical inputs produce identical
bytes suitable for detached signatures. The workflow signs it keylessly with
Sigstore (`queen-supervisor-manifest.sigstore.json`), publishes a SHA-256
sidecar, and records GitHub build-provenance attestations for both the manifest
and every archive. Archives normalize entry order, permissions, ownership,
timestamps and gzip headers and are packaged twice with a byte comparison
before publication. Every native binary is also built twice from a clean target
directory and compared byte for byte. The Rust toolchain, Linux build image and
Alpine build dependency are pinned; the macOS deployment target and linker
reproducibility flags are fixed; all Cargo dependencies remain locked.
