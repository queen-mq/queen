# queenctl

Operator CLI for [Queen MQ](https://queenmq.com), the partitioned message
queue backed by PostgreSQL. Single static binary built on top of
[`client-go`](../client-go).

```text
queenctl tail orders --cg debug --follow | jq '.data'
queenctl push events --batch 500 --partition-key user_id < events.ndjson
queenctl replay orders --cg analyzer --to '15m ago'
queenctl status                # cluster overview
queenctl lag --min-seconds 30  # alert-friendly view
```

## Install

### Pre-built binaries (recommended)

Each tag `clients/client-cli/vX.Y.Z` ships archives on
[GitHub Releases](https://github.com/queen-mq/queen/releases) for
linux/{amd64,arm64}, darwin/{amd64,arm64} and windows/amd64. Download the
matching `queenctl_<version>_<os>_<arch>.tar.gz` (or `.zip` on Windows),
extract, and drop `queenctl` somewhere on `$PATH`.

### `go install`

```bash
go install github.com/smartpricing/queen/clients/client-cli/cmd/queenctl@latest
```

This installs a `queenctl` binary into `$GOBIN` (defaults to `~/go/bin`).
Requires Go 1.22+.

### From source

```bash
cd clients/client-cli
make build              # ./bin/queenctl
make install            # $GOBIN/queenctl
```

## Quick start

```bash
# 1. Configure a context against a running broker
queenctl config set-context local --server http://localhost:6632

# 2. Ping
queenctl ping

# 3. Push some messages from NDJSON
echo '{"hello":"world"}' | queenctl push demo

# 4. Tail them back
queenctl tail demo --cg dev --follow
```

For a long-form walkthrough see the [website docs](https://queenmq.com/cli.html).

## Configuration

`~/.queen/config.yaml`, kubectl-style:

```yaml
current-context: prod
contexts:
  - name: prod
    server: https://queen.prod.internal:6632
    token-ref: keychain://prod
  - name: local
    server: http://localhost:6632
```

Precedence (lowest -> highest):

1. `current-context` from the config file
2. `$QUEEN_CONTEXT` env var
3. `--context` flag
4. `$QUEEN_SERVER` / `$QUEEN_TOKEN` env vars
5. `--server` / `--token` flags

Tokens are stored in the OS keychain by default; pass `--no-keychain` to
`config set-context` to fall back to a plaintext `literal:` reference.

## Auth

```bash
queenctl login --method password -u alice@example.com
queenctl login --method google              # or --method github
queenctl login --method token               # paste a JWT or a qk_ API key
queenctl logout
```

`--method password`, `google` and `github` flow through
[queen-proxy](../../PLAN_QUEEN_PROXY_CLOUD.md), which mounts its human-identity
endpoints under `/auth` (`/auth/login`, `/auth/google`, `/auth/github`,
`/auth/session-token`). The legacy Node proxy's `/api/login` +
`/api/auth/config` are still tried as a fallback, so one binary works against
both generations.

| method | what gets stored | lifetime |
|---|---|---|
| `password` | the proxy's session JWT, read from the `Set-Cookie` of `/auth/login` | `QUEEN_PROXY_JWT_TTL_S` (24h default) |
| `google` / `github` | the bearer printed by `/auth/session-token`, pasted from the browser | 15 minutes |
| `token` | whatever you paste - a JWT from any IdP, or a `qk_` cluster API key | API keys do not expire |

The browser flows cannot capture the session themselves: the proxy's session
cookie is httpOnly and it only accepts same-origin relative redirect targets,
so there is no loopback callback for the CLI to listen on. They land the
browser on `/auth/session-token` instead and ask you to paste the `token`
field. **For unattended use (CI, daemons) create a cluster API key and store
it with `--method token`, or set `$QUEEN_TOKEN`.**

### Operator-only surfaces

queen-proxy fails closed on broker routes that cannot be scoped to a single
tenant, and answers them with a 404. The affected commands report that
explicitly and name what to run instead:

| command | blocked route | use instead |
|---|---|---|
| `status` (no queue) | `/api/v1/status` | `queue list`, or `status <queue>` |
| `metrics [--prometheus]` | `/metrics`, `/metrics/prometheus` | `analytics queue-ops` / `queue-lag` |
| `analytics system\|worker\|postgres` | `/api/v1/analytics/*-metrics`, `/postgres-stats` | `analytics queue-ops` / `queue-lag` |
| `maintenance [get\|on\|off]` | `/api/v1/system/maintenance` | operator credentials against the broker |
| `cg refresh-stats` | `/api/v1/stats/refresh` | nothing - the broker refreshes on its own interval |
| `pop --namespace/--task` (no queue) | `/api/v1/pop` | name a queue |

All of them work normally when the context points straight at a broker.

## Command index

| Group | Commands |
|---|---|
| Top-level | `ping` `version` `status` `lag` `tail` `push` `pop` `ack` `apply` `replay` |
| Resources | `queue [list\|describe\|configure\|delete\|clear\|stats]` `partition [list\|describe\|seek\|clear]` `messages [list\|get\|delete\|retry\|dlq\|traces]` `cg [list\|describe\|lag\|seek\|delete\|refresh-stats]` `dlq [list\|describe\|requeue\|drain]` `namespace list` `task list` |
| Ops | `tx -f` `lease extend` `maintenance [get\|on\|off]` `metrics [--prometheus]` `analytics [overview\|queue-lag\|queue-ops\|queue-parked\|retention\|system\|worker\|postgres]` `traces [names\|by-name\|by-message]` `bench` |
| Plumbing | `config [view\|get-contexts\|use-context\|set-context\|delete-context]` `login` `logout` `completion` `docs` |

Run `queenctl <command> --help` for full flag descriptions.

### Not in queenctl: key/value and timers

Said here rather than left out, because an omission reads as an oversight. The broker's key/value
store and its scheduled messages have **no `kv` and no `timer` command in 1.0**, and none is
planned for it. The value of a CLI over those surfaces is inspection rather than writing, and
inspection is already one `curl` away:

```bash
curl -s localhost:6632/api/v1/kv -H 'Content-Type: application/json' \
  -d '{"operations":[{"op":"get","ns":"saga","key":"order-7"}]}'
curl -s localhost:6632/api/v1/timers/reminders          # pending timers of one queue
curl -s localhost:6632/api/v1/timers/reminders/order-7  # one timer, payload included
```

Every broker serves those routes: there is no flag that turns them on. The six language SDKs wrap
them too: see
[queenmq.com/use/kv](https://queenmq.com/use/kv) and
[queenmq.com/use/timers](https://queenmq.com/use/timers).

## Output formats

Auto-detects: table for TTY, JSON for pipes. Override with `-o`:

- `-o table` (humanised)
- `-o wide` (table + columns hidden by default)
- `-o json` (pretty)
- `-o ndjson` (one JSON object per line - composes with jq, `queenctl push`, etc.)
- `-o yaml`
- `-o jsonpath=.queues[0].name` (kubectl-style)

`tail` always emits NDJSON regardless of `-o` so pipelines compose.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | User error (bad flag, bad input, missing arg) |
| 2 | Server error / unreachable |
| 3 | Auth error |
| 4 | Successful no-op (empty pop, no DLQ messages, etc.) |

## Shell completion

```bash
queenctl completion bash       > /usr/local/etc/bash_completion.d/queenctl
queenctl completion zsh        > "${fpath[1]}/_queenctl"
queenctl completion fish       > ~/.config/fish/completions/queenctl.fish
queenctl completion powershell > queenctl.ps1
```

## Building

```bash
make build       # ./bin/queenctl
make test        # unit tests (no broker needed; ~1s)
make completion  # regenerate ./completions/
make release     # goreleaser snapshot
make e2e         # full end-to-end suite (~60s, needs a running broker)
make e2e-short   # just queue/push/pop/tx (~10s, faster sanity)
```

## Testing

The CLI ships with three layers of testing:

1. **Unit tests** under `internal/*/*_test.go` cover pure logic (config
   round-trip, output formatter, jsonpath, time parser, auth helpers,
   exit-code mapping). `go test ./...` runs them in seconds, no broker
   required.

2. **Quick smoke** (`./tests/smoke.sh`) — a 41-line shell script that
   exercises every CLI subcommand against `$QUEEN_SERVER`. Useful as a
   PR-time gate.

3. **End-to-end suite** at [`tests/`](tests/) — the parity port of
   [`clients/client-js/test-v2/*`](../../clients/client-js/test-v2/) and
   [`clients/client-py/tests/*`](../../clients/client-py/tests/) covering
   queue, push, pop, tail/consume, DLQ, transaction, subscription,
   maintenance, retention, watermark, auth, and load. ~70 Go tests with
   broker side-effect verification (e.g. `consumedBy` invariants for
   `bench`) and DB-side assertions where configured.

Running the full suite locally:

```bash
docker run -d --name qpg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
docker run -d --name queen --network host \
  -e PG_HOST=localhost -e PG_USER=postgres -e PG_PASSWORD=postgres \
  -e RETENTION_INTERVAL=2000 \
  ghcr.io/queen-mq/queen:latest

cd clients/client-cli
QUEEN_E2E=1 QUEEN_SERVER=http://localhost:6632 \
  PG_HOST=localhost PG_USER=postgres PG_PASSWORD=postgres \
  QUEEN_RETENTION_INTERVAL_MS=2000 \
  make e2e
```

> `RETENTION_INTERVAL=2000` (2s) is a **test-only** override so retention tests
> finish in real time; production uses the default `300000` (5 min).

The same suite runs in CI on push to `master`/`cli` and on `workflow_dispatch`
via [`.github/workflows/cli.yml`](../../.github/workflows/cli.yml). Each
push to a regular feature branch only triggers the unit job (matrix
Linux + macOS) for fast feedback.

Filter tests by topic:

```bash
make e2e -- -run TestPush       # just push tests
make e2e -- -run 'TestQueue|TestPop'
make e2e -- -run TestLoad       # heavier load suite
```

Override defaults via env:

| Variable | Default | Purpose |
|---|---|---|
| `QUEEN_SERVER` | `http://localhost:6632` | Broker URL |
| `PG_HOST/PORT/USER/PASSWORD/DB` | (unset → DB-side asserts skipped) | Postgres for direct `consumer_watermarks` / `messages` queries |
| `QUEEN_RETENTION_INTERVAL_MS` | (unset → retention tests skip) | Match the broker's `RETENTION_INTERVAL` env so cleanup tests run in real time |
| `QUEEN_LOAD_TOTAL` | `2000` | Bump to `100000` for parity with the JS load test |
| `QUEEN_TEST_QUEUE_PREFIX` | `ct-e2e-<unix-ts>` | Override per-test queue prefix when sharing one broker |

## License

[Apache 2.0](LICENSE.md), same as the rest of Queen MQ.
