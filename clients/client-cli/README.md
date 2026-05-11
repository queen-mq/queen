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
[GitHub Releases](https://github.com/smartpricing/queen/releases) for
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
queenctl login --method password -u alice
queenctl login --method google
queenctl login --method token              # paste a JWT
queenctl logout
```

`--method password` and `--method google` flow through the
[Queen Proxy](../../proxy/README.md) which issues the JWT.
`--method token` pastes any JWT (works with external IdPs / JWKS).

## Command index

| Group | Commands |
|---|---|
| Top-level | `ping` `version` `status` `lag` `tail` `push` `pop` `ack` `apply` `replay` |
| Resources | `queue [list\|describe\|configure\|delete\|clear\|stats]` `partition [list\|describe\|seek\|clear]` `messages [list\|get\|delete\|retry\|dlq\|traces]` `cg [list\|describe\|lag\|seek\|delete\|refresh-stats]` `dlq [list\|describe\|requeue\|drain]` `namespace list` `task list` |
| Ops | `tx -f` `lease extend` `maintenance [get\|on\|off]` `metrics [--prometheus]` `analytics [overview\|queue-lag\|queue-ops\|queue-parked\|retention\|system\|worker\|postgres]` `traces [names\|by-name\|by-message]` `bench` |
| Plumbing | `config [view\|get-contexts\|use-context\|set-context\|delete-context]` `login` `logout` `completion` `docs` |

Run `queenctl <command> --help` for full flag descriptions.

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
  smartnessai/queen-mq:0.14.3

cd clients/client-cli
QUEEN_E2E=1 QUEEN_SERVER=http://localhost:6632 \
  PG_HOST=localhost PG_USER=postgres PG_PASSWORD=postgres \
  QUEEN_RETENTION_INTERVAL_MS=2000 \
  make e2e
```

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
