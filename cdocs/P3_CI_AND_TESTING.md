# P3 — CI and Testing Coverage

## 1. Summary

The repository ships substantial test code (`lib/test_suite.cpp` ~95 KB,
`lib/test_contention.cpp` ~45 KB, per-client test directories under
each `clients/client-*/`) but **most of it is not exercised in CI**.
The four GitHub Actions workflows currently in `.github/workflows/`
cover only `queenctl` (Go CLI) tests, a release-branch-only C++ build
without test execution, a release-branch-only Docker build, and the
CLI release pipeline.

This plan defines five additional workflows that close the major
gaps, ordered by impact:

1. PR-triggered C++ test workflow with a real Postgres service.
2. Schema-only smoke workflow (sub-30-second feedback on SQL syntax).
3. Per-SDK matrix workflow (JS, Python, Go, PHP, C++).
4. Streaming SDK end-to-end workflow.
5. Cross-version upgrade workflow (run last release, restart on the
   build under test, verify no message loss / cursor drift).

## 2. Motivation

- The headline 0.15.0 streaming feature is documented in `README.md`
  as having "75 Python streams tests, 33 Go subtests, 45 JS unit
  tests pass live", but no public CI run produces those results.
  Future regressions land silently.
- `cpp-server-build.yml` runs only on `release`, so PRs against
  `master` get **no automated C++ feedback at all**. Developers
  catch broken builds on their own machine, or in code review, or
  not at all.
- `lib/test_suite.cpp` runs against a real PG and catches stored-procedure
  regressions, but it is invoked only by `make test` in `lib/`.
  No CI job calls that target.
- A schema-only smoke (`psql -f schema.sql -f procedures/*.sql`) is
  the cheapest possible regression catch and runs in seconds — adding
  it as a separate workflow gives PRs sub-minute feedback on broken
  PL/pgSQL.
- An upgrade workflow is the only mechanical way to prevent shipping
  a schema change that breaks an existing deployment. The project
  already takes upgrades seriously enough to maintain `upgrade.sh`
  and `upgrade_queen.sh`, so the test exists conceptually but is not
  automated.

## 3. Out of scope

- No change to existing workflows' inputs other than expanding
  trigger paths from `release` to all branches (where appropriate).
- No new test code. This plan adds workflows that **invoke** the
  test code that already exists. A separate plan (P5) covers writing
  new streaming-specific integration tests.
- No move to a different CI provider. GitHub Actions is fine.

## 4. Current state

```
.github/workflows/
├── cli.yml                    queenctl unit + integration (already broad)
├── cpp-server-build.yml       C++ build only, branch=release, no tests
├── docker-build.yml           docker build only, branch=release
└── release-cli.yml            queenctl release artefacts
```

`cli.yml` is the existing reference for "good shape": it triggers on
`push`, `pull_request`, and `workflow_dispatch`, sets up Go, runs
unit tests, and is matrixed across `ubuntu-latest` / `macos-latest`.
The new workflows should follow the same shape.

## 5. New workflows

### 5.1 `cpp-test.yml` — PR-triggered C++ test

```yaml
name: C++ tests

on:
  push:
    branches: [master, refactor]
    paths:
      - 'server/**'
      - 'lib/**'
      - '.github/workflows/cpp-test.yml'
  pull_request:
    paths:
      - 'server/**'
      - 'lib/**'
      - '.github/workflows/cpp-test.yml'

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: postgres
        ports: ['5432:5432']
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 3s
          --health-retries 10
    env:
      PG_HOST: localhost
      PG_PORT: 5432
      PG_PASSWORD: postgres
    steps:
      - uses: actions/checkout@v4
      - name: Install deps
        run: |
          sudo apt-get update
          sudo apt-get install -y build-essential libpq-dev libssl-dev \
                                  zlib1g-dev curl unzip
      - name: Build server
        working-directory: server
        run: make all -j$(nproc)
      - name: Build + run libqueen test_suite
        working-directory: lib
        run: make test
      - name: Build + run libqueen unit tests (no PG)
        working-directory: lib
        run: make test-unit  # added by P2 step 6.5
```

This is the highest-value single addition to the CI surface.

### 5.2 `schema.yml` — schema-only smoke test

```yaml
name: Schema smoke

on:
  pull_request:
    paths:
      - 'lib/schema/**'

jobs:
  apply:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env:
          POSTGRES_PASSWORD: postgres
        ports: ['5432:5432']
        options: >-
          --health-cmd pg_isready --health-interval 5s --health-retries 10
    steps:
      - uses: actions/checkout@v4
      - name: Apply schema
        run: |
          export PGPASSWORD=postgres
          psql -h localhost -U postgres -f lib/schema/schema.sql
          for f in lib/schema/procedures/*.sql; do
            psql -h localhost -U postgres -v ON_ERROR_STOP=1 -f "$f"
          done
      - name: Idempotency check
        run: |
          export PGPASSWORD=postgres
          psql -h localhost -U postgres -f lib/schema/schema.sql
          for f in lib/schema/procedures/*.sql; do
            psql -h localhost -U postgres -v ON_ERROR_STOP=1 -f "$f"
          done
```

Total wall time: ~30 s. Catches PL/pgSQL syntax errors, missing
function dependencies, and accidental non-idempotency before the
slower C++ jobs even start.

### 5.3 `sdk.yml` — per-SDK matrix

One workflow, five jobs, each spinning up the broker via
docker-compose against `postgres:18`:

```yaml
name: SDK tests

on: [push, pull_request]

jobs:
  start-broker:
    # Build the server image once and pass digest to per-SDK jobs.
    runs-on: ubuntu-latest
    outputs:
      image: ${{ steps.build.outputs.image }}
    steps:
      - uses: actions/checkout@v4
      - id: build
        run: |
          docker build -t queen:ci .
          echo "image=queen:ci" >> "$GITHUB_OUTPUT"

  js:
    needs: start-broker
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env: { POSTGRES_PASSWORD: postgres }
        ports: ['5432:5432']
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: '22' }
      - name: Boot broker
        run: |
          docker run -d --name queen --network host \
            -e PG_HOST=localhost -e PG_PASSWORD=postgres \
            ${{ needs.start-broker.outputs.image }}
      - name: Run JS tests
        working-directory: clients/client-js
        run: |
          npm ci
          npm test

  python:
    needs: start-broker
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.8', '3.11', '3.12']
    # … similar shape ending in `pytest`

  go:
    # … `go test ./...` on Go 1.24

  laravel:
    # … `composer test` on PHP 8.3

  cpp-client:
    # … `make test` on the C++ client
```

The matrix can be expanded later (Node 20/22, Python 3.13) as
support widens.

### 5.4 `streams.yml` — streaming SDK end-to-end

This is the most important new workflow because the streaming SDK
is the headline 0.15.0 feature with the lowest existing CI coverage.

```yaml
name: Streaming SDK

on:
  push:
    branches: [master, refactor]
    paths:
      - 'lib/schema/procedures/02*streams*'
      - 'server/src/routes/streams/**'
      - 'clients/client-js/client-v2/streams/**'
      - 'clients/client-py/queen/streams/**'
      - 'clients/client-go/streams/**'
  pull_request:
    paths: [/* same */]

jobs:
  e2e:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        sdk: [js, python, go]
    services:
      postgres:
        image: postgres:18
        env: { POSTGRES_PASSWORD: postgres }
        ports: ['5432:5432']
    steps:
      - uses: actions/checkout@v4
      - name: Boot broker
        run: |
          docker build -t queen:ci .
          docker run -d --name queen --network host \
            -e PG_HOST=localhost -e PG_PASSWORD=postgres queen:ci
          curl --retry 30 --retry-delay 1 http://localhost:6632/health
      - name: SDK ${{ matrix.sdk }} tests
        run: ./scripts/run-streams-tests-${{ matrix.sdk }}.sh
```

The three runner scripts are thin wrappers around the SDK-specific
test invocation (e.g. `npx vitest run streams/`,
`pytest tests/streams/`, `go test ./streams/...`).

P5 covers the test-content side: in particular, an exactly-once
crash test, a `config_hash` cross-language conformance test, and
the `release_lease=false` FIFO-preservation test.

### 5.5 `upgrade.yml` — schema upgrade smoke

```yaml
name: Upgrade

on:
  pull_request:
    paths:
      - 'lib/schema/**'
      - 'server/**'
      - 'lib/**'

jobs:
  upgrade:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18
        env: { POSTGRES_PASSWORD: postgres }
        ports: ['5432:5432']
    steps:
      - uses: actions/checkout@v4
      - name: Boot LAST RELEASE
        run: |
          docker run -d --name queen-old --network host \
            -e PG_HOST=localhost -e PG_PASSWORD=postgres \
            smartnessai/queen-mq:0.15.0
          curl --retry 30 --retry-delay 1 http://localhost:6632/health
      - name: Push 100 000 messages, advance some consumer cursors
        run: node scripts/upgrade-fixture.js
      - name: Stop old, build new, start new on same volume
        run: |
          docker stop queen-old
          docker build -t queen:ci .
          docker run -d --name queen-new --network host \
            -e PG_HOST=localhost -e PG_PASSWORD=postgres queen:ci
          curl --retry 30 --retry-delay 1 http://localhost:6632/health
      - name: Verify
        run: node scripts/upgrade-verify.js
```

`scripts/upgrade-fixture.js` and `scripts/upgrade-verify.js` are new
files under `examples/research/upgrade/` (or a new top-level `tools/`
directory). Verification asserts:

- All previously-pushed `transactionId`s remain readable.
- Consumer-group cursors are still positioned where the fixture left
  them.
- DLQ contents are preserved.
- Stats history is preserved (no truncation).

## 6. Modifications to existing workflows

### 6.1 `cpp-server-build.yml`

Expand `branches: [release]` to `branches: [master, release, refactor]`
and add the same paths-filter as `cpp-test.yml`. Today this workflow
only ever runs on the release branch, which is too late.

### 6.2 `docker-build.yml`

Same trigger expansion. The Docker build is the canonical "does the
whole thing assemble?" smoke and should run on PRs.

## 7. Validation

The new CI surface is correct when:

1. A pull request that breaks `lib/schema/procedures/021_streams_cycle_v1.sql`
   syntax is rejected within 60 seconds by `schema.yml` (and by
   `cpp-test.yml` ~3 minutes later as a deeper signal).
2. A pull request that regresses `pop_unified_batch_v4` semantics
   is caught by `cpp-test.yml` running `lib/test_suite.cpp`.
3. A pull request that ships a JS-side `Stream.gate()` change is
   exercised by `streams.yml` matrix=`js` against a real broker.
4. An upgrade-breaking schema change is caught by `upgrade.yml`.
5. The total CI wall time on a representative PR (~10 changed C++
   files, no schema change) stays under **8 minutes**, by leveraging
   the paths filters so unrelated workflows are skipped.

## 8. Risks & rollback

- **Cost.** GitHub Actions minutes scale with workflow count. Use
  `paths:` filters aggressively (templates above already do this) so
  unaffected PRs skip the heavy jobs.
- **Flakiness.** The matrix workflows boot a real broker; a
  transient docker-pull or PG startup hiccup causes red. Mitigate
  with `--retry` on health probes (templates above) and
  `actions/cache` for built images.
- **Rollback.** Each new workflow is a separate file. If any one
  proves unstable, delete the file in a follow-up commit and the
  rest stay green. There is no infrastructure dependency between
  workflows.

## 9. Effort estimate

- `schema.yml`, `cpp-test.yml`: half a day each.
- `sdk.yml`: 1 day (matrix shape, Docker network, health-check
  retries).
- `streams.yml`: 1 day (mostly writing the three runner scripts and
  ensuring each SDK's existing test command works against a
  Docker-launched broker).
- `upgrade.yml` + `upgrade-fixture.js` + `upgrade-verify.js`: 2 days
  (the test logic itself is straightforward but the verifier has to
  enumerate every guarantee Queen offers across an upgrade).

Total: **~1 working week** to land all five workflows.

## 10. Follow-ups

- Add a CI **quality gate** workflow that fails the build if any of
  the P0 hygiene rules (no `**/node_modules`, no `.DS_Store`) are
  violated. ~10 lines of bash.
- Once `streams.yml` is stable, surface its result as a badge in
  `README.md` next to the existing `License`/`Node`/`Python` badges.
- Add code-coverage upload (`gcov` / `lcov` for C++, `coverage.py`
  for Python) once the test surface is wide enough that a
  percentage is meaningful — premature today.
