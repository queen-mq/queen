# 14 — Release process

A release of Queen is **synchronized across many components**: the broker binary (Docker image), libqueen, and five client SDKs published to four package registries. This page is the checklist + commands.

The version we are tracking lives in `server/server.json`:

```json
{ "name": "queen-mq", "version": "0.14.1.beta.1" }
```

Stable releases drop the `.beta.N` suffix. The same version applies (with one caveat for Go, see below) to every artifact in the same release.

---

## What gets released, and where


| Artifact            | Source                                | Registry                           | Versioning               |
| ------------------- | ------------------------------------- | ---------------------------------- | ------------------------ |
| Broker Docker image | `Dockerfile`                          | Docker Hub: `smartnessai/queen-mq` | `0.14.0`, `latest`       |
| JS client           | `clients/client-js/`                  | npm: `queen-mq`                    | `0.14.0`                 |
| Python client       | `clients/client-py/`                  | PyPI: `queen-mq`                   | `0.14.0`                 |
| Go client           | `clients/client-go/`                  | Go modules (Git tag)               | tag: `client-go/v0.14.0` |
| PHP/Laravel client  | `clients/client-laravel/`             | Packagist: `smartpricing/queen-mq` | `0.14.0`                 |
| C++ client          | `clients/client-cpp/`                 | header-only, in repo               | tracked by repo tag      |
| `queenctl` CLI      | `clients/client-cli/`                 | GitHub Releases (binary + `go install`) | tag: `clients/client-cli/v0.1.0` |
| K8s templates       | `helm/`, `helm_queen/`, `proxy/helm/` (no `Chart.yaml` — not stock Helm charts) | git refs | aligned with broker image |


---

## Versioning policy

- **One version number per release.** All artifacts (except Go path) share the same `MAJOR.MINOR.PATCH`.
- **Pre-releases** use `.beta.N` suffix in `server/server.json` and the equivalent in each ecosystem (npm: `0.14.0-beta.1`, PyPI: `0.14.0b1`, Composer: `0.14.0-beta.1`, Go: `v0.14.0-beta.1`).
- **Patch** for bugfixes that don't change wire format or schema.
- **Minor** for new features, schema additions (always additive), new env vars.
- **Major** for breaking changes — wire format change, schema migration that isn't backward compatible, removal of a route or env var.

The schema is **always additive** between versions (`ADD COLUMN IF NOT EXISTS`, etc.). A new broker version should never require a manual `psql` step on existing data.

---

## Pre-release checklist

Before bumping any version:

- All tests passing locally (libqueen unit + integration, JS, Python, Go)
- Contention benchmark hasn't regressed: `cd lib && make test-contention`, compare with last release notes
- Schema is still idempotent: `DROP SCHEMA queen CASCADE; \q` on a test DB, then start the broker → no errors
- `cdocs/` has a memo for any non-trivial change (push, pop, ack, schema)
- User-facing docs updated (`docs/*.html`, especially `quickstart.html` if env vars changed)
- `README.md` examples still work
- All client READMEs reference the same version

---

## Bumping versions

The version lives in **nine places**. Update them all together (find/replace `0.14.0` is the fast path):

1. `server/server.json` — `version`
2. `clients/client-js/package.json` — `version`
3. `clients/client-py/pyproject.toml` — `version`
4. `clients/client-laravel/composer.json` — `version`
5. `clients/client-go/` — *no file change*; the Go version comes from the Git tag
6. `clients/client-cli/` — *no file change*; the CLI version comes from the `clients/client-cli/v*` Git tag and `-ldflags`
7. `README.md` — Docker run examples (`smartnessai/queen-mq:0.14.0`)
8. `docs/index.html`, `docs/quickstart.html` — same
9. `Dockerfile` — there's no version in here directly, but if you tag the image, ensure CI passes the right `--tag`

A grep before pushing:

```bash
rg "0\.13\.0" --glob '!**/dist/**' --glob '!**/node_modules/**' --glob '!**/vendor/**'
# (looking for stragglers from the previous version)
```

---

## Release order

The order matters for downstream consumers:

1. **Tag the broker** first. `git tag -a v0.14.0 -m "0.14.0"`. Build + push the Docker image.
2. **Publish the JS client** (most downstream consumers expect the npm package to be ready before they upgrade).
3. **Publish the Python client**.
4. **Tag the Go client** (`client-go/v0.14.0` — the path is monorepo-style: see Go's [submodule tagging convention](https://go.dev/ref/mod#vcs-version)).
5. **Publish the PHP client** (Packagist auto-pulls from the Git tag — no separate publish step).
6. **Publish the C++ client** — nothing to publish; consumers use the header from the same tag.
7. **Tag the CLI** (`clients/client-cli/v0.X.Y`). The `Release CLI` workflow (`.github/workflows/release-cli.yml`) runs goreleaser and attaches binaries to GitHub Releases. The directory-prefixed tag is required so `go install github.com/smartpricing/queen/clients/client-cli/cmd/queenctl@vX.Y.Z` resolves.
8. **Update Helm charts** if any default values reference the new image tag.

---

## Per-artifact release commands

### Broker Docker image

The image bundles three things: the C++ broker binary, the Vue dashboard,
and the `queenctl` operator CLI as a static Go binary. Pass
`QUEENCTL_VERSION` and `QUEENCTL_COMMIT` so `queenctl version` inside the
container reports the same string the broker's `/health` endpoint does:

```bash
DOCKER_BUILDKIT=1 docker build \
  --build-arg QUEENCTL_VERSION=$(jq -r .version server/server.json) \
  --build-arg QUEENCTL_COMMIT=$(git rev-parse --short HEAD) \
  -t smartnessai/queen-mq:0.14.0 \
  -t smartnessai/queen-mq:latest .

docker push smartnessai/queen-mq:0.14.0
docker push smartnessai/queen-mq:latest
```

Operators get the CLI for free with no extra install:

```bash
docker exec -it queen queenctl status
docker exec -it queen queenctl tail orders --cg debug --follow
```

`QUEEN_SERVER=http://localhost:6632` is baked into the image so in-container
invocations need no flags. The `.github/workflows/docker-build.yml`
workflow passes both `--build-arg` values automatically from
`server/server.json` + the commit SHA.

### npm (`queen-mq`)

```bash
cd clients/client-js
nvm use 22
npm test                          # one last run
npm publish                       # tag-based by default
```

For a beta:

```bash
npm publish --tag beta
# users install with: npm install queen-mq@beta
```

### PyPI (`queen-mq`)

The repo provides `clients/client-py/publish.sh`:

```bash
cd clients/client-py
source venv/bin/activate
./publish.sh                      # builds wheel + sdist, uploads via twine
```

For a beta, ensure `version = "0.14.0b1"` in `pyproject.toml` and use the same script.

### Go modules

Go module versioning is via Git tags only. The submodule is at `clients/client-go/`, so the tag must be `client-go/v0.14.0`:

```bash
git tag -a client-go/v0.14.0 -m "client-go 0.14.0"
git push origin client-go/v0.14.0
```

After ~5 minutes, `https://pkg.go.dev/github.com/smartpricing/queen/clients/client-go@v0.14.0` will resolve. To force-refresh:

```bash
GOPROXY=https://proxy.golang.org go install \
  github.com/smartpricing/queen/clients/client-go@v0.14.0
```

### `queenctl` CLI

The CLI lives at `clients/client-cli/` with its own `go.mod` (module path
`github.com/smartpricing/queen/clients/client-cli`) and is released on a
`clients/client-cli/vX.Y.Z` tag — directory-prefixed so Go's module proxy can
resolve the submodule, the same convention used for `client-go`:

```bash
git tag -a clients/client-cli/v0.1.0 -m "queenctl 0.1.0"
git push origin clients/client-cli/v0.1.0
```

The push triggers `.github/workflows/release-cli.yml` which:

1. Cross-compiles for linux/{amd64,arm64}, darwin/{amd64,arm64}, windows/amd64
2. Generates checksums and shell-completion scripts
3. Uploads tar.gz / zip archives to GitHub Releases

The version + commit + build date are embedded via `-ldflags` and surfaced
by `queenctl version`. See [15-cli.md](15-cli.md) for the full architecture.

After the tag is published, end users can install with:

```bash
go install github.com/smartpricing/queen/clients/client-cli/cmd/queenctl@v0.1.0
```

### Packagist (`smartpricing/queen-mq`)

Packagist is webhook-driven from GitHub. The flow:

1. Make sure `composer.json` has `"version": "0.14.0"` (or rely on the Git tag — Packagist accepts either).
2. `git tag -a v0.14.0` and push.
3. Packagist receives the GitHub webhook and exposes the new version within ~1 minute.

If the webhook is missing, log in to Packagist and click "Update."

### Kubernetes templates

The `helm/`, `helm_queen/`, and `proxy/helm/` directories contain Helm-shaped templates but **no `Chart.yaml`** — they aren't usable with stock `helm install`. The team's deployment tooling renders them from the per-environment YAML overlays (`helm/dev.yaml`, `helm/stage.yaml`, `helm/prod.yaml`). When releasing:

1. Search the templates for the previous image tag and update to the new one.
2. Update any image tags in the per-env overlay YAML files (the values referenced by `image.tag` or similar).
3. Commit; deployment tooling will pick up the new tag from git.

If you want to convert these to real Helm charts, you'll need to add a `Chart.yaml` and convert overlays to `values.yaml` — that's a separate refactor.

---

## Post-release

- Smoke-test the published packages end-to-end (don't rely on local `npm link` / `pip -e`):
  ```bash
  # JS
  mkdir /tmp/queen-smoke && cd /tmp/queen-smoke
  npm init -y && npm install queen-mq@0.14.0
  node -e "import('queen-mq').then(m => console.log(Object.keys(m)))"

  # Python
  python -m venv /tmp/queen-py && source /tmp/queen-py/bin/activate
  pip install queen-mq==0.14.0
  python -c "from queen import Queen; print(Queen)"

  # Go
  cd /tmp && mkdir queen-go && cd queen-go
  go mod init smoke
  go get github.com/smartpricing/queen/clients/client-go@v0.14.0
  ```
- Update `[docs/](../docs/)` version numbers (search for the previous version)
- Add release notes to GitHub Releases. Highlight: schema additions, new env vars, performance numbers from the contention benchmark, breaking changes.
- Tag the corresponding `cdocs/` memos with the version they shipped in.
- Bump `server/server.json` to the next dev version, e.g. `0.14.1.beta.0`.

---

## Rollback

Schema changes are additive, so **rolling back the broker is always safe** at the database level — the old broker simply ignores the new columns/tables.

To roll back:

1. `docker pull smartnessai/queen-mq:<previous>` and redeploy.
2. Pin client versions to the previous matching version in your downstream applications.
3. **Don't** roll back schema changes. They're idempotent and don't break older brokers.

If a stored procedure was changed in an incompatible way, you can manually re-apply the previous procedure file from the previous tag's `lib/schema/procedures/` against your DB:

```bash
psql -h $PG_HOST -d $PG_DB \
  -f $(git show <prev-tag>:lib/schema/procedures/002d_pop_unified_v4.sql)
```

This is one of the reasons we keep `002`, `002b`, `002c`, `002d` files in the repo — easy to roll one procedure back without rolling back the whole release.

---

## Common release-time mistakes

- **Forgetting to bump one client.** Easy to miss the PHP or C++ client. Run the grep above.
- **Publishing npm / PyPI before pushing the Git tag.** External users cloning at HEAD see a version that "doesn't exist" yet. Always tag first.
- **Publishing the Go module without the `client-go/` prefix in the tag.** Without the prefix, `go get` won't resolve. The error is "no matching versions for query."
- **Image tag mismatch in the dashboard QuickStart.** `docs/quickstart.html` references the Docker tag — search for the previous version there.
- **Schema-incompatible procedure change at a `.PATCH` bump.** Procedure changes usually warrant a `MINOR` bump to signal "if you're rolling back the broker, leave the schema alone."

