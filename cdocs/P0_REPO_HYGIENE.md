# P0 — Repository Hygiene

## 1. Summary

The repository ships a substantial amount of generated, downloaded, or
session-leftover content that should not be tracked in git. The cumulative
overhead is roughly **280 MB** of committed bytes that re-download or
re-build trivially, plus a handful of redundant Markdown files and
deprecated examples that confuse new contributors.

This plan lists every file/directory currently in the index that does not
belong, the corrective `git rm --cached` invocations, and the minimum
`.gitignore` additions to keep them out going forward. It is purely
mechanical: no source-code, schema, or runtime change is involved.

## 2. Motivation

- A 280 MB working tree slows down every clone, every CI checkout, every
IDE indexer, every editor "open folder", and every `git status` walk.
CI cache sizes inflate proportionally.
- Committed `node_modules/` and `.venv/` directories are never
authoritative — `package.json` / `pyproject.toml` are. Their presence
in the index regularly produces "why did node_modules change?" diffs
that have to be reverted by hand.
- Native binaries (`07-rate-limiter`, `08-rate-limiter-stress`) under
`clients/client-go/` are platform-specific (darwin/amd64) and have no
business in a portable repo.
- Eight overlapping `*_COMPLETE.md` / `*_SUMMARY.md` / `BUGFIX_*.md`
files in `clients/client-py/` look like AI-session leftovers; they
duplicate or contradict each other and obscure the canonical
`README.md` / `CLIENT_PY.md`.
- Two helm chart trees (`helm/` and `helm_queen/`) coexist and partially
duplicate. Both are listed in `.gitignore:27-28` yet both are tracked
— a sign the original ignore was added after the files were already
committed.

## 3. Out of scope

- Rewriting git history with `git filter-repo` to physically reclaim
pack space. The repo is small enough that the loose-object savings are
not worth the rewrite cost; new clones will be smaller from the next
commit forward, which is sufficient.
- Changing build scripts, CI workflows, Dockerfile, or any code path
that references the artifacts being removed. Every artifact below is
either generated on demand or trivially reproducible.

## 4. Current state — measured 2026-05-13

### 4.1 Tracked artifacts that should be ignored

Verified via `du -sh` and `git ls-files`:


| Path                                       | Size   | Source of truth                    |
| ------------------------------------------ | ------ | ---------------------------------- |
| `app/node_modules/`                        | 70 MB  | `app/package.json`                 |
| `app/dist/`                                | 1 MB   | `app/` build (Dockerfile stage 1)  |
| `examples/node_modules/`                   | 12 MB  | `examples/package.json`            |
| `proxy/node_modules/`                      | 14 MB  | `proxy/package.json`               |
| `streams/node_modules/`                    | 0 B    | empty directory; untrack           |
| `clients/client-py/.venv/`                 | 160 MB | `clients/client-py/pyproject.toml` |
| `clients/client-py/dist/`                  | 376 KB | `python -m build`                  |
| `clients/client-py/queen_mq.egg-info/`     | 32 KB  | setuptools regenerates             |
| `clients/client-go/07-rate-limiter`        | 8.5 MB | `go build`                         |
| `clients/client-go/08-rate-limiter-stress` | 8.5 MB | `go build`                         |
| 16 × `.DS_Store`                           | ~1 MB  | macOS Finder, never useful         |
| `helm/` or `helm_queen/` (one of)          | ~50 KB | pick the canonical one             |


**Total**: ~280 MB.

### 4.2 Redundant or session-leftover documentation

In `clients/client-py/`:


| File                          | Size  | Recommendation                   |
| ----------------------------- | ----- | -------------------------------- |
| `README.md`                   | 15 KB | **keep** — primary               |
| `CLIENT_PY.md`                | 49 KB | **keep** — long-form reference   |
| `BUGFIX_CONSUMER_HANDLER.md`  | 3 KB  | delete (resolved)                |
| `COMPLETE_SUMMARY.md`         | 22 KB | delete                           |
| `IMPLEMENTATION_COMPLETE.md`  | 9 KB  | delete                           |
| `PUBLISH_TLDR.md`             | 2 KB  | merge into `publish.sh` comments |
| `PYTHON_CLIENT_COMPLETE.md`   | 13 KB | delete                           |
| `PYTHON_CLIENT_DOCS_ADDED.md` | 6 KB  | delete                           |
| `QUICK_TEST_GUIDE.md`         | 1 KB  | merge into `run_tests.sh` header |


### 4.3 Deprecated / redundant examples

In `examples/`:


| File(s)                                                                | Action                       |
| ---------------------------------------------------------------------- | ---------------------------- |
| `16-streaming_deprecated.js`, `17-…`, `18-…`, `19-…`                   | delete (4 files)             |
| `999-sc.js`                                                            | delete                       |
| `26-load.js`, `26-load-pop.js`, `26-load-pop-cluster.js`,              | keep one canonical load test |
| `26-load-pop-clustered.js`, `26-load-clustered.js`, `26-simple-pop.js` | merge or delete duplicates   |
| `optimize-affinity-hash-ring.js` + `…-weighted.js`                     | move to `examples/research/` |


## 5. Plan

### 5.1 Step 1 — extend `.gitignore`

Append to root `.gitignore`:

```gitignore
# OS junk
.DS_Store
Thumbs.db

# Build artefacts
**/dist
**/build
**/__pycache__
**/*.egg-info
**/.venv
**/venv

# Tracked-by-mistake binaries
clients/client-go/07-rate-limiter
clients/client-go/08-rate-limiter-stress
clients/client-go/**/*.test
clients/client-go/examples/**/main

# Node modules at every depth
**/node_modules
```

The current `.gitignore` lists `app/node_modules` and a few specific
paths but no recursive `**/node_modules` rule, which is why
`examples/node_modules` and `proxy/node_modules` slipped in.

### 5.2 Step 2 — untrack already-committed paths

Run from repo root:

```bash
# Dependencies and venvs
git rm -r --cached app/node_modules examples/node_modules proxy/node_modules \
                    streams/node_modules clients/client-py/.venv

# Build artefacts
git rm -r --cached app/dist clients/client-py/dist \
                    clients/client-py/queen_mq.egg-info

# Native binaries
git rm --cached clients/client-go/07-rate-limiter \
                clients/client-go/08-rate-limiter-stress

# OS junk
git ls-files -z '*.DS_Store' | xargs -0 git rm --cached
```

### 5.3 Step 3 — collapse duplicate documentation

```bash
git rm clients/client-py/BUGFIX_CONSUMER_HANDLER.md \
       clients/client-py/COMPLETE_SUMMARY.md \
       clients/client-py/IMPLEMENTATION_COMPLETE.md \
       clients/client-py/PYTHON_CLIENT_COMPLETE.md \
       clients/client-py/PYTHON_CLIENT_DOCS_ADDED.md
```

Move the publish/test instructions inline (one-liner header in the
relevant script):

- `PUBLISH_TLDR.md` → top-of-file comment in `publish.sh`
- `QUICK_TEST_GUIDE.md` → top-of-file comment in `run_tests.sh`
- delete both source files.

### 5.4 Step 4 — pick canonical helm chart

Inspect `helm/` and `helm_queen/`. Keep the more recent one (verify by
`git log --diff-filter=M`), delete the other, document the choice in
`developer/01-getting-started.md` so contributors aren't tempted to
recreate the duplicate.

### 5.5 Step 5 — examples cleanup

```bash
git rm examples/16-streaming_deprecated.js \
       examples/17-streaming-simple_deprecated.js \
       examples/18-streaming_deprecated.js \
       examples/19-streaming-partitioned_deprecated.js \
       examples/999-sc.js
```

Choose **one** canonical load test under `examples/26-load.js`, delete
the rest, mention the move in the example's own header comment so
downstream blog posts that link the old paths can be redirected.

Move research scripts to `examples/research/`:

```bash
mkdir -p examples/research
git mv examples/optimize-affinity-hash-ring*.js examples/research/
git mv examples/analyze-logs.js examples/research/
```

## 6. Validation

The change is correct when all of the following hold:

1. `git status` after a fresh clone → `npm install` → `make all` →
  `python -m venv .venv && pip install -e .` is empty.
2. `du -sh .git` is approximately the same as before (no history
  rewrite intended); `du -sh --exclude=.git --exclude=node_modules  --exclude=.venv .` drops by ≥ 250 MB.
3. CI green (Dockerfile, cli.yml, cpp-server-build.yml all unchanged
  in inputs).
4. `find . -name '.DS_Store' -not -path './.git/*'` returns zero.
5. `clients/client-py/` contains exactly two markdown files at root:
  `README.md` and `CLIENT_PY.md`.

## 7. Risks & rollback

The only practical risk is breaking a developer workflow that
**relied on** a tracked `node_modules` or built artefact (e.g. an
internal CI pipeline that does `git pull` and immediately runs the
broker without `npm install`). Mitigations:

- Send an internal heads-up before the commit lands.
- Keep the change as a single PR titled
`chore: untrack generated artefacts and resolve repo hygiene`
so it can be reverted with one `git revert` if anyone is broken.

There is no schema, runtime, or API impact, so external rollback is
not a concern.

## 8. Effort estimate

Single contributor, no review back-and-forth: **half a day** including
verification on macOS + Ubuntu clone.

## 9. Follow-ups (separate PRs)

- Add a CI lint step that fails the build if any path matching
`**/node_modules` or `**/.DS_Store` is staged. Two-line addition to
`cpp-server-build.yml` or a dedicated `repo-hygiene.yml` workflow.
- Add `.gitattributes` `* text=auto eol=lf` to prevent CRLF surprises
if Windows contributors appear later.

