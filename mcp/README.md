# Queen MCP server

`https://queenmq.com/mcp`: Queen MQ's MCP server for coding agents. Connected once, it teaches the
agent the Queen 2.0 model and hands it tested code, the trap checklist, error meanings and Kafka client
compatibility while the developer builds. It holds no state, needs no sign-in, and never receives the
developer's code. The install page for every agent is [start/ai-agents](https://queenmq.com/start/ai-agents/).

| What | Where it comes from |
|---|---|
| Instructions, sent to every session | `content/primer.md` + `content/usage.md` |
| `guide` | the per-page markdown of the docs build, `webdoc/dist/**/index.md` |
| `example` | the tested snippets in `webdoc/src/content/partials/snippets/` (cut by `gen-snippets.mjs` from files the test suite runs), the whole programs under `examples/`, and the docs pages' own code blocks, labelled as such |
| `check` | `content/traps.json` (each entry cites a docs page or a repo file; `{{broker}}` becomes the version in `server/Cargo.toml`) |
| `explain_error` | the tables of reference/errors, reference/transaction and reference/limits, `protocols/queen-kafka/compat/ERRORS.md`, and `content/kafka-error-codes.json` (derived from the kafka-protocol crate) |
| `kafka_client` | `protocols/queen-kafka/compat/CLIENT_MATRIX.md` |
| `setup` | `content/setup.json` (feature → docs pages), `content/api-cards.json` (the calls per SDK, each cited to a file and line), the matching traps |
| Install and import lines in every SDK answer | the "Install and connect" section of start/clients |
| Prompts `design`, `review`, `from-kafka` | `content/prompts/*.md` |
| Resources | every docs page, plus the primer at `/mcp/AGENTS.md` |

`scripts/build-bundle.mjs` derives all of it into `src/bundle.generated.mjs`, which the Worker imports.
The protocol (`src/mcp.mjs`) is Streamable HTTP in its stateless JSON form: every POST is answered on
its own, there are no sessions and no SSE stream. No dependencies.

## Run it

```bash
pnpm --dir webdoc build      # the bundle reads webdoc/dist (or set QUEEN_DOCS_DIST)
cd mcp
npm test                     # rebuilds the bundle, then node:test
npm run dev                  # http://localhost:8787/mcp
claude mcp add --transport http queen-local http://localhost:8787/mcp
```

## Deploy

The `docs` workflow deploys it. On master, once the site is out, its `mcp` job takes the build the site
was published from, runs `npm test`, deploys the Worker and checks that the server reports the commit.
A change under `mcp/` starts that workflow too. By hand:

```bash
pnpm --dir webdoc build && npm --prefix mcp run deploy
```

`wrangler.jsonc` deploys the Worker `queen-mcp` with the route `queenmq.com/mcp*`. The docs Worker keeps
the domain; a route takes precedence over a custom domain on the same hostname. It goes out with the docs
so the bundle matches what the site says. Publishing to the MCP Registry: [PUBLISH.md](PUBLISH.md).

## Add a trap

Append an entry to `content/traps.json`: `id`, `title`, `applies` (`all`, `js`, `go`, `python`, `php`,
`rust`, `cpp`, `http`, `kafka`), `severity` (`breaks`, `surprise`, `perf`), `symptom`, `cause`, `fix`,
optional `code` per language, and at least one `source` (a docs URL or a repo path). The build refuses
an entry without them.
