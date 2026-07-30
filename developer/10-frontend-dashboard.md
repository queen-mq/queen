# 10 — Frontend dashboard

The dashboard is a Vue 3 + Vite app under `app/`. It is served by the QUEEN-PROXY at `/`, behind mandatory auth (`queen_proxy/src/webapp.rs`), and by the broker at `http://localhost:6632/` as the port-forward escape hatch (`server/src/handlers/static_files.rs`). Both embed the SAME build output, `server/webapp/dist`, at compile time.

This page is for people changing the dashboard.

For the proxy / auth in front of the dashboard, see [11 — Auth proxy](11-proxy.md).

---

## Layout

```
app/
├── package.json              dependencies + scripts
├── vite.config.js            build config + dev proxy to broker
├── tailwind.config.js
├── postcss.config.js
├── index.html                Vite entry
├── public/                   static assets copied as-is
├── src/
│   ├── main.js               app entry
│   ├── App.vue
│   ├── style.css             global tailwind layer
│   ├── router/index.js       vue-router
│   ├── views/                Analytics, Consumers, Dashboard, DeadLetter,
│   │                          Messages, Migration, QueueDetail, Queues,
│   │                          System, Traces (.vue)
│   ├── components/           Header, Sidebar, BaseChart, Sparkline, DataTable,
│   │                          MetricCard, MetricRow, MultiSelect, RowChart,
│   │                          QueueHealthGrid, ConsumerHealthGrid (.vue)
│   ├── composables/          useApi, useTheme, useChartTheme, useProxy, useRefresh
│   ├── stores/               Pinia stores
│   └── api/
│       ├── client.js         axios instance + interceptors
│       └── index.js          per-resource wrappers
├── dist/                     build output (gitignored, copied into the binary)
└── README.md
```

Stack:

- **Vue 3** (Composition API)
- **Vite**
- **Tailwind CSS**
- **Chart.js** for charts
- **Vue Router**
- **Axios** for HTTP

---

## Setup

```bash
cd app
nvm use 22
npm install
```

> The `app/README.md` mentions "Node.js 18+" — that's stale. The Dockerfile (`node:22-alpine`) and the rest of the repo use Node 22, and so should you for local development.

You need a Queen broker running on `http://localhost:6632`. The Vite dev server proxies API calls to it (see `vite.config.js`).

---

## Dev loop

```bash
cd app
nvm use 22
npm run dev
```

App is available at `http://localhost:5173`. Vite proxies `/api/*` → `http://localhost:6632/api/*` so the dashboard talks to your local broker.

If you're running the broker on a different port:

```bash
# In one shell
PORT=7777 ./bin/queen-server

# In another, override the proxy target
VITE_QUEEN_URL=http://localhost:7777 npm run dev
```

Or create `app/.env`:

```env
VITE_QUEEN_URL=http://localhost:7777
```

---

## Build

```bash
cd app
nvm use 22
npm run build
```

Output goes to `server/webapp/dist` (set in `app/vite.config.js`) — the one folder both Rust binaries embed with `rust_embed`:

* `server/src/handlers/static_files.rs` — `#[folder = "webapp/dist"]`
* `queen_proxy/src/webapp.rs` — `#[folder = "../server/webapp/dist"]`

Because the bytes are compiled in, a source change ships only after `npm run build` AND a rebuild of whichever binary serves it. Debug builds of rust-embed read from disk, so locally the npm build alone is usually enough; release builds are not. The `Dockerfile` runs the npm build in its own stage and copies the result into the Rust build stage for exactly this reason.

---

## Where things live


| What                              | File                              |
| --------------------------------- | --------------------------------- |
| Top-level routing                 | `src/router/index.js`             |
| API client (axios + interceptors) | `src/api/client.js`               |
| Per-resource API wrappers         | `src/api/index.js`                |
| Theme switcher                    | `src/composables/useTheme.js`     |
| Auto-refresh helper               | `src/composables/useRefresh.js`   |
| Pinia stores                      | `src/stores/`                     |
| Reusable chart base               | `src/components/BaseChart.vue`    |
| Sparkline                         | `src/components/Sparkline.vue`    |
| Queue list page                   | `src/views/Queues.vue`            |
| Single queue detail               | `src/views/QueueDetail.vue`       |
| Message browser                   | `src/views/Messages.vue`          |
| Consumer groups                   | `src/views/Consumers.vue`         |
| Trace timeline                    | `src/views/Traces.vue`            |
| Dead-letter queue                 | `src/views/DeadLetter.vue`        |
| Analytics overview                | `src/views/Analytics.vue`         |
| System health                     | `src/views/System.vue`            |
| Migration tooling                 | `src/views/Migration.vue`         |


---

## Talking to the broker

All HTTP calls go through the axios instance in `src/api/client.js`, with per-resource wrappers in `src/api/index.js`. Endpoints used by the dashboard (defined in `server/src/routes/`):


| UI page            | Backend endpoint                  |
| ------------------ | --------------------------------- |
| Queue list         | `GET /api/v1/status/queues`       |
| Queue detail       | `GET /api/v1/status/queues/:name` |
| Messages           | `GET /api/v1/messages`            |
| Traces             | `GET /api/v1/traces/:id`          |
| Consumers          | `GET /api/v1/consumer-groups`     |
| Resources/system   | `GET /api/v1/resources/overview`  |
| Prometheus metrics | `GET /metrics` (text format)      |
| Health             | `GET /health`                     |


When you add a new route on the broker, add the corresponding wrapper in `src/api/index.js` and reuse the axios instance from `src/api/client.js` so interceptors (auth, error handling) apply.

---

## Theming

Two themes (dark + light) using Tailwind's class strategy. The toggle persists to `localStorage` and falls back to `prefers-color-scheme`. Don't add custom CSS files — extend `tailwind.config.js` with new colors / spacing instead.

The brand palette (magenta / cyan / gold inspired by the logo) lives in `tailwind.config.js`. Reuse those tokens; don't hard-code hex values in components.

---

## Common changes

### Add a new chart

1. Reuse `src/components/BaseChart.vue` (Chart.js wrapper) for the chart itself, or `Sparkline.vue` for a small inline trendline.
2. If the chart needs new aggregate data, add the SQL query (or extend an existing one) in `lib/schema/procedures/013_stats.sql` (system stats) or `017_retention_analytics.sql` (retention).
3. Expose the data via `server/src/routes/<topic>.cpp`.
4. Wrap the call in `src/api/index.js` and consume from a Pinia store under `src/stores/` if multiple components need it.

### Add a new page

1. Create `src/views/MyPage.vue`.
2. Add a route in `src/router/index.js`.
3. Add a sidebar entry in `src/components/Sidebar.vue`.
4. Use existing layout primitives (`<DataTable>`, `<MetricCard>`, `<MetricRow>`, `<MultiSelect>`, …) — don't introduce a new design language for one page.

### Add a settings/config knob

1. The broker's per-queue settings live in `queen.queues`. Add the column in `lib/schema/schema.sql` (with `IF NOT EXISTS`).
2. Expose it via `server/src/routes/configure.cpp`.
3. Add the form field in `src/views/QueueDetail.vue` (settings live there) — there is no separate `QueueSettings.vue`.
4. Test that fresh-installs and existing-installs both work — the new column should be additive.

---

## Pitfalls

- **CORS** — when running the dev server (`:5173`) against a broker that's behind the auth proxy, the Vite proxy handles same-origin behaviour. If you bypass it (e.g. talk directly to a remote broker), you'll need to add CORS config there.
- **Production mounting** — the dashboard is served at `/` by the broker, so all asset URLs must be relative or root-relative. Vite's default config does this, but if you switch to a CDN-style absolute URL, the in-binary serving breaks.
- **WebSocket / SSE** — some real-time updates use `EventSource` or WebSocket. These go through the same `/api/`* prefix; the auth proxy passes through `Upgrade` headers. If you add a new realtime endpoint, verify it works behind the proxy too.
- **Bundle size** — the dashboard ships *inside* the broker binary's Docker image. Avoid pulling in heavyweight UI frameworks. Chart.js is the largest dep we tolerate.

