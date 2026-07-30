# Queen Dashboard

A beautiful, modern dashboard for monitoring and managing Queen message queues.

## Features

- **Modern UI** — monochrome dark (light mode is retired)
- **Real-time Charts** — Live throughput, latency, and resource monitoring
- **Queue Management** — View, search, configure, and manage queues
- **Queue Operations** — Push / pop / ack / transaction inspector (`QueueOperations.vue`)
- **Message Browser** — Browse, search, retry, and manage messages
- **Dead Letter Queue** — Dedicated DLQ inspector and re-queue tools
- **Consumer Groups** — Monitor consumer health, lag, and subscriptions
- **Message Tracing** — Cross-message trace timeline viewer
- **Analytics** — Per-queue / per-cg performance insights
- **System Monitoring** — cell-level server health, memory, CPU, worker status, PostgreSQL stats (operators only)

## Tech Stack

- **Vue 3** - Progressive JavaScript framework
- **Vite** - Lightning fast build tool
- **Tailwind CSS** - Utility-first CSS framework
- **Chart.js** - Beautiful responsive charts
- **Vue Router** - Client-side routing
- **Axios** - HTTP client

## Getting Started

### Prerequisites

- Node.js 22+ (the rest of the monorepo targets Node 22; we recommend using nvm)
- npm

### Installation

```bash
# Navigate to the app directory
cd app

# Install dependencies
npm install

# Start development server
npm run dev
```

The app will be available at `http://localhost:4000`, proxying `/api`, `/auth`,
`/health` and `/metrics` to the queen-proxy dev cell on `:6711`
(`queen_proxy/scripts/dev-cell.sh up`). Set `QUEEN_DEV_UPSTREAM=http://localhost:6632`
to talk to a broker directly instead — that mode exercises no auth, no tenancy,
no role checks and no 429s, so do not develop against it by default.

### Build for Production

```bash
npm run build
```

The output goes straight to `server/webapp/dist` — the ONE artifact both Rust
binaries embed at compile time:

* `server/src/handlers/static_files.rs` — `#[folder = "webapp/dist"]`
* `queen_proxy/src/webapp.rs` — `#[folder = "../server/webapp/dist"]`

Because the bytes are baked in, **a source change ships only after
`npm run build` AND a `cargo build` of whichever binary serves it.** Debug
builds of rust-embed read from disk, so locally the npm build alone is usually
enough; release builds are not.

## Configuration

### Environment Variables

Create a `.env` file in the app directory:

```env
# API Base URL (defaults to '' which uses the proxy)
VITE_API_BASE_URL=

# Optional: Override API endpoint for production
VITE_API_BASE_URL=http://your-queen-server:6632
```

### API Proxy

In development the Vite dev server proxies to the queen-proxy (`:6711`), not the
broker: auth, tenancy, role checks and rate limits all live there. Override with
`QUEEN_DEV_UPSTREAM`. `QUEEN_APP_BASE` sets the router/asset base if the app is
ever mounted under a path prefix.

## Shell contract

The shell owns identity, errors and the acting cluster. Views must not
re-implement any of it.

| Need | Use | Never |
|---|---|---|
| Who am I / what may I show | `useIdentity()` from `@/stores/identity`, `can('read'\|'produce'\|'consume'\|'queueAdmin'\|'operator')` | parse a role, infer from a 403 |
| Acting tenant / cluster | `actingCluster`, `actingTenantSlug`, `actingClusterSlug` | read a header, a hostname |
| Report a local outcome | `useToast()` from `@/composables/useToast` | `alert()` / `confirm()` |
| API failure | catch the `ApiError` (`status`, `code`, `retryAfter`) and render an inline state | swallow it — it is already on the global surface |
| Panel state | `useApi()` from `@/composables/useApi` (`data/loading/error/lastUpdated`) | render `0` for an unknown |
| Polling | `useAutoRefresh(cb)` from `@/composables/useRefresh` | a private `setInterval` |
| Cell-level numbers | `operator.*` in `@/api`, and say "cell" on screen | present them as the tenant's |

`x-queen-act-cluster` is attached by the axios request interceptor, once, for
every `/api/v1/*` call. No view sends it.

## Project Structure

```
app/
├── public/                       # Static assets
├── src/
│   ├── api/                      # API client and endpoints
│   ├── components/               # Reusable Vue components
│   │   ├── BaseChart.vue
│   │   ├── ConsumerHealthGrid.vue
│   │   ├── DataTable.vue
│   │   ├── Header.vue
│   │   ├── MetricCard.vue
│   │   ├── MetricRow.vue
│   │   ├── MultiSelect.vue
│   │   ├── QueueHealthGrid.vue
│   │   ├── RowChart.vue
│   │   ├── Sidebar.vue
│   │   └── Sparkline.vue
│   ├── composables/              # Vue composables
│   │   ├── useApi.js             # panel state: data/loading/error/lastUpdated
│   │   ├── useChartTheme.js
│   │   ├── useRefresh.js         # shell refresh registry + shared ticker
│   │   ├── useTheme.js
│   │   └── useToast.js           # notifications
│   ├── stores/                   # module singletons
│   │   ├── identity.js           # /auth/me, roles, acting cluster
│   │   ├── queuesStore.js        # tenant-keyed queue cache
│   │   └── ui.js                 # global error / toast surface
│   ├── router/                   # routes + role metadata + guard
│   ├── views/                    # Page components
│   │   ├── Analytics.vue
│   │   ├── Consumers.vue
│   │   ├── Dashboard.vue
│   │   ├── DeadLetter.vue
│   │   ├── Messages.vue
│   │   ├── QueueDetail.vue
│   │   ├── QueueOperations.vue
│   │   ├── Queues.vue
│   │   ├── System.vue
│   │   └── Traces.vue
│   ├── App.vue                   # Root component
│   ├── main.js                   # Entry point
│   └── style.css                 # Global styles & design system
├── index.html
├── package.json
├── tailwind.config.js
└── vite.config.js
```

## Design System

### Colors

Tokens live in `src/style.css` — use the CSS variables, not hex literals.

- `--crown-*` — primary accent (white)
- `--ice-*` — info / chart-in (logo cyan)
- `--ember-*` — danger / chart-out (logo pink)
- `--warn-*` — warning, and the operator/cell-level surfaces

### Components

The app includes several reusable components:

- `MetricCard` - Display metrics with icons, trends, and progress bars
- `BaseChart` - Wrapper for Chart.js with theme support
- `DataTable` - Sortable, paginated tables with custom templates
- `Sidebar` - Navigation (derived from route meta + identity), cell health
- `ClusterSelector` - Acting tenant / cluster, present on every route
- `ToastHost` - The one place a failure becomes visible
- `Header` - Search and refresh

## License

Apache 2.0 — see [`../LICENSE.md`](../LICENSE.md).

