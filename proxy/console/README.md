# queen-proxy cluster console

Slim, dependency-free Vue 3 + Vite SPA (PLAN_QUEEN_PROXY_CLOUD.md §9 "cluster
console" — not the account/billing console). Served by `queen-proxy` itself at
`/console`, embedded into the binary via `rust_embed` from `console/dist`
(see `../src/console.rs`).

## Rebuilding

`console/dist` is checked into the working tree so the crate builds without
npm. After changing anything under `console/src`, rebuild it:

```sh
cd proxy/console
npm install
npm run build
```

This regenerates `console/dist` in place (`vite.config.js` sets
`build.emptyOutDir: true`). Commit the updated `dist/` alongside your source
change — `cargo build` embeds whatever is on disk at compile time, so a stale
`dist/` silently ships stale UI.

## Local dev (hot reload, no embed)

```sh
cd proxy/console
npm install
npm run dev
```

Proxies `/api` and `/auth` to `http://127.0.0.1:6711` (this crate's dev-cell
proxy port, `../scripts/dev-cell.sh up`) — see `vite.config.js`.
