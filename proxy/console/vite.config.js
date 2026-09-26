import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// Slim cluster console (PLAN_QUEEN_PROXY_CLOUD.md §9) — served by queen-proxy
// itself at /console, embedded via rust-embed from console/dist (see
// ../src/console.rs). base MUST stay '/console/' so the built asset URLs
// match where the Rust side actually serves them from.
export default defineConfig({
  plugins: [vue()],
  base: '/console/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    // dev-only: proxy API calls to a local broker serving the embedded proxy
    // on :6711 (QUEEN_PROXY_EMBEDDED=true QUEEN_PROXY_PORT=6711) so `npm run
    // dev` works against real data without CORS juggling.
    port: 4001,
    proxy: {
      '/api': { target: 'http://127.0.0.1:6711', changeOrigin: true },
      '/auth': { target: 'http://127.0.0.1:6711', changeOrigin: true },
    },
  },
})
