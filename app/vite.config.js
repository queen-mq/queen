import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// Local development points at the PROXY, not the broker: auth, tenancy, role
// checks and 429s all live there, and they are exactly what the app has to
// behave correctly against. QUEEN_DEV_UPSTREAM=http://localhost:6632 switches
// to broker-direct for debugging the operator escape hatch.
const UPSTREAM = process.env.QUEEN_DEV_UPSTREAM || 'http://localhost:6711'
const upstream = { target: UPSTREAM, changeOrigin: true }

export default defineConfig({
  plugins: [vue(), tailwindcss()],
  // Served at the origin root today; keep it a variable so the same bundle can
  // be mounted under a prefix the way the console is at /console/.
  base: process.env.QUEEN_APP_BASE || '/',
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  build: {
    // The artifact BOTH binaries embed: server/src/handlers/static_files.rs
    // (#[folder = "webapp/dist"]) and proxy/src/webapp.rs
    // (#[folder = "../server/webapp/dist"]). Building anywhere else means the
    // source edit silently does not ship — the bytes are baked in at compile
    // time, so a Rust rebuild is required after every `npm run build`.
    outDir: fileURLToPath(new URL('../server/webapp/dist', import.meta.url)),
    emptyOutDir: true,
  },
  server: {
    port: 4000,
    proxy: {
      '/api': upstream,
      // The session lives here: login, logout, /auth/me.
      '/auth': upstream,
      '/health': upstream,
      '/metrics': upstream,
    }
  }
})
