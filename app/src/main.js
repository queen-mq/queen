import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import './style.css'
// Resolve the colour scheme before the first mount: stored choice, else the OS
// asking for light, else dark. The inline script in index.html has already
// painted the right one; this seeds the reactive ref the toggle reads and
// attaches the OS listener. Called here rather than from a component so it
// runs on every route, including hard reloads of pages with no chrome.
import { initTheme } from './composables/useTheme'
import { ensureIdentity } from './stores/identity'
import { notifyError } from './stores/ui'

initTheme()

const app = createApp(App)

// Backstop for everything the HTTP client and the views do not catch. A
// render error or a stray rejection must not leave the product looking like
// nothing happened.
app.config.errorHandler = (err, _instance, info) => {
  console.error('[vue]', info, err)
  notifyError(err?.message || String(err), 'Interface error')
}

window.addEventListener('unhandledrejection', (event) => {
  const reason = event.reason
  // Aborted requests are a caller decision (unmount, cluster switch).
  if (reason?.name === 'CanceledError' || reason?.code === 'ERR_CANCELED') return
  console.error('[unhandled]', reason)
  notifyError(reason?.message || String(reason), 'Unhandled failure')
})

// Start the identity fetch before the first navigation so the guard usually
// has it in hand; the guard awaits the same in-flight promise either way.
ensureIdentity().catch(() => { /* App.vue renders the boot failure */ })

app.use(router)
app.mount('#app')
