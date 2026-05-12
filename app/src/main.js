import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import './style.css'
// Import for side-effect: forces dark mode and scrubs stale theme preferences.
// Importing here (rather than only from chart components) guarantees the
// side-effect runs on every route, including hard reloads of pages that don't
// use charts (e.g. /messages).
import './composables/useTheme'

const app = createApp(App)

app.use(router)
app.mount('#app')

