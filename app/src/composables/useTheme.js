import { ref } from 'vue'

// Cursor theme is dark-only. Light mode has been retired and there is no
// toggle anywhere in the UI. What this module still does is prevent a stored
// `queen-theme=light` cookie / localStorage value from flipping the app, and
// hand the chart components the `isDark` they read (BaseChart, RowChart).
// `toggleTheme`/`setTheme`/`initTheme` are inert and exist only so a stray
// call site cannot throw.

const isDark = ref(true)

// Force dark class on <html>, clean up any lingering light class
document.documentElement.classList.add('dark')
document.documentElement.classList.remove('light')

// Also scrub persisted "light" preference so it doesn't try to bite again
try {
  if (typeof localStorage !== 'undefined' && localStorage.getItem('queen-theme') === 'light') {
    localStorage.setItem('queen-theme', 'dark')
  }
  if (typeof document !== 'undefined') {
    document.cookie = 'queen-theme=dark;path=/;SameSite=Lax;max-age=31536000'
  }
} catch {
  // storage/cookies may be unavailable in SSR or strict contexts; ignore
}

export function useTheme() {
  const toggleTheme = () => {
    // No-op: dark-only. Kept as a stable API so the Header button doesn't break.
  }
  const setTheme = () => {}
  const initTheme = () => {}
  return { isDark, toggleTheme, setTheme, initTheme }
}
