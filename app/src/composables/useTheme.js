// Theme bootstrap. The dashboard is dark-only: light mode has been retired,
// there is no toggle anywhere in the UI, and no component branches on the
// theme any more (chart colours resolve from :root via useChartTheme.js).
//
// What is left here is load-bearing and side-effect only: pin `.dark` on
// <html> and neutralise a persisted `queen-theme=light` preference so a stale
// cookie or localStorage value from the two-theme era cannot flip the app.
// Imported for effect from main.js; there is nothing to export — the old
// `isDark` ref and the inert toggleTheme/setTheme/initTheme had no call sites
// left once the `isDark ? A : B` chart ternaries collapsed.

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
