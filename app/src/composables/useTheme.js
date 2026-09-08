// Theme. Two schemes — dark (the default and the one the product was designed
// in) and light — selected in this order:
//
//   1. an explicit choice the operator made here, in localStorage
//   2. the OS preference, but only when it asks for LIGHT
//   3. dark
//
// Step 2 is deliberately one-sided. `prefers-color-scheme` has three states
// and `no-preference` is common on Linux and on locked-down corporate
// profiles; treating "not light" as dark keeps every existing user on the
// surface they already have and makes light strictly opt-in — by the OS
// saying so, or by the toggle in the header.
//
// The OS preference is only consulted while nothing is stored. Once someone
// picks a scheme it wins, on this device, until they pick the other one.
//
// The pre-paint script in index.html runs this same resolution before any
// bundle loads, so the first frame is already correct; initTheme() re-runs it
// to seed the reactive ref and attach the media listener.

import { computed, ref } from 'vue'

export const THEME_STORAGE_KEY = 'queen-theme'

const THEMES = ['light', 'dark']

// Pure. The whole policy above, with the two environment reads passed in, so
// it is testable without a DOM and cannot drift from the inline script.
export const resolveTheme = (stored, prefersLight) =>
  THEMES.includes(stored) ? stored : (prefersLight ? 'light' : 'dark')

export const theme = ref('dark')
export const isDark = computed(() => theme.value === 'dark')
export const isLight = computed(() => theme.value === 'light')

const LIGHT_QUERY = '(prefers-color-scheme: light)'

// Storage and matchMedia both throw rather than return null in hardened
// contexts (Safari private mode, `storage-access` denied, SSR). A theme is
// never worth an exception, so every environment read is total.
const readStored = () => {
  try {
    return typeof localStorage !== 'undefined' ? localStorage.getItem(THEME_STORAGE_KEY) : null
  } catch {
    return null
  }
}

const writeStored = (value) => {
  try {
    if (typeof localStorage !== 'undefined') localStorage.setItem(THEME_STORAGE_KEY, value)
  } catch {
    // Preference simply won't survive the reload; the app still works.
  }
}

const mediaQuery = () => {
  try {
    return typeof matchMedia === 'function' ? matchMedia(LIGHT_QUERY) : null
  } catch {
    return null
  }
}

const prefersLight = () => mediaQuery()?.matches === true

// The only place that touches <html>. `color-scheme` is what makes the form
// controls, the scrollbars and the caret flip — the CSS tokens alone leave
// native widgets painted for the wrong scheme.
const applyTheme = (value) => {
  if (typeof document === 'undefined') return
  const root = document.documentElement
  root.classList.toggle('light', value === 'light')
  root.classList.toggle('dark', value !== 'light')
  root.style.colorScheme = value === 'light' ? 'light' : 'dark'
}

/** Set the scheme and remember it. Persisting is what makes it survive reload. */
export const setTheme = (value) => {
  const next = THEMES.includes(value) ? value : 'dark'
  theme.value = next
  applyTheme(next)
  writeStored(next)
  return next
}

export const toggleTheme = () => setTheme(theme.value === 'dark' ? 'light' : 'dark')

let listening = false

/**
 * Resolve and apply the scheme. Idempotent; called once from main.js.
 * Does NOT write to storage — an unstored preference must stay unstored so
 * the app keeps following the OS until the operator picks a side.
 */
export const initTheme = () => {
  const stored = readStored()
  const next = resolveTheme(stored, prefersLight())
  theme.value = next
  applyTheme(next)

  // Follow the OS live, but only while the choice is still the OS's to make.
  if (!listening) {
    const mq = mediaQuery()
    if (mq?.addEventListener) {
      mq.addEventListener('change', (e) => {
        if (THEMES.includes(readStored())) return // operator has decided
        const followed = e.matches ? 'light' : 'dark'
        theme.value = followed
        applyTheme(followed)
      })
      listening = true
    }
  }

  return next
}

export default { theme, isDark, isLight, initTheme, setTheme, toggleTheme }
