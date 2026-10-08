// Three preferences, two rendered schemes. System follows the OS live;
// Light and Dark remain fixed until the operator selects System again.
// `theme` always contains the resolved scheme so charts keep reacting to it.
// Keep the pre-paint resolution in index.html in sync with resolveTheme().

import { computed, ref } from 'vue'

export const THEME_STORAGE_KEY = 'queen-theme'

const THEMES = ['light', 'dark']
const PREFERENCES = ['system', ...THEMES]
const LIGHT_QUERY = '(prefers-color-scheme: light)'

const resolvePreference = (value) => PREFERENCES.includes(value) ? value : 'system'
export const resolveTheme = (preference, prefersLight) =>
  THEMES.includes(preference) ? preference : (prefersLight ? 'light' : 'dark')

export const themePreference = ref('system')
export const theme = ref('dark')
export const isDark = computed(() => theme.value === 'dark')
export const isLight = computed(() => theme.value === 'light')

const readStored = () => {
  try {
    return typeof localStorage !== 'undefined' ? localStorage.getItem(THEME_STORAGE_KEY) : null
  } catch {
    return null
  }
}

const writeStored = (value) => {
  try {
    if (typeof localStorage === 'undefined') return
    if (value === 'system') localStorage.removeItem(THEME_STORAGE_KEY)
    else localStorage.setItem(THEME_STORAGE_KEY, value)
  } catch {
    // The in-memory preference still works when persistence is unavailable.
  }
}

const mediaQuery = () => {
  try {
    return typeof matchMedia === 'function' ? matchMedia(LIGHT_QUERY) : null
  } catch {
    return null
  }
}

// color-scheme also updates native form controls and scrollbars.
const applyTheme = (value) => {
  theme.value = value
  if (typeof document === 'undefined') return
  const root = document.documentElement
  root.classList.toggle('light', value === 'light')
  root.classList.toggle('dark', value === 'dark')
  root.style.colorScheme = value
}

/** Remember the preference and immediately apply its resolved scheme. */
export const setTheme = (value) => {
  themePreference.value = resolvePreference(value)
  const next = resolveTheme(themePreference.value, mediaQuery()?.matches === true)
  applyTheme(next)
  writeStored(themePreference.value)
  return next
}

export const toggleTheme = () => {
  const next = (PREFERENCES.indexOf(themePreference.value) + 1) % PREFERENCES.length
  return setTheme(PREFERENCES[next])
}

let listening = false

/** Hydrate without writing storage; attach listeners once, on app boot. */
export const initTheme = () => {
  themePreference.value = resolvePreference(readStored())
  applyTheme(resolveTheme(themePreference.value, mediaQuery()?.matches === true))

  if (!listening) {
    mediaQuery()?.addEventListener?.('change', (event) => {
      if (themePreference.value === 'system') {
        applyTheme(resolveTheme('system', event.matches))
      }
    })
    if (typeof window !== 'undefined') {
      window.addEventListener('storage', (event) => {
        if (event.key === THEME_STORAGE_KEY || event.key === null) {
          themePreference.value = resolvePreference(readStored())
          applyTheme(resolveTheme(themePreference.value, mediaQuery()?.matches === true))
        }
      })
    }
    listening = true
  }

  return theme.value
}

export default { theme, themePreference, isDark, isLight, initTheme, setTheme, toggleTheme }
