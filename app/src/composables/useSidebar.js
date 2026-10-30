// The sidebar's width on a wide screen: the full column, or a rail of icons.
// The choice is the operator's, remembered on this device like the theme
// (useTheme.js), and it only means something above the drawer breakpoint: up
// to 1100px the sidebar is a drawer over the page and always opens in full.

import { computed, ref } from 'vue'

// A new key since the pages gained their own sub sidebar (2026-10-30): the
// rail is the default now, and a choice made before it does not carry over.
export const SIDEBAR_STORAGE_KEY = 'queen-sidebar-2'

// Storage and matchMedia both throw in hardened contexts (Safari private
// mode, storage access denied); a sidebar width is never worth an exception.
const readStored = () => {
  try {
    return typeof localStorage !== 'undefined' ? localStorage.getItem(SIDEBAR_STORAGE_KEY) : null
  } catch {
    return null
  }
}

const writeStored = (value) => {
  try {
    if (typeof localStorage !== 'undefined') localStorage.setItem(SIDEBAR_STORAGE_KEY, value)
  } catch {
    // The choice simply won't survive the reload.
  }
}

// Sidebar.vue's DRAWER_MAX and the 1100px media queries in style.css.
const WIDE_QUERY = '(min-width: 1101px)'
const wideQuery = (() => {
  try {
    return typeof matchMedia === 'function' ? matchMedia(WIDE_QUERY) : null
  } catch {
    return null
  }
})()

// Always the rail since 2026-10-30: every page has its own sub sidebar, so the
// full column would only repeat the rail's names. The stored choice is kept
// for the drawer-era code that still reads it, never offered.
export const collapsed = ref(true)
/** Above the drawer breakpoint, where the rail exists at all. */
export const wide = ref(wideQuery ? wideQuery.matches : true)
wideQuery?.addEventListener?.('change', (e) => { wide.value = e.matches })

/** True when the sidebar is drawn as the icon rail right now. */
export const rail = computed(() => collapsed.value && wide.value)

/** Kept for callers; the rail no longer widens. */
export const toggleSidebar = () => {}
