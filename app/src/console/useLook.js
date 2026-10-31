// Theme (dark, soft, light, or the system's) and areas (lines or tone) of
// the new console, remembered in this browser. The app's own theme
// (useTheme.js: html.light / html.dark, read before first paint) follows it:
// soft counts as dark there, so the old pages and the boot screen agree.
import { computed, ref, watch } from 'vue'
import { setTheme, themePreference } from '@/composables/useTheme'

const KEY = 'queen-console-look'
const THEMES = ['dark', 'soft', 'light', 'system']

function read() {
  try { return JSON.parse(localStorage.getItem(KEY) || '{}') || {} } catch { return {} }
}
const saved = read()

// First visit: soft with tone areas, the console's default look. Only an
// explicit Light chosen in the old design carries over; nothing else does.
const start = THEMES.includes(saved.theme) ? saved.theme : (themePreference.value === 'light' ? 'light' : 'soft')
export const themePick = ref(start)
// Tone (filled areas) unless this browser picked lines.
export const areas = ref(saved.areas === 'lines' ? 'lines' : 'tone')

const media = typeof window !== 'undefined' && window.matchMedia ? window.matchMedia('(prefers-color-scheme: light)') : null
const systemLight = ref(media ? media.matches : false)
media?.addEventListener?.('change', (e) => { systemLight.value = e.matches })

export const theme = computed(() => (themePick.value === 'system' ? (systemLight.value ? 'light' : 'dark') : themePick.value))
export const isLight = computed(() => theme.value === 'light')

watch([themePick, areas], () => {
  try { localStorage.setItem(KEY, JSON.stringify({ theme: themePick.value, areas: areas.value })) } catch { /* private window */ }
})
watch(themePick, (pick) => {
  setTheme(pick === 'system' ? 'system' : pick === 'light' ? 'light' : 'dark')
}, { immediate: true })

export const THEME_OPTIONS = [
  { value: 'dark', label: 'Dark' },
  { value: 'soft', label: 'Soft' },
  { value: 'light', label: 'Light' },
  { value: 'system', label: 'System' },
]
export const AREA_OPTIONS = [
  { value: 'lines', label: 'Lines' },
  { value: 'tone', label: 'Tone' },
]
