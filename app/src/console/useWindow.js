// The time window of the new console. Live is the last hour, moving; a
// preset (15 min, 6 h, 24 h, 7 days) moves too; a from-to, or a zoom made
// by dragging across a chart, stays put until "Back to live". The address
// carries it the way the old pages do (?range=6h, or ?from=ISO&to=ISO), so a
// link opens the same window on either.
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

export const PRESETS = [
  { id: '1h', label: 'Live', long: 'last hour, live', minutes: 60, live: true },
  { id: '15m', label: 'Last 15 minutes', long: 'last 15 minutes', minutes: 15 },
  { id: '6h', label: 'Last 6 hours', long: 'last 6 hours', minutes: 360 },
  { id: '24h', label: 'Last 24 hours', long: 'last 24 hours', minutes: 1440 },
  { id: '7d', label: 'Last 7 days', long: 'last 7 days', minutes: 10080 },
]
const BY_ID = Object.fromEntries(PRESETS.map((p) => [p.id, p]))
const MIN = 60_000

// One clock for the band's live time and every "N min ago".
export const nowMs = ref(Date.now())
let started = false
function startClock() {
  if (started || typeof window === 'undefined') return
  started = true
  window.setInterval(() => { nowMs.value = Date.now() }, 1000)
}

const two = (n) => String(n).padStart(2, '0')
const DAY = new Intl.DateTimeFormat(undefined, { weekday: 'short', day: 'numeric' })
const LONG = new Intl.DateTimeFormat(undefined, { weekday: 'long', day: 'numeric', month: 'long' })

export function clock(ms, seconds = false) {
  const d = new Date(ms)
  return two(d.getHours()) + ':' + two(d.getMinutes()) + (seconds ? ':' + two(d.getSeconds()) : '')
}
export function stamp(ms, withDay = false) {
  return withDay ? DAY.format(new Date(ms)) + ' ' + clock(ms) : clock(ms)
}
export function longDate(ms) {
  return LONG.format(new Date(ms))
}
export function timeZone() {
  try { return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local time' } catch { return 'local time' }
}
// "14:05" today -> epoch ms, or null.
export function todayAt(text, base = Date.now()) {
  const m = /^\s*(\d{1,2})[:.](\d{2})\s*$/.exec(text || '')
  if (!m) return null
  const h = Number(m[1]); const mi = Number(m[2])
  if (h > 23 || mi > 59) return null
  const d = new Date(base); d.setHours(h, mi, 0, 0)
  return d.getTime()
}

const one = (v) => (Array.isArray(v) ? v[0] : v)
function isoMs(v) {
  const s = one(v)
  if (typeof s !== 'string' || !s) return null
  const t = Date.parse(s)
  return Number.isFinite(t) ? t : null
}

export function useWindow() {
  startClock()
  const route = useRoute()
  const router = useRouter()

  const win = computed(() => {
    const q = route.query
    const from = isoMs(q.from)
    const to = isoMs(q.to)
    const now = nowMs.value
    let w
    if (from && to && to > from) {
      const span = to - from
      const withDay = span >= 1440 * MIN || new Date(from).toDateString() !== new Date(now).toDateString()
      const label = stamp(from, withDay) + ' – ' + stamp(to, withDay)
      w = { id: 'custom', live: false, custom: true, from, to, label, long: label }
    } else {
      const p = BY_ID[one(q.range)] || BY_ID['1h']
      // Moving windows end at the last 10 s, so a fetch key stays put between ticks.
      const end = Math.floor(now / 10_000) * 10_000
      w = { id: p.id, live: true, custom: false, from: end - p.minutes * MIN, to: end, label: p.label, long: p.long }
    }
    const span = w.to - w.from
    const withDay = span >= 1440 * MIN
    w.minutes = Math.round(span / MIN)
    w.iso = { from: new Date(w.from).toISOString(), to: new Date(w.to).toISOString() }
    w.axis = [stamp(w.from, withDay), stamp(w.from + span / 2, withDay), w.live ? 'now' : stamp(w.to, withDay)]
    return w
  })

  function put(query) {
    const next = { ...route.query }
    delete next.range; delete next.from; delete next.to
    Object.assign(next, query)
    router.push({ query: next })
  }
  function pick(id) {
    if (!BY_ID[id] || id === '1h') return put({})
    put({ range: id })
  }
  // A fixed from-to in epoch ms (a zoom, or "Show this time").
  function range(fromMs, toMs) {
    const a = Math.min(fromMs, toMs); const b = Math.max(fromMs, toMs)
    if (b - a < 60_000) return
    put({ from: new Date(Math.floor(a / 1000) * 1000).toISOString(), to: new Date(Math.ceil(b / 1000) * 1000).toISOString() })
  }

  return { win, pick, range, live: () => put({}), presets: PRESETS, nowMs }
}
