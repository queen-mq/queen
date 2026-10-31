// Numbers in words, the console's way. Pure: node tests and the pure helpers
// of every page import it (useOverview.js re-exports both).

// A rate a second: whole above 100, one decimal above 10, else two.
export function rate(v) {
  if (v == null || !Number.isFinite(v)) return '–'
  const a = Math.abs(v)
  if (a >= 100) return Math.round(v).toLocaleString('en-US')
  if (a >= 10) return v.toFixed(1)
  if (a === 0) return '0'
  return v.toFixed(2).replace(/0$/, '')
}
export function span(seconds) {
  const s = Math.max(0, Math.round(seconds))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  // Round once, then split: 16 h 59.6 min reads 17 h, never "16 h 60 min".
  if (m < 24 * 60) { const h = Math.floor(m / 60); const mm = m % 60; return mm ? `${h} h ${mm} min` : `${h} h` }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24); const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}
