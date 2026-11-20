// The time window's From and To as you type them: "14:05" is today,
// "2026-11-19 14:05" is that day; seconds are optional; local time.
//
// Pure: no Vue (test/window-time.test.js).

const two = (n) => String(n).padStart(2, '0')
const MIN = 60_000

/** "14:05", "14:05:30", "2026-11-19 14:05" or "2026-11-19T14:05:30" -> epoch ms, or null. */
export function parseAt(text, base = Date.now()) {
  const m = /^\s*(?:(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+|T))?(\d{1,2})[:.](\d{2})(?:[:.](\d{2}))?\s*$/.exec(text || '')
  if (!m) return null
  const h = Number(m[4]); const mi = Number(m[5]); const s = Number(m[6] || 0)
  if (h > 23 || mi > 59 || s > 59) return null
  const d = new Date(base)
  if (m[1]) {
    const y = Number(m[1]); const mo = Number(m[2]) - 1; const day = Number(m[3])
    d.setFullYear(y, mo, day)
    // 2026-02-30 is not a day: Date would roll it into March.
    if (d.getFullYear() !== y || d.getMonth() !== mo || d.getDate() !== day) return null
  }
  d.setHours(h, mi, s, 0)
  return d.getTime()
}

/** epoch ms -> "2026-11-19 14:05" (":30" when the seconds are not zero): what From and To show when they open. */
export function atText(ms) {
  const d = new Date(ms)
  const sec = d.getSeconds()
  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())} ${two(d.getHours())}:${two(d.getMinutes())}${sec ? `:${two(sec)}` : ''}`
}

const WHY = {
  bad: ['Use a time like 14:05 or 2026-11-19 14:05', 'A time alone is today. Times are in your local time.'],
  future: ['From is still to come', 'Pick a From earlier than now.'],
  order: ['From must be earlier than To', 'For example 09:00 and 09:40.'],
  short: ['Make the window at least a minute long', 'The broker reads whole minutes.'],
}

/**
 * From and To as typed -> { from, to } in epoch ms, or { error: [title, body] }.
 * A To past now is now: the window never reaches into the future.
 */
export function rangeOf(fromText, toText, now = Date.now()) {
  const a = parseAt(fromText, now)
  const b = parseAt(toText, now)
  if (a == null || b == null) return { error: WHY.bad }
  if (a >= now) return { error: WHY.future }
  const to = Math.min(b, now)
  if (a >= to) return { error: WHY.order }
  if (to - a < MIN) return { error: WHY.short }
  return { from: a, to }
}
