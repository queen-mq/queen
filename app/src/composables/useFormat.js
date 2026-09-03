// Formatting helpers shared by the range-picker views (Analytics, System,
// Consumers). They were copied byte-for-byte into each view before; a second
// copy of a formatter is a second place for the same number to be rendered
// differently.
//
// Numeric / duration / relative-time formatting lives in useApi.js — import
// from there rather than adding a competing variant here.

/** The browser/webview timezone used for every human-readable timestamp. */
export function runtimeTimeZone() {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local time'
}

const asValidDate = (value) => {
  if (!value) return null
  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
}

const zoned = (value, locales, options, defaults) => {
  const date = asValidDate(value)
  if (!date) return value ? String(value) : '—'

  const { timeZone = runtimeTimeZone(), ...overrides } = options
  const formatted = new Intl.DateTimeFormat(locales, {
    ...defaults,
    ...overrides,
    ...(timeZone === 'local time' ? {} : { timeZone }),
  }).format(date)
  return formatted
}

/** Full timestamp rendered in the browser's local timezone. */
export function formatTimestamp(value, locales = 'en-US', options = {}) {
  return zoned(value, locales, options, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

/** Compact freshness timestamp rendered in the browser's local timezone. */
export function formatTimestampTime(value, locales = 'en-US', options = {}) {
  return zoned(value, locales, options, {
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  })
}

/** Applied custom ranges are absolute facts, so both ends name their zone. */
export function formatTimestampRange(from, to, locales = 'en-US', options = {}) {
  return `${formatTimestamp(from, locales, options)} → ${formatTimestamp(to, locales, options)}`
}

/** Exact UTC value for a timestamp's hover title. */
export function formatTimestampUtc(value) {
  const date = asValidDate(value)
  return date ? `UTC: ${date.toISOString()}` : ''
}

export function formatTimestampRangeUtc(from, to) {
  const fromUtc = formatTimestampUtc(from)
  const toUtc = formatTimestampUtc(to)
  return fromUtc && toUtc ? `${fromUtc} → ${toUtc.replace(/^UTC: /, '')}` : ''
}

/** `Date` → the exact string an <input type="datetime-local"> accepts. */
export function formatDateTimeLocal(date) {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const hours = String(date.getHours()).padStart(2, '0')
  const minutes = String(date.getMinutes()).padStart(2, '0')
  return `${year}-${month}-${day}T${hours}:${minutes}`
}

/**
 * Chart x-axis label. A range that crosses midnight needs the date or two
 * different days collapse onto the same "03:00" tick.
 */
export function formatChartLabel(date, multiDay) {
  if (multiDay) {
    return date.toLocaleString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
    })
  }
  return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
}

/** True when the first and last timestamp fall on different calendar days. */
export function isMultiDay(timestamps) {
  if (!timestamps || timestamps.length < 2) return false
  const first = new Date(timestamps[0])
  const last = new Date(timestamps[timestamps.length - 1])
  if (Number.isNaN(first.getTime()) || Number.isNaN(last.getTime())) return false
  return first.toDateString() !== last.toDateString()
}

/**
 * Parse a `datetime-local` pair into a validated range.
 * Returns `{ from, to }` or `{ error }` — never a silently-dropped Apply.
 */
export function validateRange(fromStr, toStr) {
  if (!fromStr || !toStr) return { error: 'Pick both a start and an end.' }
  const from = new Date(fromStr)
  const to = new Date(toStr)
  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    return { error: 'That is not a valid date and time.' }
  }
  if (from >= to) return { error: 'The start must be before the end.' }
  return { from, to }
}
