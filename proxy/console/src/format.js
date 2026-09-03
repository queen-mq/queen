/** Human timestamps in the account console follow the dashboard contract. */
export function runtimeTimeZone() {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local time'
}

export function formatTimestamp(value, locales = 'en-US', options = {}) {
  if (!value) return '—'
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)

  const { timeZone = runtimeTimeZone(), ...overrides } = options
  const formatted = new Intl.DateTimeFormat(locales, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    ...overrides,
    ...(timeZone === 'local time' ? {} : { timeZone }),
  }).format(date)
  return formatted
}

export function formatTimestampUtc(value) {
  if (!value) return ''
  const date = value instanceof Date ? value : new Date(value)
  return Number.isNaN(date.getTime()) ? '' : `UTC: ${date.toISOString()}`
}
