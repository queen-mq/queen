// Shared by the dashboard and proxy console. Keep durations and preset families
// here so adding a common range updates every picker, regardless of API units.
export const DEFAULT_TIME_RANGE = '1h'

const MINUTES_PER_UNIT = { m: 1, h: 60, d: 1440 }

export function rangeMinutes(value) {
  const match = /^(\d+)(m|h|d)$/.exec(String(value))
  const minutes = match ? Number(match[1]) * MINUTES_PER_UNIT[match[2]] : NaN
  return Number.isFinite(minutes) && minutes > 0 ? minutes : 60
}

export const rangeSeconds = value => rangeMinutes(value) * 60

// Durations are elapsed time, including across daylight-saving transitions.
// Accept non-preset durations too, for investigation links such as range=90m.
export function relativeTimeRange(value, to = new Date()) {
  return { from: new Date(to.getTime() - rangeSeconds(value) * 1000), to }
}

function preset(value) {
  const minutes = rangeMinutes(value)
  return Object.freeze({
    value,
    label: value,
    minutes,
    seconds: minutes * 60,
    hours: minutes / 60,
    longLabel: value === '1h' ? 'Last hour'
      : value.endsWith('d') ? `Last ${value.slice(0, -1)} days` : `Last ${value}`,
  })
}

export const TIME_RANGES = Object.freeze(['1h', '6h', '12h', '24h'].map(preset))
const week = preset('7d')

// Every family extends the same hourly choices. Views select a family instead
// of defining their own labels, durations or conversion tables.
export const EXTENDED_TIME_RANGES = Object.freeze([...TIME_RANGES, week])
export const SYSTEM_TIME_RANGES = Object.freeze([preset('15m'), ...TIME_RANGES])
export const LAG_THRESHOLDS = Object.freeze([
  ...['1m', '5m', '30m'].map(preset), ...TIME_RANGES,
])
// Console usage accepts whole hours from 1 to 168 (console.rs clamp_hours).
export const USAGE_TIME_RANGES = Object.freeze([...TIME_RANGES, preset('3d'), week])
