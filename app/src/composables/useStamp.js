/**
 * Freshness of a single `useApi` panel, as one short string.
 *
 * Three return values, three meanings, and the empty string means "never
 * loaded" — which renders nothing rather than a lie. Every `useApi` panel
 * exposes `lastUpdated` and `failed`, so any card backed by one can be
 * stamped.
 *
 * Hoisted out of the verbatim copies that lived in Analytics.vue and
 * System.vue so the app's right-hand card-header slot means freshness, and
 * only freshness, everywhere.
 */
export function stamp(panel) {
  if (panel.failed.value) {
    return panel.lastUpdated.value
      ? `stale · last good ${panel.lastUpdated.value.toLocaleTimeString()}`
      : 'unavailable'
  }
  return panel.lastUpdated.value ? `as of ${panel.lastUpdated.value.toLocaleTimeString()}` : ''
}
