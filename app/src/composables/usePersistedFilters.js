// A page's filters, kept where they survive a trip to a detail page and back.
//
// Two places, because there are two ways back:
//
//   the URL          one query key per field, written with replace (never
//                    push, so Back leaves the page instead of walking every
//                    keystroke). The browser's Back lands on it, and a pasted
//                    link reproduces the slice.
//   sessionStorage   per tab, per page, per cluster. "Back to queues", the
//                    sidebar and the Dashboard links all push a bare path;
//                    the URL has nothing to say there, so the tab's memory
//                    does.
//
// A URL that names any of the page's keys wins over the memory: it is the
// more specific instruction (a cross-link like `/messages?queue=x` means
// "this queue", not "this queue plus whatever you had last time"), and every
// field it does not name takes its default.
//
// A field's DEFAULT is its ref's value when the page binds it, so a view keeps
// declaring its refs the way it always has. A field at its default writes no
// key, which keeps an untouched page on a clean URL.
//
// "Clear filters" resets the fields that narrow what is shown. Fields marked
// `keep` — a sort, a range, a view mode — are how the page is looked at, not
// a narrowing, and stay.
import { computed, watch } from 'vue'
import { onBeforeRouteUpdate, useRoute, useRouter } from 'vue-router'

import { formatDateTimeLocal } from './useFormat.js'

const STORAGE_PREFIX = 'queen.filters:'

// vue-router hands a repeated key over as an array, and `?k` (no `=`) as null.
const first = v => (Array.isArray(v) ? v[0] : v)
const str = v => (typeof first(v) === 'string' ? first(v) : undefined)

// ---------------------------------------------------------------------------
// Codecs. `parse(raw)` takes the raw query value (string | null | string[])
// and returns the field's value, or `undefined` for "not usable, take the
// default". `format(value)` returns the query value, or `undefined` for "no
// key". Every value from outside — a hand-edited URL, a blob stored by an
// older build — goes through parse, so a bad one degrades to the default
// rather than to a filter the page cannot render.
// ---------------------------------------------------------------------------

/** Free text; '' means "no filter". */
export const text = {
  parse: str,
  format: v => (typeof v === 'string' && v !== '' ? v : undefined),
}

/**
 * Text where '' is a real value — the server's DEFAULT namespace or task —
 * and null is "all". In the URL, null is the key's absence and '' is `?k=`.
 */
export const optionalText = {
  parse: str,
  format: v => (typeof v === 'string' ? v : undefined),
}

/** A checkbox: `?k=1` or nothing. */
export const flag = {
  parse: v => (str(v) === '1' ? true : str(v) === '0' ? false : undefined),
  format: v => (v ? '1' : '0'),
}

/** One of a fixed set; the set's own values (string or number) come back. */
export function oneOf(values) {
  return {
    parse: v => values.find(x => String(x) === str(v)),
    format: v => (v === null || v === undefined ? undefined : String(v)),
  }
}

/** A list of names, as repeated keys (`?queue=a&queue=b`). */
export const list = {
  parse: v => {
    const items = (Array.isArray(v) ? v : [v]).filter(x => typeof x === 'string' && x !== '')
    return items.length ? items : undefined
  },
  format: v => (Array.isArray(v) && v.length ? [...v] : undefined),
}

/**
 * An absolute range `{ from: Date, to: Date }`, as one ISO 8601 interval
 * (`2026-09-29T10:00:00.000Z/2026-09-29T11:00:00.000Z`). null is "no custom
 * range". A reversed or unparseable interval is not a range.
 */
export const interval = {
  parse: v => {
    const [a, b, extra] = (str(v) || '').split('/')
    if (!a || !b || extra !== undefined) return undefined
    const from = new Date(a)
    const to = new Date(b)
    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime()) || from >= to) return undefined
    return { from, to }
  },
  format: (v) => {
    const from = v?.from ? new Date(v.from) : null
    const to = v?.to ? new Date(v.to) : null
    // An Invalid Date is truthy and throws in toISOString(); it is no range.
    if (!from || !to || Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) return undefined
    return `${from.toISOString()}/${to.toISOString()}`
  },
}

/**
 * The applied custom range of a range picker, as one value for `interval`:
 * null while a quick range rules. Setting it restores the picker too, so a
 * restored range reads back in the From/To inputs it was applied from.
 */
export function appliedRange({ customMode, appliedCustom, customFrom, customTo }) {
  return computed({
    get: () => (customMode.value && appliedCustom.value ? appliedCustom.value : null),
    set: (r) => {
      appliedCustom.value = r
      customMode.value = !!r
      if (r) {
        customFrom.value = formatDateTimeLocal(r.from)
        customTo.value = formatDateTimeLocal(r.to)
      }
    },
  })
}

// ---------------------------------------------------------------------------
// Pure core — no Vue, no router — so the node suite can pin it.
// ---------------------------------------------------------------------------

/** The query keys a field set owns. */
export const filterKeys = fields => Object.keys(fields)

export function hasFilterQuery(fields, query) {
  return filterKeys(fields).some(k => query?.[k] !== undefined)
}

/**
 * Field values from a query-shaped map (the URL, or the stored copy of it).
 * A key that is missing or does not parse takes the field's default.
 */
export function valuesFromQuery(fields, query, defaults) {
  const out = {}
  for (const [key, field] of Object.entries(fields)) {
    const raw = query?.[key]
    const parsed = raw === undefined ? undefined : field.codec.parse(raw)
    out[key] = parsed === undefined ? defaults[key] : parsed
  }
  return out
}

/** The query keys for these values; a field at its default writes `undefined`. */
export function queryFromValues(fields, values, defaults) {
  const out = {}
  for (const [key, field] of Object.entries(fields)) {
    const v = field.codec.format(values[key])
    const d = field.codec.format(defaults[key])
    out[key] = sameFormatted(v, d) ? undefined : v
  }
  return out
}

function sameFormatted(a, b) {
  if (Array.isArray(a) || Array.isArray(b)) {
    return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => x === b[i])
  }
  return a === b
}

/** The URL half of a field set: fields marked `url: false` live in the tab only. */
export function urlQuery(fields, query) {
  const out = {}
  for (const [key, value] of Object.entries(query)) {
    out[key] = fields[key].url === false ? undefined : value
  }
  return out
}

/** True when any narrowing (non-`keep`) field is away from its default. */
export function isFiltered(fields, values, defaults) {
  return Object.entries(fields).some(([key, field]) =>
    !field.keep && !sameFormatted(field.codec.format(values[key]), field.codec.format(defaults[key])))
}

/** Stored copy of the query map; defaults drop out, so a clean page stores nothing. */
export function loadStored(storage, key) {
  try {
    const raw = storage?.getItem(key)
    const parsed = raw ? JSON.parse(raw) : null
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null
  } catch {
    return null
  }
}

export function saveStored(storage, key, query) {
  try {
    const kept = Object.fromEntries(Object.entries(query).filter(([, v]) => v !== undefined))
    if (Object.keys(kept).length === 0) storage?.removeItem(key)
    else storage?.setItem(key, JSON.stringify(kept))
  } catch { /* private mode, quota — the URL still carries the slice */ }
}

/**
 * A restored selection missing from a <select>'s options is still offered: a
 * select whose value has no option renders blank, and a blank control hiding
 * an active filter is how an operator decides the rows are gone.
 * `all` is the "no filter" value, which needs no option of its own.
 */
export function withSelected(options, selected, all = null) {
  return selected === all || selected === undefined || options.includes(selected)
    ? options
    : [...options, selected]
}

// A list default must not be the ref's own array: a view that pushes into it
// would move the default with it.
const clone = v => (Array.isArray(v) ? [...v] : v)

// One value and a list of one are the same query to vue-router.
const sameQueryValue = (a, b) => JSON.stringify(a === undefined ? a : [].concat(a))
  === JSON.stringify(b === undefined ? b : [].concat(b))

function sessionStore() {
  try { return typeof sessionStorage !== 'undefined' ? sessionStorage : null } catch { return null }
}

/**
 * Bind a page's filter refs to the URL and the tab's memory.
 *
 * Call it right after the refs are declared and BEFORE any watcher that
 * refetches on them: the restore writes the refs synchronously, so the first
 * fetch already runs on the restored slice instead of fetching twice.
 *
 * @param {string} page      storage namespace, e.g. 'queues'
 * @param {Record<string, {ref: import('vue').Ref, codec: object, keep?: boolean, url?: boolean}>} fields
 *   keyed by the query key the field is written under
 * @param {{ scope?: () => string|null|undefined, storage?: Storage|null }} [options]
 *   `scope` names the acting cluster: filters are remembered per cluster,
 *   since a namespace on one is not a namespace on the next.
 */
export function usePersistedFilters(page, fields, { scope = () => null, storage = sessionStore() } = {}) {
  const route = useRoute()
  const router = useRouter()
  const ownRoute = route.name
  const storageKey = () => `${STORAGE_PREFIX}${page}:${scope() || 'none'}`

  const defaults = Object.fromEntries(Object.entries(fields).map(([k, f]) => [k, clone(f.ref.value)]))
  const current = () => Object.fromEntries(Object.entries(fields).map(([k, f]) => [k, f.ref.value]))
  const apply = (values) => {
    for (const [k, f] of Object.entries(fields)) f.ref.value = clone(values[k])
  }
  const restore = () => {
    const stored = loadStored(storage, storageKey())
    return stored ? valuesFromQuery(fields, stored, defaults) : { ...defaults }
  }

  // The URL only counts when it names a key it is allowed to carry; a
  // tab-only field is never read from an address bar.
  const urlFields = Object.fromEntries(Object.entries(fields).filter(([, f]) => f.url !== false))
  // The URL half of a query, as one comparable string.
  const urlKey = query => JSON.stringify(filterKeys(urlFields).map(k => (query[k] === undefined ? null : [].concat(query[k]))))
  // The URL half of a slice as written, so `?sort=health` (a default, spelled
  // out) is the same slice as no key at all.
  const sliceKey = values => urlKey(urlQuery(fields, queryFromValues(fields, values, defaults)))

  // A URL names the slice; its tab-only fields come from the tab's memory when
  // the URL is the slice this tab last wrote — Back onto `/kv?ns=a` is a
  // return, not a new instruction — and take their defaults otherwise.
  const fromUrl = (query) => {
    const incoming = { ...defaults, ...valuesFromQuery(urlFields, query, defaults) }
    const remembered = restore()
    return sliceKey(remembered) === sliceKey(incoming) ? remembered : incoming
  }
  apply(hasFilterQuery(urlFields, route.query) ? fromUrl(route.query) : restore())

  // Write-through, including once at setup: a slice restored from memory is
  // put back in the address bar, so what is on screen is what a copied link
  // reproduces. Watched as the formatted map, so a list or a range compares
  // by content, not by identity.
  const formatted = () => queryFromValues(fields, current(), defaults)
  // Write-throughs still in flight, so the route guard can tell its own
  // navigations from someone else's.
  const inFlight = new Set()
  // A slice the guard applied from an incoming URL: that navigation is already
  // under way, and writing it again would cancel it.
  let arriving = null

  watch(() => JSON.stringify(formatted()), () => {
    const query = formatted()
    saveStored(storage, storageKey(), query)
    if (route.name !== ownRoute) return
    const next = { ...route.query, ...urlQuery(fields, query) }
    const key = urlKey(next)
    if (key === arriving) {
      arriving = null
      return
    }
    if (filterKeys(urlFields).every(k => sameQueryValue(route.query[k], next[k])) || inFlight.has(key)) return
    inFlight.add(key)
    router.replace({ query: next }).finally(() => inFlight.delete(key))
  }, { immediate: true })

  // Navigations to this same page reuse the component:
  //   - a URL naming filters that this page did not write — the header's
  //     search pushing `/consumers?search=x` while already there — is an
  //     instruction, applied like a fresh load;
  //   - a bare URL (the sidebar's link) gets the slice put back, instead of
  //     the address bar and the controls disagreeing. Replacing only on the
  //     same path: on a param route (/queues/a → /queues/b) the redirect
  //     keeps the push, so Back still reaches the queue before.
  onBeforeRouteUpdate((to, from) => {
    if (hasFilterQuery(urlFields, to.query)) {
      if (inFlight.has(urlKey(to.query))) return true
      const incoming = fromUrl(to.query)
      const key = sliceKey(incoming)
      if (key === sliceKey(current())) return true
      arriving = key
      apply(incoming)
      return true
    }
    const query = urlQuery(fields, formatted())
    if (Object.values(query).every(v => v === undefined)) return true
    return {
      path: to.path, hash: to.hash, query: { ...to.query, ...query },
      ...(to.path === from.path ? { replace: true } : {}),
    }
  })

  // The view stays mounted across a cluster switch; the slice does not carry.
  // Synchronous, so the new cluster's slice is in place before the shell's
  // epoch watcher refetches every panel — a pre-flush watcher here would run
  // after it, and the first fetch on the new cluster would carry the old
  // cluster's filters.
  watch(scope, () => apply(restore()), { flush: 'sync' })

  const hasActiveFilter = computed(() => isFiltered(fields, current(), defaults))

  const clearFilters = () => {
    for (const [k, f] of Object.entries(fields)) {
      if (!f.keep) f.ref.value = clone(defaults[k])
    }
  }

  return { hasActiveFilter, clearFilters }
}
