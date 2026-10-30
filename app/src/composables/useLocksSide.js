// The Locks page's sub sidebar — PURE functions, no Vue, no DOM, so `npm test`
// can assert them without a browser.
//
// THE LIST IS A FILTER LIST: "All held locks", then one row per lock name
// prefix (`sync:`, `job:`), by name, each with the permits held under it — the
// rows the filter shows on the right. A click filters the table; the list
// itself never changes under it.
//
// SO THE LIST COUNTS THE WHOLE SET, NOT THE PAGE ON SCREEN. It is built from
// every held permit, unfiltered: the table's own page when that page already
// is all of them (the first page, no filter, nothing after it — the usual
// case, a handful of locks, no extra request), otherwise one read of its own
// of up to SIDE_READ_LIMIT permits. Filtering or paging the table never
// rebuilds it; a refresh does.
//
// A name with no ':' is its own entry (`leader`): it is one lock, and the
// filter it drives ("starts with leader") finds it.
//
// Semaphores are told apart only as far as the permits read can show
// (useLocks.permitsOf `shared`: a slot above 0, or a second permit of one
// name). A semaphore with one holder in slot 0 reads as a lock, which is what
// it is at that moment: its limit is stored nowhere to ask.
import { byteOrder, exact } from './useKvSide.js'

/** One read of the whole set for the list: the broker's own cap on a page. */
export const SIDE_READ_LIMIT = 1000

/** The entry a lock name is listed under: `sync:` for `sync:orders`, the name itself without a ':'. */
export function lockEntryOf(name) {
  if (typeof name !== 'string' || name === '') return null
  const at = name.indexOf(':')
  return at < 0 ? name : name.slice(0, at + 1)
}

/**
 * Permits → one entry per prefix, by name (byte order, the order the broker
 * walks the names in): distinct locks, distinct semaphores and their permits,
 * rows in the namespace that are not permits (`other`), and `permits`, every
 * row — the number the list shows, since it is what the filter shows.
 */
export function lockPrefixes(permits) {
  const entries = new Map()
  for (const p of Array.isArray(permits) ? permits : []) {
    const prefix = lockEntryOf(p?.name)
    if (prefix === null) continue
    let e = entries.get(prefix)
    if (!e) {
      e = { prefix, names: new Set(), locks: new Set(), semaphores: new Set(), semaphorePermits: 0, other: 0, permits: 0 }
      entries.set(prefix, e)
    }
    e.permits += 1
    e.names.add(p.name)
    if (p.foreign) e.other += 1
    else if (p.shared) { e.semaphores.add(p.name); e.semaphorePermits += 1 }
    else e.locks.add(p.name)
  }
  return [...entries.values()]
    .map((e) => ({
      prefix: e.prefix,
      names: [...e.names].sort(byteOrder),
      locks: e.locks.size,
      semaphores: e.semaphores.size,
      semaphorePermits: e.semaphorePermits,
      other: e.other,
      permits: e.permits,
    }))
    .sort((a, b) => byteOrder(a.prefix, b.prefix))
}

/** The entries summed: the "All held locks" row and the page's facts. */
export function lockTotals(entries) {
  const t = { locks: 0, semaphores: 0, semaphorePermits: 0, other: 0, permits: 0 }
  for (const e of Array.isArray(entries) ? entries : []) {
    for (const k of Object.keys(t)) t[k] += Number.isFinite(e?.[k]) ? e[k] : 0
  }
  return t
}

/** `2 locks · 1 semaphore (3 permits) · 1 other row` — only the parts that are there. */
export function describeHeld(entry) {
  if (!entry) return ''
  const parts = []
  if (entry.locks) parts.push(`${exact(entry.locks)} lock${entry.locks === 1 ? '' : 's'}`)
  if (entry.semaphores) {
    parts.push(`${exact(entry.semaphores)} semaphore${entry.semaphores === 1 ? '' : 's'} (${exact(entry.semaphorePermits)} permit${entry.semaphorePermits === 1 ? '' : 's'})`)
  }
  if (entry.other) parts.push(`${exact(entry.other)} other row${entry.other === 1 ? '' : 's'}`)
  return parts.join(' · ')
}

/**
 * The permits the list can count without a read of its own, or null.
 *
 * `loaded` describes the table's page — `{prefix, page, more, permits}`. It is
 * the whole set only when it is the first page of the unfiltered walk and the
 * broker said nothing comes after it.
 */
export function wholeSet(loaded) {
  if (!loaded || loaded.prefix !== '' || loaded.page !== 1 || loaded.more === true) return null
  return Array.isArray(loaded.permits) ? loaded.permits : null
}

/**
 * The list's rows narrowed by what was typed (case-insensitive, anywhere):
 * a row stays when its prefix or a lock name under it matches, so `feratel`
 * finds the `sync:` that holds `sync:feratel`.
 */
export function filterEntries(entries, text = '') {
  const list = Array.isArray(entries) ? entries : []
  const term = typeof text === 'string' ? text.trim().toLowerCase() : ''
  if (!term) return list
  return list.filter((e) => e.prefix.toLowerCase().includes(term)
    || (Array.isArray(e.names) && e.names.some((n) => n.toLowerCase().includes(term))))
}
