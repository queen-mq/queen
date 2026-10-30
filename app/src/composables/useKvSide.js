// The KV page's sub sidebar — PURE functions, no Vue, no DOM (the useKvView.js
// convention), so `npm test` can assert them without a browser.
//
// THE LIST IS THE TENANT'S NAMESPACES, AND ONLY THAT. A fixed list, by name,
// each with the exact key count `GET /api/v1/resources/kv/namespaces` gives
// (every row, expired ones awaiting sweep included). A click opens one on the
// right and moves the highlight; nothing is ever inserted under it. The key
// prefix is a filter of the keys on the right (the page's tools row), not a
// level of this list.
//
// The column adds no request of its own: the namespace listing is the page's.

/** Rows the list draws before "N more · search to narrow". */
export const SIDE_LIMIT = 300

/** Byte order: the order the broker lists namespaces and walks keys in. */
export const byteOrder = (a, b) => (a < b ? -1 : a > b ? 1 : 0)

/** An exact count with separators: the KV figures are exact and stay so. */
export const exact = (n) => (Number.isFinite(n) ? n.toLocaleString('en-US') : '—')

/** The namespace list narrowed by what was typed (case-insensitive, anywhere in the name). */
export function filterNamespaces(options, text = '') {
  const list = Array.isArray(options) ? options : []
  const term = typeof text === 'string' ? text.trim().toLowerCase() : ''
  return term ? list.filter((o) => o.namespace.toLowerCase().includes(term)) : list
}

/**
 * The list's rows, by name (byte order, the broker's own).
 *
 * `options` is useKvView.namespaceOptions(). The namespace open on the right
 * (`picked`) is kept even when the listing does not hold it — typed and opened
 * with Enter, or from a pasted address — as a row with no count and
 * `listed: false`, so the list always shows what the page is showing. Only
 * once the listing has `settled` (answered or failed): while it is still on
 * its way, an unknown name is not yet "not in the list".
 */
export function namespaceRows(options, { picked = '', find = '', settled = true } = {}) {
  const rows = (Array.isArray(options) ? options : [])
    .filter((o) => typeof o?.namespace === 'string' && o.namespace !== '')
    .map((o) => ({ namespace: o.namespace, keys: Number.isFinite(o.keys) ? o.keys : null, listed: true }))
  if (picked && settled && !rows.some((r) => r.namespace === picked)) {
    rows.push({ namespace: picked, keys: null, listed: false })
  }
  return filterNamespaces(rows, find).sort((a, b) => byteOrder(a.namespace, b.namespace))
}

/**
 * Every key of every namespace: the sum of the listing's exact counts, or null
 * when a namespace came without one (a sum that skipped it would undercount
 * and still look exact).
 */
export function namespaceTotal(options) {
  const list = Array.isArray(options) ? options : []
  let total = 0
  for (const o of list) {
    if (!Number.isFinite(o?.keys)) return null
    total += o.keys
  }
  return total
}
