import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  SIDE_LIMIT,
  byteOrder,
  exact,
  filterNamespaces,
  namespaceRows,
  namespaceTotal,
} from '../src/composables/useKvSide.js'

// The KV sub sidebar's pure half: a fixed list of the tenant's namespaces, by
// name, from the listing the page read. No key prefixes: those filter the keys
// on the right.

const opt = (namespace, keys) => ({ namespace, keys, label: namespace })

test('namespaceRows: by name in byte order, with the exact counts', () => {
  const rows = namespaceRows([opt('shop', 12), opt('Ops', 3), opt('ops', 4)])
  assert.deepEqual(rows.map((r) => r.namespace), ['Ops', 'ops', 'shop'], 'uppercase sorts first, as the broker walks')
  assert.deepEqual(rows.map((r) => r.keys), [3, 4, 12])
  assert.ok(rows.every((r) => r.listed))
})

test('namespaceRows: a pick the listing does not hold stays in the list, with no count', () => {
  const rows = namespaceRows([opt('shop', 12)], { picked: 'typed' })
  assert.deepEqual(rows.map((r) => [r.namespace, r.keys, r.listed]), [['shop', 12, true], ['typed', null, false]])
  assert.equal(namespaceRows([opt('shop', 12)], { picked: 'shop' }).length, 1, 'a listed pick is not added twice')
})

test('namespaceRows: an unknown pick waits for the listing to settle', () => {
  assert.deepEqual(namespaceRows([], { picked: 'shop', settled: false }), [])
  assert.deepEqual(namespaceRows([], { picked: 'shop', settled: true }).map((r) => r.namespace), ['shop'])
})

test('namespaceRows: the search narrows; junk and a missing count are tolerated', () => {
  const rows = namespaceRows([opt('shop', 1), opt('ops', 2), null, { namespace: '' }, opt('queen-locks', undefined)], { find: ' OP ' })
  assert.deepEqual(rows.map((r) => r.namespace), ['ops', 'shop'])
  assert.equal(namespaceRows([opt('x', undefined)])[0].keys, null)
  assert.deepEqual(namespaceRows(null), [])
})

test('namespaceTotal: the sum of exact counts, or nothing when one is missing', () => {
  assert.equal(namespaceTotal([opt('a', 2), opt('b', 3)]), 5)
  assert.equal(namespaceTotal([]), 0)
  assert.equal(namespaceTotal([opt('a', 2), opt('b', null)]), null)
  assert.equal(namespaceTotal(null), 0)
})

test('filterNamespaces: anywhere in the name, case-insensitive, trimmed', () => {
  const options = [{ namespace: 'perf' }, { namespace: 'Orders' }, { namespace: 'queen-locks' }]
  assert.deepEqual(filterNamespaces(options, ''), options)
  assert.deepEqual(filterNamespaces(options, ' ord ').map((o) => o.namespace), ['Orders'])
  assert.deepEqual(filterNamespaces(options, 'e').map((o) => o.namespace), ['perf', 'Orders', 'queen-locks'])
  assert.deepEqual(filterNamespaces(null, 'x'), [])
})

test('byteOrder, exact and SIDE_LIMIT', () => {
  assert.deepEqual(['b', 'B', 'a'].sort(byteOrder), ['B', 'a', 'b'])
  assert.equal(exact(1234567), '1,234,567')
  assert.equal(exact(null), '—')
  assert.equal(SIDE_LIMIT, 300)
})
