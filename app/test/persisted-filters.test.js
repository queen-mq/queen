import assert from 'node:assert/strict'
import { test } from 'node:test'
import { ref as vueRef } from 'vue'

import {
  appliedRange, flag, hasFilterQuery, interval, isFiltered, list, loadStored, oneOf, optionalText,
  queryFromValues, saveStored, text, urlQuery, valuesFromQuery, withSelected,
} from '../src/composables/usePersistedFilters.js'

const ref = value => ({ value })

const memoryStorage = () => {
  const map = new Map()
  return {
    getItem: k => (map.has(k) ? map.get(k) : null),
    setItem: (k, v) => map.set(k, String(v)),
    removeItem: k => map.delete(k),
    map,
  }
}

// The Queues page's field set: the shape every other page follows.
const queues = {
  q: { ref: ref(''), codec: text },
  ns: { ref: ref(null), codec: optionalText },
  task: { ref: ref(null), codec: optionalText },
  sort: { ref: ref('health'), codec: oneOf(['health', 'avgLagMs', 'name']), keep: true },
}
const queueDefaults = { q: '', ns: null, task: null, sort: 'health' }

test('a bare URL names no filter; any owned key does, a foreign one does not', () => {
  assert.equal(hasFilterQuery(queues, {}), false)
  assert.equal(hasFilterQuery(queues, { partitionId: 'x' }), false)
  assert.equal(hasFilterQuery(queues, { ns: '' }), true)
  assert.equal(hasFilterQuery(queues, { sort: 'name' }), true)
})

test('defaults write no keys, so an untouched page keeps a clean URL', () => {
  assert.deepEqual(queryFromValues(queues, queueDefaults, queueDefaults),
    { q: undefined, ns: undefined, task: undefined, sort: undefined })
})

test('the default namespace survives the round trip as "", distinct from ALL', () => {
  const values = { ...queueDefaults, ns: '' }
  const query = queryFromValues(queues, values, queueDefaults)
  assert.equal(query.ns, '')
  assert.deepEqual(valuesFromQuery(queues, query, queueDefaults), values)
  assert.equal(valuesFromQuery(queues, {}, queueDefaults).ns, null)
})

test('a hand-edited URL degrades field by field to the default', () => {
  assert.deepEqual(
    valuesFromQuery(queues, { q: ['agent', 'x'], ns: null, sort: 'bogus' }, queueDefaults),
    { q: 'agent', ns: null, task: null, sort: 'health' },
  )
})

test('a numeric choice comes back as the number, not its string', () => {
  const fields = { range: { ref: ref(60), codec: oneOf([15, 60, 360]), keep: true } }
  assert.deepEqual(valuesFromQuery(fields, { range: '360' }, { range: 60 }), { range: 360 })
  assert.deepEqual(valuesFromQuery(fields, { range: '7' }, { range: 60 }), { range: 60 })
  assert.deepEqual(queryFromValues(fields, { range: 360 }, { range: 60 }), { range: '360' })
})

test('a flag writes only when it leaves its default, either way', () => {
  const off = { lag: { ref: ref(false), codec: flag } }
  assert.deepEqual(queryFromValues(off, { lag: false }, { lag: false }), { lag: undefined })
  assert.deepEqual(queryFromValues(off, { lag: true }, { lag: false }), { lag: '1' })
  const on = { all: { ref: ref(true), codec: flag } }
  assert.deepEqual(queryFromValues(on, { all: false }, { all: true }), { all: '0' })
  assert.deepEqual(valuesFromQuery(on, { all: '0' }, { all: true }), { all: false })
  assert.deepEqual(valuesFromQuery(on, { all: 'yes' }, { all: true }), { all: true })
})

test('a list round-trips as repeated keys and a single key reads as a list', () => {
  const fields = { queue: { ref: ref([]), codec: list } }
  assert.deepEqual(queryFromValues(fields, { queue: ['a', 'b'] }, { queue: [] }), { queue: ['a', 'b'] })
  assert.deepEqual(queryFromValues(fields, { queue: [] }, { queue: [] }), { queue: undefined })
  assert.deepEqual(valuesFromQuery(fields, { queue: 'a' }, { queue: [] }), { queue: ['a'] })
  assert.deepEqual(valuesFromQuery(fields, { queue: ['', null] }, { queue: [] }), { queue: [] })
})

test('a custom range round-trips as one ISO interval; a reversed one is refused', () => {
  const from = new Date('2026-09-29T10:00:00Z')
  const to = new Date('2026-09-29T11:30:00Z')
  const formatted = interval.format({ from, to })
  assert.equal(formatted, '2026-09-29T10:00:00.000Z/2026-09-29T11:30:00.000Z')
  assert.deepEqual(interval.parse(formatted), { from, to })
  assert.equal(interval.parse('2026-09-29T11:00:00Z/2026-09-29T10:00:00Z'), undefined)
  assert.equal(interval.parse('garbage/2026-09-29T10:00:00Z'), undefined)
  assert.equal(interval.parse('a/b/c'), undefined)
  assert.equal(interval.format(null), undefined)
  assert.equal(interval.format({ from: new Date('nope'), to }), undefined)
})

test('a keep field is not a filter; any narrowing field away from default is', () => {
  assert.equal(isFiltered(queues, { ...queueDefaults, sort: 'name' }, queueDefaults), false)
  assert.equal(isFiltered(queues, { ...queueDefaults, q: 'x' }, queueDefaults), true)
  assert.equal(isFiltered(queues, { ...queueDefaults, ns: '' }, queueDefaults), true)
})

test('a tab-only field never reaches the URL half', () => {
  const fields = {
    ns: { ref: ref(''), codec: text },
    prefix: { ref: ref(''), codec: text, url: false },
  }
  assert.deepEqual(urlQuery(fields, { ns: 'a', prefix: 'secret/' }), { ns: 'a', prefix: undefined })
})

test('the stored copy round-trips, drops defaults, and a clean page stores nothing', () => {
  const storage = memoryStorage()
  saveStored(storage, 'k', { q: 'agent', ns: '', task: undefined })
  assert.deepEqual(loadStored(storage, 'k'), { q: 'agent', ns: '' })
  saveStored(storage, 'k', { q: undefined })
  assert.equal(storage.map.has('k'), false)
})

test('a corrupt stored blob is ignored and a throwing storage is survived', () => {
  const storage = memoryStorage()
  storage.setItem('k', '{not json')
  assert.equal(loadStored(storage, 'k'), null)
  storage.setItem('k', '[1,2]')
  assert.equal(loadStored(storage, 'k'), null)

  const broken = { getItem() { throw new Error('denied') }, setItem() { throw new Error('quota') } }
  assert.equal(loadStored(broken, 'k'), null)
  assert.doesNotThrow(() => saveStored(broken, 'k', { q: 'x' }))
  assert.equal(loadStored(null, 'k'), null)
})

test('a restored selection missing from the options is still offered', () => {
  const options = ['', 'smartchat']
  assert.equal(withSelected(options, null), options)
  assert.equal(withSelected(options, ''), options)
  assert.deepEqual(withSelected(options, 'gone'), ['', 'smartchat', 'gone'])
  assert.equal(withSelected(options, '', ''), options)
  assert.deepEqual(options, ['', 'smartchat'])
})

test('a restored custom range switches the picker to Custom and fills its inputs', () => {
  const picker = { customMode: vueRef(false), appliedCustom: vueRef(null), customFrom: vueRef(''), customTo: vueRef('') }
  const range = appliedRange(picker)
  assert.equal(range.value, null)

  const from = new Date(2026, 8, 29, 10, 0)
  const to = new Date(2026, 8, 29, 11, 30)
  range.value = { from, to }
  assert.equal(picker.customMode.value, true)
  assert.equal(picker.customFrom.value, '2026-09-29T10:00')
  assert.equal(picker.customTo.value, '2026-09-29T11:30')
  assert.deepEqual(range.value, { from, to })

  range.value = null
  assert.equal(picker.customMode.value, false)
  assert.equal(range.value, null)
})

test('an open but unapplied Custom is no range', () => {
  const picker = { customMode: vueRef(true), appliedCustom: vueRef(null), customFrom: vueRef('x'), customTo: vueRef('y') }
  assert.equal(appliedRange(picker).value, null)
})
