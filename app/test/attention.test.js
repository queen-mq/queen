// "What needs you", pinned. The Overview, the sidebar, Queues and Consumer
// groups all read this one rule; a change here moves all four at once.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { groupAttention, queueAttention, summarize } from '../src/composables/useAttention.js'

const q = (name, pending) => ({ name, messages: { pending } })
const g = (queueName, maxTimeLag, state = 'Stable') => ({ name: `${queueName}-g`, queueName, maxTimeLag, state })

test('a group is judged by the age of what it has not read', () => {
  assert.equal(groupAttention(g('a', 0)), 'ok')
  assert.equal(groupAttention(g('a', 59)), 'ok')
  assert.equal(groupAttention(g('a', 60)), 'warn')
  assert.equal(groupAttention(g('a', 300)), 'bad')
})

test('a group that has never consumed is not an alarm', () => {
  assert.equal(groupAttention(g('a', 90_000, 'Dead')), 'mute')
})

test('a queue takes the worst of its live groups', () => {
  const out = queueAttention([q('a', 10)], [g('a', 5), g('a', 120), g('a', 400)])
  assert.deepEqual(out.map((i) => [i.name, i.sev, i.reason, i.lag]), [['a', 'bad', 'lag', 400]])
})

test('messages with nobody to read them are attention, not failure', () => {
  const out = queueAttention([q('a', 83)], [])
  assert.deepEqual(out.map((i) => [i.name, i.sev, i.reason, i.deadOnly]), [['a', 'warn', 'noReader', false]])
})

test('a queue whose only groups have never consumed has no reader', () => {
  const out = queueAttention([q('a', 5)], [g('a', 90_000, 'Dead')])
  assert.deepEqual(out.map((i) => [i.sev, i.reason, i.deadOnly]), [['warn', 'noReader', true]])
})

test('an empty queue with no reader, and a queue kept up with, need nothing', () => {
  assert.deepEqual(queueAttention([q('empty', 0), q('fine', 40)], [g('fine', 2)]), [])
})

test('a missing pending count is not a backlog', () => {
  assert.deepEqual(queueAttention([{ name: 'a' }], []), [])
})

test('the input order is kept, so a caller can sort by depth first', () => {
  const out = queueAttention([q('b', 9), q('a', 1)], [])
  assert.deepEqual(out.map((i) => i.name), ['b', 'a'])
})

test('summarize reports the worst and counts only what needs you', () => {
  assert.deepEqual(summarize(['ok', 'warn', 'mute', 'bad', 'warn']), { sev: 'bad', count: 3 })
  assert.deepEqual(summarize(['ok', 'mute']), { sev: null, count: 0 })
})
