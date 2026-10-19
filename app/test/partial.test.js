import { test } from 'node:test'
import assert from 'node:assert/strict'

import { missingNodes, partialText, PARTIAL_TITLE } from '../src/composables/usePartial.js'

test('missingNodes: every answer that says, once each, ascending; silence is not completeness', () => {
  assert.deepEqual(missingNodes({ nodesMissing: [] }), [])
  assert.deepEqual(missingNodes({ series: [] }), [], 'an older broker says nothing')
  assert.deepEqual(missingNodes(null, undefined), [])
  assert.deepEqual(missingNodes({ nodesMissing: [3, 2] }, { nodesMissing: [2] }, null), [2, 3])
  assert.deepEqual(missingNodes({ nodesMissing: [10, 9] }), [9, 10], 'numeric, not text, order')
})

test('partialText: names the nodes and says what their absence does', () => {
  assert.equal(partialText([]), '')
  assert.equal(partialText(undefined), '')
  assert.equal(
    partialText([2]),
    'Partial: node 2 did not answer — its traffic is missing from these numbers',
  )
  assert.equal(
    partialText([1, 2, 3]),
    'Partial: nodes 1, 2 and 3 did not answer — their traffic is missing from these numbers',
  )
  assert.match(PARTIAL_TITLE, /Kafka produce/)
})
