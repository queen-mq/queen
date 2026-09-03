import { test } from 'node:test'
import assert from 'node:assert/strict'

import { formatDlqMarkdown } from '../src/composables/useDlqMarkdown.js'
import { formatTimestamp } from '../src/composables/useFormat.js'

const message = {
  id: 'row-17',
  transactionId: 'tx-123',
  partitionId: 'partition-uuid',
  partition: 'connection:abc',
  queue: 'channel.opsync_in',
  consumerGroup: 'channel.opsync_in.core',
  retryCount: 5,
  failedAt: '2026-09-03T14:03:06.000Z',
  createdAt: '2026-09-03T14:03:06.000Z',
  errorMessage: 'calendar.apply names no property',
  data: { kind: 'calendar.apply', payload: { from: '2026-09-03' } },
}

test('timestamp renders in the requested browser timezone', () => {
  assert.equal(
    formatTimestamp(message.failedAt, 'en-US', { timeZone: 'Europe/Rome' }),
    'Sep 3, 2026, 04:03:06 PM',
  )
})

test('Markdown report carries the complete debugging handoff', () => {
  const markdown = formatDlqMarkdown(message, {
    tenant: 'acme',
    cluster: 'production',
    cell: 'eu-south',
    locales: 'en-US',
    timeZone: 'Europe/Rome',
  })

  for (const expected of [
    '# DLQ message investigation',
    '**Queue:** `channel.opsync_in`',
    '**Partition:** `connection:abc`',
    '**Partition ID:** `partition-uuid`',
    '**Transaction ID:** `tx-123`',
    '**Consumer group:** `channel.opsync_in.core`',
    '**Retry count:** `5`',
    '**Failed at (UTC):** `2026-09-03T14:03:06.000Z`',
    '04:03:06 PM (Europe/Rome)',
    'calendar.apply names no property',
    '"kind": "calendar.apply"',
    '**Original enqueue time:** not recorded',
  ]) assert.match(markdown, new RegExp(expected.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')))
})

test('Markdown fences cannot be broken by payload or error content', () => {
  const markdown = formatDlqMarkdown({
    ...message,
    errorMessage: 'bad ``` text',
    data: { snippet: '````' },
  }, { timeZone: 'UTC' })

  assert.match(markdown, /````text\nbad ``` text\n````/)
  assert.match(markdown, /`````json\n[\s\S]*"snippet": "````"[\s\S]*\n`````/)
})

test('encrypted envelope is labelled as ciphertext', () => {
  const markdown = formatDlqMarkdown(message, { encryptedEnvelope: true, timeZone: 'UTC' })
  assert.match(markdown, /stored encrypted envelope, not the plaintext/)
})
