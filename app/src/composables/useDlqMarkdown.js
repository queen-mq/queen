import { formatTimestamp, runtimeTimeZone } from './useFormat.js'

/** Presentation helpers kept pure so clipboard reports need no mounted drawer. */

const validDate = (value) => {
  if (!value) return null
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? null : date
}

const longestBacktickRun = (text) => {
  const runs = String(text).match(/`+/g) || []
  return runs.reduce((longest, run) => Math.max(longest, run.length), 0)
}

/** A fence longer than anything in the value keeps arbitrary errors/JSON valid Markdown. */
const fenced = (value, language) => {
  const text = String(value)
  const fence = '`'.repeat(Math.max(3, longestBacktickRun(text) + 1))
  return `${fence}${language}\n${text}\n${fence}`
}

const inlineCode = (value) => {
  const text = String(value)
  const delimiter = '`'.repeat(Math.max(1, longestBacktickRun(text) + 1))
  const padded = /^\s|\s$|^`|`$/.test(text) ? ` ${text} ` : text
  return `${delimiter}${padded}${delimiter}`
}

const bullet = (label, value) =>
  value === null || value === undefined || value === ''
    ? null
    : `- **${label}:** ${inlineCode(value)}`

const timestampLines = (label, value, locales, timeZone) => {
  if (!value) return []
  const date = validDate(value)
  if (!date) return [bullet(label, value)]
  return [
    bullet(`${label} (UTC)`, date.toISOString()),
    bullet(`${label} (local)`, `${formatTimestamp(date, locales, { timeZone })} (${timeZone})`),
  ]
}

/**
 * Build a self-contained incident handoff for a coding agent. Values from the
 * broker remain exact; only timestamps gain an additional, explicit local view.
 */
export function formatDlqMarkdown(message, context = {}) {
  if (!message) return ''

  const locales = context.locales || 'en-US'
  const timeZone = context.timeZone || runtimeTimeZone()
  const failure = [
    bullet('Status', 'dead_letter'),
    bullet('Retry count', message.retryCount ?? 0),
    ...timestampLines('Failed at', message.failedAt, locales, timeZone),
  ]

  if (message.createdAt && message.createdAt !== message.failedAt) {
    failure.push(...timestampLines('Created at', message.createdAt, locales, timeZone))
  } else {
    failure.push('- **Original enqueue time:** not recorded for this DLQ entry')
  }

  const routing = [
    bullet('Tenant', context.tenant),
    bullet('Cluster', context.cluster),
    bullet('Cell', context.cell),
    bullet('Queue', message.queue),
    bullet('Partition', message.partition),
    bullet('Partition ID', message.partitionId),
    bullet('Consumer group', message.consumerGroup),
    bullet('Transaction ID', message.transactionId),
    bullet('Message ID', message.id),
  ].filter(Boolean)

  const payload = JSON.stringify(message.data ?? null, null, 2)
  const payloadNotes = []
  if (context.encryptedEnvelope) {
    payloadNotes.push('> Warning: this is the stored encrypted envelope, not the plaintext message body.')
  } else if (message.isEncrypted === true) {
    payloadNotes.push('> This payload was encrypted at rest and decrypted by the broker for this DLQ response.')
  }

  return [
    '# DLQ message investigation',
    '',
    'A message was routed to Queen\'s dead-letter queue. Use the failure context and payload below to identify the producer or consumer defect.',
    '',
    '## Failure',
    '',
    ...failure.filter(Boolean),
    '',
    '### Error',
    '',
    fenced(message.errorMessage || 'No error text was recorded.', 'text'),
    '',
    '## Routing',
    '',
    ...routing,
    '',
    '## Payload',
    '',
    ...payloadNotes,
    ...(payloadNotes.length ? [''] : []),
    fenced(payload, 'json'),
    '',
  ].join('\n')
}
