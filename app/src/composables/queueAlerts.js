// What needs you on ONE queue, for its Alerts tab and the count on that tab.
// The rules are useAttention's, so the queue list's mark, the Overview's issue
// list and this tab never disagree about the same queue:
//
//   no reader      the queue holds messages nothing is set up to read
//   behind         a live consumer group of this queue past its lag lines
//
// Each alert names the tab where it is worked on. Worst first, then by name.
//
// Pure: no Vue, no '@/' imports, so test/queue-alerts.test.js pins it directly.
import { formatSpan } from './settingsDoc.js'
import { groupAttention, queueAttention } from './useAttention.js'

const QUEUE_MODE = '__QUEUE_MODE__'
const RANK = { bad: 0, warn: 1 }

const num = (v) => {
  const x = Number(v)
  return v === null || v === undefined || v === '' || !Number.isFinite(x) ? null : x
}

/** A consumer group as a person names it: queue mode has no name of its own. */
export const groupLabel = (g) => (g?.name === QUEUE_MODE ? 'Queue mode' : String(g?.name ?? ''))

/** `[{ key, sev, title, detail, tab }]` for one queue row of the queues listing. */
export function queueAlerts(queue, groups = [], linesFor) {
  if (!queue?.name) return []
  const mine = (groups || []).filter((g) => g?.queueName === queue.name)
  const out = []

  const [verdict] = queueAttention([queue], mine, linesFor)
  if (verdict?.reason === 'noReader') {
    const pending = num(queue.messages?.pending) || 0
    out.push({
      key: 'noReader',
      sev: 'warn',
      title: 'No consumer group reads it',
      detail: `${pending.toLocaleString('en-US')} pending, and no consumer group is set up to read ${verdict.deadOnly ? 'them: its queue-mode reader has never consumed' : 'them'}.`,
      tab: 'consumers',
    })
  }

  for (const g of mine) {
    const sev = groupAttention(g, linesFor)
    if (sev !== 'warn' && sev !== 'bad') continue
    const lag = num(g.maxTimeLag)
    out.push({
      key: `lag:${g.name}`,
      sev,
      title: `${groupLabel(g)} is ${sev === 'bad' ? 'falling behind' : 'behind'}`,
      detail: lag === null ? 'Its lag is past the line set for this queue.' : `Its oldest unconsumed message is ${formatSpan(lag)} old.`,
      tab: 'consumers',
    })
  }

  return out.sort((a, b) => RANK[a.sev] - RANK[b.sev] || a.title.localeCompare(b.title))
}
