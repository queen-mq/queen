// A queue's last hour, from the Workload answer grouped by queue
// (GET /api/v1/analytics/workload?groupBy=queue): its totals over the window
// and its dead letters now. Read by stores/queueOpsStore for the one rule of
// what needs you (useAttention), which judges a queue's acks over that hour.
//
// Pure: no Vue, no '@/' imports (test/queue-ops.test.js).

const num = (v) => {
  const x = Number(v)
  return v === null || v === undefined || v === '' || !Number.isFinite(x) ? 0 : x
}

/** The answer's rows as Map name → `{ ackFailed, ackSuccess, popMessages, pushMessages, deadLetter }`. */
export function opsByQueue(payload) {
  const out = new Map()
  for (const r of payload?.rows || []) {
    if (!r?.key) continue
    const w = r.window || {}
    out.set(r.key, {
      ackFailed: num(w.ackFailed),
      ackSuccess: num(w.ackSuccess),
      popMessages: num(w.popMessages),
      pushMessages: num(w.pushMessages),
      deadLetter: r.now?.deadLetter === undefined || r.now?.deadLetter === null ? null : num(r.now.deadLetter),
    })
  }
  return out
}

/** "2,132 of 2,132 acks failed (100%)": a queue's hour of acks in words. */
export function acksText(o) {
  if (!o) return ''
  const failed = num(o.ackFailed)
  const total = failed + num(o.ackSuccess)
  if (!failed) return 'no ack failed'
  const pct = total > 0 ? Math.round((failed / total) * 1000) / 10 : null
  const f = failed.toLocaleString('en-US')
  return total > 0
    ? `${f} of ${total.toLocaleString('en-US')} acks failed (${pct}%)`
    : `${f} acks failed`
}
