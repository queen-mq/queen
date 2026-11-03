// Pure: the node tests import it (test/deadletters.test.js).

const offsetOf = (m) => (m?.offset == null || m.offset === '' || !Number.isFinite(Number(m.offset)) ? null : Number(m.offset))

/**
 * The order replays go back in (queen-mq/queen#147): one list per partition,
 * oldest first, by offset (the order its messages were first written), then
 * by failure time. A replay still lands at the tail of its partition; this
 * only keeps one partition's replays in order among themselves.
 */
export function replayOrder(rows) {
  const by = new Map()
  for (const m of rows || []) {
    const k = m?.partitionId ?? m?.partition ?? ''
    if (!by.has(k)) by.set(k, [])
    by.get(k).push(m)
  }
  for (const list of by.values()) {
    list.sort((a, b) => {
      const x = offsetOf(a); const y = offsetOf(b)
      if ((x == null) !== (y == null)) return x == null ? 1 : -1
      if (x != null && x !== y) return x - y
      const fa = String(a?.failedAt || ''); const fb = String(b?.failedAt || '')
      if (fa !== fb) return fa < fb ? -1 : 1
      return String(a?.id || '').localeCompare(String(b?.id || ''))
    })
  }
  return [...by.values()]
}
