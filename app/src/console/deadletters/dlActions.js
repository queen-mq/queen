// What the Dead letters page sends, and the files it saves.
//
//   replay   POST /api/v1/dlq/{id}/replay {}, one per row: the old page's move
//            (lock, push, delete in one transaction, as dlq:<row id>), so a
//            second replay of the same row writes nothing
//   discard  DELETE /api/v1/dlq?queue= when every dead letter of the queue is
//            the cause; else DELETE /api/v1/messages/{partitionId}/{transactionId},
//            one per message (it takes every consumer group's row of it)
//   download JSON as the broker returned the rows, or Markdown (useDlqMarkdown)
//
// The answers are judged as the old page judges them (useDlqReplay).
import { dlq as dlqApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { replayVerdict } from '@/composables/useDlqReplay'
import { formatDlqMarkdown } from '@/composables/useDlqMarkdown'

// Answers that say the next call fails the same way: stop sending.
const STOP = new Set([0, 401, 403, 429])
const PARALLEL = 4

async function pool(items, fn) {
  const queue = [...items]
  const run = async () => { while (queue.length) await fn(queue.shift()) }
  await Promise.all(Array.from({ length: Math.min(PARALLEL, queue.length) }, run))
}

/** Replay each row where it failed. → { asked, moved, duplicate, gone, failed, first, stopped } */
export async function replayRows(rows, onProgress) {
  const list = (rows || []).filter((m) => m?.id)
  const out = { asked: list.length, moved: 0, duplicate: 0, gone: 0, failed: 0, first: null, stopped: false }
  let done = 0
  await pool(list, async (m) => {
    if (out.stopped) return
    let v; let status = 200
    try {
      v = replayVerdict(await dlqApi.replay(m.id, {}, { probe: true, quiet: true }))
    } catch (e) {
      v = replayVerdict(e)
      status = Number.isFinite(e?.status) ? e.status : 0
    }
    if (v.kind === 'success') out.moved++
    else if (v.removeRow) out.gone++
    else if (v.kind === 'warning' && v.target) out.duplicate++
    else {
      out.failed++
      if (!out.first) out.first = v.detail || v.title
      if (v.unavailable || STOP.has(status)) out.stopped = true
    }
    onProgress?.(++done, list.length)
  })
  return out
}

/** Every dead letter of `queue`, in one call. → the number the broker deleted */
export async function purgeQueue(queue) {
  const r = await dlqApi.purge({ queue }, { quiet: true })
  if (r.data?.success !== true || !Number.isInteger(r.data?.deleted)) {
    throw new Error(r.data?.message || r.data?.error || 'The broker did not confirm the purge.')
  }
  return r.data.deleted
}

/** The rows' messages, one call each. → { asked, deleted, missed, failed, first, stopped } */
export async function deleteRows(rows, onProgress) {
  const seen = new Set()
  const list = []
  for (const m of rows || []) {
    if (!m?.partitionId || !m?.transactionId) continue
    const k = `${m.partitionId}\u0000${m.transactionId}`
    if (!seen.has(k)) { seen.add(k); list.push(m) }
  }
  const out = { asked: list.length, deleted: 0, missed: 0, failed: 0, first: null, stopped: false }
  let done = 0
  await pool(list, async (m) => {
    if (out.stopped) return
    try {
      const r = await dlqApi.delete(m.partitionId, m.transactionId, { quiet: true })
      if (r.data?.success === true) out.deleted += Number(r.data.deleted) || 1
      else out.missed++
    } catch (e) {
      if (e?.status === 404) out.missed++
      else {
        out.failed++
        if (!out.first) out.first = describeApiError(e)
        if (STOP.has(Number.isFinite(e?.status) ? e.status : 0)) out.stopped = true
      }
    }
    onProgress?.(++done, list.length)
  })
  return out
}

/** The rows as Markdown, one report each (the old page's "Copy as Markdown"). */
export function markdownOf(rows, context = {}, head = '') {
  const parts = (rows || []).map((m) => formatDlqMarkdown(m, context))
  return [head, ...parts].filter(Boolean).join('\n\n---\n\n')
}

/** Save `text` as a file in the browser: nothing is sent. */
export function saveFile(name, text, type = 'application/json') {
  const url = URL.createObjectURL(new Blob([text], { type }))
  const a = document.createElement('a')
  a.href = url
  a.download = name
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 2000)
}
