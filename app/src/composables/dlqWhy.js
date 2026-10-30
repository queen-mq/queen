// The Dead letter tab's "Why they failed": one row per kind of error, worst
// first, each with its share and its text in full.
//
// With a queue picked, the kinds are the broker's (GET /api/v1/analytics/
// dlq-signatures?queue=): the newest 200 rows of the queue's dead letter,
// each error folded into a signature — ids and uuids as <id>, numbers as <n>,
// dates as <date>, at most 120 characters. A signature is matched back to a
// row of the page for its full text and for the table's filter. Without a
// queue, the kinds are the page's own error texts.
//
// Pure: no Vue, no '@/' imports (test/dlq-why.test.js).

const PLACEHOLDER = /<(?:id|n|date)>/

/** The literal head of a signature: its text up to the first placeholder. */
export function sigPrefix(text) {
  const s = String(text ?? '')
  const at = s.search(PLACEHOLDER)
  return (at < 0 ? s : s.slice(0, at)).trim()
}

const collapse = (s) => String(s ?? '').trim().split(/\s+/).join(' ')

/**
 * The full error of the first row in `rows` that carries signature `text`, or
 * null. A row matches when its error starts with the signature's literal head
 * (whitespace collapsed); a head under 12 characters says too little, so the
 * whole error must then equal the signature.
 */
export function errorExample(text, rows = []) {
  const sig = String(text ?? '')
  if (sig === '(no message)') {
    const empty = (rows || []).find((m) => !collapse(m?.errorMessage))
    return empty ? '' : null
  }
  const head = sigPrefix(sig)
  for (const m of rows || []) {
    const err = collapse(m?.errorMessage)
    if (!err) continue
    if (head.length >= 12 ? err.startsWith(head) : err === sig) return m.errorMessage
  }
  return null
}

/** "poll-28cade7a-0726-5…bc2a-ae36cf0f8e92": the head and the tail of a long id, which tell rows apart. */
export function midCut(s, max = 28) {
  const t = String(s ?? '')
  if (t.length <= max) return t
  const keep = max - 1
  const head = Math.ceil(keep / 2)
  return `${t.slice(0, head)}…${t.slice(t.length - (keep - head))}`
}

/**
 * The card's rows: `[{ key, text, cut, n, share, filter }]`, worst first.
 * `filter` is the exact error the table can be narrowed to (null when no row
 * of the page carries it); `cut` says the text is the broker's 120-character
 * signature, not a full error.
 *
 * @param {object|null} why     the dlq-signatures answer for the picked queue, or null
 * @param {Array} pageGroups    the page's own `{ error, count, share }`, worst first
 * @param {Array} rows          the page's rows
 */
export function whyRows(why, pageGroups = [], rows = []) {
  if (why && Array.isArray(why.signatures) && why.signatures.length) {
    return why.signatures.map((s) => {
      const example = errorExample(s.text, rows)
      return {
        key: s.text,
        text: example === null ? s.text : (example || '(no error text recorded)'),
        cut: example === null && String(s.text).length >= 120,
        n: Number(s.n) || 0,
        share: Number(s.share) || 0,
        filter: example === null ? null : (example || '(no error text recorded)'),
      }
    })
  }
  return (pageGroups || []).map((g) => ({
    key: g.error, text: g.error, cut: false, n: g.count, share: g.share, filter: g.error,
  }))
}

/** The card's subtitle: what the rows were counted over. */
export function whyScope(why, pageCount, time = (iso) => iso) {
  if (!why || !Array.isArray(why.signatures) || !why.signatures.length) {
    return `${pageCount.toLocaleString('en-US')} ${pageCount === 1 ? 'message' : 'messages'} on this page`
  }
  const sample = Number(why.sample) || 0
  const total = Number(why.rowsNow) || 0
  const over = sample >= total
    ? `all ${total.toLocaleString('en-US')}`
    : `the newest ${sample.toLocaleString('en-US')} of ${total.toLocaleString('en-US')}`
  const span = why.oldest && why.newest ? ` · ${time(why.oldest)} to ${time(why.newest)}` : ''
  return `${over} in dead letter${span}`
}
