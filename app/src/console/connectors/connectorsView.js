// The Connectors page, in words (queen-mq/queen#155): each PostgreSQL source
// and sink and each MySQL source of the tenant, what it moves where, whether
// it runs, and what it has moved, from GET /api/v1/connectors
// (server/src/handlers/connectors.rs).
//
// Each entry is the stored document, redacted by the broker (never the
// password), plus `status`: what THE NODE THAT ANSWERED knows of it. Every
// node runs every connector; a source streams only on the node holding its
// lease, and the others report `standby` with that node's name, so a
// source's counters are the owner's to say. `state` is a source's pointer
// and lease, read from the replicated KV, the same on every node.
//
// Pure: the node tests import it (test/connectors-page.test.js).

const num = (v) => (v == null || v === '' || !Number.isFinite(Number(v)) ? null : Number(v))
const count = (v) => (num(v) == null ? '–' : Math.round(num(v)).toLocaleString('en-US'))

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB']
export function bytes(v) {
  let x = num(v)
  if (x == null) return '–'
  let i = 0
  while (x >= 1024 && i < UNITS.length - 1) { x /= 1024; i++ }
  return `${i === 0 ? Math.round(x) : x < 10 ? x.toFixed(1) : Math.round(x)} ${UNITS[i]}`
}

export function ago(ms, now = Date.now()) {
  if (ms == null || !Number.isFinite(ms)) return ''
  const s = Math.max(0, Math.round((now - ms) / 1000))
  if (s < 60) return `${s} s ago`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min ago`
  const h = Math.round(m / 60)
  if (h < 48) return `${h} h ago`
  return `${Math.round(h / 24)} d ago`
}
const at = (iso) => { const t = Date.parse(iso || ''); return Number.isFinite(t) ? t : null }
const unprefixed = (err) => {
  const msg = String(err?.message || ''); const code = String(err?.code || '')
  return code && msg.startsWith(`${code}: `) ? msg.slice(code.length + 2) : msg
}

/**
 * A phase in one or two words, and its tone: colour only where it will not
 * fix itself (amber) or fails (red), as everywhere in the console.
 */
export const PHASES = {
  streaming: { word: 'Streaming', tone: '', title: 'Sending the tables’ committed changes to their queues.' },
  snapshot: { word: 'Snapshot', tone: '', title: 'Copying the tables’ rows first, then it streams their changes.' },
  running: { word: 'Running', tone: '', title: 'Writing its queue’s messages into the table.' },
  standby: { word: 'Standby', tone: '', title: 'Another node holds its lease and runs it; this node takes over if that one stops.' },
  starting: { word: 'Starting', tone: '', title: '' },
  connecting: { word: 'Connecting', tone: '', title: 'Opening its connection to the database.' },
  waiting_for_slot: { word: 'Waiting for its slot', tone: 'amber', title: 'Another connection holds the replication slot, so the source cannot read it.' },
  error: { word: 'Failing', tone: 'red', title: '' },
  disabled: { word: 'Off', tone: '', title: 'Its document says enabled: false.' },
  stopped: { word: 'Stopped', tone: '', title: '' },
  torn_down: { word: 'Deleted', tone: '', title: 'Its slot and publication are gone; the document goes next.' },
  unknown: { word: 'Not running here', tone: 'amber', title: 'The node that answered does not run the connectors, so it cannot say how this one is.' },
}
export const phaseOf = (p) => PHASES[p] || { word: p ? String(p).replace(/_/g, ' ').replace(/^./, (c) => c.toUpperCase()) : 'Unknown', tone: '', title: '' }

/** "public.orders → pg.orders" for a source, "pg.orders → public.orders_copy" for a sink. */
export function flowsOf(c) {
  if (c?.kind === 'sink') {
    const s = c.sink || {}
    return [{ from: s.queue || '?', to: s.table || '?' }]
  }
  return (c?.source?.tables || []).map((t) => ({ from: t.table || '?', to: t.queue || '?' }))
}

/** host:port/database, as the document names it. */
export function databaseOf(c) {
  const k = c?.connection || {}
  const host = k.host || '?'
  return `${host}${k.port ? `:${k.port}` : ''}/${k.database || '?'}`
}

/** `mysql` for a MySQL source, else `postgres` (a document without one). */
export const engineOf = (c) => (c?.engine === 'mysql' ? 'mysql' : 'postgres')

/**
 * One connector as the table draws it. `moved` is what it has moved on the
 * node that runs it (a source's messages, a sink's applied rows), `behind`
 * how far a source trails its database (Postgres: the WAL its slot still
 * holds; MySQL: seconds since the last transaction it has not sent, 0 at the
 * end of the binlog), and `last` its last commit or apply.
 */
export function rowOf(c, now = Date.now()) {
  const st = c?.status || {}
  const enabled = c?.enabled !== false
  const phaseKey = enabled ? (st.phase || 'unknown') : 'disabled'
  const phase = phaseOf(phaseKey)
  const owner = c?.state?.lease?.node || st.owner || null
  const source = c?.kind !== 'sink'
  const mysql = engineOf(c) === 'mysql'
  const elsewhere = phaseKey === 'standby'
  const moved = source ? num(st.messages) : num(st.applied)
  const last = at(source ? st.lastCommitAt : st.lastAppliedAt)
  const err = st.error && typeof st.error === 'object' ? st.error : null
  return {
    key: c?.name || '',
    name: c?.name || '',
    kind: source ? 'Source' : 'Sink',
    engine: mysql ? 'MySQL' : 'PostgreSQL',
    label: source ? (mysql ? 'MySQL source' : 'Source') : 'Sink',
    phase: phaseKey,
    word: phase.word,
    tone: phase.tone,
    title: elsewhere && owner ? `${phase.title} It runs on ${owner}.` : phase.title,
    owner,
    elsewhere,
    flows: flowsOf(c),
    database: databaseOf(c),
    moved: elsewhere || moved == null ? '–' : count(moved),
    movedTitle: source ? 'Messages it pushed to its queues since it started on this node' : 'Messages it applied to its table since it started on this node',
    behind: !source || elsewhere ? '–'
      : mysql ? (num(st.lagSeconds) != null ? `${count(st.lagSeconds)} s` : '–')
        : num(st.slotLagBytes) != null ? bytes(st.slotLagBytes) : '–',
    last: elsewhere || last == null ? '' : ago(last, now),
    // The broker's message starts with its code ("table_missing: the table …"): said once.
    error: err ? { code: err.code || '', message: unprefixed(err), at: at(err.at) } : null,
    retryIn: num(st.retryInMs),
  }
}

/**
 * The line under the title. A connector that fails or waits needs you; the
 * rest is said in one sentence.
 */
export function verdictOf(rows) {
  if (!rows.length) return null
  const needs = rows.filter((r) => r.tone)
  if (!needs.length) {
    const off = rows.filter((r) => r.phase === 'disabled').length
    const all = rows.length === 1 ? 'It runs' : `All ${rows.length} run`
    return { tone: '', names: [], text: off === rows.length ? (rows.length === 1 ? 'It is off.' : 'Every one is off.') : off ? `${rows.length - off} of ${rows.length} run; ${off} ${off === 1 ? 'is' : 'are'} off.` : `${all}.` }
  }
  const worst = needs.find((r) => r.tone === 'red') || needs[0]
  const why = worst.error?.message || worst.word.toLowerCase()
  if (needs.length === 1) return { tone: worst.tone, names: [worst.name], text: `needs you: ${why}${/[.!?]$/.test(why) ? '' : '.'}` }
  return { tone: worst.tone, names: needs.slice(0, 2).map((r) => r.name), more: Math.max(0, needs.length - 2), text: 'need you.' }
}

/** Key and value pairs for the drawer, in the order a reader asks. */
export function detailOf(c, now = Date.now()) {
  if (!c) return null
  const r = rowOf(c, now)
  const st = c.status || {}
  const k = c.connection || {}
  const facts = [
    { k: 'Kind', v: r.kind === 'Sink' ? 'Sink: a queue into a table' : r.engine === 'MySQL' ? 'Source: a MySQL table’s changes into a queue, from the binlog' : 'Source: a table’s changes into a queue' },
    { k: 'State', v: r.word + (st.since ? `, since ${ago(at(st.since), now)}` : ''), tone: r.tone },
  ]
  if (r.owner) facts.push({ k: 'Runs on', v: r.owner, mono: true })
  facts.push({ k: 'Database', v: r.database, mono: true })
  facts.push({ k: 'Login', v: `${k.user || '?'}${k.passwordSet ? ', password set' : ', no password'}`, mono: false })
  if (k.sslMode) facts.push({ k: 'TLS', v: k.sslMode, mono: true })
  if (num(st.restarts)) facts.push({ k: 'Restarts', v: count(st.restarts) })

  const work = []
  if (r.kind === 'Source' && r.engine === 'MySQL') {
    if (st.serverUuid) work.push({ k: 'Server', v: st.serverUuid, mono: true, sub: st.serverVersion ? `MySQL ${st.serverVersion}` : '' })
    if (num(st.serverId) != null) work.push({ k: 'Replica id', v: String(st.serverId), mono: true })
    if (!r.elsewhere) {
      if (num(st.messages) != null) work.push({ k: 'Messages', v: count(st.messages), sub: `${count(st.transactions)} transactions, ${count(st.bundles)} pushes` })
      if (num(st.lagSeconds) != null) work.push({ k: 'Lag', v: `${count(st.lagSeconds)} s`, sub: st.binlog?.file ? `at ${st.binlog.file}:${st.binlog.position}` : '' })
      if (st.pointerGtids) work.push({ k: 'Pointer', v: st.pointerGtids, mono: true, sub: 'the GTIDs in Queen' })
      if (st.lastCommitAt) work.push({ k: 'Last commit', v: ago(at(st.lastCommitAt), now) })
    } else if (c.state?.pointer?.gtids) {
      work.push({ k: 'Pointer', v: c.state.pointer.gtids, mono: true, sub: 'as the owner last committed it' })
    }
  } else if (r.kind === 'Source') {
    const s = c.source || {}
    if (st.slot || s.slot) work.push({ k: 'Slot', v: st.slot || s.slot, mono: true })
    if (st.publication || s.publication) work.push({ k: 'Publication', v: st.publication || s.publication, mono: true })
    if (!r.elsewhere) {
      if (num(st.messages) != null) work.push({ k: 'Messages', v: count(st.messages), sub: `${count(st.transactions)} transactions, ${count(st.bundles)} pushes` })
      if (num(st.slotLagBytes) != null) work.push({ k: 'Slot lag', v: bytes(st.slotLagBytes), sub: st.walStatus ? `WAL ${st.walStatus}` : '' })
      if (st.pointerLsn) work.push({ k: 'Pointer', v: st.pointerLsn, mono: true, sub: st.confirmedFlushLsn ? `confirmed ${st.confirmedFlushLsn}` : '' })
      if (st.lastCommitAt) work.push({ k: 'Last commit', v: ago(at(st.lastCommitAt), now) })
    } else if (c.state?.pointer?.lsn) {
      work.push({ k: 'Pointer', v: c.state.pointer.lsn, mono: true, sub: 'as the owner last committed it' })
    }
  } else {
    const s = c.sink || {}
    work.push({ k: 'Group', v: st.group || s.consumerGroup || '–', mono: true })
    work.push({ k: 'Mode', v: s.mode || st.mode || '–', mono: true, sub: s.key?.length ? `key ${s.key.join(', ')}` : '' })
    if (num(st.applied) != null) work.push({ k: 'Applied', v: count(st.applied), sub: `${count(st.batches)} batches, ${count(st.skipped)} skipped, ${count(st.dlq)} dead-lettered` })
    if (st.lastAppliedAt) work.push({ k: 'Last apply', v: ago(at(st.lastAppliedAt), now) })
    if (num(st.workers) != null) work.push({ k: 'Workers', v: count(st.workers) })
  }
  return { row: r, facts, work, flows: r.flows, error: r.error, retryIn: r.retryIn }
}
