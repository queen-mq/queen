// The Connectors page's words (src/console/connectors/connectorsView.js,
// queen-mq/queen#155), held against what GET /api/v1/connectors answered on a
// scratch 2.7 broker on 2026-11-03: a source streaming, a sink running, a
// source failing on a missing table, plus a standby and a disabled one.

import { test } from 'node:test'
import assert from 'node:assert/strict'

import { bytes, detailOf, flowsOf, phaseOf, rowOf, verdictOf } from '../src/console/connectors/connectorsView.js'

const NOW = Date.parse('2026-11-03T07:30:00Z')
const conn = { connectTimeoutMs: 10000, database: 'postgres', host: '127.0.0.1', passwordSet: false, port: 17854, sslMode: 'disable', user: 'postgres' }
const source = (over = {}) => ({
  name: 'orders-src', kind: 'source', enabled: true, connection: conn,
  source: { tables: [{ table: 'public.orders', queue: 'pg.orders' }], managePublication: true },
  status: {
    bundles: 4, confirmedFlushLsn: '0/17C1E78', enabled: true, error: null, generation: 3, kind: 'source',
    lastCommitAt: '2026-11-03T07:27:18.716234Z', messages: 4, name: 'orders-src', phase: 'streaming',
    pointerLsn: '0/17C2388', publication: 'queen_orders_src', restarts: 0, since: '2026-11-03T07:27:08.364392Z',
    slot: 'queen_orders_src', slotLagBytes: 952, transactions: 1, walStatus: 'reserved',
  },
  state: { lease: { node: 'queen-ebca5594' }, pointer: { lsn: '0/17C2388' } },
  ...over,
})
const sink = {
  name: 'orders-copy', kind: 'sink', enabled: true, connection: conn,
  sink: { queue: 'pg.orders', table: 'public.orders_copy', mode: 'cdc' },
  status: {
    applied: 4, batches: 4, dlq: 0, enabled: true, error: null, group: 'pg-orders-copy', kind: 'sink',
    lastAppliedAt: '2026-11-03T07:27:18.930911Z', mode: 'cdc', phase: 'running', queue: 'pg.orders',
    restarts: 0, since: '2026-11-03T07:27:08.125765Z', skipped: 0, table: 'public.orders_copy', workers: 1,
  },
  state: null,
}
const failing = source({
  name: 'invoices-src',
  source: { tables: [{ table: 'public.missing_invoices', queue: 'pg.invoices' }] },
  status: {
    enabled: true, generation: 1, kind: 'source', phase: 'error', restarts: 0, retryInMs: 30000,
    since: '2026-11-03T07:27:08.150881Z', slot: 'queen_invoices_src', publication: 'queen_invoices_src',
    error: { at: '2026-11-03T07:27:08.150987Z', code: 'table_missing', message: 'table_missing: the table public.missing_invoices does not exist (or is not a table)' },
  },
  state: { lease: null, pointer: null },
})

test('a row says what the connector moves, whether it runs, and what it moved', () => {
  const s = rowOf(source(), NOW)
  assert.equal(s.kind, 'Source')
  assert.equal(s.word, 'Streaming')
  assert.equal(s.tone, '')
  assert.deepEqual(s.flows, [{ from: 'public.orders', to: 'pg.orders' }])
  assert.equal(s.moved, '4')
  assert.equal(s.behind, '952 B')
  assert.equal(s.last, '3 min ago') // 2 min 42 s, rounded
  assert.equal(s.database, '127.0.0.1:17854/postgres')
  const k = rowOf(sink, NOW)
  assert.equal(k.kind, 'Sink')
  assert.equal(k.word, 'Running')
  assert.deepEqual(flowsOf(sink), [{ from: 'pg.orders', to: 'public.orders_copy' }])
  assert.equal(k.moved, '4')
  assert.equal(k.behind, '–', 'a sink holds no slot')
})

test('a failing connector is red, and its message is said once, without its code in front', () => {
  const r = rowOf(failing, NOW)
  assert.equal(r.word, 'Failing')
  assert.equal(r.tone, 'red')
  assert.equal(r.error.code, 'table_missing')
  assert.equal(r.error.message, 'the table public.missing_invoices does not exist (or is not a table)')
  assert.equal(r.retryIn, 30000)
  const v = verdictOf([rowOf(source(), NOW), rowOf(sink, NOW), r])
  assert.equal(v.tone, 'red')
  assert.deepEqual(v.names, ['invoices-src'])
  assert.equal(v.text, 'needs you: the table public.missing_invoices does not exist (or is not a table).')
})

test('a standby source runs elsewhere: its node is named, and this node says no counters', () => {
  const r = rowOf(source({ status: { phase: 'standby', owner: 'queen-mq-v2-1', messages: 0 }, state: { lease: { node: 'queen-mq-v2-1' } } }), NOW)
  assert.equal(r.word, 'Standby')
  assert.equal(r.tone, '')
  assert.equal(r.owner, 'queen-mq-v2-1')
  assert.equal(r.elsewhere, true)
  assert.equal(r.moved, '–')
  assert.equal(r.behind, '–')
  assert.match(r.title, /runs on queen-mq-v2-1/)
})

test('the verdict when all is well, when some are off, and when there are none', () => {
  assert.equal(verdictOf([rowOf(source(), NOW), rowOf(sink, NOW)]).text, 'All 2 run.')
  assert.equal(verdictOf([rowOf(sink, NOW)]).text, 'It runs.')
  const off = rowOf({ ...sink, enabled: false }, NOW)
  assert.equal(off.word, 'Off')
  assert.equal(verdictOf([rowOf(source(), NOW), off]).text, '1 of 2 run; 1 is off.')
  assert.equal(verdictOf([]), null)
})

test('a phase the console does not know is said as the broker wrote it; no status at all is amber', () => {
  assert.equal(phaseOf('some_new_phase').word, 'Some new phase')
  const unknown = rowOf({ ...sink, status: { phase: 'unknown' } }, NOW)
  assert.equal(unknown.word, 'Not running here')
  assert.equal(unknown.tone, 'amber')
  assert.equal(rowOf({ ...sink, status: undefined }, NOW).tone, 'amber')
})

test('the drawer: facts, then the work, by kind', () => {
  const d = detailOf(source(), NOW)
  assert.deepEqual(d.facts.map((f) => f.k), ['Kind', 'State', 'Runs on', 'Database', 'Login', 'TLS'])
  assert.equal(d.facts.find((f) => f.k === 'Login').v, 'postgres, no password')
  assert.deepEqual(d.work.map((f) => f.k), ['Slot', 'Publication', 'Messages', 'Slot lag', 'Pointer', 'Last commit'])
  assert.equal(d.work.find((f) => f.k === 'Messages').sub, '1 transactions, 4 pushes')
  const k = detailOf(sink, NOW)
  assert.deepEqual(k.work.map((f) => f.k), ['Group', 'Mode', 'Applied', 'Last apply', 'Workers'])
  assert.equal(k.work.find((f) => f.k === 'Applied').sub, '4 batches, 0 skipped, 0 dead-lettered')
  assert.equal(detailOf(null), null)
})

test('bytes read in the unit that fits', () => {
  assert.equal(bytes(952), '952 B')
  assert.equal(bytes(4 * 1024 * 1024), '4.0 MB')
  assert.equal(bytes(73 * 1024 ** 3), '73 GB')
  assert.equal(bytes(null), '–')
})

test('a MySQL source says how far behind it is in seconds and keeps its place in GTIDs', () => {
  const gtids = 'c013ad95-de73-11f1-bb13-3215ccb915f6:1-42'
  const my = {
    name: 'shop-src', kind: 'source', engine: 'mysql', enabled: true,
    connection: { ...conn, port: 3306, database: 'shop', user: 'repl' },
    source: { tables: [{ table: 'shop.orders', queue: 'orders' }] },
    status: {
      phase: 'streaming', engine: 'mysql', messages: 12, transactions: 5, bundles: 3, lagSeconds: 0,
      binlog: { file: 'binlog.000002', position: 4123 }, pointerGtids: gtids,
      serverUuid: 'c013ad95-de73-11f1-bb13-3215ccb915f6', serverVersion: '8.4.13', serverId: 2918571043,
      lastCommitAt: '2026-11-03T07:29:58Z',
    },
    state: { lease: { node: 'queen-ebca5594' }, pointer: { gtids } },
  }
  const r = rowOf(my, NOW)
  assert.equal(r.kind, 'Source')
  assert.equal(r.label, 'MySQL source')
  assert.equal(r.engine, 'MySQL')
  assert.equal(rowOf(source(), NOW).engine, 'PostgreSQL')
  assert.equal(r.behind, '0 s')
  assert.equal(r.database, '127.0.0.1:3306/shop')
  const d = detailOf(my, NOW)
  assert.deepEqual(d.work.map((f) => f.k), ['Server', 'Replica id', 'Messages', 'Lag', 'Pointer', 'Last commit'])
  assert.equal(d.work.find((f) => f.k === 'Lag').sub, 'at binlog.000002:4123')
  assert.equal(d.work.find((f) => f.k === 'Replica id').v, '2918571043')
  // Standing by: the pointer as the owner last committed it, no counters.
  const standby = { ...my, status: { phase: 'standby', engine: 'mysql', owner: 'n2' } }
  assert.deepEqual(detailOf(standby, NOW).work.map((f) => f.k), ['Pointer'])
  assert.equal(rowOf(standby, NOW).behind, '–')
  // A Postgres source still says its slot lag in bytes.
  assert.equal(rowOf(source(), NOW).behind, '952 B')
})
