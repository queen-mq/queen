import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  GRANT_KINDS, blankRow, blockRows, ceilingChoices, grantKind, grantOf, grantProblem, grantText, grantsSummary,
  lengthOf, limitsOf, namesSub, patternProblem, rowOf, rpsProblem, switchKind, validSub,
} from '../src/composables/scopedKeys.js'

// Shapes as proxy/src/console.rs list_keys answers a scoped key's grants.
const telemetry = { queue: 'telemetry', partition: '{sub}', ops: ['produce'] }
const settings = { kv: 'devices', key: '{sub}/*', ops: ['read', 'write'] }

test('a grant reads as one line: surface, the fields it names, its ops', () => {
  assert.equal(grantText(telemetry), 'queue telemetry · partition {sub} · produce')
  assert.equal(grantText(settings), 'kv devices · key {sub}/* · read, write')
  assert.equal(grantText({ timers: 'alarms*', ops: [] }), 'timers alarms* · nothing')
  assert.equal(grantText({ queue: 'a', kv: 'b', ops: ['read'] }), 'a grant this page cannot read')
  assert.equal(grantsSummary([telemetry]), grantText(telemetry))
  assert.equal(grantsSummary([telemetry, settings, telemetry]), `${grantText(telemetry)} · +2 more`)
  assert.equal(grantsSummary(null), 'nothing')
})

test('the surface is the one field of queue, kv and timers that is set', () => {
  assert.equal(grantKind(telemetry), 'queue')
  assert.equal(grantKind(settings), 'kv')
  assert.equal(grantKind({ timers: 'x', ops: ['read'] }), 'timers')
  assert.equal(grantKind({ ops: ['read'] }), '')
  assert.equal(grantKind({ queue: 'a', timers: 'b' }), '')
  assert.ok(namesSub([settings]))
  assert.ok(!namesSub([{ queue: 'orders', ops: ['produce'] }]))
})

test('patterns follow the proxy: a name, prefix*, *, or {sub} once', () => {
  for (const ok of ['orders', 'orders.*', '*', '{sub}', 'dev/{sub}', '{sub}/*', '{sub}:*', 'a{sub}b']) {
    assert.equal(patternProblem('queue', ok), '', ok)
  }
  assert.match(patternProblem('queue', ''), /empty/)
  assert.match(patternProblem('queue', 'a*b'), /may only end/)
  assert.match(patternProblem('queue', '**'), /may only end/)
  assert.match(patternProblem('key', '{sub}{sub}'), /once/)
  assert.match(patternProblem('key', '{x}'), /only as \{sub\}/)
  // `dev-1*` would cover `dev-12`: a * right after {sub} needs a separator.
  assert.match(patternProblem('partition', '{sub}*'), /separator/)
  assert.match(patternProblem('partition', '{sub}x*'), /separator/)
  assert.match(patternProblem('queue', 'x'.repeat(257)), /256 bytes/)
})

test('a grant is refused for its kind’s rules, with the field named', () => {
  assert.equal(grantProblem(telemetry), '')
  assert.equal(grantProblem(settings), '')
  assert.match(grantProblem({ ops: ['read'] }), /Pick a queue/)
  assert.match(grantProblem({ kv: 'devices', partition: 'p', ops: ['read'] }), /kv grant has no partition/)
  assert.match(grantProblem({ queue: 'q', key: 'k', ops: ['produce'] }), /queue grant has no key/)
  assert.match(grantProblem({ queue: 'q', ops: [] }), /Pick what it may do: push, or pop and acknowledge/)
  assert.match(grantProblem({ queue: 'q', ops: ['read'] }), /takes produce and consume, not read/)
  assert.match(grantProblem({ kv: '', ops: ['read'] }), /namespace is empty/)
})

test('a form row becomes the grant the proxy takes, and back', () => {
  assert.deepEqual(grantOf({ kind: 'queue', name: ' telemetry ', partition: '{sub}', group: '', key: 'stale', ops: ['consume', 'produce'] }),
    { queue: 'telemetry', partition: '{sub}', ops: ['produce', 'consume'] })
  assert.deepEqual(grantOf(rowOf(settings)), settings)
  assert.deepEqual(grantOf(rowOf(telemetry)), telemetry)
  assert.deepEqual(blankRow('kv'), { kind: 'kv', name: '', partition: '', group: '', key: '', ops: ['read'] })
  // Switching kind keeps the name, and only ops the new kind has.
  const r = switchKind({ ...blankRow(), name: 'alarms', ops: ['produce'] }, 'timers')
  assert.equal(r.name, 'alarms')
  assert.deepEqual(r.ops, ['read'])
  assert.deepEqual(switchKind({ ...blankRow('kv'), ops: ['read', 'write'] }, 'timers').ops, ['read', 'write'])
  assert.equal(GRANT_KINDS.length, 3)
})

test('limits: typed rates over the key’s own, a cleared rate takes its burst', () => {
  assert.deepEqual(limitsOf(null, '50', ''), { key_rps: 50 })
  assert.deepEqual(limitsOf({ key_rps: 10, key_burst: 40, sub_rps: 2, sub_burst: 5 }, '', '3'),
    { sub_rps: 3, sub_burst: 5 })
  assert.equal(rpsProblem(''), '')
  assert.equal(rpsProblem('20'), '')
  assert.match(rpsProblem('0'), /whole number/)
  assert.match(rpsProblem('2.5'), /whole number/)
  assert.match(rpsProblem('1000001'), /whole number/)
})

test('a token ceiling reads as a length; a key’s own one joins the choices', () => {
  assert.equal(lengthOf(86_400), '1 day')
  assert.equal(lengthOf(600), '10 minutes')
  assert.equal(lengthOf(7200), '2 hours')
  assert.equal(lengthOf(90), '90 seconds')
  assert.equal(lengthOf(0), '')
  assert.equal(ceilingChoices(3600).length, 5)
  const withOwn = ceilingChoices(600)
  assert.equal(withOwn.length, 6)
  assert.deepEqual(withOwn[0], { s: 600, name: '10 minutes' })
})

test('blocked clients: a sub is what a token can carry; rows carry the key name', () => {
  for (const ok of ['dev-8f3a', 'user@example.com', 'a.b_c+d', 'x'.repeat(128)]) assert.ok(validSub(ok), ok)
  for (const bad of ['', 'a b', 'dev/1', 'x'.repeat(129), 'é']) assert.ok(!validSub(bad), bad)
  const rows = blockRows(
    [{ key: 'k1', sub: 'dev-2', at: '2026-11-10T10:00:00Z' }, { key: 'gone', sub: 'dev-1', at: '2026-11-11T09:00:00Z' }],
    [{ id: 'k1', name: 'devices' }],
  )
  assert.deepEqual(rows.map(r => [r.sub, r.keyName]), [['dev-1', 'gone'], ['dev-2', 'devices']])
})
