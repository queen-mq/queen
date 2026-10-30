import { test } from 'node:test'
import assert from 'node:assert/strict'
import { SYSTEM_VIEWS, cellVerdict, systemSideGroups, systemView, systemViewAlias, systemViewOf, systemViewQuery } from '../src/composables/systemViews.js'

test('the view: the Overview by default, every view by its key, the old Resources tab as CPU and memory', () => {
  assert.equal(systemView(undefined), 'overview')
  assert.equal(systemView('nope'), 'overview')
  assert.equal(systemView(['raft']), 'overview')
  for (const v of SYSTEM_VIEWS) assert.equal(systemView(v.key), v.key)
  assert.equal(systemView('resources'), 'compute')
  assert.equal(systemViewAlias('resources'), 'compute')
  assert.equal(systemView('nodes'), 'overview', 'the Nodes view is the Overview\'s table now')
  assert.equal(systemViewAlias('nodes'), 'overview')
  assert.equal(systemViewOf('overview').charts, true, 'the Overview draws charts, so it reads the metrics')
  assert.equal(systemViewOf('storage').charts, true)
  assert.equal(systemViewAlias('raft'), null)
  assert.equal(systemViewQuery('overview'), undefined)
  assert.equal(systemViewQuery('disk'), 'disk')
  assert.equal(systemViewOf('disk').picker, true)
  assert.equal(systemViewOf('storage').picker, undefined)
})

test('the list: three groups in a fixed order, the number coloured only on trouble', () => {
  const groups = systemSideGroups({ disk: { num: '85%', tone: 'warn' }, overview: { num: 'Healthy', tone: 'ok' } })
  assert.deepEqual(groups.map((g) => g.label), ['', 'Replication', 'Resources'])
  const rows = Object.fromEntries(groups.flatMap((g) => g.rows).map((r) => [r.key, r]))
  assert.equal(rows.disk.tone, 'warn')
  assert.equal(rows.overview.tone, '', 'ok is not a tone')
  assert.equal(rows.nodes, undefined, 'no Nodes row')
  assert.equal(rows.raft.num, '—')
})

test('the cell verdict: healthy, degraded or failing, the failing issue leading', () => {
  assert.equal(cellVerdict(null).word, 'Checking')
  assert.deepEqual(cellVerdict({}, []), { tone: '', word: 'Healthy', headline: 'The cell is healthy' })
  const disk = { sev: 'warn', title: "3 nodes' disks are nearly full" }
  const quorum = { sev: 'bad', title: 'Quorum lost' }
  assert.deepEqual(cellVerdict({}, [disk]), { tone: 'warn', word: 'Degraded', headline: "3 nodes' disks are nearly full" })
  assert.deepEqual(cellVerdict({}, [disk, quorum]), { tone: 'bad', word: 'Failing', headline: 'Quorum lost' })
  assert.equal(cellVerdict({}, [disk, disk]).headline, '2 problems in the cell')
})

test('every list row of the Overview and System has an icon the app draws', async () => {
  const { ICONS } = await import('../src/composables/icons.js')
  const { OVERVIEW_VIEWS } = await import('../src/composables/overviewViews.js')
  for (const v of [...SYSTEM_VIEWS, ...OVERVIEW_VIEWS]) {
    assert.ok(v.icon, `${v.key} has an icon`)
    assert.ok(ICONS[v.icon], `${v.icon} is in composables/icons.js`)
  }
})
