// The Overview's three panels under what needs you: plain functions over the
// shapes the page reads (queue rows, the cell read, stream and connector rows).
import { test } from 'node:test'
import assert from 'node:assert/strict'

import { brokerStatus } from '../src/composables/useBrokerStatus.js'
import { clusterView } from '../src/composables/useRaftCluster.js'
import { clusterNodes, insideBroker, namespaceTraffic } from '../src/console/overviewPanels.js'

const row = (name, ns, inHour, ackHour = inHour) => ({
  name, ns, task: '', inHour, ackHour, pending: 0, dead: 0, partitions: 1, lag: null,
  groupNames: [], groupsKnown: true, state: null, sev: null, spark: null,
})

test('where the traffic is: namespaces by arrivals, busiest first, quiet ones counted', () => {
  const t = namespaceTraffic([
    row('pms.a', 'smartpms', 3600), row('pms.b', 'smartpms', 2196),
    row('tf.x', 'traceflow', 3240),
    row('ch.a', 'channel', 1800, 900),
    row('acc.a', 'accounting', 0),
    row('loose', '', 36),
  ])
  assert.deepEqual(t.rows.map((r) => [r.ns, Math.round(r.inS * 100) / 100]), [['smartpms', 1.61], ['traceflow', 0.9], ['channel', 0.5], ['', 0.01]])
  assert.equal(t.rows[0].share, 1)
  assert.equal(Math.round(t.rows[2].share * 100), 31)
  assert.equal(t.rows[2].ackS, 0.25)
  assert.equal(t.rows[0].queues, 2)
  assert.deepEqual([t.quiet, t.more], [1, 0])
  // the first four, and how many more
  const many = namespaceTraffic(Array.from({ length: 8 }, (_, i) => row(`q${i}`, `ns${i}`, 100 * (i + 1))))
  assert.deepEqual([many.rows.length, many.more, many.rows[0].ns], [4, 4, 'ns7'])
  // the hour not read: nothing to rank
  assert.equal(namespaceTraffic([{ ...row('q', 'a', 0), inHour: null }]), null)
})

const member = (id, over = {}) => ({
  nodeId: id, hostname: `queen-mq-v2-${id - 1}`, role: 'voter', state: id === 3 ? 'leader' : 'follower', reachable: true,
  httpAddr: `queen-mq-v2-${id - 1}.queen-mq-v2-headless.queen.svc.cluster.local:6632`,
  term: 123, appliedIndex: 100, committedIndex: 100, lagEntries: id === 3 ? null : 0, heartbeatAgeMs: id === 3 ? null : 56,
  host: {
    anonBytes: 199245824, memLimitBytes: 8 * 1024 ** 3, cpuPct: 23.78, cpuWindowSeconds: 60, cpus: 8,
    disk: { gate: true, highPct: 85, lowPct: 80, totalBytes: 52521566208, usedBytes: 1010446336, usedPct: 1.92 },
  },
  uptimeSeconds: 59721, version: '2.8.1', ...over,
})

test('the cluster underneath: each node as the Cluster page reads it, the versions and the write gate', () => {
  const payload = { clusterId: 'raft-1', leaderId: 3, term: 123, self: 3, members: [member(1), member(2), member(3)] }
  const n = clusterNodes(brokerStatus(payload), clusterView(payload))
  assert.deepEqual(n.rows.map((r) => [r.name, r.role]), [['queen-mq-v2-0', 'Follower'], ['queen-mq-v2-1', 'Follower'], ['queen-mq-v2-2', 'Leader']])
  assert.equal(n.rows[2].behind, null, 'the leader is behind nobody')
  assert.equal(n.rows[0].behind, '0')
  assert.equal(n.rows[0].disk, '2%')
  assert.equal(n.rows[0].up, '16 h 35 min')
  assert.deepEqual([n.versions.mixed, n.versions.text, n.gate], [false, '2.8.1', 85])
  // a roll in progress: two versions, said
  const mixed = { ...payload, members: [member(1, { version: '2.8.0' }), member(2), member(3)] }
  assert.equal(clusterNodes(brokerStatus(mixed), clusterView(mixed)).versions.text, '2.8.0 · 2.8.1')
  // no cell read (a tenant through the proxy): no panel
  assert.equal(clusterNodes(null, null), null)
})

test('inside the broker: streams by kind and state, connectors, what needs you, what could not be read', () => {
  const r = insideBroker({
    feedRows: [{ name: 'flowmon', status: 'running', needs: '' }, { name: 'old', status: 'stopped', needs: '' }, { name: 'tf', status: 'down', needs: 'amber' }],
    queueStreams: [{ name: 'rollup', needs: '' }, { name: 'late', needs: 'amber' }],
    connectorRows: [{ name: 'pms-in', word: 'Streaming', tone: '' }, { name: 'bi-out', word: 'Failing', tone: 'red' }, { name: 'legacy', word: 'Off', tone: '' }],
  })
  assert.deepEqual(r.streams, { total: 5, feed: 3, queue: 2, running: 1, stopped: 1, needs: ['tf', 'late'] })
  assert.deepEqual(r.connectors, { total: 3, off: 1, needs: ['bi-out'] })
  // a part that could not be read says nothing rather than zero
  assert.deepEqual(insideBroker({ feedRows: [] }), { streams: { total: 0, feed: 0, queue: 0, running: 0, stopped: 0, needs: [] }, connectors: null })
  assert.deepEqual(insideBroker(), { streams: null, connectors: null })
})
