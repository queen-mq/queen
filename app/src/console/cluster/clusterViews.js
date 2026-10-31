// The Cluster page's models, from what the broker answers. Pure: no Vue, no
// '@/', so test/cluster-page.test.js holds them against real payloads.
//
//   GET /api/v1/raft/members             every node: brokerStatus + clusterView
//   GET /api/v1/analytics/system-metrics one point a minute per node (replica)
//   GET /api/v1/system/link              the standby link, as one node knows it
//
// A reading nobody gave is '–', never a 0.

import { describeApiError, CODE_ROUTE_BLOCKED } from '../../api/errors.js'
import { describeRaftFailure, formatIndex, formatMapPct } from '../../composables/useRaftCluster.js'
import { RAFT_CHANNELS, formatRate, rateUnit } from '../../composables/useTraffic.js'

const num = (v) => {
  if (v === null || v === undefined || v === '') return null
  const x = Number(v)
  return Number.isFinite(x) ? x : null
}
const SEV_TONE = { bad: 'red', warn: 'amber' }
export const toneOf = (sev) => SEV_TONE[sev] || ''

// ---- words for numbers ------------------------------------------------------

export const count = (v) => (num(v) === null ? '–' : Math.round(num(v)).toLocaleString('en-US'))

/** 1024-based, as the band's cluster chip says it: "190 MB", "8 GB", "1.1 GB". */
export function size(b) {
  const v0 = num(b)
  if (v0 === null) return '–'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let v = v0; let i = 0
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++ }
  return `${v < 10 && i > 0 ? v.toFixed(1).replace(/\.0$/, '') : Math.round(v)} ${u[i]}`
}

/** A share 0..1 as the chip says it; a sliver that is not zero is "<1%". */
export function pct(share) {
  const s = num(share)
  if (s === null) return '–'
  return s > 0 && s < 0.01 ? '<1%' : `${Math.round(s * 100)}%`
}

/**
 * "38 s", "12 min", "16 h 35 min", "3 d 4 h": the Overview's words, rounded
 * whole (the total in minutes, or hours, then split), so 16 h 59 min 50 s is
 * "17 h", never "16 h 60 min".
 */
export function duration(seconds) {
  const s = Math.max(0, Math.round(num(seconds) ?? 0))
  if (s < 60) return `${s} s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m} min`
  if (m < 24 * 60) {
    const h = Math.floor(m / 60); const mm = m % 60
    return mm ? `${h} h ${mm} min` : `${h} h`
  }
  const hours = Math.round(s / 3600)
  const d = Math.floor(hours / 24); const hh = hours % 24
  return hh ? `${d} d ${hh} h` : `${d} d`
}

export const ms = (v) => {
  const x = num(v)
  if (x === null) return '–'
  return x < 1000 ? `${Math.round(x)} ms` : `${(x / 1000).toFixed(1).replace(/\.0$/, '')} s`
}

function inUnitText(v, unit) {
  const x = v / unit.div
  const digits = unit.div === 1 || x >= 100 ? 0 : x >= 10 ? 1 : 2
  return String(parseFloat(x.toFixed(digits)))
}

/** Two rates in the unit of the larger: "254 / 273 KB/s"; '–' when neither is known. */
export function ratePair(a, b) {
  const x = num(a); const y = num(b)
  if (x === null && y === null) return '–'
  const unit = rateUnit(Math.max(x ?? 0, y ?? 0))
  return `${x === null ? '–' : inUnitText(x, unit)} / ${y === null ? '–' : inUnitText(y, unit)} ${unit.label}`
}

const capital = (s) => (s ? s.charAt(0).toUpperCase() + s.slice(1) : s)
const plural = (n, one, many) => (n === 1 ? one : many)
const WORD_COUNT = ['No', 'One', 'Two', 'Three', 'Four', 'Five']
const inWords = (n) => WORD_COUNT[n] || String(n)

// ---- the nodes ----------------------------------------------------------------

/** The host an address names ("queen-mq-v2-1.queen-mq-v2-headless…:6632" → "queen-mq-v2-1"); null for an IP or localhost. */
export function hostLabel(addr) {
  if (!addr || typeof addr !== 'string' || addr.startsWith('[')) return null
  const host = addr.replace(/:\d+$/, '')
  if (!host || /^[\d.]+$/.test(host) || host === 'localhost') return null
  return host.split('.')[0] || null
}

/**
 * The name the page shows for node i: the members' hostname, which is also
 * its samples' name. A member nobody reached says no hostname (brokerStatus
 * calls it "node-2"), so the host its address names stands in: its samples
 * from before it went quiet carry that name.
 */
export function nameOf(node, row) {
  if (row?.hostname) return row.hostname
  return hostLabel(row?.httpAddr) || hostLabel(row?.raftAddr) || node?.name || null
}
const namesOf = (status, view) => (status?.nodes || []).map((n, i) => nameOf(n, view?.rows?.[i]))

/** A node's part in the cluster, one word, and its tone (only when it needs you). */
export function roleOf(node) {
  if (!node) return { word: '–', tone: '' }
  if (node.unreachable) return { word: 'Unreachable', tone: 'red' }
  if (node.state === 'shutdown') return { word: 'Shut down', tone: 'red' }
  if (node.state === 'candidate') return { word: 'Candidate', tone: 'amber' }
  if (node.role === 'learner' || node.state === 'learner') return { word: 'Learner', tone: '' }
  if (node.state === 'leader') return { word: 'Leader', tone: '' }
  if (node.state === 'follower') return { word: 'Follower', tone: '' }
  return { word: node.state ? capital(String(node.state)) : 'Unknown', tone: '' }
}

/** Every node's version, and whether they differ (a rolling upgrade, or one left behind). */
export function versionsOf(view) {
  const rows = view?.rows || []
  const list = [...new Set(rows.map((r) => r.version).filter(Boolean))].sort()
  return { list, mixed: list.length > 1, text: list.join(' · ') }
}

/**
 * The table's rows, in node-id order (brokerStatus and clusterView sort the
 * same payload the same way, so node i is row i): a reading never moves a row.
 */
export function nodeRows(status, view) {
  if (!status) return []
  return status.nodes.map((n, i) => {
    const r = view?.rows?.[i] || {}
    const role = roleOf(n)
    const d = n.disk || {}
    const t = n.traffic
    return {
      key: n.nodeId ?? n.name,
      name: nameOf(n, r),
      nodeId: n.nodeId,
      leader: !!n.isLeader,
      unreachable: !!n.unreachable,
      error: r.error || null,
      role: role.word,
      roleTone: role.tone,
      version: r.version || '–',
      up: r.uptimeSeconds == null ? '–' : duration(r.uptimeSeconds),
      cpu: pct(n.cpu?.share),
      cpuTone: toneOf(n.cpu?.sev),
      memory: n.mem?.held != null && n.mem?.limit ? `${size(n.mem.held)} of ${size(n.mem.limit)}` : pct(n.mem?.share),
      memoryTone: toneOf(n.mem?.sev),
      disk: {
        used: d.usedPct ?? null,
        text: d.usedPct == null ? '–' : pct(d.usedPct / 100),
        gate: d.gate && d.highPct != null ? d.highPct : null,
        tone: toneOf(d.sev),
      },
      behind: n.isLeader ? '–' : count(n.lag),
      behindTone: n.isLeader ? '' : toneOf(n.lagSeverity),
      heard: n.isLeader || n.heartbeatMs == null ? '–' : `${ms(n.heartbeatMs)} ago`,
      heardTone: n.isLeader ? '' : toneOf(n.heartbeatSeverity),
      network: t?.network ? ratePair(t.network.received, t.network.sent) : null,
      raft: t && (t.received != null || t.sent != null) ? ratePair(t.received, t.sent) : null,
    }
  })
}

/** The one write gate every node shares, said once in the Disk header; null when they differ or none. */
export function sharedGate(status) {
  const gates = (status?.nodes || []).filter((n) => !n.unreachable).map((n) => (n.disk?.gate ? n.disk.highPct : null))
  if (!gates.length || gates.some((g) => g == null)) return null
  return gates.every((g) => g === gates[0]) ? gates[0] : null
}

// ---- the verdict --------------------------------------------------------------

function spareLine(q) {
  if (!q?.hasQuorum) return ''
  if (q.spare === 0) return 'One more node down stops all writes.'
  return `Writes go on with ${q.spare === 1 ? 'one node' : `${inWords(q.spare).toLowerCase()} nodes`} down.`
}

/** An issue's second sentence: its why, without saying its title twice. */
function issueWhy(issue, status) {
  if (issue.key === 'cell:quorum' && status?.raft?.alert) return capital(status.raft.alert.detail) + '.'
  return issue.why ? `${issue.why}.` : ''
}

/**
 * The line under the title: the cell's issues (cellIssues, the band's chip
 * reads the same), else who votes and who leads. `tone` is amber or red only
 * for a real issue. `list` holds the issues when there are several.
 */
export function verdictOf({ status, view, issues = [], error = null, absent = false }) {
  if (absent) {
    return { tone: '', badge: '', text: 'This broker does not report its nodes: `GET /api/v1/raft/members` answers 404.', sub: '', list: [] }
  }
  if (!status) {
    if (error) return { tone: 'red', badge: 'No answer', text: `The broker did not answer: ${describeRaftFailure(error)}.`, sub: '', list: [] }
    return { tone: '', badge: '', text: 'Reading the nodes…', sub: '', list: [], loading: true }
  }
  if (issues.length) {
    const worst = issues[0]
    const tone = toneOf(worst.sev) || 'amber'
    const list = issues.map((i) => ({ key: i.key, tone: toneOf(i.sev) || 'amber', title: i.title, why: issueWhy(i, status) }))
    if (issues.length === 1) return { tone, badge: tone === 'red' ? 'Failing' : 'Degraded', text: `${worst.title}.`, sub: list[0].why, list: [] }
    return { tone, badge: tone === 'red' ? 'Failing' : 'Degraded', text: `${issues.length} problems in the cell.`, sub: '', list }
  }
  const q = view?.quorum
  const leader = status.raft.leader
  if (status.raft.single) return { tone: '', badge: 'Healthy', text: 'One node: nothing replicates its log.', sub: '', list: [] }
  const voters = status.raft.voters || status.nodes.length
  const learners = q?.learners || 0
  const who = learners ? `All ${voters} voters are voting` : `All ${voters} nodes are voting`
  const text = leader ? `${who}; \`${leader}\` leads.` : `${who}; no node leads.`
  const sub = [spareLine(q), learners ? `${inWords(learners)} ${plural(learners, 'learner gets', 'learners get')} the log without voting.` : '']
    .filter(Boolean).join(' ')
  return { tone: '', badge: 'Healthy', text, sub, list: [] }
}

// ---- one node, in the drawer -------------------------------------------------

const DATE = (() => {
  try { return new Intl.DateTimeFormat(undefined, { weekday: 'short', day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' }) } catch { return null }
})()
const defaultWhen = (t) => (DATE ? DATE.format(new Date(t)) : new Date(t).toISOString())

/**
 * Everything the members read says about one node: who it is, its log and
 * store, its disk by what fills it, its machine, and what crossed its Raft
 * port, channel by channel. null when there is no such node.
 */
export function nodeDetail(status, view, name, { now = Date.now(), when = defaultWhen } = {}) {
  const names = namesOf(status, view)
  let i = names.indexOf(name)
  if (i < 0) i = (status?.nodes || []).findIndex((n) => n.name === name)
  if (i < 0) return null
  const n = status.nodes[i]
  const r = view?.rows?.[i] || {}
  const role = roleOf(n)
  const term = num(r.term) ?? num(view?.term)
  const facts = []
  facts.push({
    k: 'Role',
    v: n.unreachable ? 'Unreachable' : role.word === 'Leader' || role.word === 'Follower' ? `${role.word}${term != null ? ` in term ${count(term)}` : ''}` : role.word,
    tone: role.tone,
  })
  if (r.version) facts.push({ k: 'Version', v: r.version, mono: true })
  if (r.uptimeSeconds != null) facts.push({ k: 'Started', v: when(now - r.uptimeSeconds * 1000) })
  if (r.httpAddr) facts.push({ k: 'HTTP', v: r.httpAddr, mono: true })
  if (r.raftAddr) facts.push({ k: 'Raft', v: r.raftAddr, mono: true })
  const out = { name: names[i], nodeId: n.nodeId, leader: !!n.isLeader, unreachable: !!n.unreachable, error: r.error || null, facts, log: [], disk: null, machine: [], channels: [], window: null }
  if (n.unreachable) return out

  // The replicated log on this node, and its store.
  if (r.applied != null) {
    // Its place in the log, and how far its commit and durable points are from it.
    const ahead = r.committed != null && r.committed > r.applied ? r.committed - r.applied : 0
    const notDurable = r.durable != null && r.durable < r.applied ? r.applied - r.durable : 0
    out.log.push({
      k: 'Applied',
      v: formatIndex(r.applied),
      sub: [ahead ? `${count(ahead)} committed, not applied yet` : '', notDurable ? `${count(notDurable)} not durable yet` : '']
        .filter(Boolean).join(' · ') || 'committed and durable',
    })
  }
  if (r.inflight) out.log.push({ k: 'In flight', v: `${count(r.inflight)} appended, not applied yet` })
  if (r.logBytes != null) out.log.push({ k: 'Log', v: `${size(r.logBytes)}${r.logFiles != null ? ` in ${count(r.logFiles)} files` : ''}` })
  if (r.mapPct != null) {
    out.log.push({
      k: 'Store',
      v: `${formatMapPct(r.mapPct)} of its ${size(r.mapBytes)} map`,
      sub: [r.mapUsed != null ? `${size(r.mapUsed)} used` : '', r.readers != null && r.maxReaders != null ? `${count(r.readers)} of ${count(r.maxReaders)} readers` : ''].filter(Boolean).join(' · '),
      tone: toneOf(r.mapSeverity),
    })
  }

  // Its disk, by what fills it: the log and the store are measured; the rest
  // of the volume is what is left of what the filesystem says is used.
  const d = n.disk || {}
  if (d.usedBytes != null) {
    const parts = []
    if (r.logBytes != null) parts.push({ label: 'Raft log', bytes: r.logBytes })
    if (r.mapUsed != null) parts.push({ label: 'Store', bytes: r.mapUsed })
    const known = parts.reduce((s, p) => s + p.bytes, 0)
    if (parts.length && d.usedBytes - known > 0) parts.push({ label: 'Everything else', bytes: d.usedBytes - known })
    out.disk = {
      line: `${size(d.usedBytes)} of ${size(d.totalBytes)} used`,
      gate: d.refused
        ? `It refuses writes: its disk passed ${d.highPct ?? '–'}%.`
        : d.gate && d.highPct != null
          ? `Writes stop at ${d.highPct}%${d.lowPct != null ? ` and start again under ${d.lowPct}%` : ''}.`
          : 'No write gate: writes go on until the disk is full.',
      gateTone: d.refused ? 'red' : '',
      parts: parts.map((p) => ({ ...p, size: size(p.bytes), share: d.usedBytes ? p.bytes / d.usedBytes : 0 })),
    }
  }
  const t = n.traffic
  if (t?.disk) out.disk = { ...(out.disk || { line: '', gate: '', parts: [] }), io: `${formatRate(t.disk.read)} read, ${formatRate(t.disk.written)} written` }

  // The machine under it.
  if (n.cpu?.pct != null) out.machine.push({ k: 'CPU', v: `${Math.round(n.cpu.pct)}% of one core${n.cpu.cpus ? `, of the ${n.cpu.cpus} it may use` : ''}` })
  if (n.mem?.rss != null) out.machine.push({ k: 'Resident', v: size(n.mem.rss), sub: n.mem.anon != null ? "With the store's file pages, which the kernel drops first." : '' })

  // What crossed its Raft port, by what it carries.
  if (t) {
    out.window = t.windowSeconds ? `last ${t.windowSeconds} s` : 'last interval'
    out.channels = t.channels.map((c) => {
      const meta = RAFT_CHANNELS.find((x) => x.key === c.key)
      return { key: c.key, label: c.label, note: meta?.note || '', in: formatRate(c.received), out: formatRate(c.sent), idle: !(c.received || c.sent) }
    })
  }
  return out
}

// ---- the charts ---------------------------------------------------------------

// Lines per node in white and grey, told apart by dash: four before they repeat.
export const LINE_STYLES = [
  { ink: 'in', dashed: false },
  { ink: 'out', dashed: false },
  { ink: 'in', dashed: true },
  { ink: 'out', dashed: true },
]

/** A round top a little above `max` (and at least `floor`): 1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8 × 10ⁿ. */
export function niceTop(max, floor = 0) {
  const m = num(max) ?? 0
  if (!(m > 0)) return floor || 1
  if (m * 1.04 <= floor) return floor
  const p = 10 ** Math.floor(Math.log10(m))
  for (const f of [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (f * p >= m * 1.04 - 1e-12) return parseFloat((f * p).toPrecision(6))
  return 10 * p
}

const BYTE_UNITS = [{ label: 'B', div: 1 }, { label: 'KB', div: 1024 }, { label: 'MB', div: 1024 ** 2 }, { label: 'GB', div: 1024 ** 3 }, { label: 'TB', div: 1024 ** 4 }]
const RATE_UNITS = BYTE_UNITS.slice(0, 4).map((u) => ({ ...u, label: `${u.label}/s` }))

/** The chart's top and how it is said, in the unit its top reads best in: "0 to 1 GB", not "0 to 1000 MB". */
function scaleIn(units, max, floorInFirst = 0) {
  const at = (i) => {
    const u = units[i]
    return { u, t: niceTop(max / u.div, i === 0 ? floorInFirst : 0) }
  }
  let i = 0
  while (i < units.length - 1 && (num(max) ?? 0) >= units[i + 1].div) i++
  let s = at(i)
  if (s.t >= 1000 && i < units.length - 1) s = at(i + 1)
  return { top: s.t * s.u.div, text: `0 to ${s.t} ${s.u.label}` }
}
const pctText = (v) => `${v < 10 ? parseFloat(v.toFixed(1)) : Math.round(v)}%`

const leafOf = (m, path) => {
  let at = m
  for (const k of path) { at = at?.[k]; if (at == null) return null }
  return num(at?.avg)
}
const sumOf = (...vs) => (vs.every((v) => v === null) ? null : vs.reduce((s, v) => s + (v || 0), 0))

/**
 * GET /api/v1/analytics/system-metrics → one small chart per measure, a line
 * per node (named by the members' hostname, which is the replica's), each
 * point at the middle of its bucket. CPU is a share of the cores the node may
 * use (`cpu.user_us` + `cpu.system_us` are percent × 100 of one core, the
 * members' `cpus` the cores) when every node says its cores, else % of one
 * core; memory is resident; disk is used of the data disk; the network and
 * the Raft port are what went in and out.
 */
export function chartsOf(metrics, status, { from, to, view = null } = {}) {
  const replicas = Array.isArray(metrics?.replicas) ? metrics.replicas.filter((r) => r?.hostname) : []
  const nodes = status?.nodes || []
  const nodeNames = namesOf(status, view)
  const byName = new Map(nodes.map((n, i) => [nodeNames[i], n]))
  const names = [...nodeNames]
  for (const r of replicas) if (!names.includes(r.hostname)) names.push(r.hostname)
  const legend = names.map((name, i) => ({
    name,
    ...LINE_STYLES[i % LINE_STYLES.length],
    leader: !!byName.get(name)?.isLeader,
    quiet: !replicas.some((r) => r.hostname === name),
  }))
  const styleOf = new Map(legend.map((l) => [l.name, l]))
  const bm = num(metrics?.bucketMinutes) || 1
  const lo = (from ?? -Infinity) - bm * 60_000
  const hi = (to ?? Infinity) + bm * 60_000
  // A node that does not say its cores (one nobody reached) takes the cores
  // every other node says, when they all say the same: one StatefulSet.
  const known = nodes.map((n) => num(n.cpu?.cpus)).filter(Boolean)
  const common = known.length && known.every((c) => c === known[0]) ? known[0] : null
  const cpusOf = (name) => num(byName.get(name)?.cpu?.cpus) ?? common
  const share = replicas.length > 0 && replicas.every((r) => cpusOf(r.hostname))

  const MEASURES = [
    {
      id: 'cpu', title: 'CPU', sub: share ? 'share of its cores' : '% of one core', floor: 10, unit: 'pct',
      value: (m, name) => {
        const v = sumOf(leafOf(m, ['cpu', 'user_us']), leafOf(m, ['cpu', 'system_us']))
        return v === null ? null : share ? v / 100 / cpusOf(name) : v / 100
      },
      none: 'No CPU samples in this window.',
    },
    { id: 'memory', title: 'Memory', sub: 'resident', unit: 'bytes', value: (m) => leafOf(m, ['memory', 'rss_bytes']), none: 'No memory samples in this window.' },
    {
      id: 'disk', title: 'Disk', sub: 'used of the data disk', floor: 10, unit: 'pct',
      value: (m) => {
        const used = leafOf(m, ['disk', 'used_bytes']); const total = leafOf(m, ['disk', 'total_bytes'])
        return used === null || !total ? null : (used / total) * 100
      },
      none: 'Not recorded by this broker: a node records its disk use from 2.7.0-beta.2.',
    },
    {
      id: 'network', title: 'Network', sub: 'in and out', unit: 'rate',
      value: (m) => sumOf(leafOf(m, ['network', 'received_bytes_per_sec']), leafOf(m, ['network', 'sent_bytes_per_sec'])),
      none: 'Not reported: a broker records the network on Linux only.',
    },
    {
      id: 'raft', title: 'Raft port', sub: 'in and out', unit: 'rate',
      value: (m) => sumOf(leafOf(m, ['raft_traffic', 'received_bytes_per_sec']), leafOf(m, ['raft_traffic', 'sent_bytes_per_sec'])),
      none: status?.raft?.single ? 'One node: nothing crosses the Raft port.' : 'Not recorded by this broker.',
    },
  ]

  const charts = MEASURES.map((meas) => {
    let max = 0
    const series = replicas.map((r) => {
      const st = styleOf.get(r.hostname)
      const points = []
      for (const p of r.timeSeries || []) {
        const t0 = Date.parse(p?.timestamp)
        if (!Number.isFinite(t0)) continue
        const t = t0 + (bm * 60_000) / 2
        if (t < lo || t > hi) continue
        const v = meas.value(p.metrics, r.hostname)
        if (v !== null && v > max) max = v
        points.push([t, v])
      }
      points.sort((a, b) => a[0] - b[0])
      return { id: r.hostname, label: r.hostname, ink: st.ink, dashed: st.dashed, points }
    })
    const has = series.some((s) => s.points.some((p) => p[1] !== null))
    let top; let topText; let format
    if (meas.unit === 'pct') {
      top = niceTop(max, meas.floor)
      topText = `0 to ${pctText(top)}`
      format = pctText
    } else if (meas.unit === 'bytes') {
      ({ top, text: topText } = scaleIn(BYTE_UNITS, max))
      format = size
    } else {
      ({ top, text: topText } = scaleIn(RATE_UNITS, max, 1))
      format = formatRate
    }
    return { id: meas.id, title: meas.title, sub: meas.sub, top, topText, format, series, has, none: meas.none }
  })

  const missingIds = Array.isArray(metrics?.nodesMissing) ? metrics.nodesMissing : []
  const missing = missingIds.map((id) => {
    const i = nodes.findIndex((n) => num(n.nodeId) === num(id))
    return i < 0 ? `node ${id}` : nodeNames[i]
  })
  return { legend, charts, missing, bucketMinutes: bm, empty: !replicas.length }
}

/** A sentence that may hold a call between backticks → [{ t, mono }], for the template. */
export const segments = (text) => String(text || '').split('`').map((t, i) => ({ t, mono: i % 2 === 1 })).filter((x) => x.t)

// ---- operations -------------------------------------------------------------

const isAbsent = (e) => !!e && e.status === 404 && e.code !== CODE_ROUTE_BLOCKED
const isBlocked = (e) => !!e && e.code === CODE_ROUTE_BLOCKED

/** "queen-dr", "queen-dr and queen-dr2", "3 standbys". */
const names = (list) => (list.length <= 2 ? list.join(' and ') : `${list.length} standbys`)

/**
 * GET /api/v1/system/link → the Standby row: a sentence, its tone, and
 * whether this cluster can be promoted (it is a standby: a promotion is sent
 * to the standby, so a primary's readers are promoted from their own cluster).
 * `selfApplied` is the applied index of the node that answered the members
 * read, to say how far behind a reader is.
 */
export function linkModel({ data, error } = {}, { selfApplied = null } = {}) {
  if (error && !data) {
    if (isBlocked(error)) return { text: 'queen-proxy does not pass this read to the broker, so the link is not shown here.', tone: '', canPromote: false }
    if (isAbsent(error)) return { text: 'This broker has no standby link: it answers 404.', tone: '', canPromote: false }
    return { text: `The broker did not answer: ${describeApiError(error)}.`, tone: 'red', canPromote: false }
  }
  if (!data) return { text: 'Reading the link…', tone: '', canPromote: false, loading: true }
  const role = data.role
  if (role === 'standby') {
    const src = data.source || 'its source'
    const f = data.follower
    let text = `This cluster is a standby of \`${src}\``
    let tone = ''
    if (!f) text += '.'
    else if (f.state === 'following') {
      text += `, ${count(f.lagEntries)} ${plural(num(f.lagEntries), 'entry', 'entries')} behind${num(f.lagMs) != null ? ` (${ms(f.lagMs)})` : ''}.`
      if (num(f.lastAnswerMs) != null) text += ` The source answered ${ms(f.lastAnswerMs)} ago.`
    } else if (f.state === 'waiting') { text += `, waiting: ${f.error || 'held up by something that passes'}.`; tone = 'amber' }
    else if (f.state === 'halted') { text += `, halted: ${f.error || 'stopped'}. It needs a new seed.`; tone = 'red' }
    else text += '. This node does not lead it, so it does not know how far behind it is.'
    return { text, tone, canPromote: true, source: src, role }
  }
  if (role === 'promoted') {
    const at = num(data.promotedAtUs)
    return {
      text: `This cluster was a standby of ${data.source ? `\`${data.source}\`` : 'another cluster'} and was promoted${at ? ` at ${new Date(at / 1000).toLocaleString()}` : ''}: it takes writes now.`,
      tone: '', canPromote: false, role,
    }
  }
  const readers = Array.isArray(data.readers) ? data.readers : []
  if (!readers.length) return { text: 'No standby reads this cluster, and it follows no other.', tone: '', canPromote: false, role: 'primary' }
  const parts = readers.map((r) => {
    const behind = selfApplied != null && num(r.after) != null ? Math.max(0, selfApplied - num(r.after)) : null
    return `\`${r.name}\` last said where it was ${duration((num(r.idleMs) ?? 0) / 1000)} ago${behind != null ? `, ${count(behind)} ${plural(behind, 'entry', 'entries')} behind` : ''}`
  })
  let text = `${names(readers.map((r) => `\`${r.name}\``))} ${readers.length === 1 ? 'reads' : 'read'} this cluster's log: ${parts.join('; ')}.`
  let tone = ''
  if (data.holding === false) { text += " This node's disk is too full to keep its log for them."; tone = 'amber' }
  return { text, tone, canPromote: false, role: 'primary', readers: readers.map((r) => r.name) }
}
