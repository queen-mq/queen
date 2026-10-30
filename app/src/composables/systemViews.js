// The System page's views (views/System.vue), picked in its sub sidebar and
// kept in the address as ?view=:
//
//   Overview (no ?view=), Nodes        the cell at a glance; each node now
//   Replication: Raft, Storage         the replicated log: members, traffic,
//                                      the log and the store on each node
//   Resources: CPU and memory, Disk,   each node over time
//     Network
//
// A node is not a view: it is the header's picker, on the views it narrows.
// Pure: no Vue, no '@/' (test/system-views.test.js).

export const SYSTEM_VIEWS = Object.freeze([
  { key: 'overview', name: 'Overview', group: '', icon: 'gauge' },
  { key: 'nodes', name: 'Nodes', group: '', icon: 'boxes' },
  { key: 'raft', name: 'Raft', group: 'Replication', charts: true, picker: true, icon: 'network' },
  { key: 'storage', name: 'Storage', group: 'Replication', icon: 'database' },
  { key: 'compute', name: 'CPU and memory', group: 'Resources', charts: true, picker: true, icon: 'cpu' },
  { key: 'disk', name: 'Disk', group: 'Resources', charts: true, picker: true, icon: 'hard-drive' },
  { key: 'network', name: 'Network', group: 'Resources', charts: true, picker: true, icon: 'arrow-up-down' },
])
const BY_KEY = new Map(SYSTEM_VIEWS.map((v) => [v.key, v]))

// The page's two tabs before 2026-10-30 night.
const ALIASES = { resources: 'compute' }

/** ?view= → the view shown; anything unknown is the Overview. */
export function systemView(value) {
  const v = typeof value === 'string' ? (ALIASES[value] || value) : ''
  return BY_KEY.has(v) ? v : 'overview'
}

/** The ?view= an old address is rewritten to, or null when it is current. */
export const systemViewAlias = (value) => (typeof value === 'string' && ALIASES[value] ? ALIASES[value] : null)

/** The ?view= of a view: none for the Overview, the page's landing. */
export const systemViewQuery = (key) => (key === 'overview' ? undefined : key)

/** The view's entry: its name, and whether it draws charts or takes the node picker. */
export const systemViewOf = (key) => BY_KEY.get(systemView(key))

const TONES = new Set(['warn', 'bad'])

/**
 * The list, group by group: `[{ label, rows: [{ key, name, num, tone, title }] }]`.
 * `status` gives each view `{ num, tone, title }` from what the page reads; a
 * view it does not mention shows '—'. `tone` is kept only when it is 'warn'
 * or 'bad': the number itself turns colour, there is no sign.
 */
export function systemSideGroups(status = {}) {
  const groups = []
  for (const v of SYSTEM_VIEWS) {
    let g = groups[groups.length - 1]
    if (!g || g.label !== v.group) { g = { label: v.group, rows: [] }; groups.push(g) }
    const st = status[v.key] || {}
    g.rows.push({
      key: v.key,
      name: v.name,
      icon: v.icon || null,
      num: st.num === undefined || st.num === null || st.num === '' ? '—' : String(st.num),
      tone: TONES.has(st.tone) ? st.tone : '',
      title: st.title || '',
    })
  }
  return groups
}

/** The cell's verdict: its word, its tone and its headline, from brokerStatus and cellIssues. */
export function cellVerdict(cell, issues = []) {
  if (!cell) return { tone: '', word: 'Checking', headline: 'Reading the cell…' }
  const bad = issues.filter((i) => i.sev === 'bad')
  const tone = bad.length ? 'bad' : issues.length ? 'warn' : ''
  const word = tone === 'bad' ? 'Failing' : tone === 'warn' ? 'Degraded' : 'Healthy'
  const lead = bad.length ? bad : issues
  const headline = !lead.length ? 'The cell is healthy'
    : lead.length === 1 ? lead[0].title
      : `${lead.length} problems in the cell`
  return { tone, word, headline }
}
