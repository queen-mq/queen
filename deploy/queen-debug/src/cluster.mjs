// The nodes as this server sees them. Every node is asked directly, including
// one Kubernetes marks not ready: that is the node a debugger needs most.

import { BrokerError } from './broker.mjs'

const HEALTH_TTL_MS = 1_500
const HEALTH_TIMEOUT_MS = 2_000
const STATUS_TIMEOUT_MS = 2_500
const MEMBERS_TIMEOUT_MS = 4_000

const hostOf = (url) => new URL(url).host

/** A node named by a pod, a hostname, a raft id or an address. */
export function matchesNode(n, wanted) {
  const w = String(wanted ?? '').trim()
  if (!w) return false
  return [n.name, n.hostname, n.pod?.name, n.nodeId, n.url, n.url && hostOf(n.url), n.httpAddr]
    .some((x) => x !== undefined && x !== null && String(x) === w)
}

export class Cluster {
  #config
  #call
  #kube
  #cache = null
  #names = new Map()

  constructor({ config, call, kube = null }) {
    this.#config = config
    this.#call = call
    this.#kube = kube
  }

  get kube() {
    return this.#kube
  }

  /** The pods, or `{ pods: null, error }` when Kubernetes cannot say. */
  async pods() {
    if (!this.#kube) return { pods: null, error: null }
    try {
      return { pods: await this.#kube.pods(), error: null }
    } catch (e) {
      return { pods: null, error: e.message }
    }
  }

  async #targets(pods) {
    if (this.#config.brokers.length) {
      const list = pods || []
      return this.#config.brokers.map((url) => {
        const host = hostOf(url)
        const bare = new URL(url).hostname
        // Brokers named by their pods' DNS names still get their pod's state.
        const pod = list.find((p) => p.name === host || p.name === bare)
          ?? list.find((p) => bare.startsWith(`${p.name}.`))
          ?? list.find((p) => p.ip === bare)
          ?? null
        return { name: pod?.name ?? host, url, pod }
      })
    }
    // A pod with no address is listed too: it is down, and that is the news.
    return (pods || [])
      .filter((p) => !p.deleting)
      .map((p) => {
        if (!p.ip) return { name: p.name, url: null, pod: p }
        const host = p.ip.includes(':') ? `[${p.ip}]` : p.ip
        return { name: p.name, url: `http://${host}:${this.#config.brokerPort}`, pod: p }
      })
  }

  async #ask(target) {
    if (!target.url) return { ...target, reachable: false, ready: false, error: 'no_address' }
    try {
      // A node without a quorum answers 503 with the same body: that is data.
      const { status, data } = await this.#call(target, 'GET', '/health', { timeoutMs: HEALTH_TIMEOUT_MS, accept: [503] })
      const raft = data && typeof data === 'object' ? data.raft ?? null : null
      return {
        ...target,
        reachable: true,
        ready: status === 200,
        version: data?.version ?? null,
        state: raft?.role ?? null,
        leaderKnown: raft?.leader ?? null,
        term: raft?.term ?? null,
        applied: raft?.applied ?? null,
        commit: raft?.commit ?? null,
        lagMs: raft?.lag ?? null,
        quorumAckMs: raft?.quorumAckMs ?? null,
        storageReady: raft?.storageReady ?? null,
        applyFailure: raft?.apply?.failure ?? null,
        applySkipped: raft?.apply?.skipped?.length ? raft.apply.skipped : null,
      }
    } catch (e) {
      if (!(e instanceof BrokerError)) throw e
      return { ...target, reachable: false, ready: false, error: e.kind }
    }
  }

  /**
   * An address says less than the node's own name and id, so ask it once and
   * remember: a node that goes down later is still the same node, even when
   * the address used here is not the one the cluster knows it by.
   */
  async #named(n) {
    if (!n.url) return n
    if (!this.#names.has(n.url) && n.reachable) {
      try {
        const { data } = await this.#call(n, 'GET', '/api/v1/raft/status', { timeoutMs: STATUS_TIMEOUT_MS })
        if (data?.hostname) this.#names.set(n.url, { name: data.hostname, nodeId: data.nodeId ?? null })
      } catch (e) {
        if (!(e instanceof BrokerError)) throw e
        if (e.kind === 'no_access') this.#names.set(n.url, { name: n.name, nodeId: null })
      }
    }
    const known = this.#names.get(n.url)
    if (!known) return n
    return { ...n, name: n.pod ? n.name : known.name, nodeId: known.nodeId ?? undefined }
  }

  /** Every node's `/health`, read at most once in a short window. */
  async health({ fresh = false } = {}) {
    const now = Date.now()
    if (!fresh && this.#cache && now - this.#cache.at < HEALTH_TTL_MS) return this.#cache.value
    const { pods, error } = await this.pods()
    const targets = await this.#targets(pods)
    const asked = await Promise.all(targets.map((t) => this.#ask(t)))
    const nodes = await Promise.all(asked.map((n) => this.#named(n)))
    const value = { nodes, pods, podsError: error }
    this.#cache = { at: now, value }
    return value
  }

  /** Who to ask first: ready nodes, freshest first, then any that answers. */
  async #order() {
    const { nodes } = await this.health()
    const rank = (n) => (n.ready ? 0 : n.reachable ? 1 : 2)
    return [...nodes].sort((a, b) => rank(a) - rank(b) || (a.lagMs ?? 0) - (b.lagMs ?? 0))
  }

  async hasQuorum() {
    const { nodes } = await this.health()
    return nodes.some((n) => n.ready)
  }

  static source(n) {
    return { node: n.name, state: n.state ?? null, ready: Boolean(n.ready), lagMs: n.lagMs ?? null }
  }

  /** A read from whichever node answers first in that order. */
  async read(target, { method = 'GET', body, timeoutMs, relay = false } = {}) {
    let last = null
    for (const n of await this.#order()) {
      if (!n.reachable) continue
      try {
        const { data } = await this.#call(n, method, target, { body, timeoutMs, relay })
        return { data, source: Cluster.source(n) }
      } catch (e) {
        if (!(e instanceof BrokerError) || !e.retryable) throw e
        last = e
      }
    }
    throw last ?? new BrokerError('unreachable', 'no broker answered')
  }

  /** A read from one named node. */
  async readOn(node, target, opts = {}) {
    const { data } = await this.#call(node, opts.method ?? 'GET', target, opts)
    return data
  }

  /** The write the operator allowed, on one node. */
  put(node, target, body, timeoutMs) {
    return this.#call(node, 'PUT', target, { body, timeoutMs })
  }

  /**
   * Every node, joined from what it says about itself and what the others say
   * about it. The two raft routes are optional: a read-only token gets 403
   * there, and `/health` still holds the role, the term, the indexes and the
   * lag. `members` is the broker's own answer, which the console's rules read.
   */
  async view() {
    const { nodes, pods, podsError } = await this.health({ fresh: true })
    let refused = false

    const probed = await Promise.all(nodes.map(async (n) => {
      if (!n.reachable) return n
      try {
        const { data: s } = await this.#call(n, 'GET', '/api/v1/raft/status', { timeoutMs: STATUS_TIMEOUT_MS })
        return {
          ...n,
          nodeId: s.nodeId ?? null,
          hostname: s.hostname ?? null,
          httpAddr: s.httpAddr ?? null,
          state: s.state ?? n.state,
          leaderId: s.leaderId ?? null,
          voters: s.voters ?? null,
        }
      } catch (e) {
        if (e instanceof BrokerError && e.kind === 'no_access') refused = true
        return n
      }
    }))

    let members = null
    for (const n of probed) {
      if (!n.reachable || refused) continue
      try {
        ;({ data: members } = await this.#call(n, 'GET', '/api/v1/raft/members', { timeoutMs: MEMBERS_TIMEOUT_MS }))
        break
      } catch (e) {
        if (!(e instanceof BrokerError)) throw e
        if (e.kind === 'no_access') refused = true
      }
    }

    const rows = joinMembers(probed, members)
    const voters = Array.isArray(members?.voters)
      ? members.voters.length
      : Number.isInteger(members?.voters)
        ? members.voters
        : rows.map((n) => n.voters).find((v) => Number.isInteger(v)) ?? rows.length
    return { nodes: rows, voters, members, pods, podsError, raftRoutesRefused: refused }
  }

  async findNode(wanted) {
    const view = await this.view()
    return { ...view, node: view.nodes.find((n) => matchesNode(n, wanted)) ?? null }
  }
}

/**
 * What to call a node known only by its address: the first label of a DNS
 * name, which for a StatefulSet is the pod's name, or the IP as it is.
 */
export function addressName(addr) {
  const text = String(addr ?? '')
  const v6 = /^\[([^\]]+)\]/.exec(text)
  if (v6) return v6[1]
  const host = text.split(':')[0]
  return /^[\d.]+$/.test(host) ? host : host.split('.')[0]
}

/** One row per node: asked directly, reported by a peer, or both. */
function joinMembers(probed, members) {
  const list = Array.isArray(members?.members) ? members.members.filter(Boolean) : []
  const used = new Set()
  const rows = list.map((m) => {
    const addrHost = String(m.httpAddr ?? '').split(':')[0]
    const p = probed.find((x) => !used.has(x) && (
      (x.nodeId !== undefined && x.nodeId !== null && x.nodeId === m.nodeId)
      || (x.pod && (x.pod.name === m.hostname || addrHost === x.pod.name || addrHost.startsWith(`${x.pod.name}.`)))
      || (x.url && hostOf(x.url) === m.httpAddr)
    ))
    if (p) used.add(p)
    const base = p ?? {
      name: String(m.httpAddr),
      url: null,
      pod: null,
      // Not asked directly: the peers' word is all there is.
      reachable: m.reachable === true,
      ready: false,
      viaPeer: true,
    }
    // A member nobody reaches has no hostname: its address still names it.
    const learned = p?.url && p.name !== hostOf(p.url) ? p.name : null
    return {
      ...base,
      name: base.pod?.name ?? m.hostname ?? base.hostname ?? learned ?? (addressName(m.httpAddr) || (m.nodeId != null ? `node-${m.nodeId}` : base.name)),
      nodeId: m.nodeId ?? base.nodeId ?? null,
      httpAddr: m.httpAddr ?? base.httpAddr ?? null,
      state: p?.reachable ? (p.state ?? m.state ?? null) : (m.state ?? base.state ?? null),
      peersReach: m.reachable === true,
      peerError: m.error ?? null,
      lagEntries: m.lagEntries ?? null,
      heartbeatAgeMs: m.heartbeatAgeMs ?? null,
      uptimeSeconds: m.uptimeSeconds ?? null,
      version: base.version ?? m.version ?? null,
      hostStats: m.host ?? null,
      store: m.store ?? null,
      log: m.log ?? null,
      work: m.work ?? null,
    }
  })
  for (const p of probed) {
    if (used.has(p)) continue
    // A node that does not answer, at an address the cluster does not use, is
    // already in the members' list under its own name.
    if (!p.reachable && list.length >= probed.length) continue
    rows.push({ ...p, name: p.pod?.name ?? p.hostname ?? p.name })
  }
  return rows
}
