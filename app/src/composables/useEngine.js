// Which storage engine the broker behind the acting cluster runs: the
// Postgres engine this dashboard was built on, or raft mode (no Postgres; one
// node, or three or more replicating one log).
//
// ONE PLACE DECIDES IT, from the /health answer the shell already polls
// (components/Sidebar.vue). Nothing negotiates a mode, and every view that
// changes shape in raft mode reads the verdict from here instead of sniffing a
// payload of its own:
//
//     raft       {"status":"healthy","engine":"raft","raft":{…}}
//                — and 503 {"status":"settling","engine":"raft",…} while the
//                node knows no leader, so the verdict also rides on a FAILED
//                /health (server/src/handlers/raft.rs handle_health)
//     postgres   {"status":"healthy","database":"connected","engine":"segments-rust"}
//                — and 503 {"status":"unhealthy",…} when its database is down
//
// Anything that is a health answer and does not say raft is the Postgres
// engine; an answer we never got (proxy down, not JSON) is no verdict at all,
// and a view with no verdict renders exactly what it rendered before raft mode
// existed.
//
// REMEMBERED PER CLUSTER. A cell does not change engines between two page
// loads, so the last confirmed verdict is kept (stores/engine.js puts it in
// localStorage) and a reload paints the right page at once instead of flashing
// the Postgres layout for one /health round trip. The live /health answer
// always wins over the remembered one.
//
// Pure apart from Vue refs, so test/engine.test.js drives it with a fake
// /health; stores/engine.js binds it to the API client and the identity epoch.

import { computed, ref } from 'vue'

export const ENGINE_RAFT = 'raft'
export const ENGINE_POSTGRES = 'postgres'

const ENGINES = new Set([ENGINE_RAFT, ENGINE_POSTGRES])

/** The verdict a /health body carries, or null when it is not a health answer. */
export function engineFromHealth(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null
  if (payload.engine === ENGINE_RAFT) return ENGINE_RAFT
  if (typeof payload.status === 'string' || typeof payload.engine === 'string') return ENGINE_POSTGRES
  return null
}

/**
 * The verdict a FAILED /health still carries. Both engines answer 503 with a
 * full health body (raft: settling, Postgres: database down); an error with no
 * body — the proxy unreachable, a timeout — says nothing about the engine.
 */
export function engineFromHealthError(err) {
  const body = err?.body
  return body && typeof body === 'object' ? engineFromHealth(body) : null
}

/** The label of System's second Source option: the storage the page is about. */
export function storageSourceLabel(engine) {
  return engine === ENGINE_RAFT ? 'Raft' : 'Postgres stats'
}

/**
 * The verdict for one acting cluster at a time.
 *
 * @param {object} o
 * @param {(config?: object) => Promise<{data: any}>} o.fetchHealth  GET /health
 * @param {() => number} [o.getEpoch]  the identity epoch: an answer that lands
 *        after a cluster switch belongs to the old cell and is dropped
 * @param {() => string|null} [o.recall]  the verdict remembered for the acting cluster
 * @param {(engine: string) => void} [o.remember]  keep a confirmed verdict
 */
export function createEngineStore({
  fetchHealth,
  getEpoch = () => 0,
  recall = () => null,
  remember = () => {},
} = {}) {
  // 'raft' | 'postgres' | null — confirmed, or remembered until confirmed.
  const engine = ref(null)
  // A /health answer (or failure) has been seen for this cluster.
  const settled = ref(false)
  let inflight = null

  const isRaft = computed(() => engine.value === ENGINE_RAFT)

  const prime = () => {
    if (settled.value || engine.value !== null) return
    let hint = null
    try { hint = recall() } catch { hint = null }
    if (ENGINES.has(hint)) engine.value = hint
  }

  const record = (verdict) => {
    // No verdict (an unreachable proxy) keeps what we knew: a cell does not
    // stop being raft because one poll failed.
    if (verdict) {
      engine.value = verdict
      try { remember(verdict) } catch { /* storage is a convenience */ }
    }
    settled.value = true
  }

  /**
   * One /health round trip, shared by every concurrent caller. Resolves with
   * the response or rejects with the error, like the call it wraps — the shell
   * renders both — after recording whatever verdict it carried.
   */
  const loadHealth = (config) => {
    prime()
    if (inflight) return inflight
    const epochAtStart = getEpoch()
    const current = Promise.resolve()
      .then(() => fetchHealth(config))
      .then(
        (res) => {
          if (getEpoch() === epochAtStart) record(engineFromHealth(res?.data))
          return res
        },
        (err) => {
          if (getEpoch() === epochAtStart) record(engineFromHealthError(err))
          throw err
        },
      )
      .finally(() => { if (inflight === current) inflight = null })
    inflight = current
    return current
  }

  /** The verdict, asking /health only if nothing has answered for this cluster yet. Never rejects. */
  const ensure = async () => {
    prime()
    if (settled.value) return engine.value
    try { await loadHealth() } catch { /* recorded; the shell reports it */ }
    return engine.value
  }

  /** A cluster switch: the next cell may run the other engine. */
  const reset = () => {
    engine.value = null
    settled.value = false
    inflight = null
  }

  return { engine, settled, isRaft, loadHealth, ensure, reset }
}
