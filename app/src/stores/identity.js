// Identity store — module singleton, the ONE source of truth for who the user
// is, what they may do, and which cluster the UI is acting on.
//
// Hydrated from GET /auth/me, which is deliberately answered by WHOEVER serves
// the SPA: the proxy's cookie-authenticated session endpoint
// (proxy/src/oauth.rs), or the broker's fixed standalone identity when served
// broker-direct with auth off (server/src/handlers/standalone.rs,
// `standalone: true`). Same shape either way — the app never sniffs which
// binary answered. No view may re-derive a permission from anything else: not
// from a role string it parsed itself, not from whether a call happened to 403.
//
// ACT-AS-CLUSTER. `/auth/me` names the header (`act_cluster_header`) — it is
// read from there and never hardcoded twice. Picking a cluster sets it on
// every /api/v1/* request (api/client.js), which is what drives the injected
// tenant, the plan limits, the rate limiting and the meter attribution.
//
// SCOPED vs CELL. A cluster row names both a TENANT and a CELL, so one
// selector drives both surfaces. Anything sourced from an operator route is
// cell-level and must be labelled as such — a cell number rendered as if it
// were the tenant's is the exact lie this shell exists to prevent.
import { computed, ref } from 'vue'

import { notifyWarn } from '@/stores/ui'

// Fallback only. The live name always comes from /auth/me's act_cluster_header
// (proxy/src/config.rs ACT_CLUSTER_HEADER); this is what we send if the
// payload predates that field.
const DEFAULT_ACT_HEADER = 'x-queen-act-cluster'
const STORAGE_PREFIX = 'queen.acting_cluster.'

const identity = ref(null)
// 'idle' | 'loading' | 'ready' | 'error'
const status = ref('idle')
const loadError = ref(null)
const acting = ref(null)
// Bumped on every cluster switch. Any cache keyed off it is automatically
// invalid for the previous tenant, and any in-flight response that captured an
// older epoch must be discarded rather than written into the new tenant's view.
const epoch = ref(0)

let bootstrapping = null
let redirecting = false
const clusterHooks = new Set()

// ---------------------------------------------------------------------------
// login / logout
// ---------------------------------------------------------------------------

/**
 * Leave for the proxy's login page, carrying where we were so it can land the
 * user back. Idempotent: a burst of 401s from parallel pollers must navigate
 * once, not fight each other.
 */
export function redirectToLogin(next = null) {
  if (redirecting) return
  redirecting = true
  const target = next || `${window.location.pathname}${window.location.search}`
  window.location.assign(`/auth/login?next=${encodeURIComponent(target)}`)
}

export async function logout() {
  try {
    await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin' })
  } catch {
    // Ending the session client-side matters more than the call succeeding:
    // a dashboard that stays open because logout 502'd is the worse failure.
  }
  identity.value = null
  acting.value = null
  window.location.assign('/auth/login')
}

// ---------------------------------------------------------------------------
// bootstrap
// ---------------------------------------------------------------------------

function storageKey(userId) { return `${STORAGE_PREFIX}${userId}` }

function readStoredCluster(userId) {
  try { return localStorage.getItem(storageKey(userId)) } catch { return null }
}

function storeCluster(userId, clusterId) {
  try { localStorage.setItem(storageKey(userId), clusterId) } catch { /* private mode */ }
}

/** First cluster that is actually usable, preferring the remembered one. */
function pickInitialCluster(me) {
  const list = me.clusters || []
  if (!list.length) return null
  const remembered = readStoredCluster(me.user_id)
  const byId = id => list.find(c => c.id === id)
  return (
    byId(remembered) ||
    byId(me.acting_cluster?.id) ||
    list.find(c => c.slug === me.acting_cluster?.slug) ||
    list.find(c => c.status === 'active') ||
    list[0]
  )
}

/**
 * Load /auth/me once. Concurrent callers share the in-flight request.
 * A 401 navigates to login and never settles — the caller is on its way out.
 */
export function ensureIdentity() {
  if (status.value === 'ready') return Promise.resolve(identity.value)
  if (bootstrapping) return bootstrapping
  status.value = 'loading'
  loadError.value = null
  bootstrapping = (async () => {
    let res
    try {
      res = await fetch('/auth/me', {
        credentials: 'same-origin',
        headers: { Accept: 'application/json' },
      })
    } catch (e) {
      status.value = 'error'
      loadError.value = e?.message || 'network error'
      bootstrapping = null
      throw e
    }
    if (res.status === 401) {
      redirectToLogin()
      // Navigation is underway; never resolve so nothing renders behind it.
      return new Promise(() => {})
    }
    if (!res.ok) {
      status.value = 'error'
      loadError.value = `HTTP ${res.status}`
      bootstrapping = null
      throw new Error(`/auth/me failed: HTTP ${res.status}`)
    }
    let me
    try {
      me = await res.json()
    } catch {
      // A 200 that is not JSON means whatever served this page has no identity
      // endpoint at all (a pre-standalone broker's SPA fallback answers
      // /auth/me with index.html). Fail the boot loudly, not mid-parse.
      status.value = 'error'
      loadError.value = 'no identity endpoint on this server'
      bootstrapping = null
      throw new Error('/auth/me did not return JSON')
    }
    identity.value = me
    acting.value = pickInitialCluster(me)
    if (acting.value) storeCluster(me.user_id, acting.value.id)
    status.value = 'ready'
    bootstrapping = null
    if (!acting.value) {
      notifyWarn(
        'No cluster available',
        'This account has no cluster membership, so nothing can be shown. Ask an administrator for access.',
      )
    }
    return me
  })()
  return bootstrapping
}

/** Re-read /auth/me (membership or operator state may have changed). */
export async function reloadIdentity() {
  status.value = 'idle'
  bootstrapping = null
  const keep = acting.value?.id
  const me = await ensureIdentity()
  if (keep) {
    const same = (me.clusters || []).find(c => c.id === keep)
    if (same) acting.value = same
  }
  return me
}

// ---------------------------------------------------------------------------
// acting cluster
// ---------------------------------------------------------------------------

/**
 * Register a reset hook, run on every cluster switch BEFORE any new request
 * goes out. Every module-level cache must have one, or it serves the previous
 * tenant's data under the new tenant's name.
 * Returns an unregister function.
 */
export function onClusterChange(fn) {
  clusterHooks.add(fn)
  return () => clusterHooks.delete(fn)
}

export function setActingCluster(idOrSlug) {
  const list = identity.value?.clusters || []
  const next = list.find(c => c.id === idOrSlug) || list.find(c => c.slug === idOrSlug)
  if (!next || next.id === acting.value?.id) return acting.value
  acting.value = next
  if (identity.value?.user_id) storeCluster(identity.value.user_id, next.id)
  epoch.value += 1
  for (const fn of clusterHooks) {
    try { fn(next) } catch (e) { console.error('[identity] cluster reset hook failed', e) }
  }
  return next
}

/**
 * The act-as header to attach, or null when no cluster is picked. Name comes
 * from /auth/me; value is the cluster uuid (the proxy accepts slug or uuid,
 * and a uuid cannot be ambiguous across tenants).
 */
export function actingClusterHeader() {
  const cluster = acting.value
  if (!cluster) return null
  return {
    name: identity.value?.act_cluster_header || DEFAULT_ACT_HEADER,
    value: cluster.id,
  }
}

/** Epoch at call time — capture before a request, compare after. */
export function currentEpoch() { return epoch.value }

// ---------------------------------------------------------------------------
// derived identity
// ---------------------------------------------------------------------------

const clusters = computed(() => identity.value?.clusters || [])
const email = computed(() => identity.value?.email || null)
// Broker-direct with auth off (server/src/handlers/standalone.rs): one
// synthetic cluster, no session behind the identity — the shell hides the
// session UI (logout, cluster selector) because both would be lies here.
const standalone = computed(() => identity.value?.standalone === true)
const isOperator = computed(() => identity.value?.is_operator === true)
const operatorEnabled = computed(() => identity.value?.operator_enabled === true)
/** Operator AND the cell honours it — the only flag that grants anything. */
const operatorLive = computed(() => identity.value?.operator_live === true)

const actingCluster = computed(() => acting.value)
const actingClusterSlug = computed(() => acting.value?.slug || null)
const actingTenantSlug = computed(() => acting.value?.tenant_slug || null)
// The cell backing the acting cluster (/auth/me clusters[].cell_slug). A cluster
// row names both a tenant and a cell, so the one selector picks what to scope by
// AND which broker answers — which is why every cell-level surface can name the
// cell instead of just claiming to be cell-level.
const actingCellSlug = computed(() => acting.value?.cell_slug || null)

/**
 * Effective role on the acting cluster. A live operator acts as admin on ANY
 * cluster (proxy/src/acting.rs); everyone else gets the role on their
 * cluster_roles row, which /auth/me already resolved.
 */
const role = computed(() => {
  if (!acting.value) return null
  return operatorLive.value ? 'admin' : acting.value.role || null
})

// Capability names mirror the proxy's RouteClass (proxy/src/routes.rs)
// one-for-one, so "may I show this?" and "will the call be authorized?" cannot
// drift apart.
const CAPABILITIES = {
  read: r => r != null,
  produce: r => r === 'admin' || r === 'producer',
  consume: r => r === 'admin' || r === 'consumer',
  queueAdmin: r => r === 'admin',
}

/**
 * The only permission question a view may ask.
 *   can('read') | can('produce') | can('consume') | can('queueAdmin')
 *   can('operator')  -> cell-level surfaces, live operators only
 */
export function can(capability) {
  if (capability === 'operator') return operatorLive.value
  const check = CAPABILITIES[capability]
  if (!check) {
    console.warn(`[identity] unknown capability "${capability}" — refusing`)
    return false
  }
  return check(role.value)
}

export function useIdentity() {
  return {
    identity,
    status,
    loadError,
    clusters,
    email,
    standalone,
    isOperator,
    operatorEnabled,
    operatorLive,
    actingCluster,
    actingClusterSlug,
    actingTenantSlug,
    actingCellSlug,
    role,
    epoch,
    can,
    setActingCluster,
    ensureIdentity,
    reloadIdentity,
    logout,
  }
}
