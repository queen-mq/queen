// Global error/notification surface — module singleton.
//
// The rule this store exists to enforce: NO failed call is invisible. The
// shared HTTP client reports every API failure here (see api/client.js), so a
// view that swallows an error still cannot make the product look healthy. A
// dead broker paints a banner, not a dashboard of zeros.
//
// Views should not re-announce API failures they catch — those are already on
// screen. Use notifySuccess/notifyError for things the shell cannot see: the
// outcome of a local action, a validation refusal, a copy-to-clipboard.
import { computed, ref } from 'vue'

import { ApiError, describeApiError } from '@/api/errors'

const TTL_MS = { success: 4000, info: 5000, warn: 9000, error: 12000 }
// Pollers retry on a fixed cadence; the same failure must not stack up into a
// wall of identical toasts. Repeats inside this window bump a counter instead.
const DEDUPE_MS = 8000
// A page-load storm can fail a dozen distinct endpoints at once. Show the most
// recent few — enough to make the failure undeniable without burying the app.
const MAX_VISIBLE = 5

const toasts = ref([])
let seq = 0

// Transport health, derived from what actually happens on the wire rather
// than from a health endpoint that is itself unreachable when it matters.
const offline = ref(false)
const offlineSince = ref(null)
const lastFailure = ref(null)

function remove(id) {
  const i = toasts.value.findIndex(t => t.id === id)
  if (i !== -1) toasts.value.splice(i, 1)
}

function push({ level = 'info', title, detail = null, key = null, sticky = false }) {
  const dedupeKey = key || `${level}|${title}|${detail || ''}`
  const now = Date.now()
  const existing = toasts.value.find(t => t.key === dedupeKey && now - t.at < DEDUPE_MS)
  if (existing) {
    existing.count += 1
    existing.at = now
    return existing.id
  }
  const id = ++seq
  toasts.value.push({ id, level, title, detail, key: dedupeKey, count: 1, at: now, sticky })
  while (toasts.value.length > MAX_VISIBLE) toasts.value.shift()
  if (!sticky) setTimeout(() => remove(id), TTL_MS[level] || TTL_MS.info)
  return id
}

export function notifySuccess(title, detail = null) {
  return push({ level: 'success', title, detail })
}

export function notifyInfo(title, detail = null) {
  return push({ level: 'info', title, detail })
}

export function notifyWarn(title, detail = null) {
  return push({ level: 'warn', title, detail })
}

/**
 * Surface a failure. Accepts an ApiError, any Error, or a plain string.
 * `context` names what the user was doing ("Delete queue", "Load consumers").
 */
export function notifyError(err, context = null) {
  const detail = typeof err === 'string' ? err : describeApiError(err)
  const status = err instanceof ApiError ? err.status : null
  return push({
    level: 'error',
    title: context || 'Request failed',
    detail,
    key: `error|${context || ''}|${status}|${detail}`,
  })
}

export function dismiss(id) { remove(id) }
export function dismissAll() { toasts.value = [] }

// ---------------------------------------------------------------------------
// Called by api/client.js only — the shell-wide guarantee that a failed call
// is never silent, whatever the caller does with the rejection.
// ---------------------------------------------------------------------------
export function reportApiFailure(err) {
  lastFailure.value = { at: Date.now(), status: err.status, code: err.code, path: err.path, message: err.message }
  if (err.isOffline) {
    if (!offline.value) {
      offline.value = true
      offlineSince.value = Date.now()
    }
    // The banner already says it, permanently and in one place.
    return
  }
  notifyError(err, err.path ? `${err.path} failed` : null)
}

export function reportApiSuccess() {
  if (offline.value) {
    offline.value = false
    offlineSince.value = null
    notifyInfo('Connection restored')
  }
}

const offlineFor = computed(() => (offlineSince.value ? Date.now() - offlineSince.value : 0))

export function useUiStore() {
  return {
    toasts,
    offline,
    offlineSince,
    offlineFor,
    lastFailure,
    notifySuccess,
    notifyInfo,
    notifyWarn,
    notifyError,
    dismiss,
    dismissAll,
  }
}
