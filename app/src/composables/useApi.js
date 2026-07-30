import { computed, ref, onUnmounted } from 'vue'

import { currentEpoch, onClusterChange } from '@/stores/identity'

/**
 * One panel's worth of data, with the three states a panel must be able to
 * show: loading, error, and "as of when".
 *
 * `error` holds the ApiError itself (status / code / retryAfter preserved), so
 * a view can distinguish 403 from 404 route_blocked from 429 — render
 * `describeApiError(error)` for the human sentence. The failure is ALSO
 * already on the global surface; what this gives you is the inline state, so
 * an unreachable panel never renders as an empty or zeroed one.
 *
 * `fetchFn` receives `{ signal }` as its last argument — pass it through to
 * the api wrapper (every wrapper takes an axios config) and the request is
 * aborted on unmount and on a cluster switch, so a late response can never
 * overwrite the new tenant's state.
 */
export function useApi(fetchFn, options = {}) {
  const {
    immediate = true,
    onSuccess = null,
    onError = null,
  } = options

  const data = ref(null)
  const loading = ref(false)
  const error = ref(null)
  const lastUpdated = ref(null)

  let controller = null

  const abort = () => {
    if (controller) {
      controller.abort()
      controller = null
    }
  }

  const execute = async (...args) => {
    abort()
    controller = new AbortController()
    const signal = controller.signal
    const epochAtStart = currentEpoch()
    loading.value = true
    error.value = null

    try {
      const response = await fetchFn(...args, { signal })
      // Discard anything that belongs to a cluster we are no longer on.
      if (signal.aborted || epochAtStart !== currentEpoch()) return data.value
      data.value = response.data
      lastUpdated.value = new Date()
      if (onSuccess) onSuccess(response.data)
      return response.data
    } catch (err) {
      if (signal.aborted) return data.value
      error.value = err
      if (onError) onError(err)
      throw err
    } finally {
      if (!signal.aborted) loading.value = false
    }
  }

  /** Fire-and-forget variant: never produces an unhandled rejection. */
  const refresh = () => execute().catch(() => {})

  const stopClusterWatch = onClusterChange(() => {
    abort()
    data.value = null
    lastUpdated.value = null
    error.value = null
  })

  if (immediate) refresh()

  onUnmounted(() => {
    abort()
    stopClusterWatch()
  })

  return {
    data,
    loading,
    error,
    lastUpdated,
    /** True once a load failed and nothing newer succeeded. */
    failed: computed(() => error.value !== null),
    execute,
    refresh,
    abort,
  }
}

/**
 * Format duration in milliseconds to human readable
 */
export function formatDuration(ms) {
  if (!ms || ms === 0) return '0ms'
  
  if (ms < 1000) return `${Math.round(ms)}ms`
  
  const seconds = ms / 1000
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  
  const minutes = Math.floor(seconds / 60)
  const remainingSeconds = Math.round(seconds % 60)
  
  if (minutes < 60) {
    return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`
  }
  
  const hours = Math.floor(minutes / 60)
  const remainingMinutes = minutes % 60
  
  if (hours < 24) {
    return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`
  }
  
  const days = Math.floor(hours / 24)
  const remainingHours = hours % 24
  
  return remainingHours > 0 ? `${days}d ${remainingHours}h` : `${days}d`
}

/**
 * Format number with abbreviations for large values
 * - Numbers < 1M: show with commas (e.g., "999,999")
 * - Numbers >= 1M: show as "1.2M", "123.5M"
 * - Numbers >= 1B: show as "1.2B", "12.3B"
 */
export function formatNumber(num) {
  if (num === null || num === undefined) return '0'
  
  const absNum = Math.abs(num)
  const sign = num < 0 ? '-' : ''
  
  // Billions (1,000,000,000+)
  if (absNum >= 1_000_000_000) {
    const value = absNum / 1_000_000_000
    // Show 1 decimal for < 10B, no decimal for >= 10B
    const formatted = value >= 10 ? Math.round(value) : value.toFixed(1).replace(/\.0$/, '')
    return `${sign}${formatted}B`
  }
  
  // Millions (1,000,000+)
  if (absNum >= 1_000_000) {
    const value = absNum / 1_000_000
    // Show 1 decimal for < 10M, no decimal for >= 100M
    const formatted = value >= 100 
      ? Math.round(value)
      : value >= 10 
        ? value.toFixed(1).replace(/\.0$/, '')
        : value.toFixed(1).replace(/\.0$/, '')
    return `${sign}${formatted}M`
  }
  
  // Under 1 million: show with commas
  return num.toLocaleString('en-US')
}

/**
 * Format bytes to human readable
 */
export function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B'
  
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`
}

/**
 * Format date/time
 */
export function formatDateTime(date) {
  if (!date) return '-'
  
  const d = new Date(date)
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  })
}

/**
 * Convert a value to a finite number, or `null` when missing or non-numeric.
 *
 * Use this when building chart series so a missing bucket value renders as
 * a gap in the line rather than collapsing to a misleading zero.
 *
 *   toNum(0)         === 0
 *   toNum(1.5)       === 1.5
 *   toNum('2.3')     === 2.3
 *   toNum(null)      === null
 *   toNum(undefined) === null
 *   toNum('abc')     === null
 *
 * Note: `Number(null) === 0` and `null || 0 === 0`, which is exactly the
 * trap this helper avoids.
 */
export function toNum(v) {
  if (v === null || v === undefined) return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

/**
 * Walk a series from the end and return the most recent finite value.
 * Useful for "Now" displays where the latest bucket may be null/missing
 * and falling back to 0 would lie about the metric (e.g. throughput
 * appearing to drop to zero when really we just don't have the sample yet).
 *
 * Returns `null` when the series is empty or every value is missing.
 */
export function latestFinite(arr) {
  if (!arr || !arr.length) return null
  for (let i = arr.length - 1; i >= 0; i--) {
    const n = toNum(arr[i])
    if (n !== null) return n
  }
  return null
}

/**
 * Drop the most recent bucket from a time series if its window covers the
 * current wall-clock moment (i.e. it's still aggregating). Otherwise the
 * partial-minute sample reads low or zero and paints a phantom "drop
 * towards zero" on the right edge of every chart.
 *
 * Workers buffer metrics in memory and flush on minute boundaries, so a
 * bucket with start time T is only complete once `now >= T + bucketMinutes`.
 *
 * Options:
 *   - bucketKey: field name on each row that holds the bucket timestamp
 *                (default 'bucket'). Pass 'timestamp' for status_v3 rows.
 *   - bucketMinutes: rollup width in minutes (default 1). Read from the
 *                    response's bucketMinutes field when available.
 *
 * The input is assumed to be sorted oldest → newest. Returns a *new* array
 * (never mutates) and is safe to call on null/empty inputs.
 */
export function trimIncompleteBuckets(rows, { bucketKey = 'bucket', bucketMinutes = 1 } = {}) {
  if (!rows || rows.length === 0) return rows || []
  // Find the latest bucket by string-compare (ISO-like timestamps sort
  // lexicographically). This works whether rows are sorted or not, and
  // whether the response carries one row per bucket (worker_metrics) or
  // many (queue-ops with one row per queue per bucket).
  let latest = null
  for (const r of rows) {
    const ts = r && r[bucketKey]
    if (typeof ts === 'string' && (latest === null || ts > latest)) latest = ts
  }
  if (!latest) return rows
  const startMs = new Date(latest).getTime()
  if (!Number.isFinite(startMs)) return rows
  const bucketMs = (Number(bucketMinutes) || 1) * 60 * 1000
  // Bucket is "in-flight" if its start is more recent than one bucket-width
  // ago — i.e. its end time hasn't been reached. Trim every row that
  // belongs to that bucket; older buckets have completed and are safe.
  if (Date.now() - startMs < bucketMs) {
    return rows.filter(r => r && r[bucketKey] !== latest)
  }
  return rows
}

/**
 * Format relative time
 */
export function formatRelativeTime(date) {
  if (!date) return '-'
  
  const now = new Date()
  const d = new Date(date)
  const diff = now - d
  
  if (diff < 60000) return 'just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  return `${Math.floor(diff / 86400000)}d ago`
}

