import { computed, onMounted, onUnmounted, ref, unref } from 'vue'

/**
 * "4s ago" — how long since the value behind `lastUpdated` last moved.
 *
 * ONE ticker for the whole app. Four auto-polling views render a live tick;
 * before this they each owned a private `setInterval(…, 1000)` plus a verbatim
 * copy of the same formatter, so the wording drifted and the app spent four
 * timers to render one kind of string.
 *
 * The ticker only runs while at least one view is mounted, and it never
 * fetches anything — it re-reads the clock so an already-known timestamp can
 * be re-rendered as a relative string.
 *
 * `lastUpdated` may be a ref (or getter) holding a Date, an epoch millisecond
 * number, or null. Null renders '—': "we have never loaded" is not "0s ago".
 * The caller decides WHICH timestamp it is — a poll attempt or a last-good
 * load — and the two are not interchangeable: a tick fed by an attempt keeps
 * resetting while the data rots.
 */
const now = ref(Date.now())
let handle = null
let subscribers = 0

function subscribe() {
  subscribers += 1
  if (handle === null) {
    now.value = Date.now()
    handle = setInterval(() => { now.value = Date.now() }, 1000)
  }
}

function unsubscribe() {
  subscribers = Math.max(0, subscribers - 1)
  if (subscribers === 0 && handle !== null) {
    clearInterval(handle)
    handle = null
  }
}

export function useRefreshAgo(lastUpdated) {
  onMounted(subscribe)
  onUnmounted(unsubscribe)

  return computed(() => {
    const raw = typeof lastUpdated === 'function' ? lastUpdated() : unref(lastUpdated)
    if (!raw) return '—'
    const ms = raw instanceof Date ? raw.getTime() : raw
    const sec = Math.max(0, Math.floor((now.value - ms) / 1000))
    if (sec < 5) return 'just now'
    if (sec < 60) return `${sec}s ago`
    if (sec < 3600) return `${Math.floor(sec / 60)}m ago`
    return `${Math.floor(sec / 3600)}h ago`
  })
}
