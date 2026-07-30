import { inject, onMounted, onUnmounted } from 'vue'

/**
 * Register a callback with the shell's refresh registry.
 *
 * The key is a per-instance Symbol captured at setup: keying by route path
 * unregistered the WRONG entry (vue-router has already moved `route.path` to
 * the destination by the time a component unmounts), so every visited page
 * leaked its callback and kept fetching from off screen.
 *
 * Options:
 *   auto      — also fire on the shell's shared ticker (paused while the tab
 *               is hidden). Use this INSTEAD of a private setInterval; seven
 *               independent pollers in a hidden tab are seven metered,
 *               rate-limited requests per tick under the proxy.
 */
export function useRefresh(callback, { auto = false } = {}) {
  const key = Symbol('refresh')
  const registerRefreshCallback = inject('registerRefreshCallback', null)
  const unregisterRefreshCallback = inject('unregisterRefreshCallback', null)

  onMounted(() => {
    if (registerRefreshCallback && callback) {
      registerRefreshCallback(key, callback, { auto })
    }
  })

  onUnmounted(() => {
    if (unregisterRefreshCallback) unregisterRefreshCallback(key)
  })

  return { key }
}

/**
 * Auto-refreshing registration: same cadence for the whole app, one timer,
 * suspended while `document.hidden`. Equivalent to
 * `useRefresh(cb, { auto: true })`, named for the call site that wants polling.
 */
export function useAutoRefresh(callback) {
  return useRefresh(callback, { auto: true })
}
