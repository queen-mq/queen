import { ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { readRouteValue } from './navigation.js'

const pending = new WeakMap()
export function replaceQueryFields(router, route, patch) {
  const previous = pending.get(router)
  if (previous && previous.path === route.path) {
    Object.assign(previous.patch, patch)
    return
  }
  const change = { path: route.path, patch }
  pending.set(router, change)
  queueMicrotask(() => {
    if (pending.get(router) !== change) return
    pending.delete(router)
    if (route.path !== change.path) return
    const query = { ...route.query }
    for (const [key, value] of Object.entries(change.patch)) {
      if (value == null) delete query[key]
      else query[key] = value
    }
    if (JSON.stringify(query) !== JSON.stringify(route.query)) router.replace({ query })
  })
}

/** Bidirectional, bookmarkable filters. Back/Forward and same-page links use
 * the same contract as the first load. Unrelated query parameters survive. */
export function useRouteState(fields) {
  const route = useRoute(), router = useRouter()
  const pageName = route.name
  const defaults = Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, value.value]))
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b)
  const restoring = ref(false)
  const restore = () => {
    if (route.name !== pageName) return
    restoring.value = true
    for (const [key, value] of Object.entries(fields)) {
      const next = readRouteValue(route.query[key], defaults[key])
      if (!same(value.value, next)) value.value = next
    }
    // Remains true through the view's filter watchers, which must not reset a
    // page restored by Back/Forward.
    queueMicrotask(() => { restoring.value = false })
  }
  restore()
  watch(() => route.query, restore)
  watch(Object.values(fields), () => {
    if (route.name !== pageName) return
    const patch = {}
    for (const [key, value] of Object.entries(fields)) {
      patch[key] = same(value.value, defaults[key]) || value.value == null ? null : Array.isArray(value.value) ? [...value.value] : String(value.value)
    }
    replaceQueryFields(router, route, patch)
  }, { flush: 'post' })
  return { restoring }
}
