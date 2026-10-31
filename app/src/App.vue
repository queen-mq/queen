<template>
  <!-- Boot gate: nothing renders until /auth/me says who this is. A shell
       drawn before identity is a shell that has to guess a permission. -->
  <div v-if="status === 'loading' || status === 'idle'" class="boot-screen">
    <span class="boot-mark" aria-hidden="true" v-html="mark" />
    <p>Starting session…</p>
  </div>

  <div v-else-if="status === 'error'" class="boot-screen boot-error">
    <span class="boot-mark" aria-hidden="true" v-html="mark" />
    <h2>Cannot start the dashboard</h2>
    <p>
      <code>/auth/me</code> did not answer ({{ loadError }}). The server may be
      down, or this session may no longer be valid.
    </p>
    <div class="boot-actions">
      <button class="boot-btn" @click="retry">Retry</button>
      <a class="boot-btn" href="/auth/login">Sign in again</a>
    </div>
  </div>

  <!-- Until the router has resolved the first page (its chunk is loading),
       the mark rather than an empty window. -->
  <div v-else-if="!route.matched.length" class="boot-screen">
    <span class="boot-mark" aria-hidden="true" v-html="mark" />
  </div>

  <!-- Every page is the console's: it draws its own rail and band (app/src/console). -->
  <router-view v-else />
</template>

<script setup>
import { onUnmounted, provide, watch } from 'vue'
import { useRoute } from 'vue-router'

import mark from '@/assets/q.svg?raw'
import { toast } from '@/console/toasts'
import { useIdentity } from '@/stores/identity'
import { useUiStore } from '@/stores/ui'

const route = useRoute()
const { status, loadError, epoch, ensureIdentity } = useIdentity()
const { toasts: notices } = useUiStore()

// The app's own notices (a page refused by the guard, a failed call nobody
// caught, an interface error) show as console toasts: one place for them.
// The store keeps its dedupe and expiry; only new ones are passed on.
const TONE = { error: 'red', warn: 'amber' }
const passed = new Set()
watch(notices, (list) => {
  for (const n of list) {
    if (passed.has(n.id)) continue
    passed.add(n.id)
    toast(n.title, n.detail || '', TONE[n.level] || '')
  }
}, { deep: true })

const retry = () => { ensureIdentity().catch(() => {}) }

// ---------------------------------------------------------------------------
// Refresh registry. Keyed by a per-instance Symbol handed over by useRefresh —
// never by route path, which is already the destination's by the time a view
// unmounts.
// ---------------------------------------------------------------------------
const AUTO_REFRESH_MS = 30_000
const refreshCallbacks = new Map()

const registerRefreshCallback = (key, callback, opts = {}) => {
  refreshCallbacks.set(key, { callback, auto: opts.auto === true })
}
const unregisterRefreshCallback = (key) => { refreshCallbacks.delete(key) }
provide('registerRefreshCallback', registerRefreshCallback)
provide('unregisterRefreshCallback', unregisterRefreshCallback)

const fire = (onlyAuto) => {
  for (const entry of refreshCallbacks.values()) {
    if (onlyAuto && !entry.auto) continue
    try { entry.callback() } catch (e) { console.error('[refresh] callback failed', e) }
  }
}

// A cluster switch invalidates everything on screen. The stores have already
// cleared themselves (identity's reset hooks); this pulls the new tenant's
// data in, so the user never sits looking at an empty panel wondering whether
// that is the answer or just the absence of one.
watch(epoch, () => fire(false))

// One ticker for the whole app, suspended while the tab is hidden: under the
// proxy every poll is rate-limited and metered, so a backgrounded tab must not
// keep spending the tenant's budget.
const ticker = setInterval(() => {
  if (document.hidden) return
  fire(true)
}, AUTO_REFRESH_MS)
onUnmounted(() => clearInterval(ticker))
</script>
