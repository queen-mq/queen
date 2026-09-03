<template>
  <!-- Boot gate: nothing renders until /auth/me says who this is. A shell
       drawn before identity is a shell that has to guess a permission. -->
  <div v-if="status === 'loading' || status === 'idle'" class="boot-screen">
    <img src="/queen-mark.svg" alt="" class="boot-mark" />
    <p>Starting session…</p>
  </div>

  <div v-else-if="status === 'error'" class="boot-screen boot-error">
    <img src="/queen-mark.svg" alt="" class="boot-mark" />
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

  <div v-else class="app-shell" :class="{ collapsed }">
    <!-- Sidebar wrapper - hidden from grid flow on mobile -->
    <div class="sidebar-slot">
      <Sidebar :collapsed="collapsed" @toggle-collapse="collapsed = !collapsed" />
    </div>

    <section class="app-main">
      <Header @refresh="handleRefresh" />

      <!-- One banner, always true, never guessed: it is driven by what the
           wire actually did, not by a health endpoint that is itself
           unreachable when it matters. -->
      <div v-if="offline" class="status-strip">
        <div class="status-banner banner-bad">
          <span class="pulse-ember" style="width:7px; height:7px; flex-shrink:0;" />
          <span :title="formatTimestampUtc(offlineSince)">
            <strong>Cannot reach the API</strong> ·
            everything on screen is from before {{ offlineSinceText }} and may be wrong
          </span>
        </div>
      </div>
      <div v-else-if="!actingCluster" class="status-strip">
        <div class="status-banner banner-warn">
          <span>
            <strong>No cluster selected</strong> ·
            this account has no cluster membership, so there is nothing to show
          </span>
        </div>
      </div>

      <main class="app-page">
        <router-view />
      </main>
    </section>
  </div>

  <ToastHost />
</template>

<script setup>
import { computed, onUnmounted, provide, ref, watch } from 'vue'

import Sidebar from '@/components/Sidebar.vue'
import Header from '@/components/Header.vue'
import ToastHost from '@/components/ToastHost.vue'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useIdentity } from '@/stores/identity'
import { useUiStore } from '@/stores/ui'

const { status, loadError, actingCluster, epoch, ensureIdentity } = useIdentity()
const { offline, offlineSince } = useUiStore()

const collapsed = ref(false)

const offlineSinceText = computed(() =>
  offlineSince.value ? formatTimestamp(offlineSince.value) : 'the last successful load'
)

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

const handleRefresh = () => fire(false)

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
