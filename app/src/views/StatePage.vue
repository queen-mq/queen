<script setup>
// State, in the new console: what the tenant keeps besides its queues. KV keys
// by namespace (a fixed list on the left, the keys of the one picked on the
// right), the locks and semaphores held, and the timers of every queue. The
// view and the namespace are the address (?view=, ?ns=); a key prefix is not,
// because it is a piece of a key (useStatePage.js says why).
import { computed, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { useStatePage } from '@/console/useStatePage'
import { namespaceRows } from '@/composables/useKvSide'
import { fmt, summaryLine } from '@/console/state/stateViews'
import StateKeys from '@/console/state/StateKeys.vue'
import StateLocks from '@/console/state/StateLocks.vue'
import StateTimers from '@/console/state/StateTimers.vue'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const where = computed(() => frame.cluster.value.name)

const VIEWS = ['keys', 'locks', 'timers']
const view = computed(() => (VIEWS.includes(route.query.view) ? route.query.view : 'keys'))
const nsAsked = computed(() => (view.value === 'keys' && typeof route.query.ns === 'string' ? route.query.ns : ''))
const s = useStatePage({ view, nsAsked })

const path = computed(() => {
  const root = { label: 'State', to: '/state' }
  if (view.value === 'locks') return [root, { label: 'Locks and semaphores' }]
  if (view.value === 'timers') return [root, { label: 'Timers' }]
  return s.ns.value ? [root, { label: s.ns.value, mono: true }] : [{ label: 'State' }]
})

const summary = computed(() => summaryLine({ kv: s.kvTotals.value, locks: s.lockSum.value, timers: s.timerSum.value }))

// ---- the fixed list: a click moves the highlight and changes the right side ----
const find = ref('')
const settled = computed(() => s.nsData.value != null || s.nsError.value != null)
const rows = computed(() => namespaceRows(s.nsData.value || [], { picked: view.value === 'keys' ? s.ns.value : '', find: find.value, settled: settled.value }))
const manyNs = computed(() => (s.nsData.value || []).length > 10)
function pickNs(n) { router.replace({ query: { ns: n } }) }
function pickView(v) { router.replace({ query: v === 'keys' ? {} : { view: v } }) }

// On a phone the list would push the page below the fold: one picker instead.
const allRows = computed(() => namespaceRows(s.nsData.value || [], { picked: view.value === 'keys' ? s.ns.value : '', settled: settled.value }))
const current = computed(() => (view.value === 'keys' ? `ns:${s.ns.value}` : `view:${view.value}`))
function pickAny(v) {
  if (v.startsWith('ns:')) pickNs(v.slice(3))
  else pickView(v.slice(5))
}
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="st">
      <h1 class="st-title">State</h1>
      <p class="st-sum qn-num">{{ summary }}</p>

      <label class="qn-field st-pickm">
        <span class="qn-t3" style="font-size: 12px">Show</span>
        <select :value="current" aria-label="What to show" @change="pickAny($event.target.value)">
          <optgroup label="KV namespaces">
            <option v-for="r in allRows" :key="r.namespace" :value="`ns:${r.namespace}`">{{ r.namespace }}{{ r.listed ? ` · ${fmt(r.keys)}` : '' }}</option>
          </optgroup>
          <option value="view:locks">Locks and semaphores</option>
          <option value="view:timers">Timers</option>
        </select>
      </label>

      <div class="st-cols">
        <nav class="st-side" aria-label="What to show">
          <label v-if="manyNs" class="qn-field" style="margin-bottom: 14px">
            <svg width="14" height="14" viewBox="0 0 18 18" aria-hidden="true" style="fill: none; stroke: currentColor; stroke-width: 1.4"><circle cx="7.5" cy="7.5" r="5" /><line x1="11.3" y1="11.3" x2="15.5" y2="15.5" /></svg>
            <span class="qn-vh">Filter the namespaces</span>
            <input v-model="find" type="text" placeholder="Filter the namespaces" spellcheck="false" />
          </label>
          <p class="st-side-h">KV namespaces</p>
          <p v-if="s.kvVerdict.value && s.kvVerdict.value !== 'paused'" class="st-side-note">Not served here.</p>
          <p v-else-if="!settled" class="st-side-note">Reading them…</p>
          <p v-else-if="!rows.length && find" class="st-side-note">Nothing matches “{{ find }}”. <button type="button" class="qn-link" @click="find = ''">Clear</button></p>
          <p v-else-if="!rows.length" class="st-side-note">None yet.</p>
          <button
            v-for="r in rows"
            :key="r.namespace"
            type="button"
            class="st-li"
            :class="{ on: view === 'keys' && s.ns.value === r.namespace }"
            :aria-current="view === 'keys' && s.ns.value === r.namespace ? 'true' : undefined"
            :title="r.listed ? `${r.namespace}: ${fmt(r.keys)} ${r.keys === 1 ? 'key' : 'keys'}, expired ones awaiting the sweeper included` : `${r.namespace}: not in the namespace list`"
            @click="pickNs(r.namespace)"
          >
            <span class="qn-mono st-li-name">{{ r.namespace }}</span>
            <span class="qn-num qn-t3 st-li-n">{{ r.listed ? fmt(r.keys) : '–' }}</span>
          </button>
          <div class="st-sep" />
          <button type="button" class="st-li" :class="{ on: view === 'locks' }" :aria-current="view === 'locks' ? 'true' : undefined" @click="pickView('locks')">
            <span class="st-li-name">Locks and semaphores</span>
          </button>
          <button type="button" class="st-li" :class="{ on: view === 'timers' }" :aria-current="view === 'timers' ? 'true' : undefined" @click="pickView('timers')">
            <span class="st-li-name">Timers</span>
          </button>
        </nav>

        <section class="st-main" :aria-label="view === 'keys' ? `The keys of ${s.ns.value}` : view === 'locks' ? 'Locks and semaphores' : 'Timers'">
          <StateKeys v-if="view === 'keys'" :s="s" :where="where" />
          <StateLocks v-else-if="view === 'locks'" :s="s" />
          <StateTimers v-else :s="s" :where="where" />
        </section>
      </div>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.st { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.st-title { font-size: 22px; font-weight: 500; }
.st-sum { font-size: 15px; margin-top: 14px; max-width: 920px; }
.st-cols { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 24px 40px; margin-top: 26px; }
.st-side { flex: 1 1 220px; max-width: 250px; min-width: 0; }
.st-main { flex: 999 1 640px; min-width: 0; }
.st-side-h { font-size: 12px; color: var(--text3); padding: 0 10px 6px; }
.st-side-note { font-size: 12.5px; color: var(--text2); padding: 4px 10px; }
.st-li { display: flex; align-items: center; gap: 10px; width: 100%; height: 34px; padding: 0 10px; border-radius: var(--r-ctl); color: var(--text); text-align: left; }
.st-li:hover { background: var(--bg2); }
.st-li.on, .st-li.on:hover { background: var(--bg3); font-weight: 500; }
.st-li-name { flex: 1 1 0; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.st-li-n { flex: 0 0 auto; font-size: 12px; font-weight: 400; }
.st-sep { height: 1px; background: var(--line); margin: 14px 0; }
:global(.qn.tone .st-sep) { background: var(--bg3); }
.st-pickm { display: none; margin-top: 18px; }
.st-pickm select { flex: 1 1 0; min-width: 0; border: 0; outline: 0; background: transparent; color: var(--text); font-size: 13px; }
@media (max-width: 640px) {
  .st { padding: 18px 16px 40px; }
  .st-title { font-size: 19px; }
  .st-side { display: none; }
  .st-pickm { display: flex; }
  .st-cols { margin-top: 20px; }
}
</style>
