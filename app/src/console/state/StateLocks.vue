<script setup>
// The locks and semaphores held, read from their rows in the KV namespace
// queen-locks: a permit is one row, its value the holder, its expiry the end
// of the lease, its version the fencing token (server/src/locks.rs). Nothing
// here takes or frees one: a lease ends by itself unless its holder renews it.
import { computed, nextTick, ref } from 'vue'
import { describeVerdict } from '@/composables/useGatedVerdict'
import { LOCKS_NAMESPACE } from '@/composables/useLocks'
import { nowMs, clock } from '../useWindow'
import { fmt, span, ago, inOrAgo, leaseLeft } from './stateViews'

const props = defineProps({ s: { type: Object, required: true } })
const { lockList, locks, locksError, kvVerdict } = props.s
const now = nowMs

const quiet = computed(() => (kvVerdict.value && kvVerdict.value !== 'paused' ? describeVerdict(kvVerdict.value, 'kv') : null))
const rows = computed(() => lockList.value?.rows || [])
const picked = ref(null)
const drawer = ref(null)
const sel = computed(() => rows.value.find((r) => r.name === picked.value) || null)
function toggle(name) {
  picked.value = picked.value === name ? null : name
  if (picked.value) nextTick(() => drawer.value?.scrollIntoView?.({ block: 'nearest' }))
}
const at = (ms) => (ms == null ? '–' : clock(ms, true))
const heldFor = (r) => (r.sinceMs == null ? '–' : span((now.value - r.sinceMs) / 1000))
const read = computed(() => (locks.value?.absent ? 'GET /api/v1/resources/kv/namespaces' : 'POST /api/v1/resources/kv/list'))
</script>

<template>
  <div class="stl">
    <div class="stl-list">
      <h2 class="stl-h">Locks and semaphores</h2>

      <template v-if="quiet">
        <p class="stl-lead">{{ quiet.title }}.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">{{ quiet.detail }}</p>
      </template>
      <p v-else-if="locksError && !locks" class="stl-state"><span class="qn-mark red" aria-hidden="true" style="margin-right: 8px" />The broker did not answer: {{ locksError }}</p>
      <p v-else-if="!locks" class="stl-state qn-t2">Reading the locks…</p>
      <template v-else-if="!rows.length">
        <p class="stl-lead">No locks are held.</p>
        <p class="qn-t2" style="margin-top: 6px; max-width: 640px">
          A lock lets one worker at a time do a job no message starts, a cron run or a singleton; a semaphore lets a few.
          Each is a lease in KV: it ends by itself unless its holder renews it.
        </p>
      </template>

      <template v-else>
        <div class="stl-scroll">
          <div class="stl-table" role="table" aria-label="Locks and semaphores held" style="--cols: minmax(0, 1.3fr) 150px minmax(0, 1.2fr) 88px 96px">
            <div class="qn-tr head" role="row">
              <span role="columnheader">Name</span><span role="columnheader">Kind</span><span role="columnheader">Held by</span>
              <span role="columnheader" class="r">Held for</span><span role="columnheader" class="r">Lease left</span>
            </div>
            <div
              v-for="r in rows" :key="r.name" class="qn-tr click" :class="{ on: picked === r.name }" role="row"
              tabindex="0" @click="toggle(r.name)" @keydown.enter="toggle(r.name)"
            >
              <span role="cell" class="qn-mono" :title="r.name" style="font-weight: 500">{{ r.name }}</span>
              <span role="cell" class="qn-t2">{{ r.kind }}</span>
              <span v-if="r.kind === 'lock'" role="cell" :class="r.holders[0]?.owner ? 'qn-mono' : 'qn-t3'" :title="r.holders[0]?.owner || ''">{{ r.holders[0]?.owner || 'no owner given' }}</span>
              <span v-else role="cell" class="qn-t2">{{ fmt(r.holders.length) }} holders</span>
              <span role="cell" class="r qn-num" :title="r.kind === 'lock' ? `since ${at(r.sinceMs)}` : 'the permit held longest'">{{ heldFor(r) }}</span>
              <span role="cell" class="r qn-num" :class="leaseLeft(r.endsMs, now) === 'ending' ? 'qn-t3' : ''" :title="r.kind === 'lock' ? `ends at ${at(r.endsMs)}` : 'the permit that ends first'">{{ leaseLeft(r.endsMs, now) }}</span>
            </div>
          </div>
        </div>
        <p v-if="lockList.other" class="qn-t2" style="margin-top: 10px">{{ lockList.other === 1 ? 'One row' : `${fmt(lockList.other)} rows` }} in <span class="qn-mono">{{ LOCKS_NAMESPACE }}</span> {{ lockList.other === 1 ? 'is' : 'are' }} not a permit the broker wrote; open the namespace among the keys to see {{ lockList.other === 1 ? 'it' : 'them' }}.</p>
        <p v-if="locks.truncated" class="qn-t2" style="margin-top: 10px">More than 1,000 permits are held: these are the first 1,000 by name.</p>
        <p class="qn-t3" style="margin-top: 12px">A lease ends by itself unless its holder renews it, so to free a lock, stop its holder.</p>
      </template>

      <p class="qn-read" style="margin-top: 12px">
        Read from <span class="qn-mono">{{ read }}</span><template v-if="locks?.absent">, which lists no <span class="qn-mono">{{ LOCKS_NAMESPACE }}</span> namespace</template><template v-else>: the live rows of the namespace <span class="qn-mono">{{ LOCKS_NAMESPACE }}</span>, one per permit</template>.
      </p>
    </div>

    <aside v-if="sel" ref="drawer" class="stl-drawer qn-panel" :aria-label="`The ${sel.kind} ${sel.name}`">
      <div style="display: flex; align-items: flex-start; gap: 8px">
        <div style="flex: 1 1 0; min-width: 0">
          <p class="qn-t3" style="font-size: 12px">{{ sel.kind === 'lock' ? 'Lock' : 'Semaphore' }}</p>
          <h3 class="qn-mono stl-name">{{ sel.name }}</h3>
        </div>
        <button type="button" class="qn-btn3" aria-label="Close" style="height: 28px; flex: 0 0 auto" @click="picked = null">✕</button>
      </div>
      <template v-if="sel.kind === 'lock'">
        <dl class="qn-dl" style="margin-top: 14px">
          <dt>Holder</dt><dd :class="sel.holders[0]?.owner ? 'qn-mono' : 'qn-t3'">{{ sel.holders[0]?.owner || 'no owner given' }}</dd>
          <dt>Held since</dt><dd class="qn-num">{{ at(sel.sinceMs) }}, for {{ heldFor(sel) }}</dd>
          <dt>Lease ends</dt><dd class="qn-num">{{ at(sel.endsMs) }}, {{ leaseLeft(sel.endsMs, now) === 'ending' ? 'now' : inOrAgo(sel.endsMs, now) }}</dd>
          <dt>Lifetime</dt><dd class="qn-num">{{ sel.holders[0]?.lifetime ? `${span(sel.holders[0].lifetime)}, set at each renewal` : '–' }}</dd>
          <dt>Renewed</dt><dd class="qn-num">{{ ago(sel.holders[0]?.renewedMs, now) || '–' }}</dd>
          <dt>Token</dt><dd class="qn-num">{{ fmt(sel.holders[0]?.token) }}</dd>
        </dl>
      </template>
      <template v-else>
        <p class="qn-t3" style="font-size: 12px; margin-top: 14px">Holders</p>
        <ul class="stl-holders">
          <li v-for="h in sel.holders" :key="h.key">
            <span :class="h.owner ? 'qn-mono' : 'qn-t3'" style="flex: 1 1 0; min-width: 0; overflow-wrap: anywhere">{{ h.owner || 'no owner given' }}</span>
            <span class="qn-num qn-t2">slot {{ h.slot }}, {{ leaseLeft(h.endsMs, now) === 'ending' ? 'ending' : `${leaseLeft(h.endsMs, now)} left` }}</span>
          </li>
        </ul>
        <p class="qn-t3" style="font-size: 12px; margin-top: 8px">Its limit is the one its holders ask for; the broker does not keep it.</p>
      </template>
      <p v-if="sel.kind === 'lock'" class="qn-t2" style="margin-top: 14px">A transaction guarded by this lock commits only while its holder still has it.</p>
    </aside>
  </div>
</template>

<style scoped>
.stl { display: flex; flex-wrap: wrap; align-items: flex-start; gap: 20px 28px; }
.stl-list { flex: 999 1 520px; min-width: 0; }
.stl-h { font-size: 18px; font-weight: 500; min-height: 32px; display: flex; align-items: center; }
.stl-lead { font-size: 14.5px; margin-top: 14px; }
.stl-state { padding: 18px 0 6px; }
.stl-scroll { margin-top: 12px; overflow-x: auto; }
.stl-table { min-width: 620px; }
.stl-drawer { flex: 1 1 340px; min-width: 0; max-width: 440px; padding: 16px 18px; position: sticky; top: 64px; }
.stl-name { font-size: 13.5px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.stl-holders { margin-top: 6px; }
.stl-holders li { display: flex; align-items: baseline; gap: 10px; padding: 7px 0; border-bottom: 1px solid var(--line2); font-size: 12.5px; }
@media (max-width: 900px) { .stl-drawer { position: static; max-width: none; } }
</style>
