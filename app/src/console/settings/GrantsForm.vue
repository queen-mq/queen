<script setup>
// What a scoped key reaches, as a form: its grants (one queue, KV namespace or
// timers queue each, with what may be done there), its rate limits and the
// longest token it mints. Used by Settings › Access to make a scoped key and
// to edit one. The words and every rule are composables/scopedKeys.js; this
// file only lays them out.
//
// v-model: { rows: [{ id, kind, name, partition, group, key, ops }], keyRps,
// subRps, ttl }. Every field is a string (empty = not named), but ttl, a number.
import { computed } from 'vue'
import { GRANT_KINDS, blankRow, ceilingChoices, grantOf, grantProblem, namesSub, rpsProblem, switchKind } from '@/composables/scopedKeys'

const form = defineModel({ type: Object, required: true })
const MAX_GRANTS = 64

const kindOf = (kind) => GRANT_KINDS.find((k) => k.kind === kind) || GRANT_KINDS[0]
const nameLabel = (kind) => ({ queue: 'Queue', kv: 'Namespace', timers: 'Timers queue' })[kind] || 'Name'
const FIELD = { partition: 'Partition', group: 'Consumer group', key: 'Key' }

let next = 1 + Math.max(0, ...form.value.rows.map((r) => Number(r.id) || 0))
const add = () => { if (form.value.rows.length < MAX_GRANTS) form.value.rows.push({ ...blankRow(), id: next++ }) }
const remove = (i) => form.value.rows.splice(i, 1)
const setKind = (i, kind) => { form.value.rows[i] = { ...switchKind(form.value.rows[i], kind), id: form.value.rows[i].id } }
function toggleOp(i, op, on) {
  const row = form.value.rows[i]
  const rest = row.ops.filter((o) => o !== op)
  row.ops = on ? [...rest, op] : rest
}

// A problem shows under its grant once something is typed there, so a fresh
// form is not a page of red.
const touched = (row) => !!(row.name || row.partition || row.group || row.key)
const problems = computed(() => form.value.rows.map((r) => (touched(r) ? grantProblem(grantOf(r)) : '')))
// Easy to miss on a key made for devices: without {sub} anywhere, every token
// it mints reaches all of its grants, so one device reads another's data.
const shared = computed(() => form.value.rows.some((r) => r.name) && !namesSub(form.value.rows.map(grantOf)))
const ceilings = computed(() => ceilingChoices(form.value.ttl))
</script>

<template>
  <div class="qgf">
    <fieldset v-for="(row, i) in form.rows" :key="row.id" class="qgf-grant">
      <legend class="qn-vh">Grant {{ i + 1 }}</legend>
      <div class="qgf-top">
        <div class="qn-seg" role="group" :aria-label="`Grant ${i + 1} reaches`">
          <button v-for="k in GRANT_KINDS" :key="k.kind" type="button" :aria-pressed="row.kind === k.kind" @click="setKind(i, k.kind)">{{ k.name }}</button>
        </div>
        <div style="flex: 1 1 0" />
        <button v-if="form.rows.length > 1" type="button" class="qn-btn3 qgf-x" :aria-label="`Remove grant ${i + 1}`" @click="remove(i)">Remove</button>
      </div>
      <div class="qgf-fields">
        <label class="qgf-name">
          <span class="qgf-l">{{ nameLabel(row.kind) }}</span>
          <span class="qn-field"><input v-model="row.name" class="qn-mono" type="text" maxlength="256" autocomplete="off" spellcheck="false" :placeholder="row.kind === 'kv' ? 'devices' : 'telemetry'" /></span>
        </label>
        <label v-for="f in kindOf(row.kind).fields" :key="f">
          <span class="qgf-l">{{ FIELD[f] }}</span>
          <span class="qn-field"><input v-model="row[f]" class="qn-mono" type="text" maxlength="256" autocomplete="off" spellcheck="false" placeholder="any" /></span>
        </label>
      </div>
      <div class="qgf-ops" role="group" :aria-label="`What grant ${i + 1} allows`">
        <span class="qgf-l" aria-hidden="true">May</span>
        <label v-for="o in kindOf(row.kind).ops" :key="o.op" class="qgf-check">
          <input type="checkbox" :checked="row.ops.includes(o.op)" @change="toggleOp(i, o.op, $event.target.checked)" />
          <span>{{ o.can }}</span>
        </label>
      </div>
      <p v-if="problems[i]" class="qgf-hint">{{ problems[i] }}</p>
    </fieldset>
    <button v-if="form.rows.length < MAX_GRANTS" type="button" class="qn-btn3 qgf-add" @click="add">+ Add a grant</button>
    <p class="qn-t2 qgf-help">
      A name, a prefix ending in <span class="qn-mono">*</span>, or <span class="qn-mono">*</span> for any; an empty field is any.
      <span class="qn-mono">{sub}</span> is the client a token is for: with partition <span class="qn-mono">{sub}</span>, each device reaches its own partition and no other.
    </p>
    <p v-if="shared" class="qgf-hint">No grant names <span class="qn-mono">{sub}</span>: every token this key mints reaches all of the above, whichever client holds it.</p>

    <fieldset class="qgf-more">
      <legend class="qgf-l">Limits, requests per second on each node (empty: none)</legend>
      <div class="qgf-fields">
        <label>
          <span class="qgf-l">All its clients together</span>
          <span class="qn-field"><input v-model="form.keyRps" class="qn-num" type="text" inputmode="numeric" autocomplete="off" placeholder="none" /></span>
        </label>
        <label>
          <span class="qgf-l">Each client</span>
          <span class="qn-field"><input v-model="form.subRps" class="qn-num" type="text" inputmode="numeric" autocomplete="off" placeholder="none" /></span>
        </label>
      </div>
      <p v-if="rpsProblem(form.keyRps) || rpsProblem(form.subRps)" class="qgf-hint">{{ rpsProblem(form.keyRps) || rpsProblem(form.subRps) }}</p>
    </fieldset>

    <fieldset class="qgf-more">
      <legend class="qgf-l">The longest token it mints</legend>
      <div class="qn-seg" role="group" aria-label="The longest token it mints">
        <button v-for="c in ceilings" :key="c.s" type="button" :aria-pressed="form.ttl === c.s" @click="form.ttl = c.s">{{ c.name }}</button>
      </div>
    </fieldset>
  </div>
</template>

<style scoped>
.qgf { min-width: 0; }
.qgf-grant { border: 1px solid var(--line); border-radius: var(--r-ctl); margin: 0 0 8px; padding: 10px 12px 8px; min-width: 0; }
/* the tone themes draw no lines: a grant is a well instead */
:global(.qn.tone .qgf-grant) { background: var(--well); }
.qgf-top { display: flex; align-items: center; gap: 8px; }
.qgf-x { height: 26px; }
.qgf-fields { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px 10px; margin-top: 10px; }
.qgf-fields label { display: block; min-width: 0; }
.qgf-name { grid-column: 1 / -1; }
.qgf-l { display: block; margin-bottom: 4px; font-size: 12px; color: var(--text3); }
.qgf-ops { display: flex; flex-wrap: wrap; align-items: center; gap: 4px 16px; margin-top: 10px; }
.qgf-ops .qgf-l { margin: 0; }
.qgf-check { display: inline-flex; align-items: center; gap: 8px; min-height: 28px; }
.qgf-check input { accent-color: var(--text); width: 15px; height: 15px; margin: 0; }
.qgf-hint { margin: 6px 0 2px; font-size: 12px; color: var(--text2); }
.qgf-add { margin-left: -8px; }
.qgf-help { margin-top: 6px; font-size: 12px; }
.qgf-more { border: 0; margin: 16px 0 0; padding: 0; min-width: 0; }
.qgf-more legend { padding: 0; }
.qgf-more .qn-seg { margin-top: 2px; }
</style>
