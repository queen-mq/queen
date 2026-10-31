<script setup>
import QcJson from '../QcJson.vue'
// Settings › Alerts: the lines the console judges by (the tenant's), then the
// queues with lines of their own, then one Save for the whole document. Save
// shows what changes and the PUT before anything runs. `a` is the page's
// useAlertLines(), made reactive, so a page switch keeps what was typed.
import { computed, nextTick, watch } from 'vue'
import { useRoute } from 'vue-router'
import QcDialog from '../QcDialog.vue'
import { DOC_PATH, parts } from './alertLines'
import { when } from '../useSettingsPage'

const props = defineProps({
  a: { type: Object, required: true },
  where: { type: String, default: '' },
})
const route = useRoute()
const enc = encodeURIComponent

// ?queue= : the queue to look at (its block lit, scrolled to).
const focus = computed(() => (typeof route.query.queue === 'string' ? route.query.queue : ''))
const focusHasLines = computed(() => !focus.value || props.a.blocks.some((b) => b.name === focus.value))
// Scrolled to once, when its block is first on screen (the lines arrive after the first read).
let scrolledTo = ''
watch(() => (focus.value && props.a.state === 'ready' ? focus.value : ''), async (q) => {
  if (!q || scrolledTo === q) return
  scrolledTo = q
  await nextTick()
  const el = document.querySelector(`[data-queue="${CSS.escape(q)}"]`) || (focusHasLines.value ? null : document.getElementById('qa-own'))
  el?.scrollIntoView({ block: 'center' })
}, { immediate: true })

const OFF = {
  absent: 'this broker has no KV to keep them in.',
  gated: 'they are kept in KV, which is not in this cluster’s plan.',
  paused: 'they are kept in KV, which is paused on this cell.',
}
const saved = computed(() => {
  const d = props.a.doc
  if (!d) return ''
  if (!d.version) return 'no document yet, so every line is the built-in one'
  return d.updatedAt ? `last saved ${when(d.updatedAt)}` : ''
})
const title = computed(() => (props.a.count === 1 ? 'Save one change?' : `Save ${props.a.count} changes?`))
function enter(e) { e.target.blur(); props.a.openSave() }
</script>

<template>
  <div class="qa">
    <!-- the tenant's lines -->
    <h2 id="qa-lines" class="qa-h">The lines the console judges by</h2>
    <p class="qn-t2 qa-lead">Past one of these lines, a queue or a node goes on the Overview. {{ a.canEdit ? 'An empty field uses the built-in line.' : 'The grey ones are built in.' }}</p>
    <p v-if="a.state === 'off'" class="qa-note">These lines cannot be moved here: {{ OFF[a.verdict] || OFF.absent }} The built-in ones are in force.</p>
    <p v-else-if="a.state === 'error'" class="qa-note">The broker did not answer: {{ a.error }}. The built-in lines are in force.</p>
    <p v-else-if="a.error" class="qa-note">The broker did not answer the last read: {{ a.error }}. What you see was read before.</p>
    <p v-else-if="a.state === 'ready' && !a.canEdit" class="qa-note">You can read these; moving them needs the admin role on {{ where }}.</p>

    <p v-if="a.state === 'unread'" class="qn-t2" style="padding: 16px 0">Reading the lines…</p>
    <div v-else role="table" aria-labelledby="qa-lines" class="qa-table qa-cols">
      <div class="qn-tr head" role="row">
        <span role="columnheader">What</span><span role="columnheader">Line</span><span role="columnheader" class="qa-keycol">Key</span>
      </div>
      <template v-for="g in a.groups" :key="g.id">
        <div class="qn-tr qa-group" role="row">
          <span role="cell" class="wrap"><span style="font-weight: 500">{{ g.title }}</span><span class="qn-t3"> · {{ g.about }}</span></span>
        </div>
        <div v-for="row in g.rows" :key="row.key" class="qn-tr qa-row" role="row">
          <span role="cell" class="qa-what">
            {{ row.words.what }}
            <span class="qn-mono qn-t3 qa-keyin">{{ row.key }}</span>
          </span>
          <span role="cell" class="qa-line">
            <span class="qn-t2 qa-leadw">{{ row.words.lead }}</span>
            <label v-if="a.canEdit" class="qn-field qa-in">
              <span class="qn-vh">{{ g.title }}: {{ row.words.what }}</span>
              <input
                :value="row.text" :placeholder="row.builtIn" class="qn-num" type="text"
                autocomplete="off" spellcheck="false"
                @input="a.setTenant(row.key, $event.target.value)" @blur="a.tidy('', row.key)" @keydown.enter.prevent="enter"
              />
            </label>
            <span v-else class="qn-num" :class="row.own ? '' : 'qn-t3'">{{ row.inForce }}</span>
            <span v-if="row.changed" class="qn-t3 qa-was">was {{ row.was }}</span>
          </span>
          <span role="cell" class="qn-mono qn-t3 qa-key qa-keycol">{{ row.key }}</span>
          <span v-if="row.error" role="cell" class="red qa-err wrap">{{ row.error }}</span>
        </div>
      </template>
    </div>
    <p v-if="a.state !== 'unread'" class="qn-read" style="margin-top: 10px">
      Write a number and its unit, like 90 s, 5 min, 2 h or 5%.
      Read from <span class="qn-mono">GET {{ DOC_PATH }}</span><template v-if="saved">, {{ saved }}</template>.
    </p>

    <!-- the queues with lines of their own -->
    <template v-if="a.state === 'ready'">
      <div class="qa-headrow">
        <h2 id="qa-own" class="qa-h">Lines for one queue</h2>
        <div style="flex: 1 1 0" />
        <button v-if="a.canEdit" type="button" class="qn-btn2" aria-haspopup="dialog" @click="a.openAdd(focusHasLines ? '' : focus)">Add a line</button>
      </div>
      <p class="qn-t2 qa-lead">A queue’s own line replaces the one above, for that queue only. Remove it to use the line above again.</p>
      <p v-if="!focusHasLines" class="qa-note">
        <span class="qn-mono">{{ focus }}</span> has no lines of its own: the lines above judge it.
        <button v-if="a.canEdit" type="button" class="qn-link" @click="a.openAdd(focus)">Add one</button>
      </p>

      <div v-if="a.blocks.length" role="table" aria-labelledby="qa-own" class="qa-table qa-cols">
        <div class="qn-tr head" role="row">
          <span role="columnheader">What</span><span role="columnheader">Line</span><span role="columnheader" class="qa-keycol">Key</span><span role="columnheader"><span class="qn-vh">Remove</span></span>
        </div>
        <template v-for="b in a.blocks" :key="b.name">
          <div class="qn-tr qa-qhead" :class="{ on: focus === b.name }" role="row" :data-queue="b.name">
            <span role="cell" class="qa-qcell">
              <span v-if="b.missing" class="qn-mono qa-qname">{{ b.name }}</span>
              <RouterLink v-else :to="`/queues/${enc(b.name)}`" class="qn-mono qa-qname">{{ b.name }}</RouterLink>
              <span v-if="b.missing" class="qn-t3">not on {{ where }}</span>
              <span style="flex: 1 1 0" />
              <span v-if="b.now.text" class="qa-now" :class="b.now.tone || 'qn-t3'">
                <span v-if="b.now.tone" class="qn-mark" :class="b.now.tone" aria-hidden="true" />{{ b.now.text }}
              </span>
            </span>
          </div>
          <div v-for="row in b.rows" :key="row.key" class="qn-tr qa-row" :class="{ removed: row.removed }" role="row">
            <span role="cell" class="qa-what">
              {{ row.words.label }}
              <span class="qn-mono qn-t3 qa-keyin">{{ row.key }}</span>
            </span>
            <span role="cell" class="qa-line">
              <template v-if="row.kind === 'line'">
                <span class="qn-t2 qa-leadw">{{ row.words.lead }}</span>
                <label v-if="a.canEdit" class="qn-field qa-in">
                  <span class="qn-vh">{{ b.name }}: {{ row.words.label }}</span>
                  <input
                    :value="row.text" :placeholder="row.inherit" class="qn-num" type="text"
                    autocomplete="off" spellcheck="false"
                    @input="a.setQueue(b.name, row.key, $event.target.value)" @blur="a.tidy(b.name, row.key)" @keydown.enter.prevent="enter"
                  />
                </label>
                <span v-else class="qn-num">{{ row.text || row.inherit }}</span>
                <span v-if="row.removed" class="qn-t3 qa-was">removed, was {{ row.was }}</span>
                <span v-else-if="row.added" class="qn-t3 qa-was">new</span>
                <span v-else-if="row.changed" class="qn-t3 qa-was">was {{ row.was }}</span>
              </template>
              <template v-else>
                <span class="qn-seg" role="group" :aria-label="`${b.name}: ${row.words.label}`" :title="row.help">
                  <button type="button" :aria-pressed="row.on" :disabled="!a.canEdit" @click="a.setQueue(b.name, row.key, true)">Yes</button>
                  <button type="button" :aria-pressed="!row.on" :disabled="!a.canEdit" @click="a.setQueue(b.name, row.key, false)">No</button>
                </span>
                <span v-if="row.added" class="qn-t3 qa-was">new</span>
                <span v-else-if="row.changed" class="qn-t3 qa-was">was yes</span>
              </template>
            </span>
            <span role="cell" class="qn-mono qn-t3 qa-key qa-keycol">{{ row.key }}</span>
            <span role="cell" class="r">
              <template v-if="a.canEdit && row.kind === 'line'">
                <button v-if="row.removed" type="button" class="qn-btn3 qa-act" :aria-label="`Keep ${row.words.label} of ${b.name}`" @click="a.keep(b.name, row.key)">Keep</button>
                <button v-else type="button" class="qn-btn3 qa-act" :aria-label="`Remove ${row.words.label} of ${b.name}`" @click="a.remove(b.name, row)">Remove</button>
              </template>
            </span>
            <span v-if="row.error" role="cell" class="red qa-err wrap">{{ row.error }}</span>
          </div>
        </template>
      </div>
      <p v-else class="qn-t2" style="padding: 12px 0 4px">No queue has lines of its own: the lines above judge every queue.</p>
      <p v-if="a.blocks.length" class="qn-read" style="margin-top: 10px">
        Where each queue stands now is read from <span class="qn-mono">GET /api/v1/consumer-groups</span> and <span class="qn-mono">GET /api/v1/analytics/workload</span>, by the saved lines.
      </p>

      <!-- one Save for the whole document -->
      <div v-if="a.canEdit" class="qa-bar">
        <span class="qa-barnote" :class="a.count || a.bad ? '' : 'qn-t3'">{{ a.note }}</span>
        <span class="qa-barbtns">
          <button v-if="a.typedAnything" type="button" class="qn-btn2" @click="a.discard()">Discard changes</button>
          <button type="button" class="qn-btn" aria-haspopup="dialog" :disabled="!a.count || a.bad > 0" @click="a.openSave()">Save</button>
        </span>
      </div>
    </template>

    <!-- Save: what changes, the call -->
    <QcDialog :open="a.asking" :title="title" :where="where" :call="a.put.call" confirm-label="Save" :busy="a.busy" @confirm="a.save()" @cancel="a.asking = false">
      <div class="qa-diff">
        <template v-for="g in a.diff" :key="g.scope || '-'">
          <p class="qa-diffhead" :class="g.scope ? 'qn-mono' : ''">{{ g.scope || 'Every queue and node' }}</p>
          <p v-for="row in g.rows" :key="row.key" class="qn-mono qa-diffrow">
            <span class="qn-t2 qa-diffkey">{{ row.key }}</span>
            <span class="qa-diffval">
              <span class="qn-t3" style="text-decoration: line-through">{{ row.was }}</span>
              <span class="qn-t3" aria-hidden="true">→</span>
              <span style="font-weight: 500">{{ row.now }}</span>
            </span>
          </p>
        </template>
        <p class="qn-t3 qa-diffnote">Everything else in the document stays as it is.</p>
      </div>
      <p v-for="(m, i) in a.moves" :key="i" class="qn-t2" style="margin-top: 12px">
        <template v-for="(p, j) in parts(m)" :key="j"><span :class="p.mono ? 'qn-mono' : ''">{{ p.t }}</span></template>
      </p>
      <p class="qn-t2" style="margin-top: 12px">Every page of the console judges by the saved lines from its next read, within a minute.</p>
      <details v-if="a.put.long" style="margin-top: 12px">
        <summary class="qn-t3" style="cursor: pointer; font-size: 12px">The whole document it stores</summary>
        <QcJson :value="a.body.value" style="margin-top: 8px" />
      </details>
    </QcDialog>

    <!-- Add a line: nothing is sent until Save -->
    <QcDialog :open="a.adding.open" title="Add a line for one queue" :where="where" confirm-label="Add the line" :blocked="a.addCheck.blocked" @confirm="a.confirmAdd()" @cancel="a.adding.open = false">
      <form class="qa-form" @submit.prevent="a.confirmAdd()">
        <label class="qa-label">
          <span class="qn-t3">Queue</span>
          <span class="qn-field qa-pickq"><input v-model="a.adding.queue" list="qa-queues" class="qn-mono" type="text" autocomplete="off" spellcheck="false" placeholder="type or pick its name" /></span>
        </label>
        <datalist id="qa-queues"><option v-for="n in a.queueChoices" :key="n" :value="n" /></datalist>
        <label class="qa-label">
          <span class="qn-t3">Line</span>
          <span class="qn-field qa-select">
            <select v-model="a.adding.key">
              <option v-for="o in a.addOptions" :key="o.value" :value="o.value">{{ o.label }}</option>
            </select>
            <svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><polyline points="2,3.5 5,6.5 8,3.5" style="fill: none; stroke: currentColor; stroke-width: 1.3" /></svg>
          </span>
        </label>
        <label v-if="!a.addCheck.isFlag" class="qa-label">
          <span class="qn-t3">Value</span>
          <span style="display: flex; align-items: center; gap: 10px">
            <span class="qn-field" style="width: 140px"><input v-model="a.adding.text" class="qn-num" type="text" autocomplete="off" spellcheck="false" /></span>
            <span class="qn-t3" style="font-size: 12px">the tenant’s line is {{ a.addCheck.inherit }}</span>
          </span>
        </label>
        <p v-else class="qn-t2" style="margin-top: 14px">The queue then never goes on the Overview for having no reader.</p>
        <p class="qa-hint" :class="a.addCheck.tone">{{ a.addCheck.hint }}</p>
        <p class="qn-t3" style="font-size: 12px">Nothing is sent until you save the page.</p>
        <button type="submit" class="qn-vh" tabindex="-1">Add the line</button>
      </form>
    </QcDialog>
  </div>
</template>

<style scoped>
.qa { min-width: 0; max-width: 880px; }
.qa-h { font-size: 17px; font-weight: 500; }
.qa-lead { margin-top: 6px; }
.qa-note { margin-top: 12px; padding: 10px 12px; border-radius: var(--r-ctl); background: var(--bg2); color: var(--text2); }
.qa-headrow { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-top: 44px; }

/* tables: the same three columns on both, a fourth for Remove */
.qa-table { margin-top: 16px; }
.qa-cols { --cols: minmax(0, 1fr) 280px 170px 76px; }
.qn-tr.qa-group { min-height: 0; padding: 18px 0 8px; }
.qn-tr.qa-group > span { grid-column: 1 / -1; font-size: 13px; }
.qn-tr.qa-row { min-height: 44px; }
.qa-what { color: var(--text); }
.qa-keyin { display: none; font-size: 11px; }
.qa-line { display: flex; align-items: center; gap: 8px; }
.qa-leadw { width: 34px; flex: 0 0 auto; }
.qa-in { width: 120px; height: 28px; padding: 0 8px; flex: 0 0 auto; }
.qa-was { font-size: 12px; }
.qa-key { font-size: 11.5px; }
.qn-tr > .qa-err { grid-column: 1 / -1; font-size: 12px; padding: 0 0 10px; margin-top: -4px; }
.qa-row.removed .qa-what { color: var(--text3); text-decoration: line-through; text-decoration-color: var(--text3); }
.qa-act { height: 28px; }
.qn-tr.qa-qhead { min-height: 0; padding: 18px 0 8px; }
.qn-tr.qa-qhead > .qa-qcell { grid-column: 1 / -1; display: flex; align-items: baseline; gap: 6px 12px; flex-wrap: wrap; }
.qa-qname { font-weight: 500; overflow-wrap: anywhere; }
.qa-now { display: inline-flex; align-items: center; gap: 6px; font-size: 12px; }
.qn-tr.qa-qhead.on { background: var(--bg3); margin: 0 -8px; padding-left: 8px; padding-right: 8px; border-radius: var(--r-ctl); }

/* Save stays in reach while the page scrolls (the shell scrolls inside itself). */
.qa-bar {
  position: sticky; bottom: 0; z-index: 5;
  display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px;
  margin-top: 32px; padding: 14px 0; border-top: 1px solid var(--line); background: var(--bg);
}
:global(.qn.tone .qa-bar) { border-top-color: var(--bg3); }

/* the dialogs */
.qa-diff { border: 1px solid var(--line); border-radius: var(--r-card); overflow: hidden; }
:global(.qn.tone .qa-diff) { background: var(--well); }
.qa-diffhead { padding: 8px 14px; font-size: 12.5px; color: var(--text2); border-bottom: 1px solid var(--line2); }
.qa-diffhead:not(:first-child) { border-top: 1px solid var(--line2); }
.qa-diffrow { display: flex; flex-wrap: wrap; align-items: baseline; gap: 4px 12px; padding: 9px 14px; font-size: 13px; }
.qa-diffkey { flex: 1 1 150px; min-width: 0; overflow-wrap: anywhere; }
.qa-diffval { display: inline-flex; flex-wrap: wrap; align-items: baseline; gap: 4px 12px; margin-left: auto; }
.qa-diffnote { padding: 10px 14px; font-size: 12px; border-top: 1px solid var(--line2); }
.qa-form { display: block; }
.qa-label { display: block; margin-top: 14px; }
.qa-label:first-child { margin-top: 0; }
.qa-label > .qn-t3 { display: block; font-size: 12px; margin-bottom: 6px; }
.qa-select { position: relative; padding-right: 8px; color: var(--text3); }
.qa-select select { flex: 1 1 0; min-width: 0; appearance: none; -webkit-appearance: none; border: 0; outline: 0; background: transparent; color: var(--text); cursor: pointer; }
.qa-select select option { background: var(--pop); color: var(--text); }
.qa-hint { min-height: 18px; margin: 10px 0 6px; font-size: 12px; color: var(--text2); }
.qa-pickq input::-webkit-calendar-picker-indicator { display: none !important; }
.qa-barnote { flex: 1 1 auto; }
.qa-barbtns { display: inline-flex; gap: 8px; margin-left: auto; }

@media (max-width: 900px) {
  .qa-cols { --cols: minmax(0, 1fr) 280px 72px; }
  .qa-keycol { display: none; }
  .qa-keyin { display: block; }
}
@media (max-width: 640px) {
  .qa-cols { --cols: minmax(0, 1fr) auto; }
  .qa-table > .qn-tr.head { display: none; }
  .qn-tr.qa-row { padding: 8px 0; row-gap: 6px; }
  .qa-line { grid-column: 1 / 2; grid-row: 2; }
  .qn-tr.qa-row > .r { grid-column: 2; grid-row: 1 / 3; }
  .qa-headrow .qn-btn2 { margin-left: auto; }
  .qa-barnote { flex-basis: 100%; }
}
</style>
