<template>
  <SubSidebar label="Settings" :current="forQueue || 'Lines'">
    <template #side>
      <SettingsSide :selected="forQueue" :queues="queueRows" :state="state" />
    </template>

    <!-- One header line: the section picked in the list, a few facts, the
         buttons. "Add a queue" is the list's, so it is on every section. -->
    <PaneHead :title="forQueue || 'Lines'" :facts="facts">
      <template v-if="canEdit || forQueue" #actions>
        <AddQueueMenu v-if="canEdit" :names="queueNames" :taken="settings.queues" @pick="editing = $event" @open="openSection($event)" />
        <!-- The tenant's lines: Save only once something was typed. A Save
             that is always there, on a page that is mostly read, reads as
             "there is something to save". -->
        <template v-if="!forQueue">
          <template v-if="canEdit && dirty">
            <button class="btn btn-ghost" :disabled="saving" @click="fill">Discard</button>
            <button
              class="btn btn-primary"
              :disabled="invalid || saving"
              :title="invalid ? 'A line cannot be saved as typed' : undefined"
              @click="submit"
            >{{ saving ? 'Saving…' : 'Save' }}</button>
          </template>
        </template>
        <!-- A queue's entry is edited in its own form and saved by itself: it
             does not wait for the Save of the tenant's lines, and that Save
             does not touch it. -->
        <template v-else>
          <router-link v-if="forQueueExists" :to="queueTabLocation('alerts', forQueue, route)" class="btn btn-ghost" title="This queue's Alerts tab: what needs you on it now, by these lines">Open queue</router-link>
          <template v-if="canEdit">
            <button
              v-if="queueView.hasOwn"
              class="btn btn-ghost"
              :disabled="removing === forQueue"
              title="Drop this queue's own lines: the tenant's judge it again"
              @click="remove(forQueue)"
            >{{ removing === forQueue ? 'Resetting…' : 'Reset' }}</button>
            <button class="btn btn-primary" :disabled="removing === forQueue" @click="editing = forQueue">Edit lines</button>
          </template>
        </template>
      </template>
    </PaneHead>

    <div class="view-container set">
      <!-- Where the document cannot be read or kept, said once. The lines
           below are then the built-in ones, stated and not editable. -->
      <div v-if="state === 'off'" class="status-banner banner-info view-banner">
        <span><strong>Built-in lines only</strong> · {{ offReason }}</span>
      </div>
      <div v-else-if="state === 'error'" class="status-banner banner-bad view-banner">
        <span :title="version ? 'The last document read is in force' : 'The built-in lines are in force'">
          <strong>Could not read the settings</strong> · {{ describeApiError(error) }}
        </span>
      </div>
      <div v-if="conflict" class="status-banner banner-info view-banner">
        <span><strong>Somebody saved first</strong> · the page shows what they saved: make your change again.</span>
      </div>

      <!-- =================== THE TENANT'S LINES =================== -->
      <section v-if="!forQueue" class="card">
        <div v-if="state === 'unread'" class="set-quiet">Reading the settings…</div>
        <form v-else-if="canEdit" class="set-body" @submit.prevent="submit">
          <div v-for="group in groups" :key="group.name" class="set-group">
            <h3 class="set-group-title">{{ group.name }}</h3>
            <LineRow
              v-for="meta in group.lines"
              :key="meta.key"
              v-model="form[meta.key]"
              :meta="meta"
              :fallback="THRESHOLDS[meta.key]"
              :error="errors[meta.key] || ''"
            />
          </div>
          <!-- Enter in a field saves, as in every other form of the console. -->
          <button type="submit" class="sr-only" tabindex="-1">Save</button>
        </form>
        <!-- Who may not move them reads them: the line in force, and whose. -->
        <div v-else class="set-body">
          <div v-for="group in tenantView.groups" :key="group.name" class="set-group">
            <h3 class="set-group-title">{{ group.name }}</h3>
            <div v-for="line in group.rows" :key="line.key" class="set-line" :title="line.help">
              <span class="set-line-label">{{ line.label }}</span>
              <span class="set-line-value" :class="{ own: line.own }">{{ line.text }}</span>
              <span class="set-line-from">{{ line.own ? 'tenant' : 'built-in' }}</span>
            </div>
          </div>
        </div>
      </section>

      <!-- =================== ONE QUEUE ===================
           The lines it is judged by: its own where it has one, the tenant's
           otherwise. Read here; changed in its form (Edit lines). -->
      <section v-else class="card">
        <div class="set-body">
          <div v-for="group in queueView.groups" :key="group.name" class="set-group">
            <h3 class="set-group-title">{{ group.name }}</h3>
            <div v-for="line in group.rows" :key="line.key" class="set-line" :title="line.help">
              <span class="set-line-label">{{ line.label }}</span>
              <span class="set-line-value" :class="{ own: line.own }">{{ line.text }}</span>
              <span class="set-line-from">{{ line.own ? 'this queue' : 'tenant' }}</span>
            </div>
          </div>
          <div class="set-group">
            <h3 class="set-group-title">Readers</h3>
            <div v-for="flag in queueView.flags" :key="flag.key" class="set-line" :title="flag.help">
              <span class="set-line-label">{{ flag.label }}</span>
              <span class="set-line-value" :class="{ own: flag.on }">{{ flag.on ? 'Yes' : 'No' }}</span>
              <span class="set-line-from">{{ flag.on ? 'this queue' : 'default' }}</span>
            </div>
          </div>
        </div>
      </section>

      <QueueLinesModal :open="editing !== null" :queue="editing || ''" @close="editing = null" />
    </div>
  </SubSidebar>
</template>

<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'

import { describeApiError } from '@/api'
import { queueTabLocation, textParam } from '@/composables/navigation'
import { useRefresh } from '@/composables/useRefresh'
import {
  LINES, QUEUE_FLAGS, QUEUE_LINES, SETTINGS_KEY, SETTINGS_NS,
  fromInput, lineGroups, toInput, validateLines, withDefaults, withQueue,
} from '@/composables/settingsDoc'
import { ownCount, ownQueueEntries, queueLinesInForce, tenantLinesInForce } from '@/composables/settingsSide'
import { THRESHOLDS } from '@/composables/useSeverity'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import AddQueueMenu from '@/components/AddQueueMenu.vue'
import LineRow from '@/components/LineRow.vue'
import PaneHead from '@/components/PaneHead.vue'
import QueueLinesModal from '@/components/QueueLinesModal.vue'
import SettingsSide from '@/components/SettingsSide.vue'
import SubSidebar from '@/components/SubSidebar.vue'

const { can } = useIdentity()
const { notifySuccess } = useToast()
const queuesStore = useQueuesStore()
const { settings, lines, version, state, verdict, error, fetchSettings, save } = useSettingsStore()
const route = useRoute()
const router = useRouter()

// The section open, from the list: ?for=<queue>, nothing = the lines. A new
// section is a new place (Back returns); one that no longer exists is
// replaced.
const forQueue = computed(() => textParam(route.query.for) || null)
const openSection = (name, { replace = false } = {}) =>
  router[replace ? 'replace' : 'push']({ query: { ...route.query, for: name || undefined } })

// Saving is the capability that configures a queue. Only on a document that
// was read: a form filled from nothing must not be offered as the one to save.
const canEdit = computed(() => can('queueAdmin') && state.value === 'ready')

const OFF = {
  absent: 'this broker has no KV to keep them in.',
  gated: 'they are kept in KV, which is not in this cluster’s plan.',
  paused: 'they are kept in KV, which is paused on this cell.',
}
const offReason = computed(() => OFF[verdict.value] || OFF.absent)

const groups = lineGroups(LINES)

// ---------------------------------------------------------------------------
// The tenant's lines: every field is the text of its input. `baseline` is the
// form as the stored document fills it, so "dirty" is a comparison, not a flag.
// ---------------------------------------------------------------------------
const form = reactive({})
const baseline = ref('')
const saving = ref(false)
const conflict = ref(false)
const snapshot = () => JSON.stringify(LINES.map((m) => form[m.key]))

function fill() {
  for (const meta of LINES) form[meta.key] = toInput(meta, settings.value.defaults[meta.key])
  baseline.value = snapshot()
}
fill()
const dirty = computed(() => snapshot() !== baseline.value)
// The document moved under a form nobody has touched: show it. A touched form
// is left alone, and its save is refused if it was made on an older version.
watch(settings, () => { if (!dirty.value) fill() })

const parsed = computed(() => Object.fromEntries(LINES.map((m) => [m.key, fromInput(m, form[m.key])])))
const errors = computed(() => validateLines(THRESHOLDS, parsed.value))
const invalid = computed(() => Object.keys(errors.value).length > 0)

async function submit() {
  if (!canEdit.value || !dirty.value || invalid.value || saving.value) return
  saving.value = true
  conflict.value = false
  const defaults = Object.fromEntries(Object.entries(parsed.value).filter(([, v]) => v !== null))
  try {
    const { saved } = await save((raw) => withDefaults(raw, defaults))
    if (saved) notifySuccess('Settings saved')
    else conflict.value = true
    fill()
  } catch {
    // The shell's failure toast carries the reason; the form keeps what was typed.
  } finally {
    saving.value = false
  }
}

// What a reader without the capability sees: the lines in force, and whose.
const tenantView = computed(() => tenantLinesInForce(settings.value.defaults, THRESHOLDS))

// ---------------------------------------------------------------------------
// Queues: the ones that exist are offered by "Add a queue"; a name that does
// not exist yet may be typed, so a queue can have its settings before its
// first push.
// ---------------------------------------------------------------------------
const queueNames = computed(() => queuesStore.queues.value.map((q) => q.name).filter((n) => typeof n === 'string'))
// "Not created yet" only once the queue list was read: before that, every
// queue would be said not to exist.
const queuesRead = computed(() => queuesStore.lastFetched.value > 0 || queueNames.value.length > 0)
const known = computed(() => (queuesRead.value ? new Set(queueNames.value) : null))
const queueRows = computed(() => ownQueueEntries(settings.value.queues, known.value))
const forQueueExists = computed(() => Boolean(forQueue.value) && (!known.value || known.value.has(forQueue.value)))
const queueView = computed(() => queueLinesInForce(
  forQueue.value ? settings.value.queues.get(forQueue.value) : null,
  lines.value,
))

// ---------------------------------------------------------------------------
// The header's facts: the same few words the list's tooltips hold.
// ---------------------------------------------------------------------------
const machineTitle = computed(() => [
  `Stored as one JSON document in this tenant's KV${version.value ? `, version ${version.value}` : ' (none yet: nothing has been moved)'}.`,
  'Every page reads it within a minute of a save; anything that may write that key may move these lines.',
  'Lines not here are limits of the broker itself: quorum, the store’s map gate, the disk gate, a follower’s heartbeat.',
].join('\n'))
const facts = computed(() => {
  if (state.value === 'unread') return []
  if (forQueue.value) {
    const own = ownCount(settings.value.queues.get(forQueue.value))
    return [
      { k: 'Own', v: `${own} of ${QUEUE_LINES.length + QUEUE_FLAGS.length}`, title: 'Lines and flags this queue sets itself; the tenant’s judge the rest' },
      forQueueExists.value ? null : { v: 'Not created yet', title: 'No queue with this name exists on this cluster right now' },
    ].filter(Boolean)
  }
  if (state.value === 'off') return [{ v: 'Built-in', title: `These lines cannot be moved here: ${offReason.value}` }]
  return [
    { k: 'Moved', v: `${tenantView.value.moved} of ${LINES.length}`, title: 'Lines this tenant moved off the built-in table. A blank field keeps the built-in line.' },
    { k: 'KV', v: `${SETTINGS_NS} / ${SETTINGS_KEY}`, title: machineTitle.value },
  ]
})

const editing = ref(null) // the queue whose form is open
// A queue given its own settings from "Add a queue" opens its section once
// its form is saved; a form closed without saving leaves the page as it was.
watch(editing, (now, before) => {
  if (now === null && before && before !== forQueue.value && settings.value.queues.has(before)) openSection(before)
})

const removing = ref(null)
async function remove(name) {
  removing.value = name
  conflict.value = false
  try {
    const { saved } = await save((raw) => withQueue(raw, name, null))
    if (saved) {
      notifySuccess(`${name} is judged by the tenant's lines`)
      // Its section would only say so again: back to the lines.
      if (forQueue.value === name) openSection(null, { replace: true })
    } else conflict.value = true
  } catch {
    // The shell's failure toast carries the reason.
  } finally {
    removing.value = null
  }
}

const load = () => Promise.allSettled([fetchSettings({ force: true }), queuesStore.fetchQueues()])
onMounted(load)
useRefresh(load)
</script>

<style scoped>
/* One section at a time (the list picks it): the lines, or one queue. Short
   rows read best at a measure, not across a wide page: the Alerts tab's. The
   container keeps the page's width, so it starts under the header line. */
.set > * { max-width: 980px; }
.set-quiet { padding: 14px 16px; font-size: 13px; color: var(--text-low); }
.btn:disabled { opacity: .45; cursor: default; }

.set-body { padding: 4px 16px 12px; }
/* A group's name over its rows, as the Alerts tab sets it. */
.set-group-title { margin: 0; padding: 12px 0 4px; font-size: 12px; font-weight: 600; color: var(--text-low); }

/* A line as it is in force: what it is, its value, and whose it is. The
   section's own value in ink, the one under it quiet. */
.set-line {
  display: grid; grid-template-columns: minmax(0, 1fr) 96px 80px;
  column-gap: 16px; align-items: baseline;
  padding: 8px 0; border-top: 1px solid var(--bd-soft);
}
.set-line-label { font-size: 13px; color: var(--text-mid); }
.set-line-value { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); text-align: right; font-variant-numeric: tabular-nums; }
.set-line-value.own { color: var(--text-hi); font-weight: 500; }
.set-line-from { font-size: 12px; color: var(--text-low); }

@media (max-width: 560px) {
  .set-line { grid-template-columns: minmax(0, 1fr) auto 64px; column-gap: 12px; }
}
</style>
