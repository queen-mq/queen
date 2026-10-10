<template>
  <div class="view-container">

    <PageHead title="Settings" sub="The lines this console judges by">
      <!-- Only once something was typed: a Save that is always there, on a
           page that is mostly read, reads as "there is something to save". -->
      <template v-if="canEdit && dirty" #actions>
        <span v-if="invalid" class="set-invalid">A line below cannot be saved as typed</span>
        <button class="btn btn-ghost" :disabled="saving" @click="fill">Discard</button>
        <button class="btn btn-primary" :disabled="invalid || saving" @click="submit">
          {{ saving ? 'Saving…' : 'Save' }}
        </button>
      </template>
    </PageHead>

    <!-- Where the document cannot be read or kept, said once. The lines below
         are then the built-in ones, stated and not editable. -->
    <div v-if="state === 'off'" class="status-banner banner-info view-banner">
      <span><strong>These lines cannot be moved on this cluster</strong> · {{ offReason }} The built-in lines are in force.</span>
    </div>
    <div v-else-if="state === 'error'" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not read the settings</strong> · {{ describeApiError(error) }} ·
        {{ version ? 'the last document read is in force' : 'the built-in lines are in force' }}
      </span>
    </div>
    <div v-if="conflict" class="status-banner banner-info view-banner">
      <span>
        <strong>Somebody saved these settings while you were editing</strong> · the page now shows
        what they saved, and your change was not stored. Make it again on top of theirs.
      </span>
    </div>

    <div class="set-grid">
    <form class="card" @submit.prevent="submit">
      <div class="card-header">
        <h3>Lines</h3>
        <span class="card-sub">for every queue of {{ actingTenantSlug || 'this tenant' }} · a blank field keeps the built-in line</span>
      </div>
      <div class="card-body set-body">
        <section v-for="group in groups" :key="group.name" class="set-group">
          <h4 class="set-group-title">{{ group.name }}</h4>
          <LineRow
            v-for="meta in group.lines"
            :key="meta.key"
            v-model="form[meta.key]"
            :meta="meta"
            :fallback="THRESHOLDS[meta.key]"
            :error="errors[meta.key] || ''"
            :disabled="!canEdit"
          />
        </section>
      </div>
      <!-- Enter in a field saves, as in every other form of the console. -->
      <button type="submit" class="sr-only" tabindex="-1" :disabled="!canEdit">Save</button>
    </form>

    <div class="set-side">
    <div class="card">
      <div class="card-header">
        <h3>Queues with their own settings</h3>
        <span class="card-sub">{{ queueRows.length ? `${queueRows.length} of ${formatNumber(queueNames.length)}` : 'none' }}</span>
      </div>
      <!-- A queue's entry is edited in its own form (the one its page opens)
           and saved by itself: it does not wait for the Save of the lines on
           the left, and that Save does not touch it. -->
      <ul v-if="queueRows.length" class="set-queues">
        <li v-for="q in queueRows" :key="q.name">
          <div class="set-queue">
            <router-link :to="`/queues/${encodeURIComponent(q.name)}`">{{ q.name }}</router-link>
            <span v-if="!known.has(q.name)" class="chip chip-mute" title="No queue with this name exists on this cluster right now">not created yet</span>
            <span class="set-queue-own">{{ q.summary.join(' · ') }}</span>
          </div>
          <div v-if="canEdit" class="set-queue-act">
            <button type="button" class="btn btn-ghost" :disabled="removing === q.name" @click="editing = q.name">Edit</button>
            <button type="button" class="btn btn-ghost" :disabled="removing === q.name" @click="remove(q.name)">
              {{ removing === q.name ? 'Removing…' : 'Remove' }}
            </button>
          </div>
        </li>
      </ul>
      <div v-else class="set-none">
        Every queue is judged by the tenant's lines. Give one its own when it is expected to behave
        differently: a nightly batch that runs behind, a queue nothing reads until later.
      </div>
      <!-- Picking a queue opens its form; there is no second button to press.
           A name that does not exist yet is taken with Enter, never on blur,
           or half a typed name would open a form. -->
      <div v-if="canEdit" class="set-add">
        <Autocomplete
          id="set-add-queue"
          v-model="adding"
          :options="addable"
          label="Add a queue"
          placeholder="Add a queue…"
          allow-custom
          custom-hint="press Enter to add this name anyway"
        />
      </div>
    </div>

    <p class="set-machine">
      Stored as one JSON document in this tenant's KV, namespace
      <code>{{ SETTINGS_NS }}</code>, key <code>{{ SETTINGS_KEY }}</code><template v-if="version">, version {{ version }}</template><template v-else-if="state === 'ready'">; there is none yet, so nothing has been moved</template>.
      Every page reads it within a minute of a save, and anything that may write that key may move these lines.
      The lines that are not here are limits of the broker itself (quorum, the store's map gate, the disk gate, a follower's heartbeat).
    </p>
    </div>
    </div>

    <QueueLinesModal :open="editing !== null" :queue="editing || ''" @close="editing = null" />
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref, watch } from 'vue'

import { describeApiError } from '@/api'
import { formatNumber } from '@/composables/useApi'
import { useRefresh } from '@/composables/useRefresh'
import {
  LINES, SETTINGS_KEY, SETTINGS_NS,
  fromInput, lineGroups, summarizeQueue, toInput, validateLines, withDefaults, withQueue,
} from '@/composables/settingsDoc'
import { THRESHOLDS } from '@/composables/useSeverity'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useSettingsStore } from '@/stores/settingsStore'
import Autocomplete from '@/components/Autocomplete.vue'
import LineRow from '@/components/LineRow.vue'
import PageHead from '@/components/PageHead.vue'
import QueueLinesModal from '@/components/QueueLinesModal.vue'

const { can, actingTenantSlug } = useIdentity()
const { notifySuccess } = useToast()
const queuesStore = useQueuesStore()
const { settings, version, state, verdict, error, fetchSettings, save } = useSettingsStore()

// Saving is the capability that configures a queue. Only on a document that
// was read: a form filled from nothing must not be offered as the one to save.
const canEdit = computed(() => can('queueAdmin') && state.value === 'ready')

const OFF = {
  absent: 'This broker has no KV to keep them in.',
  gated: 'They are kept in KV, which is not in this cluster’s plan.',
  paused: 'They are kept in KV, which is paused on this cell.',
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

// ---------------------------------------------------------------------------
// Queues: the ones that exist are offered; a name that does not exist yet may
// be typed, so a queue can have its settings before its first push.
// ---------------------------------------------------------------------------
const queueRows = computed(() => [...settings.value.queues]
  .sort(([a], [b]) => a.localeCompare(b))
  .map(([name, own]) => ({ name, summary: summarizeQueue(own) })))
const queueNames = computed(() => queuesStore.queues.value.map((q) => q.name).filter((n) => typeof n === 'string'))
const known = computed(() => new Set(queueNames.value))
const addable = computed(() => queueNames.value
  .filter((n) => !settings.value.queues.has(n))
  .sort((a, b) => a.localeCompare(b)))

const editing = ref(null) // the queue whose form is open
const adding = ref('')
watch(adding, (picked) => {
  const name = picked.trim()
  if (!name) return
  editing.value = name
  adding.value = ''
})

const removing = ref(null)
async function remove(name) {
  removing.value = name
  conflict.value = false
  try {
    const { saved } = await save((raw) => withQueue(raw, name, null))
    if (saved) notifySuccess(`${name} is judged by the tenant's lines`)
    else conflict.value = true
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
/* The tenant's lines beside the queues that have their own: the second is
   what this page is opened for most, so it must not sit under ten rows. */
.set-grid { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; align-items: start; }
.set-grid > .card, .set-side > .card { margin: 0; }
@media (max-width: 1100px) { .set-grid { grid-template-columns: minmax(0, 1fr); } }
.set-invalid { color: var(--ember-400); font-size: 12px; }
.btn:disabled { opacity: .45; cursor: default; }

.set-body { padding-top: 4px; padding-bottom: 6px; }
.set-group + .set-group { margin-top: 18px; }
.set-group-title { margin: 12px 0 8px; font-size: 13px; font-weight: 600; color: var(--text-hi); }

/* One queue per row: its name, what it has of its own, and the two things to
   do with it. */
.set-queues { list-style: none; margin: 0; padding: 0; }
.set-queues li {
  display: flex; align-items: center; justify-content: space-between; gap: 12px;
  padding: 10px 8px 10px 16px; border-top: 1px solid var(--bd-soft);
}
.set-queues li:first-child { border-top: 0; }
.set-queue { display: flex; flex-wrap: wrap; align-items: baseline; gap: 2px 10px; min-width: 0; }
.set-queue a { color: var(--text-hi); font-size: 13px; font-weight: 500; overflow-wrap: anywhere; }
.set-queue-own { flex-basis: 100%; font-size: 12px; line-height: 1.5; color: var(--text-low); font-variant-numeric: tabular-nums; }
.set-queue-act { display: flex; flex: none; }
.set-none { padding: 18px 16px; color: var(--text-low); font-size: 13px; line-height: 1.55; max-width: 72ch; }
.set-add { padding: 12px 16px; border-top: 1px solid var(--bd-soft); }
.set-add > :first-child { max-width: 320px; }

.set-machine { margin: 14px 2px 0; max-width: 96ch; color: var(--text-low); font-size: 12px; line-height: 1.6; }
.set-machine code { color: var(--text-mid); }

@media (max-width: 560px) {
  .set-queues li { flex-direction: column; align-items: stretch; padding-right: 16px; }
  .set-queue-act { margin-left: -10px; }
}
</style>
