<template>
  <Teleport to="body">
    <div v-if="open" class="modal-backdrop" @click.self="close">
      <form class="card modal-card ql-card" @submit.prevent="submit">
        <div class="card-header">
          <h3>Queue settings</h3>
          <span class="card-sub font-mono">{{ queue }}</span>
        </div>
        <div class="card-body ql-body">
          <div v-if="conflict" class="panel-err">
            Somebody saved the settings while this was open. The fields now show what is stored;
            make your change again.
          </div>
          <p class="ql-lead">
            The lines the console judges this queue by. A blank field keeps the tenant's line.
          </p>
          <section v-for="group in groups" :key="group.name" class="ql-group">
            <h4 class="ql-group-title">{{ group.name }}</h4>
            <LineRow
              v-for="meta in group.lines"
              :key="meta.key"
              v-model="form[meta.key]"
              :meta="meta"
              :fallback="lines[meta.key]"
              fallback-name="tenant"
              :error="errors[meta.key] || ''"
            />
          </section>
          <section class="ql-group">
            <h4 class="ql-group-title">Readers</h4>
            <!-- Not a <label>: the switch is two buttons, and a label around
                 them would forward every click to the first. -->
            <div v-for="flag in QUEUE_FLAGS" :key="flag.key" class="ql-flag">
              <span class="ql-flag-label">{{ flag.label }}</span>
              <span class="ql-flag-help">{{ flag.help }}</span>
              <span class="seg ql-flag-control" role="group" :aria-label="flag.label">
                <button type="button" :class="{ on: form[flag.key] === true }" :aria-pressed="form[flag.key] === true ? 'true' : 'false'" @click="form[flag.key] = true">Yes</button>
                <button type="button" :class="{ on: form[flag.key] !== true }" :aria-pressed="form[flag.key] !== true ? 'true' : 'false'" @click="form[flag.key] = false">No</button>
              </span>
            </div>
          </section>
        </div>
        <div class="modal-foot">
          <button v-if="hasOwn" type="button" class="btn btn-ghost ql-clear" @click="clear">Use the tenant's lines</button>
          <button type="button" class="btn btn-ghost" @click="close">Cancel</button>
          <button type="submit" class="btn btn-primary" :disabled="!dirty || invalid || saving">
            {{ saving ? 'Saving…' : 'Save' }}
          </button>
        </div>
      </form>
    </div>
  </Teleport>
</template>

<script setup>
// One queue's own lines and flags, edited where the queue is looked at or from
// the Settings page. The same document either way (stores/settingsStore.js);
// this form replaces this queue's entry in it and touches nothing else.
import { computed, reactive, ref, watch } from 'vue'

import LineRow from '@/components/LineRow.vue'
import {
  QUEUE_FLAGS, QUEUE_LINES, fromInput, lineGroups, toInput, validateLines, withQueue,
} from '@/composables/settingsDoc'
import { useToast } from '@/composables/useToast'
import { useSettingsStore } from '@/stores/settingsStore'

const props = defineProps({
  open: { type: Boolean, default: false },
  queue: { type: String, required: true },
})
const emit = defineEmits(['close'])

const { notifySuccess } = useToast()
const { settings, lines, save } = useSettingsStore()

const groups = lineGroups(QUEUE_LINES)
const form = reactive({})
const baseline = ref('')
const saving = ref(false)
const conflict = ref(false)
const snapshot = () => JSON.stringify([...QUEUE_LINES, ...QUEUE_FLAGS].map((m) => form[m.key]))

function fill() {
  const own = settings.value.queues.get(props.queue) || {}
  for (const meta of QUEUE_LINES) form[meta.key] = toInput(meta, own[meta.key])
  for (const flag of QUEUE_FLAGS) form[flag.key] = own[flag.key] === true
  baseline.value = snapshot()
}
watch(() => [props.open, props.queue], ([open]) => { if (open) { conflict.value = false; fill() } }, { immediate: true })

const parsed = computed(() => Object.fromEntries(QUEUE_LINES.map((m) => [m.key, fromInput(m, form[m.key])])))
const errors = computed(() => validateLines(lines.value, parsed.value, { perQueue: true }))
const invalid = computed(() => Object.keys(errors.value).length > 0)
const dirty = computed(() => snapshot() !== baseline.value)
const hasOwn = computed(() =>
  QUEUE_LINES.some((m) => (form[m.key] || '').trim() !== '') || QUEUE_FLAGS.some((f) => form[f.key] === true))

function clear() {
  for (const meta of QUEUE_LINES) form[meta.key] = ''
  for (const flag of QUEUE_FLAGS) form[flag.key] = false
}

function close() {
  if (!saving.value) emit('close')
}

async function submit() {
  if (!dirty.value || invalid.value || saving.value) return
  saving.value = true
  const own = Object.fromEntries(Object.entries(parsed.value).filter(([, v]) => v !== null))
  for (const flag of QUEUE_FLAGS) if (form[flag.key] === true) own[flag.key] = true
  try {
    const { saved } = await save((raw) => withQueue(raw, props.queue, own))
    if (!saved) {
      conflict.value = true
      fill()
      return
    }
    notifySuccess(Object.keys(own).length
      ? `${props.queue} has its own settings`
      : `${props.queue} is judged by the tenant's lines`)
    emit('close')
  } catch {
    // The shell's failure toast carries the reason; the form keeps what was typed.
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.ql-card { max-width: 600px; }
.ql-body { display: grid; }
.ql-body .panel-err { margin-bottom: 12px; }
.ql-lead { margin: 0 0 4px; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.ql-group-title { margin: 14px 0 8px; font-size: 13px; font-weight: 600; color: var(--text-hi); }
/* The same row as a line: what it is on the left, its control on the right. */
.ql-flag {
  display: grid; grid-template-columns: minmax(0, 1fr) auto;
  column-gap: 28px; row-gap: 4px; align-items: start;
  padding: 14px 0; border-top: 1px solid var(--bd-soft);
}
.ql-flag-label { grid-column: 1; font-size: 13px; font-weight: 500; color: var(--text-hi); }
.ql-flag-help { grid-column: 1; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.ql-flag-control { grid-column: 2; grid-row: 1 / span 2; }
.ql-clear { margin-right: auto; }
/* The form is taller than a small window: Save stays in reach while it scrolls. */
.ql-card .modal-foot { position: sticky; bottom: 0; background: var(--ink-2); }
.btn:disabled { opacity: .45; cursor: default; }
@media (max-width: 560px) {
  .ql-flag { grid-template-columns: minmax(0, 1fr); }
  .ql-flag-control { grid-column: 1; grid-row: auto; justify-self: start; }
}
</style>
