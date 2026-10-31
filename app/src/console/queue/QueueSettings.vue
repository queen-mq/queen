<script setup>
// The queue's options. Save shows what changes and the call before anything
// runs; /configure gets only the options you changed (merge).
import { computed, ref, shallowRef, watch } from 'vue'
import { queues as queuesApi } from '@/api'
import { describeApiError } from '@/api/errors'
import {
  optionsInGroup, toFormValues, readFormValues, configDiff, validate, buildBody,
  formatOptionValue, summariseEffective, describeConfigureRefusal, OPTION_META,
} from '@/composables/useQueueConfig'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import QcDialog from '../QcDialog.vue'
import { toast } from '../toasts'

const props = defineProps({
  queue: { type: String, required: true },
  where: { type: String, default: '' },
})
const { can } = useIdentity()
const qs = useQueuesStore()

const COMMON = optionsInGroup('common')
const ADVANCED = optionsInGroup('advanced')
const current = shallowRef(null) // the queue's options as the broker answers them
const form = ref({})
const error = ref(null)
const more = ref(false)

async function load() {
  try {
    const r = await queuesApi.get(props.queue, { quiet: true })
    current.value = { ...(r.data?.options || {}), namespace: r.data?.namespace ?? '', task: r.data?.task ?? '' }
    form.value = toFormValues(current.value)
    error.value = null
  } catch (e) { error.value = describeApiError(e) }
}
load()
watch(() => props.queue, load)

const edited = computed(() => readFormValues(Object.fromEntries([...COMMON, ...ADVANCED].map((m) => [m.key, form.value[m.key]]))))
const diff = computed(() => (current.value ? configDiff(current.value, edited.value) : {}))
const changed = computed(() => Object.keys(diff.value))
const errors = computed(() => validate(diff.value))
const canEdit = computed(() => can('queueAdmin'))
const was = (key) => {
  const m = OPTION_META[key]
  return current.value ? formatOptionValue(m, current.value[key] ?? m.default) : ''
}
const short = (help) => (help || '').split(/(?<=\.)\s/)[0]

const asking = ref(false)
const busy = ref(false)
const body = computed(() => buildBody({ queue: props.queue, options: diff.value, mode: 'merge' }))
const call = computed(() => `POST /api/v1/configure ${JSON.stringify(body.value)}`)
async function save() {
  busy.value = true
  const keys = changed.value
  try {
    const r = await queuesApi.configure(body.value)
    toast(`Saved on ${props.where || 'this cluster'}`, summariseEffective(r.data, keys) || `The broker answered ${r.status || 200}.`)
    asking.value = false
    qs.invalidate?.()
    await load()
  } catch (e) {
    toast('Not saved', describeConfigureRefusal(e) || describeApiError(e), 'red')
  } finally { busy.value = false }
}
function discard() { if (current.value) form.value = toFormValues(current.value) }
</script>

<template>
  <div class="qset">
    <p v-if="error" class="qn-t2" style="padding: 16px 0">The broker did not answer: {{ error }}</p>
    <template v-else-if="current">
      <p v-if="!canEdit" class="qn-t2" style="padding: 6px 0 4px">You can read these; changing them needs the admin role on this cluster.</p>
      <template v-for="(group, gi) in [COMMON, ADVANCED]" :key="gi">
        <button v-if="gi === 1" type="button" class="qn-btn3" style="margin-top: 14px; padding: 0" @click="more = !more">
          {{ more ? 'Fewer options' : `More options (${ADVANCED.length}): retention, encryption, waiting` }}
        </button>
        <template v-if="gi === 0 || more">
          <component :is="m.type === 'bool' ? 'div' : 'label'" v-for="m in group" :key="m.key" class="qset-row" :class="{ changed: m.key in diff }">
            <span class="qset-name">
              <span style="font-weight: 500">{{ m.label }}</span>
              <span class="qn-mono qn-t3" style="font-size: 11.5px; margin-left: 8px">{{ m.key }}</span>
              <span class="qn-t3 qset-help">{{ short(m.help) }}</span>
            </span>
            <span class="qset-input">
              <template v-if="m.type === 'bool'">
                <span class="qn-seg">
                  <button type="button" :aria-pressed="form[m.key] === true" :disabled="!canEdit" @click="form[m.key] = true">On</button>
                  <button type="button" :aria-pressed="form[m.key] === false" :disabled="!canEdit" @click="form[m.key] = false">Off</button>
                </span>
              </template>
              <span v-else class="qn-field" style="width: 112px"><input v-model="form[m.key]" :disabled="!canEdit" :inputmode="m.type === 'int' ? 'numeric' : 'text'" class="qn-num" type="text" :placeholder="String(m.default ?? '')" /></span>
              <span class="qn-t2" style="width: 26px">{{ m.unit || '' }}</span>
              <span class="qn-t3 qn-num" style="width: 92px; font-size: 12px">{{ m.key in diff ? `was ${was(m.key)}` : '' }}</span>
            </span>
            <span v-if="errors[m.key]" class="red qset-err">{{ errors[m.key] }}</span>
          </component>
        </template>
      </template>
      <div v-if="canEdit" style="display: flex; flex-wrap: wrap; align-items: center; gap: 10px 12px; margin-top: 20px">
        <button type="button" class="qn-btn" :disabled="!changed.length || Object.keys(errors).length > 0" @click="asking = true">Save</button>
        <button type="button" class="qn-btn2" :disabled="!changed.length" @click="discard">Discard changes</button>
        <span class="qn-t2" style="font-size: 12px">{{ changed.length ? `${changed.length} change${changed.length === 1 ? '' : 's'}` : 'Nothing changed' }}</span>
      </div>
      <p class="qn-read" style="margin-top: 14px">Read from <span class="qn-mono">GET /api/v1/resources/queues/{{ queue }}</span>. Save sends only what you changed, with <span class="qn-mono">POST /api/v1/configure</span>.</p>
    </template>
    <p v-else class="qn-t2" style="padding: 16px 0">Reading its options…</p>

    <QcDialog :open="asking" :title="`Change ${changed.length === 1 ? 'one option' : `${changed.length} options`} of ${queue}?`" :where="where" :call="call" confirm-label="Save" :busy="busy" @confirm="save" @cancel="asking = false">
      <div class="qset-diff">
        <p class="qn-mono qn-t2" style="padding: 8px 14px; font-size: 12.5px; border-bottom: 1px solid var(--line2)">{{ queue }}</p>
        <p v-for="k in changed" :key="k" class="qn-mono" style="display: flex; align-items: baseline; gap: 12px; padding: 9px 14px; font-size: 13px">
          <span class="qn-t2" style="flex: 1 1 0">{{ k }}</span>
          <span class="qn-t3" style="text-decoration: line-through">{{ was(k) }}</span>
          <span class="qn-t3" aria-hidden="true">→</span>
          <span style="font-weight: 500">{{ diff[k] === null ? `default (${formatOptionValue(OPTION_META[k], OPTION_META[k].default)})` : formatOptionValue(OPTION_META[k], diff[k]) }}</span>
        </p>
      </div>
      <p class="qn-t2" style="margin-top: 12px">Every other option stays as it is. Messages already leased keep their lease.</p>
    </QcDialog>
  </div>
</template>

<style scoped>
.qset { max-width: 820px; margin-top: 4px; }
.qset-row { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: center; gap: 4px 24px; padding: 12px 0; border-bottom: 1px solid var(--line2); }
.qset-row.changed .qset-name > span:first-child { text-decoration: underline; text-decoration-color: var(--text3); text-underline-offset: 4px; }
.qset-name { min-width: 0; }
.qset-help { display: block; font-size: 12px; margin-top: 2px; }
.qset-input { display: inline-flex; align-items: center; gap: 8px; }
.qset-err { grid-column: 1 / -1; font-size: 12px; }
.qset-diff { border: 1px solid var(--line); border-radius: var(--r-card); overflow: hidden; }
</style>
