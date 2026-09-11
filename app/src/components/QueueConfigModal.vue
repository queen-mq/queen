<template>
  <Teleport to="body">
    <div v-if="open" class="modal-backdrop qc-over" @click.self="emit('close')">
      <form class="card modal-card qc-card" @submit.prevent="submit">
        <div class="card-header">
          <!-- `isEditing`, not `isEdit`: typing the name of a queue that already
               exists turns this form into an edit of that queue — the body says
               so, the submit button says so, and a heading still reading
               "Create queue" over both was the one part of the form claiming a
               creation that cannot happen. -->
          <h3>{{ isEditing ? 'Edit configuration' : 'Create queue' }}</h3>
          <span v-if="isEdit" class="card-sub font-mono">{{ queue }}</span>
        </div>

        <div class="card-body qc-form">
          <!-- Prefill in flight. An edit form that paints its fields before the
               queue's own values arrive would show every option at its default
               for a moment, and a fast operator would submit that. -->
          <template v-if="isEdit && loadingCurrent">
            <div class="skeleton" style="height:22px; width:40%;" />
            <div class="skeleton" style="height:120px; width:100%;" />
            <div class="skeleton" style="height:120px; width:100%;" />
          </template>

          <!-- The queue could not be read, so there is nothing to edit FROM,
               and the fields are not rendered at all. Under merge semantics a
               blind form would still be SAFE — an untouched field sends nothing
               — but a form showing twenty-one defaults next to "we could not
               read this queue" states values the queue may not hold, which is
               the failure this whole workstream exists to end. -->
          <div v-else-if="prefillError" class="panel-err">{{ prefillError }}</div>

          <template v-else>
            <!-- The refusal, kept next to the form that caused it: the SP's
                 sentence about which option is wrong, the proxy's about which
                 cap was hit. The form stays up with the values still in it. -->
            <div v-if="refusal" class="panel-err">{{ refusal }}</div>

            <!-- ---------------------------------------------------- identity -->
            <label v-if="!isEdit" class="qc-field">
              <span class="label-xs">Queue name</span>
              <input
                v-model="queueName"
                class="input font-mono"
                autocomplete="off"
                spellcheck="false"
                placeholder="orders.created"
                @input="nameTouched = true"
                @blur="nameTouched = true"
              />
              <!-- Only once the field has been used: an empty form that opens
                   already telling you it is wrong is scolding, not helping. The
                   submit button is disabled either way. -->
              <span v-if="nameError && nameTouched" class="qc-invalid">{{ nameError }}</span>
              <!-- Not an error: /configure is the one route for both, so a name
                   that exists is an EDIT of that queue, merged. Saying so is
                   what stops "Create" from reading as "reset". -->
              <span v-else-if="existingWarn" class="qc-warn">{{ existingWarn }}</span>
              <span v-else-if="nameExists" class="qc-warn">
                A queue named <span class="font-mono">{{ queueName.trim() }}</span> already exists on this
                cluster, so this is an edit of it and the fields below now show its configuration.
                Saving sends only what you change.
              </span>
              <span v-else class="qc-help">
                The name consumers and producers address. A queue also appears the first time
                something pushes to it; configuring it up front is how you move off the defaults.
              </span>
            </label>

            <!-- Not wrapped in a <label>: the Autocomplete owns an input plus
                 its own clear button, and a label around both makes a click on
                 "clear" read as a click on the field. It takes an id instead,
                 the way every other filter row in the app addresses it. -->
            <div class="qc-pair">
              <div class="qc-field">
                <label class="label-xs" for="qc-namespace">Namespace</label>
                <!-- commit-on-blur, unlike every filter row: a name typed here
                     is a value to SAVE, not a filter to narrow with. Clicking
                     "Save changes" blurs the field first, and without this the
                     namespace the operator just typed would be discarded on the
                     way to the button they pressed to keep it. -->
                <Autocomplete
                  id="qc-namespace"
                  v-model="form.namespace"
                  :options="namespaceOptions"
                  :loading="labelsLoading"
                  label="Namespace"
                  placeholder="(default)"
                  allow-custom
                  commit-on-blur
                />
              </div>
              <div class="qc-field">
                <label class="label-xs" for="qc-task">Task</label>
                <Autocomplete
                  id="qc-task"
                  v-model="form.task"
                  :options="taskOptions"
                  :loading="labelsLoading"
                  label="Task"
                  placeholder="(default)"
                  allow-custom
                  commit-on-blur
                />
              </div>
            </div>
            <p class="qc-help qc-pair-help">
              The two discovery labels, matched for exact equality by
              <span class="font-mono">/pop?namespace=…&amp;task=…</span>. Changing them moves no
              messages; the queue starts matching a different discovery pop from the next call.
            </p>

            <!-- ----------------------------------------------- common options -->
            <div class="qc-group">
              <span class="label-xs qc-group-title">Options</span>
              <!-- A text row is a <label> wrapping its input, the way every
                   form in this app writes one; a boolean row cannot be, because
                   its own <label> wraps the checkbox and a label inside a label
                   is invalid markup that toggles the box from the wrong click. -->
              <component
                :is="meta.type === 'bool' ? 'div' : 'label'"
                v-for="meta in commonOptions"
                :key="meta.key"
                class="qc-field"
              >
                <span class="qc-option-head">
                  <span class="label-xs">{{ meta.label }}</span>
                  <span class="qc-default">default {{ formatOptionValue(meta, meta.default) }}</span>
                </span>

                <label v-if="meta.type === 'bool'" class="qc-check">
                  <input v-model="form[meta.key]" type="checkbox" />
                  <span>{{ form[meta.key] ? 'on' : 'off' }}</span>
                </label>
                <input
                  v-else
                  v-model="form[meta.key]"
                  class="input font-mono"
                  autocomplete="off"
                  spellcheck="false"
                  :inputmode="meta.type === 'int' ? 'numeric' : 'text'"
                  :placeholder="blankPlaceholder(meta)"
                />

                <span v-if="errors[meta.key]" class="qc-invalid">{{ errors[meta.key] }}</span>
                <span v-else class="qc-help">{{ meta.help }}</span>
              </component>
            </div>

            <!-- --------------------------------------------- advanced options -->
            <button type="button" class="qc-toggle" @click="showAdvanced = !showAdvanced">
              <span class="qc-toggle-chev" :class="{ 'qc-toggle-open': showAdvanced }" aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
                </svg>
              </span>
              Advanced options
              <!-- Retention and encryption live under this fold. If any of them
                   is already off its default, the fold says so rather than
                   hiding the fact that this queue deletes data on a timer. -->
              <span v-if="advancedOffDefault" class="chip chip-mute qc-toggle-badge">
                {{ advancedOffDefault }} not at the default
              </span>
            </button>

            <div v-if="showAdvanced" class="qc-group">
              <component
                :is="meta.type === 'bool' ? 'div' : 'label'"
                v-for="meta in advancedOptions"
                :key="meta.key"
                class="qc-field"
              >
                <span class="qc-option-head">
                  <span class="label-xs">{{ meta.label }}</span>
                  <span class="qc-default">default {{ formatOptionValue(meta, meta.default) }}</span>
                </span>

                <label v-if="meta.type === 'bool'" class="qc-check">
                  <input v-model="form[meta.key]" type="checkbox" />
                  <span>{{ form[meta.key] ? 'on' : 'off' }}</span>
                </label>
                <input
                  v-else
                  v-model="form[meta.key]"
                  class="input font-mono"
                  autocomplete="off"
                  spellcheck="false"
                  :inputmode="meta.type === 'int' ? 'numeric' : 'text'"
                  :placeholder="blankPlaceholder(meta)"
                />

                <span v-if="errors[meta.key]" class="qc-invalid">{{ errors[meta.key] }}</span>
                <span v-else class="qc-help">{{ meta.help }}</span>

                <!-- Turning encryption on is not retroactive in either
                     direction, and an operator who reads this switch as "make
                     this queue's data encrypted" would be wrong about
                     everything already stored. -->
                <span v-if="meta.key === 'encryptionEnabled' && isEditing" class="qc-caution">
                  Not retroactive: frames already in the queue stay exactly as they are, and only
                  pushes after this save are encrypted. The broker that serves this call drops its
                  cached flag at once and tells its peers to do the same; a peer that misses that
                  frame picks it up on the next cache refresh (60 s by default).
                </span>
              </component>
            </div>

            <!-- --------------------------------------------------- the rule -->
            <p class="qc-note">
              <template v-if="isEditing">
                This sends only the options you changed
                <template v-if="changedKeys.length">
                  (<span class="font-mono">{{ changedKeys.join(', ') }}</span>)</template>, as a
                merge: everything else keeps the value the queue has now, and a field you clear
                goes back to its default.
              </template>
              <template v-else>
                A blank field is not sent: the queue is created with that option's default.
              </template>
            </p>
          </template>
        </div>

        <div class="modal-foot">
          <!-- An edit with nothing in it is not a save. Saying so beats a
               button that posts an empty bag and toasts a success. -->
          <span
            v-if="isEditing && !loadingCurrent && !prefillError && !changedKeys.length"
            class="qc-foot-note"
          >
            Nothing changed yet
          </span>
          <button type="button" class="btn btn-ghost" @click="emit('close')">
            {{ prefillError ? 'Close' : 'Cancel' }}
          </button>
          <!-- Disabled in flight: /configure is idempotent, but a second call
               would race the first one's echo and could report the older row. -->
          <button v-if="!prefillError" type="submit" class="btn btn-primary" :disabled="!canSubmit">
            {{ saving ? 'Saving…' : (isEditing ? 'Save changes' : 'Create queue') }}
          </button>
        </div>
      </form>
    </div>
  </Teleport>
</template>

<script setup>
// Create a queue, or edit one's options, from the console
// (PLAN_DASHBOARD_ACTIONS.md §2.2) — one component behind both entry points
// (the Queues header button, Queue Detail's "Edit configuration"), so the
// option list, the wording and the merge rule cannot drift between them.
//
// WHY THIS IS SAFE TO SHIP NOW AND WAS NOT BEFORE. Until 1.6.0 `/configure`
// re-parsed every option from defaults on every call, so an edit form that
// posted three fields reset the other eighteen: dedup back to 3600, retention
// off, the sink hold off — silently, on a queue the operator meant to nudge.
// The broker now merges (012_configure.sql), and this form is built on that:
// it posts `configDiff`, the keys the operator actually touched, and nothing
// else. That is also what makes it concurrency-safe: an option a colleague
// changed while this modal was open is not in the diff, so it is not
// overwritten with the value this modal read minutes ago.
//
// The rules live in composables/useQueueConfig.js as pure functions, held
// against the SQL by app/test/queue-config.test.js.
import { computed, ref, watch } from 'vue'

import Autocomplete from '@/components/Autocomplete.vue'
import { queues as queuesApi, resources as resourcesApi } from '@/api'
import {
  OPTION_META, buildBody, configDiff, describeConfigureRefusal, existingQueueName, formatOptionValue,
  optionsInGroup, queueNameError, readFormValues, summariseEffective, toFormValue, toFormValues,
  validate,
} from '@/composables/useQueueConfig'
import { useToast } from '@/composables/useToast'
import { useQueuesStore } from '@/stores/queuesStore'

const props = defineProps({
  open: { type: Boolean, required: true },
  /**
   * 'create' opens an empty form and lets the operator name the queue;
   * 'edit' pins the name and prefills every option from the broker's echo.
   */
  mode: {
    type: String,
    default: 'create',
    validator: (v) => v === 'create' || v === 'edit',
  },
  /** The queue to edit. Ignored in 'create', where the form owns the name. */
  queue: { type: String, default: '' },
})

const emit = defineEmits(['close', 'saved'])

const { notifySuccess } = useToast()
const { queueMeta, invalidate } = useQueuesStore()

const isEdit = computed(() => props.mode === 'edit')

const commonOptions = optionsInGroup('common')
const advancedOptions = optionsInGroup('advanced')

const queueName = ref('')
const nameTouched = ref(false)
const form = ref(toFormValues({}))
// The queue as the broker last described it — the left-hand side of every
// diff. `{}` on create, where "what it is now" is "the defaults".
const current = ref({})
const loadingCurrent = ref(false)
const prefillError = ref(null)
const refusal = ref(null)
const saving = ref(false)
const showAdvanced = ref(false)

const namespaceOptions = ref([])
const taskOptions = ref([])
const labelsLoading = ref(false)
// Create mode only: the queue whose name was typed exists, but its stored
// configuration could not be read. The form stays up (a merge still only sends
// what changed) and says what it is showing instead.
const existingWarn = ref(null)

// ---------------------------------------------------------------------------
// Derived
// ---------------------------------------------------------------------------
const edited = computed(() => readFormValues(form.value))
const diff = computed(() => configDiff(current.value, edited.value))
// VALIDATED: the diff, not the whole form. `readFormValues` walks all 21
// options — the three inert ones included, which this form does not render — so
// validating `edited` let a queue holding a negative `maxSize` (reachable from
// the JS SDK or raw HTTP; the SP bounds none of them) disable "Save changes"
// for good, with the message pinned to a field that has no DOM node to show it
// in. Nothing rendered is lost: a value the operator typed badly always differs
// from the stored one, so it is in the diff by construction.
const errors = computed(() => validate(diff.value))
const changedKeys = computed(() => Object.keys(diff.value))
const nameError = computed(() => (isEdit.value ? null : queueNameError(queueName.value)))
// The queue the typed name resolves to, as a NAME: the repaint below is driven
// by WHICH queue it is, not by whether there is one (see existingQueueName).
const existingQueue = computed(() => existingQueueName(queueName.value, queueMeta.value, { isEdit: isEdit.value }))
const nameExists = computed(() => existingQueue.value !== null)
/**
 * Whether this save EDITS a queue that already exists — true for the edit
 * entry point, and equally true for a "Create queue" typed onto a name the
 * cluster already carries, because `/configure` is one route for both and the
 * broker will merge into that queue. Every sentence the form says about the
 * outcome reads off this, not off the button that opened it.
 */
const isEditing = computed(() => isEdit.value || nameExists.value)

/** Advanced options this queue does not hold at their default, for the fold. */
const advancedOffDefault = computed(() =>
  advancedOptions.filter((meta) => {
    const value = current.value[meta.key]
    return value !== undefined && value !== null && String(value) !== String(meta.default)
  }).length
)

const canSubmit = computed(() => {
  if (saving.value || loadingCurrent.value || prefillError.value) return false
  if (Object.keys(errors.value).length) return false
  if (!isEdit.value && nameError.value) return false
  // An edit with an empty diff would take the queue's row lock to write nothing
  // — and on a name that already exists it would ALSO report a creation that
  // did not happen. A create of a genuinely new queue needs no diff: every
  // option absent is every option at its default, which is a real request.
  if (isEditing.value) return changedKeys.value.length > 0
  return true
})

/**
 * What an empty field means, spelled in the field itself.
 *
 * On create the broker parses an absent option from its default, and on an edit
 * a cleared field is sent as `null`, which lands on the same default — so one
 * placeholder is true in both modes.
 */
const blankPlaceholder = (meta) =>
  meta.type === 'text' ? (meta.default === '' ? '(none)' : String(meta.default)) : String(meta.default)

// ---------------------------------------------------------------------------
// Opening
// ---------------------------------------------------------------------------

/** The two discovery labels the cluster already uses, for the pickers. */
const fetchLabels = async () => {
  labelsLoading.value = true
  try {
    const [ns, tasks] = await Promise.all([
      resourcesApi.getNamespaces(),
      resourcesApi.getTasks(),
    ])
    namespaceOptions.value = (ns.data?.namespaces || [])
      .map(row => row?.namespace).filter(Boolean).sort()
    taskOptions.value = (tasks.data?.tasks || [])
      .map(row => row?.task).filter(Boolean).sort()
  } catch {
    // A picker with no suggestions still accepts a typed value (allow-custom),
    // and the failure is already on the global surface. Nothing to say here.
    namespaceOptions.value = []
    taskOptions.value = []
  } finally {
    labelsLoading.value = false
  }
}

// The queue's current configuration is read HERE, by the editor, and nowhere
// else: Queue Detail reads /api/v1/status/queues/:name, whose `config` block
// carries six keys, and its second fetch of /resources/queues/:name was deleted
// precisely because it doubled the request rate of the busiest detail page. So
// the editor pays for the full read itself, once, when an operator opens it —
// and once more per existing name typed into the create form.
const OLD_BROKER_PREFILL =
  'This broker does not report a queue’s full configuration (it predates 1.6.0), so there ' +
  'is nothing to prefill this form from. Configure the queue with queenctl or an SDK, ' +
  'where the whole option set is sent explicitly.'

/** The 21-option echo for one queue, or a throw carrying what to say about it. */
const fetchOptions = async (name) => {
  const r = await queuesApi.get(name)
  const options = r.data?.options
  // A broker older than 1.6.0: get_queue_v2 answers without `options`, so there
  // is nothing to prefill from and nothing honest to render.
  if (!options || typeof options !== 'object') throw new Error(OLD_BROKER_PREFILL)
  return options
}

// Only the newest prefill may paint the form: an operator typing a name walks
// through several existing ones, and a slow answer for a name they have already
// left must not overwrite the one they are on.
let prefillSeq = 0

/**
 * Point the form at what `name` actually holds — or at the defaults, when there
 * is no such queue (`name` null).
 *
 * WHY A CREATE READS A QUEUE AT ALL. `/configure` is one route for both verbs,
 * and it merges: typing a name that already exists makes this form an EDITOR of
 * that queue, whatever the button that opened it says. Painted from the
 * defaults it would show a dead-letter checkbox ticked on a queue that has it
 * off, drop every typed value that happens to equal a default (`configDiff`
 * compares against what the queue holds, which it did not know), and then
 * report "Created queue X — every option at its default" over a queue it
 * neither created nor changed.
 *
 * The operator's own edits survive the repaint: they are re-applied on top, so
 * a prefill that lands mid-typing cannot take back what was typed.
 */
const adoptQueue = async (name) => {
  const seq = ++prefillSeq
  const keep = diff.value
  loadingCurrent.value = true
  prefillError.value = null
  existingWarn.value = null
  let options = {}
  try {
    if (name) options = await fetchOptions(name)
  } catch (err) {
    if (seq !== prefillSeq) return
    const said = describeConfigureRefusal(err)
    if (isEdit.value) {
      prefillError.value = said
      loadingCurrent.value = false
      return
    }
    // On a create the form stays up: a merge still sends only what changed, so
    // it is safe — it simply must not pretend the values on screen are that
    // queue's.
    existingWarn.value =
      `A queue named ${name} already exists here, but its configuration could not be read. ` +
      `${said} The fields below are the broker’s defaults, not that queue’s values; ` +
      'saving still sends only the options you change.'
  }
  if (seq !== prefillSeq) return
  current.value = options
  form.value = toFormValues(options)
  for (const [key, value] of Object.entries(keep)) {
    const meta = OPTION_META[key]
    if (meta) form.value[key] = toFormValue(meta, value)
  }
  loadingCurrent.value = false
}

// Every opening starts from the props, never from what the last one left
// behind: a modal reopened on another queue must not carry the previous
// queue's options into a merge.
watch(() => props.open, (isOpen) => {
  if (!isOpen) return
  prefillSeq++
  refusal.value = null
  prefillError.value = null
  existingWarn.value = null
  saving.value = false
  showAdvanced.value = false
  queueName.value = ''
  nameTouched.value = false
  loadingCurrent.value = false
  current.value = {}
  form.value = toFormValues({})
  fetchLabels()
  if (isEdit.value) adoptQueue(props.queue)
}, { immediate: true })

// A typed name that names an existing queue turns this form into that queue's
// editor, and back into a create the moment it does not. Both directions
// repaint from the truth and keep the operator's edits.
//
// Watched on the NAME, not on "does it exist": one existing queue replaced by
// another — a pasted name, a second suggestion picked out of the autocomplete —
// never passes through "no such queue", so a boolean would not change and the
// first queue's configuration would stay on screen, and in `current`, under the
// second queue's name.
watch(existingQueue, (name) => {
  if (!props.open || isEdit.value) return
  adoptQueue(name)
})

// ---------------------------------------------------------------------------
// Saving
// ---------------------------------------------------------------------------
const submit = async () => {
  if (!canSubmit.value) return
  const name = isEdit.value ? props.queue : queueName.value.trim()

  // namespace and task ride the same diff as every other option — they are
  // options to the SP — but they are handed to `buildBody` by name so the body
  // is built through the one function that knows where they have to sit.
  const { namespace, task, ...options } = diff.value

  saving.value = true
  refusal.value = null
  try {
    const res = await queuesApi.configure(buildBody({
      queue: name,
      namespace,
      task,
      options,
      // MERGE, in both modes and stated explicitly. On an edit it is the whole
      // point; on a create it is what keeps "Create queue" from resetting a
      // queue that already carries that name. (A broker older than 1.6.0
      // ignores the key and replaces — which is why the form warns when the
      // name already exists.)
      mode: 'merge',
    }))

    // The verdict, not the status: `configured: true` is what the SP says when
    // it wrote the row. Anything else reached a 200 without configuring.
    const echo = res.data
    if (!echo || echo.configured !== true) {
      refusal.value =
        'The broker answered without confirming the configuration, so the queue may be unchanged. ' +
        'Reload the queue before saving again.'
      return
    }

    // A create adds a queue and an edit can move its namespace or task, both of
    // which the shared list carries.
    invalidate()
    // "Created" only for a queue that did not exist a moment ago. On a name the
    // cluster already carried, this call merged into it — saying "created"
    // there, and worse "every option at its default", describes a queue nobody
    // touched and hides that its non-default options are still in force.
    notifySuccess(
      isEditing.value ? `Configured ${echo.queue}` : `Created queue ${echo.queue}`,
      // From the ECHO, which is the effective row: a value the SP clamped
      // (minPopWaitTime past 60000) is reported as what the queue now holds,
      // not as what was typed.
      summariseEffective(echo, changedKeys.value) ||
        (isEditing.value ? 'nothing changed' : 'every option at its default'),
    )
    emit('saved', {
      queue: echo.queue,
      mode: isEditing.value ? 'edit' : 'create',
      changed: changedKeys.value,
    })
    emit('close')
  } catch (err) {
    refusal.value = describeConfigureRefusal(err)
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
/* One entry point is Queue Detail's header, which sits under no drawer — but
   the Autocomplete menus inside this form are teleported at z-index 60, and the
   shared backdrop is 50. 55 keeps this form above anything the page raised it
   from and still under its own pickers. */
.qc-over { z-index: 55; }

/* Wider than the 480px shell: this form is two columns of labels plus a helper
   sentence per option, and at 480px every sentence wraps to four lines. */
.qc-card { max-width: 560px; }

.qc-form { display: grid; gap: 14px; }
.qc-form .panel-err { margin-bottom: 0; }
.qc-field { display: grid; gap: 6px; }
.qc-help { color: var(--text-low); font-size: 11.5px; line-height: 1.45; }
.qc-invalid { color: var(--ember-400); font-size: 11.5px; line-height: 1.45; }
.qc-warn { color: var(--warn-400); font-size: 11.5px; line-height: 1.45; }
.qc-caution {
  padding: 7px 9px; font-size: 11.5px; line-height: 1.45; color: var(--warn-400);
  border: 1px solid var(--warn-bd); border-radius: var(--r-control);
  background: var(--warn-glow);
}

/* Namespace and task are one decision in two fields, so they share a row and
   one helper sentence below them. */
.qc-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.qc-pair-help { margin: -6px 0 0; }

.qc-group { display: grid; gap: 12px; }
.qc-group-title { color: var(--text-low); }

.qc-option-head { display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
/* The default, stated on every row: it is what a blank field means, and the
   number an operator is deciding to move away from. */
.qc-default { color: var(--text-low); font-size: 11px; white-space: nowrap; }

.qc-check {
  display: flex; align-items: center; gap: 8px;
  font-size: 12.5px; color: var(--text-mid); cursor: pointer;
}
.qc-check input { width: 16px; height: 16px; accent-color: var(--accent); }

.qc-toggle {
  display: flex; align-items: center; gap: 8px;
  padding: 0; border: none; background: none; cursor: pointer;
  font-size: 12px; font-weight: 600; color: var(--text-mid);
}
.qc-toggle:hover { color: var(--text-hi); }
.qc-toggle-chev { display: inline-flex; width: 12px; height: 12px; transition: transform 0.15s ease; }
.qc-toggle-chev svg { width: 12px; height: 12px; }
.qc-toggle-open { transform: rotate(90deg); }
.qc-toggle-badge { font-weight: 500; }

.qc-note {
  margin: 0; padding: 9px 10px;
  font-size: 11.5px; line-height: 1.45; color: var(--text-mid);
  border: 1px solid var(--bd); border-radius: var(--r-control);
  background: var(--ink-3);
}

/* The footer's left-hand statement; the buttons stay right-aligned. */
.qc-foot-note { margin-right: auto; font-size: 11.5px; color: var(--text-low); }

@media (max-width: 560px) {
  .qc-pair { grid-template-columns: 1fr; }
}
</style>
