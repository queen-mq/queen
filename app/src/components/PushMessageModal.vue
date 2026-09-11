<template>
  <Teleport to="body">
    <div v-if="open" class="modal-backdrop push-over" @click.self="closeUnlessInFlight">
      <form class="card modal-card push-card" @submit.prevent="submit">
        <div class="card-header">
          <h3>{{ copy ? 'Push a copy' : 'Push message' }}</h3>
          <span v-if="queueFixed" class="card-sub font-mono">{{ queue }}</span>
        </div>

        <div class="card-body push-form">
          <!-- A copy is a NEW message and the word "retry" is never used for it:
               it carries a fresh transaction id, it lands at the tail of the
               partition rather than in the original's position, and it is not
               deduplicated against the message it was copied from. -->
          <p v-if="copy" class="push-note">
            Push a copy: a new transaction id, appended at the tail, not deduplicated
            against the original.
          </p>

          <!-- The outcome, and the only thing on screen that may call a push a
               success. The form stays under it so a duplicate or a buffered
               answer can be acted on without retyping the message. -->
          <div v-if="verdict" class="push-verdict" :class="`push-verdict-${verdict.kind}`">
            <strong>{{ verdict.title }}</strong>
            <p>{{ verdict.detail }}</p>

            <!-- Ids only for the three statuses where the broker took
                 responsibility for the message (PushStatus::accepted): after an
                 `error` or a `failed` nothing was stored, and a message id
                 beside that sentence would name something that does not exist. -->
            <dl v-if="showIds" class="push-ids">
              <!-- Not for a `buffered` result: the spool keeps no message id
                   and the drain mints a new one, so the id the broker returned
                   there names nothing (`showsMessageId`). -->
              <div v-if="showMessageId">
                <dt>Message id</dt>
                <dd class="font-mono">{{ result.message_id }}</dd>
              </div>
              <div>
                <dt>Transaction id</dt>
                <dd class="font-mono">{{ result.transaction_id }}</dd>
              </div>
              <div v-if="offset !== null">
                <dt>Offset</dt>
                <dd class="font-mono tabular-nums">{{ offset }}</dd>
              </div>
            </dl>

            <router-link
              v-if="verdict.kind === 'success' && messagesLink"
              class="btn btn-ghost push-open"
              :to="{ path: '/messages', query: { queue: result?.queueName || queue } }"
              @click="emit('close')"
            >
              Open in Messages
            </router-link>
          </div>

          <!-- The proxy refused before the broker ever saw the message: a role,
               a plan limit, a rate limit or a size cap. Rendered here rather
               than left to the global toast, because the modal is where the
               message still is. -->
          <div v-if="refusal" class="panel-err">{{ refusal }}</div>

          <label class="push-field">
            <span class="label-xs">Queue</span>
            <!-- Fixed on a page that IS one queue: a picker there would offer to
                 push somewhere the page does not describe. -->
            <span v-if="queueFixed" class="push-fixed font-mono">{{ queue }}</span>
            <!-- The one call site of this widget where the typed name is a
                 WRITE TARGET, not a filter: `commit-on-blur` applies what is in
                 the field when focus leaves it (clicking "Push message" blurs
                 it first), because discarding half-typed text here means either
                 a dead button or a message written to the previously committed
                 queue. `custom-hint` and `reset-label` drop the filter
                 vocabulary the other call sites want. -->
            <Autocomplete
              v-else
              v-model="form.queue"
              :options="queueNames"
              :loading="queuesLoading"
              label="Queue"
              placeholder="Queue name"
              allow-custom
              commit-on-blur
              custom-hint="press Enter to push to this queue name"
              :reset-label="null"
            />
            <span v-if="!queueFixed" class="push-help">
              A name this cluster does not carry yet is created by the push, with the default
              options — including the 60-second lease a queue gets when nothing configured it.
            </span>
          </label>

          <label class="push-field">
            <span class="label-xs">Partition</span>
            <input v-model="form.partition" class="input font-mono" autocomplete="off" placeholder="Default" />
            <span class="push-help">
              Blank pushes to <span class="font-mono">Default</span>. A partition is an ordering
              lane: everything in one is delivered in order, and a name that does not exist yet is
              created by this push.
            </span>
          </label>

          <label class="push-field">
            <span class="label-xs">Payload</span>
            <textarea
              v-model="form.payload"
              class="input push-payload font-mono"
              rows="8"
              spellcheck="false"
              placeholder='{"id": 1}'
            ></textarea>
            <span v-if="payloadError" class="push-invalid">{{ payloadError }}</span>
            <span v-else class="push-help">
              Any JSON value: an object, an array, a quoted "string", a number, true / false
              or null. It is stored verbatim.
            </span>
          </label>

          <label class="push-field">
            <span class="label-xs">Transaction ID</span>
            <input v-model="form.transactionId" class="input font-mono" autocomplete="off" placeholder="a fresh id" />
            <span class="push-help">
              Blank mints a fresh id, which makes the push non-idempotent — send it twice and the
              queue holds two messages. Set one to make the push idempotent inside the queue's
              dedup window: a second push of the same id answers
              <span class="font-mono">duplicate</span> and writes nothing.
            </span>
          </label>

          <p class="push-help push-meter">
            Metered as a producer push for
            <strong>{{ actingClusterSlug || 'the acting cluster' }}</strong>, exactly like one
            from an SDK.
          </p>
        </div>

        <div class="modal-foot">
          <template v-if="queued">
            <button type="button" class="btn btn-ghost" @click="pushAnother">Push another</button>
            <button type="button" class="btn btn-primary" @click="emit('close')">Done</button>
          </template>
          <template v-else>
            <!-- Cancel is disabled in flight for the same reason the modal is
                 the only place a verdict is rendered: the push has already
                 left, `failed` comes back with 201 and therefore raises no
                 toast, and a modal dismissed mid-request would drop the one
                 report that a message was lost. -->
            <button type="button" class="btn btn-ghost" :disabled="submitting" @click="emit('close')">Cancel</button>
            <!-- Disabled in flight: a push is not idempotent unless the caller
                 made it so, and a second click would enqueue a second message. -->
            <button type="submit" class="btn btn-primary" :disabled="submitting || !targetQueue">
              {{ submitting ? 'Pushing…' : 'Push message' }}
            </button>
          </template>
        </div>
      </form>
    </div>
  </Teleport>
</template>

<script setup>
// One message, pushed from the console — the single component behind the three
// entry points (Messages header, the message drawer's "push a copy", Queue
// Detail's header), so the form, the wording and above all the VERDICT cannot
// drift between them.
//
// The rule this component exists to enforce: 201 is not success. The broker
// answers a push with one result per item and only `queued` means the message
// is on the queue, so the modal renders `pushVerdict` and closes itself for
// nothing — a `duplicate` (nothing written), a `buffered` (on the broker's
// spool, not consumable) and an `error` all leave the form up with the message
// still in it. See composables/usePushVerdict.js for the mapping.
import { computed, ref, watch } from 'vue'

import Autocomplete from '@/components/Autocomplete.vue'
import { messages as messagesApi } from '@/api'
import {
  describePushRefusal, offsetLine, parsePayload, payloadToText, pushVerdict,
  showsIds, showsMessageId, worstResult,
} from '@/composables/usePushVerdict'
import { useToast } from '@/composables/useToast'
import { useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'

const props = defineProps({
  open: { type: Boolean, required: true },
  /** Queue to start on: the page's queue, or the list's current filter. */
  queue: { type: String, default: '' },
  partition: { type: String, default: '' },
  /**
   * The payload as a JSON VALUE (what the API returned), not as text — the
   * drawer hands over `messageDetail.payload` and the field is filled by
   * `payloadToText`, which round-trips that VALUE unchanged (not the original
   * bytes: the transport parsed them, so a number outside what an IEEE-754
   * double holds exactly was already rewritten before it got here). Absent
   * leaves the field empty; an explicit `null` fills it with `null`, which is a
   * payload a message can legitimately carry.
   */
  payload: { default: undefined },
  transactionId: { type: String, default: '' },
  /** The queue is the page's subject and cannot be changed (Queue Detail). */
  queueFixed: { type: Boolean, default: false },
  /** Label and explain this as a copy of an existing message, never a retry. */
  copy: { type: Boolean, default: false },
  /**
   * Offer "Open in Messages" after a queued push. Messages itself passes false:
   * `<router-view>` is not keyed, so a push to /messages from /messages would
   * change the URL without re-reading the query — a link that looks like it
   * filtered and did not. That page applies the queue to its own filter on
   * `pushed` instead.
   */
  messagesLink: { type: Boolean, default: true },
})

const emit = defineEmits(['close', 'pushed'])

const { actingClusterSlug } = useIdentity()
const { notifySuccess } = useToast()
const { queues, loading: queuesLoading, fetchQueues, invalidate } = useQueuesStore()

const form = ref({ queue: '', partition: '', payload: '', transactionId: '' })
const submitting = ref(false)
const payloadError = ref(null)
const refusal = ref(null)
const verdict = ref(null)
const result = ref(null)

const queueNames = computed(() =>
  queues.value.map(q => q.name).filter(Boolean).sort()
)

// A fixed queue is the prop, never the field: nothing in the form can move the
// push off the page's own queue.
const targetQueue = computed(() =>
  (props.queueFixed ? props.queue : form.value.queue || '').trim()
)

const queued = computed(() => verdict.value?.kind === 'success')
// PushStatus::accepted(), and the buffered exception to it, live in the
// composable so `test/push.test.js` holds them; this component only renders
// what they answer.
const showIds = computed(() => showsIds(result.value?.status))
const showMessageId = computed(() => showsMessageId(result.value?.status))
const offset = computed(() => offsetLine(result.value))

// Every submit carries a token. `resetForm` bumps it, so a push that is still
// in flight when the modal is reopened on another message can no longer write
// its verdict — and its ids — into a session that is not its own.
let submitSeq = 0

const resetForm = () => {
  submitSeq += 1
  submitting.value = false
  form.value = {
    queue: props.queue || '',
    partition: props.partition || '',
    payload: payloadToText(props.payload),
    transactionId: props.transactionId || '',
  }
  payloadError.value = null
  refusal.value = null
  verdict.value = null
  result.value = null
}

// Every opening starts from the props, not from what the last one left behind:
// a modal reopened on another message must not carry the previous payload.
watch(() => props.open, (isOpen) => {
  if (!isOpen) return
  resetForm()
  // The picker's options; the store de-duplicates this against whatever the
  // page already asked for and serves it from cache when it is fresh.
  if (!props.queueFixed) fetchQueues()
}, { immediate: true })

/** Render an answer — the array from a 201, or the one a 500 carried. */
const report = (results) => {
  verdict.value = pushVerdict(results)
  result.value = worstResult(results)
  if (verdict.value.kind !== 'success') return

  // A push creates the queue and the partition when they did not exist, so the
  // shared queue list is stale the moment one lands.
  invalidate()
  const item = result.value
  notifySuccess(
    `Queued on ${item?.queueName || targetQueue.value}`,
    `Transaction ${item?.transaction_id}`,
  )
  emit('pushed', {
    queue: item?.queueName || targetQueue.value,
    partition: form.value.partition.trim() || 'Default',
    messageId: item?.message_id || null,
    transactionId: item?.transaction_id || null,
    offset: item?.offset ?? null,
  })
}

const submit = async () => {
  if (submitting.value) return
  const queue = targetQueue.value
  if (!queue) return

  // The previous answer goes BEFORE the parse, not after it: a payload this
  // form refuses never left the browser, and leaving the last verdict panel up
  // would describe an earlier push — with its ids — as the outcome of this one.
  payloadError.value = null
  refusal.value = null
  verdict.value = null
  result.value = null
  const parsed = parsePayload(form.value.payload)
  if (!parsed.ok) {
    payloadError.value = parsed.message
    return
  }

  // Absent, not empty: the broker's own defaults apply to a missing partition
  // ("Default") and a missing transaction id (a minted one), and sending ""
  // for either would push to a partition named "" and dedup on "".
  const item = { queue, payload: parsed.value }
  const partition = form.value.partition.trim()
  if (partition) item.partition = partition
  const transactionId = form.value.transactionId.trim()
  if (transactionId) item.transactionId = transactionId

  submitting.value = true
  const token = ++submitSeq
  try {
    const res = await messagesApi.push({ items: [item] })
    if (token !== submitSeq) return
    report(res.data)
  } catch (err) {
    if (token !== submitSeq) return
    // A spool write that failed under maintenance answers 500 with the SAME
    // per-item results array (`buffer_all`), and that array is one of the two
    // places `failed` is ever reported — the other is a 201 from `handle_push`.
    // Reading it is what turns "HTTP 500" into "the message is lost", which is
    // a different sentence.
    if (Array.isArray(err?.body)) {
      report(err.body)
      return
    }
    refusal.value = describePushRefusal(err)
  } finally {
    if (token === submitSeq) submitting.value = false
  }
}

/**
 * The backdrop closes the modal — unless a push is in flight, where it would
 * throw away the verdict of a request that has already left.
 */
const closeUnlessInFlight = () => {
  if (!submitting.value) emit('close')
}

/** Keep the queue and the partition, clear what identifies one message. */
const pushAnother = () => {
  form.value.payload = ''
  form.value.transactionId = ''
  payloadError.value = null
  refusal.value = null
  verdict.value = null
  result.value = null
}
</script>

<style scoped>
/* One of the entry points is a button INSIDE the message drawer, and the
   drawer is `z-index: 51` while the shared `.modal-backdrop` is 50 — the form
   would open behind the panel that raised it. 55 clears the drawer and still
   passes under the Autocomplete's teleported menu (60), which has to open
   inside this form. */
.push-over { z-index: 55; }

/* Wider than the 480px shell: the payload field is the subject of this form,
   and JSON wrapped at 480px is unreadable while you are writing it. */
.push-card { max-width: 560px; }

.push-form { display: grid; gap: 14px; }
/* The grid owns the spacing here; the shared block carries its own bottom
   margin for the card bodies that stack it above content. */
.push-form .panel-err { margin-bottom: 0; }
.push-field { display: grid; gap: 6px; }
.push-help { color: var(--text-low); font-size: 11.5px; line-height: 1.45; }
.push-invalid { color: var(--ember-400); font-size: 11.5px; line-height: 1.45; }
.push-meter { margin: 0; }
.push-meter strong { color: var(--text-mid); font-weight: 600; }

/* The queue of a page that is one queue: stated, not editable. Same box as the
   inputs beside it so the row still reads as a field. */
.push-fixed {
  padding: 5px 9px; font-size: 12.5px; color: var(--text-hi);
  border: 1px solid var(--bd); border-radius: var(--r-control);
  background: var(--ink-3);
  overflow-wrap: anywhere;
}

.push-payload { min-height: 120px; resize: vertical; line-height: 1.5; }

.push-note {
  padding: 9px 10px; font-size: 11.5px; line-height: 1.45; color: var(--text-mid);
  border: 1px solid var(--bd); border-radius: var(--r-control);
  background: var(--ink-3);
}

/* The verdict block. `.panel-err` is the shell for "we could not ask" and is
   single-toned on purpose, and `.status-banner` has no success variant, so the
   three outcomes of a push get one block with three tones here — built from the
   same tokens, so both schemes follow. */
.push-verdict {
  padding: 10px 12px;
  border: 1px solid; border-radius: var(--r-card);
  font-size: 12.5px; line-height: 1.45;
}
.push-verdict strong { display: block; font-weight: 600; margin-bottom: 4px; }
.push-verdict p { margin: 0; color: var(--text-mid); }
.push-verdict-success { border-color: var(--ok-bd); background: var(--ok-glow); color: var(--ok-500); }
.push-verdict-warning { border-color: var(--warn-bd); background: var(--warn-glow); color: var(--warn-400); }
.push-verdict-error { border-color: var(--ember-bd); background: var(--ember-glow); color: var(--ember-400); }

.push-ids {
  display: grid; gap: 4px; margin: 8px 0 0;
  font-size: 11.5px;
}
.push-ids > div { display: flex; gap: 8px; align-items: baseline; }
.push-ids dt { flex: 0 0 96px; color: var(--text-low); }
.push-ids dd { margin: 0; min-width: 0; color: var(--text-hi); overflow-wrap: anywhere; user-select: all; }

.push-open { margin-top: 10px; }

@media (max-width: 640px) {
  .push-ids > div { flex-direction: column; gap: 0; }
  .push-ids dt { flex: none; }
}
</style>
