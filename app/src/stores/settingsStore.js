// The console's settings — module-level singleton, like queuesStore and
// groupsStore.
//
// One JSON document in the tenant's KV (composables/settingsDoc.js: its key,
// its shape, which lines it may move). This store reads it, keeps the lines in
// force as computeds, and hands useSeverity a reader of them, so every verdict
// on screen follows the document without the pages knowing it exists. The two
// places that judge an age against ONE QUEUE's lines ask `linesFor(queue)`.
//
// A tenant with no document, a plan without KV, a read that failed: all three
// leave the built-in lines in force. `state` says which, for the Settings page.
//
//   'unread'  not read yet
//   'ready'   read; `version` 0 means there is no document yet
//   'off'     KV is not available on this cluster (`verdict`: absent | gated | paused)
//   'error'   the read failed; the last document read stays in force
//
// Same rules as the other two stores: TTL + in-flight de-duplication, reset on
// a cluster switch, and an answer for a cluster we have since left is dropped.
//
// A save is a compare-and-set on the version the document was read at, so two
// people on the page cannot overwrite each other: the second one is told, and
// gets the document as it now is.
import { computed, ref } from 'vue'

import { kv as kvApi } from '@/api'
import { gatedVerdict } from '@/composables/useGatedVerdict'
import { SETTINGS_KEY, SETTINGS_NS, readSettings, resolveLines } from '@/composables/settingsDoc'
import { THRESHOLDS, setLinesReader } from '@/composables/useSeverity'
import { currentEpoch, onClusterChange } from '@/stores/identity'

const DEFAULT_TTL_MS = 60_000

const stored = ref(null) // the value as stored, untouched; null = no document
const version = ref(0)
const state = ref('unread')
const verdict = ref(null)
const error = ref(null)
const lastFetched = ref(0)
let inflight = null
let inflightEpoch = -1

const settings = computed(() => readSettings(stored.value))
/** The tenant's lines: built-in, with the document's `defaults` over them. */
const lines = computed(() => Object.freeze(resolveLines(THRESHOLDS, settings.value)))
const queueLines = computed(() => {
  const out = new Map()
  for (const name of settings.value.queues.keys()) {
    out.set(name, Object.freeze(resolveLines(THRESHOLDS, settings.value, name)))
  }
  return out
})
/** The lines in force for one queue. */
const linesFor = (queue) => queueLines.value.get(queue) || lines.value

setLinesReader(() => lines.value)

const take = (answer) => {
  stored.value = answer?.found === false ? null : (answer?.value ?? null)
  version.value = answer?.found === false ? 0 : (Number(answer?.version) || 0)
  state.value = 'ready'
  verdict.value = null
  error.value = null
  lastFetched.value = Date.now()
}

const fetchSettings = async ({ force = false, ttlMs = DEFAULT_TTL_MS } = {}) => {
  const fresh = lastFetched.value > 0 && Date.now() - lastFetched.value < ttlMs
  if (!force && fresh) return state.value
  if (inflight && inflightEpoch === currentEpoch()) return inflight

  const epochAtStart = currentEpoch()
  inflightEpoch = epochAtStart
  inflight = (async () => {
    try {
      const { data } = await kvApi.get(SETTINGS_NS, SETTINGS_KEY, { probe: true })
      if (epochAtStart === currentEpoch()) take(data)
    } catch (err) {
      if (epochAtStart !== currentEpoch()) return state.value
      const v = gatedVerdict(err)
      error.value = err
      lastFetched.value = Date.now()
      if (v === 'transient') {
        state.value = 'error'
      } else {
        // Not a failure: there is no KV to keep a document in.
        stored.value = null
        version.value = 0
        state.value = 'off'
        verdict.value = v
      }
    } finally {
      inflight = null
    }
    return state.value
  })()
  return inflight
}

/**
 * Store `change(value as stored)`. Resolves `{ saved: true }`, or
 * `{ saved: false }` when somebody else saved first — the store then holds
 * their document, and the caller's form has to be redone on top of it. A
 * failed call rejects, and the shell's toast carries the reason.
 */
const save = async (change) => {
  const epochAtStart = currentEpoch()
  const { data } = await kvApi.put(SETTINGS_NS, SETTINGS_KEY, {
    value: change(stored.value),
    forever: true,
    expect: version.value,
  })
  if (epochAtStart !== currentEpoch()) return { saved: false }
  take({ value: data?.value ?? null, version: data?.version, found: data?.value !== undefined && data?.value !== null })
  return { saved: data?.applied === true }
}

const reset = () => {
  inflight = null
  inflightEpoch = -1
  stored.value = null
  version.value = 0
  state.value = 'unread'
  verdict.value = null
  error.value = null
  lastFetched.value = 0
}

onClusterChange(reset)

export function useSettingsStore() {
  return { settings, lines, linesFor, version, state, verdict, error, fetchSettings, save, reset }
}
