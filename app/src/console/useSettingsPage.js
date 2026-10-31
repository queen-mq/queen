// Settings, in the new console: a fixed list of four pages (?view=) and, for
// the Alerts page, the lines this console judges by.
//
// Alerts reads ONE document, every 30 s (the app's ticker):
//   GET /api/v1/kv/queen.console/settings   { found, value, version, updatedAt }
// and saves it whole with a compare-and-set on the version it read:
//   PUT /api/v1/kv/queen.console/settings   { value, forever: true, expect }
// exactly as stores/settingsStore.js does, but quiet (the page says what went
// wrong in its own toast), and then asks the store to read it again so every
// page judges by the new lines. The draft is what was typed and nothing else
// (console/settings/alertLines.js), so a page switch keeps it and a save by
// somebody else moves only the fields nobody touched.
import { computed, reactive, ref, shallowRef, watch } from 'vue'
import { kv as kvApi } from '@/api'
import { describeApiError } from '@/api/errors'
import { useAutoRefresh } from '@/composables/useRefresh'
import { gatedVerdict } from '@/composables/useGatedVerdict'
import { QUEUE_FLAGS, QUEUE_LINES, LINE_META, SETTINGS_KEY, SETTINGS_NS } from '@/composables/settingsDoc'
import { THRESHOLDS } from '@/composables/useSeverity'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { useQueuesStore } from '@/stores/queuesStore'
import { useGroupsStore } from '@/stores/groupsStore'
import { useQueueOpsStore } from '@/stores/queueOpsStore'
import { useSettingsStore } from '@/stores/settingsStore'
import { toast } from './toasts'
import {
  WORDS, lineErrors, lineText, linesForDoc, nextDocument, overviewMoves, parseLine, putCall,
  queueNow, readDraft, sideText, tenantGroups,
} from './settings/alertLines'

export const VIEWS = Object.freeze([
  { id: 'access', label: 'Access' },
  { id: 'alerts', label: 'Alerts' },
  { id: 'users', label: 'Users and tenants', cell: true },
  { id: 'audit', label: 'Audit log' },
])
export const DEFAULT_VIEW = 'alerts'

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const two = (n) => String(n).padStart(2, '0')

/** "3 Sep", or "3 Sep 2025" in another year, local time; '–' for no date. */
export function dayMonth(v) {
  const t = v ? new Date(v) : null
  if (!t || Number.isNaN(t.getTime())) return '–'
  const year = t.getFullYear() === new Date().getFullYear() ? '' : ` ${t.getFullYear()}`
  return `${t.getDate()} ${MONTHS[t.getMonth()]}${year}`
}

/** "21 Oct at 07:54", local time. */
export function when(iso) {
  const t = iso ? new Date(iso) : null
  if (!t || Number.isNaN(t.getTime())) return ''
  return `${dayMonth(t)} at ${two(t.getHours())}:${two(t.getMinutes())}`
}

export function useAlertLines(where) {
  const id = useIdentity()
  const qs = useQueuesStore()
  const gs = useGroupsStore()
  const os = useQueueOpsStore()
  const ss = useSettingsStore()

  const doc = shallowRef(null) // { value, version, updatedAt }
  const state = ref('unread') // unread | ready | off | error
  const verdict = ref(null)
  const error = ref(null)
  const draft = reactive({ tenant: {}, queues: {} })

  async function read() {
    const epoch = currentEpoch()
    try {
      const r = await kvApi.get(SETTINGS_NS, SETTINGS_KEY, { quiet: true, probe: true })
      if (epoch !== currentEpoch()) return
      const d = r?.data || {}
      const found = d.found !== false && d.value !== undefined && d.value !== null
      doc.value = { value: found ? d.value : null, version: found ? Number(d.version) || 0 : 0, updatedAt: found ? d.updatedAt || null : null }
      state.value = 'ready'; verdict.value = null; error.value = null
    } catch (e) {
      if (epoch !== currentEpoch()) return
      const v = gatedVerdict(e)
      error.value = describeApiError(e).replace(/[.\s]+$/, '')
      if (v === 'transient') { state.value = doc.value ? 'ready' : 'error' } else { doc.value = null; state.value = 'off'; verdict.value = v }
    }
  }
  useAutoRefresh(read)
  read()

  function discard() {
    for (const k of Object.keys(draft.tenant)) delete draft.tenant[k]
    for (const k of Object.keys(draft.queues)) delete draft.queues[k]
  }
  // Another cluster is another document: what was typed belonged to the old one.
  watch(id.epoch, () => { discard(); doc.value = null; state.value = 'unread'; error.value = null; read() })

  const raw = computed(() => doc.value?.value ?? null)
  const r = computed(() => readDraft(raw.value, draft))
  const changed = computed(() => new Set(r.value.rows.map((x) => `${x.scope}\u0000${x.key}`)))
  const isChanged = (scope, key) => changed.value.has(`${scope}\u0000${key}`)
  const canEdit = computed(() => id.can('queueAdmin') && state.value === 'ready')

  // ---- the tenant's lines, under their headings ----
  const groups = computed(() => tenantGroups().map((g) => ({
    ...g,
    rows: g.lines.map((meta) => {
      const stored = r.value.settings.defaults[meta.key]
      const typed = Object.hasOwn(draft.tenant, meta.key)
      return {
        key: meta.key, meta, words: WORDS[meta.key],
        text: typed ? draft.tenant[meta.key] : lineText(meta, stored),
        own: stored !== undefined,
        builtIn: lineText(meta, THRESHOLDS[meta.key]),
        inForce: lineText(meta, r.value.tenant.inForce[meta.key]),
        changed: isChanged('', meta.key),
        was: stored !== undefined ? lineText(meta, stored) : lineText(meta, THRESHOLDS[meta.key]),
        error: r.value.tenant.errors[meta.key] || '',
      }
    }),
  })))

  // ---- the queues with lines of their own ----
  const queuesRead = computed(() => (qs.queues.value || []).length > 0)
  const queueRow = (name) => (qs.queues.value || []).find((q) => q.name === name) || null
  const linesNow = computed(() => linesForDoc(raw.value))
  const blocks = computed(() => r.value.queues.map((q) => {
    const typed = draft.queues[q.name] || {}
    const rows = []
    for (const meta of QUEUE_LINES) {
      const stored = q.stored[meta.key]
      const isTyped = Object.hasOwn(typed, meta.key)
      if (stored === undefined && !isTyped) continue
      const text = isTyped ? String(typed[meta.key] ?? '') : lineText(meta, stored)
      rows.push({
        kind: 'line', key: meta.key, meta, words: WORDS[meta.key], text,
        inherit: lineText(meta, r.value.tenant.inForce[meta.key]),
        added: stored === undefined,
        removed: stored !== undefined && isTyped && text.trim() === '',
        changed: isChanged(q.name, meta.key),
        was: stored !== undefined ? lineText(meta, stored) : '',
        error: q.errors[meta.key] || '',
      })
    }
    for (const f of QUEUE_FLAGS) {
      const stored = q.stored[f.key] === true
      const isTyped = Object.hasOwn(typed, f.key)
      if (!stored && !isTyped) continue
      rows.push({
        kind: 'flag', key: f.key, words: WORDS[f.key], help: f.help,
        on: isTyped ? typed[f.key] === true : stored,
        added: !stored, changed: isChanged(q.name, f.key),
      })
    }
    const row = queueRow(q.name)
    return {
      name: q.name, rows,
      missing: queuesRead.value && !row,
      now: queueNow(row, gs.groups.value, linesNow.value, os.ops.value),
    }
  }).filter((b) => b.rows.length))

  // ---- typing ----
  function setTenant(key, text) { draft.tenant[key] = text }
  function setQueue(queue, key, v) {
    if (!draft.queues[queue]) draft.queues[queue] = {}
    draft.queues[queue][key] = v
  }
  function forget(queue, key) {
    const t = draft.queues[queue]
    if (!t) return
    delete t[key]
    if (!Object.keys(t).length) delete draft.queues[queue]
  }
  /** On leaving a field: a line it can read is written back the way it is shown ("90" → "90 s"). */
  function tidy(queue, key) {
    const meta = LINE_META[key]
    const t = queue ? draft.queues[queue] : draft.tenant
    if (!meta || !t || !Object.hasOwn(t, key)) return
    const v = parseLine(meta, t[key])
    if (typeof v === 'number' && Number.isFinite(v)) t[key] = lineText(meta, v)
  }
  /** Remove a queue's own line: blank (the tenant's line again), or gone if it was only typed. */
  function remove(queue, row) {
    if (row.added) forget(queue, row.key)
    else setQueue(queue, row.key, row.kind === 'flag' ? false : '')
  }
  function keep(queue, key) { forget(queue, key) }

  const typedAnything = computed(() => Object.keys(draft.tenant).length > 0 || Object.keys(draft.queues).length > 0)
  const count = computed(() => r.value.rows.length)
  const bad = computed(() => r.value.bad)
  const note = computed(() => {
    if (bad.value) return bad.value === 1 ? 'One line cannot be saved as typed.' : `${bad.value} lines cannot be saved as typed.`
    if (count.value) return `${count.value === 1 ? 'One change' : `${count.value} changes`}, not saved yet.`
    return 'No changes.'
  })

  // ---- add a line for one queue (nothing is sent until Save) ----
  const adding = reactive({ open: false, queue: '', key: 'lagWarnSeconds', text: '' })
  function openAdd(queue = '') {
    adding.queue = queue
    adding.key = 'lagWarnSeconds'
    adding.text = ''
    adding.open = true
  }
  const addCheck = computed(() => {
    const queue = adding.queue.trim()
    const key = adding.key
    const isFlag = QUEUE_FLAGS.some((f) => f.key === key)
    const meta = LINE_META[key]
    const block = r.value.queues.find((q) => q.name === queue)
    const inherit = meta ? lineText(meta, r.value.tenant.inForce[key]) : ''
    const out = { queue, key, isFlag, inherit, hint: '', tone: '', blocked: true, value: null }
    if (!queue) return out
    if (/\s/.test(queue)) return { ...out, hint: 'A queue name has no spaces.', tone: 'red' }
    const typed = draft.queues[queue] || {}
    const ownNow = block && (isFlag ? block.flags[key] === true || Object.hasOwn(typed, key) : (block.values[key] !== undefined || Object.hasOwn(typed, key)))
    if (ownNow) return { ...out, hint: `${queue} already has this line. Change it in the list.`, tone: '' }
    const absent = queuesRead.value && !queueRow(queue) ? `There is no queue named ${queue} on ${where.value} yet; the line waits for it.` : ''
    if (isFlag) return { ...out, hint: absent, blocked: false, value: true }
    const v = parseLine(meta, adding.text)
    if (v === null) return { ...out, hint: absent }
    const errors = lineErrors(r.value.tenant.inForce, { ...(block?.values || {}), [key]: v }, true, new Set([key]))
    if (errors[key]) return { ...out, hint: errors[key], tone: 'red' }
    return { ...out, hint: absent, blocked: false, value: v }
  })
  function confirmAdd() {
    const c = addCheck.value
    if (c.blocked) return
    setQueue(c.queue, c.key, c.isFlag ? true : lineText(LINE_META[c.key], c.value))
    adding.open = false
  }
  const addOptions = [...QUEUE_LINES.map((m) => ({ value: m.key, label: WORDS[m.key].label })), ...QUEUE_FLAGS.map((f) => ({ value: f.key, label: WORDS[f.key].label }))]
  const queueChoices = computed(() => (qs.queues.value || []).map((q) => q.name).filter(Boolean).sort((a, b) => a.localeCompare(b)))

  // ---- save: what changes, the call, then the PUT ----
  const asking = ref(false)
  const busy = ref(false)
  const next = computed(() => nextDocument(raw.value, r.value))
  const body = computed(() => ({ value: next.value, forever: true, expect: doc.value?.version || 0 }))
  const put = computed(() => putCall(body.value))
  const diff = computed(() => {
    const out = []
    for (const row of r.value.rows) {
      let g = out.find((x) => x.scope === row.scope)
      if (!g) { g = { scope: row.scope, rows: [] }; out.push(g) }
      g.rows.push({ ...row, was: sideText(row, 'before', r.value), now: sideText(row, 'after', r.value) })
    }
    return out.sort((a, b) => (a.scope === '' ? -1 : b.scope === '' ? 1 : a.scope.localeCompare(b.scope)))
  })
  const moves = computed(() => (asking.value
    ? overviewMoves(qs.queues.value, gs.groups.value, os.ops.value, raw.value, next.value)
    : []))
  function openSave() { if (count.value && !bad.value && canEdit.value) asking.value = true }

  async function save() {
    if (busy.value || !count.value || bad.value) return
    busy.value = true
    const epoch = currentEpoch()
    const n = count.value
    const sent = body.value
    try {
      const res = await kvApi.put(SETTINGS_NS, SETTINGS_KEY, sent, { quiet: true })
      if (epoch !== currentEpoch()) return
      const d = res?.data || {}
      if (d.applied === false) {
        toast('Not saved: somebody saved first', 'The page now shows what they saved. What you typed is still in its fields; check it and save again.', 'red')
      } else {
        doc.value = { value: sent.value, version: Number(d.version) || doc.value?.version || 0, updatedAt: new Date().toISOString() }
        discard()
        toast(`Saved ${n === 1 ? 'one change' : `${n} changes`} on ${where.value}`,
          `The broker answered ${res?.status || 200}${d.version ? `, version ${d.version}` : ''}. Every page judges by these from its next read, within a minute.`)
      }
      asking.value = false
      await read()
      ss.fetchSettings?.({ force: true })?.catch?.(() => {})
    } catch (e) {
      toast('Not saved', describeApiError(e), 'red')
    } finally {
      busy.value = false
    }
  }

  return {
    doc, state, verdict, error, draft, canEdit, read,
    groups, blocks, queueChoices,
    setTenant, setQueue, tidy, remove, keep, discard,
    typedAnything, count, bad, note,
    adding, addCheck, addOptions, openAdd, confirmAdd,
    asking, busy, put, body, diff, moves, openSave, save,
  }
}
