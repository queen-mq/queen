<template>
  <div class="chg">
    <!-- header row (column labels) -->
    <div class="chead">
      <div></div>
      <div class="h-name">Consumer group</div>
      <div class="h-c">Partitions</div>
      <div class="h-c">Lagging</div>
      <div class="h-c">Time lag</div>
      <div></div>
    </div>

    <!-- skeleton -->
    <template v-if="loading && !consumers.length">
      <div v-for="i in 8" :key="`s-${i}`" class="crow crow-skeleton">
        <span></span>
        <span class="skeleton" style="height: 12px; width: 60%;"></span>
        <span class="skeleton" style="height: 18px; width: 40px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 50px; margin: 0 auto;"></span>
        <span class="skeleton" style="height: 18px; width: 60px; margin: 0 auto;"></span>
        <span></span>
      </div>
    </template>

    <!-- empty state -->
    <div v-else-if="!consumers.length">
      <!-- The slot owns the whole block, `.empty-state` padding included: a
           wrapper that padded too meant the host view's empty state sat in
           96px of air while every other empty state in the app sat in 48. -->
      <slot name="empty">
        <div class="empty-state">
          <h3>No consumer groups match your filters</h3>
        </div>
      </slot>
    </div>

    <!-- rows -->
    <template v-else>
      <div
        v-for="g in displayed"
        :key="`${g.name}@${g.queueName}`"
        class="crow"
        :class="`sev-${cardSev(g)}`"
        @click="$emit('select', g)"
      >
        <span class="cdot"></span>

        <span class="cname">
          <!-- Queue mode: queue name leads (the meaningful identifier);
               named groups: group name leads with queue as subtitle. -->
          <span class="cname-tag" v-if="g.name === '__QUEUE_MODE__'">qmode</span>
          <span class="nm">{{ primaryName(g) }}</span>
          <span class="ns" v-if="g.name !== '__QUEUE_MODE__'"> · {{ g.queueName }}</span>
        </span>

        <span class="cell-c">
          <!-- "members" in the API = partitions assigned to this consumer group;
               the column reflects the real meaning, not the API field name. -->
          <span class="cc sev-mute">
            {{ fmt(g.members || 0) }}<i>parts</i>
          </span>
        </span>

        <span class="cell-c">
          <span
            v-if="(g.partitionsWithLag || 0) > 0"
            class="cc"
            :class="`sev-${lagPartsSev(g)}`"
          >
            {{ fmt(g.partitionsWithLag) }}<i>behind</i>
          </span>
          <span v-else class="cc-empty">—</span>
        </span>

        <span class="cell-c">
          <span
            v-if="(g.maxTimeLag || 0) > 0"
            class="cc"
            :class="`sev-${timeLagSev(g.maxTimeLag)}`"
          >
            {{ fmtLag(g.maxTimeLag) }}
          </span>
          <span v-else class="cc-empty">—</span>
        </span>

        <span class="cell-c cactions">
          <button
            class="caction"
            title="View details"
            @click.stop="$emit('view', g)"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z"/>
              <path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"/>
            </svg>
          </button>
          <button
            v-if="g.queueName && props.canAdmin"
            class="caction"
            title="Move cursor to now (skip pending)"
            @click.stop="$emit('move-now', g)"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M3 8.688c0-.864.933-1.405 1.683-.977l7.108 4.062a1.125 1.125 0 010 1.953l-7.108 4.062A1.125 1.125 0 013 16.81V8.688zM12.75 8.688c0-.864.933-1.405 1.683-.977l7.108 4.062a1.125 1.125 0 010 1.953l-7.108 4.062a1.125 1.125 0 01-1.683-.977V8.688z"/>
            </svg>
          </button>
          <button
            v-if="g.queueName && props.canAdmin"
            class="caction"
            title="Seek to timestamp"
            @click.stop="$emit('seek', g)"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z"/>
            </svg>
          </button>
          <button
            v-if="props.canAdmin"
            class="caction caction-danger"
            title="Delete consumer group"
            @click.stop="$emit('delete', g)"
          >
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
              <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0"/>
            </svg>
          </button>
        </span>
      </div>
    </template>
  </div>
</template>

<script setup>
import { computed } from 'vue'

const props = defineProps({
  /**
   * Consumer-group rows. Each entry should expose:
   *   { name, queueName, members, maxTimeLag, partitionsWithLag, state }
   * `name === '__QUEUE_MODE__'` indicates the synthetic queue-mode group;
   * the grid surfaces the queue name as the lead in that case.
   */
  consumers: { type: Array, default: () => [] },
  loading: { type: Boolean, default: false },
  /** Sort key. One of: 'health' | 'name' | 'lag' | 'members' */
  sortBy: { type: String, default: 'health' },
  /** Optional cap on rendered rows. */
  limit: { type: Number, default: null },
  /**
   * Whether the caller's role may mutate cursors on this cluster
   * (identity `can('queueAdmin')`). False hides move-now / seek / delete —
   * the proxy classifies all three as QueueAdmin and would 403 them.
   */
  canAdmin: { type: Boolean, default: true },
})

defineEmits(['select', 'view', 'move-now', 'seek', 'delete'])

/* ---------------- severity rules ----------------
 * cardSev drives the leading status stripe + dot. We combine the API
 * `state` field with the actual `maxTimeLag` so a "Lagging" row escalates
 * from warn to bad once it crosses the 5-minute threshold. Dead groups
 * are mute (informational, not an active alert). */
const SEV_RANK = { ok: 0, ice: 0, mute: 1, warn: 2, bad: 3 }

function cardSev(g) {
  const state = g.state
  const lag = g.maxTimeLag || 0
  const hasLag = lag >= 60 || (g.partitionsWithLag || 0) > 0
  if (state === 'Dead') return 'mute'
  if (state === 'Lagging' && lag >= 300) return 'bad'
  if (state === 'Lagging' || hasLag) return 'warn'
  return 'ok'  // Stable
}

function timeLagSev(seconds) {
  if (!seconds) return 'mute'
  if (seconds < 60) return 'ok'
  if (seconds < 300) return 'warn'
  return 'bad'
}

function lagPartsSev(g) {
  const n = g.partitionsWithLag || 0
  if (n === 0) return 'mute'
  if (n < 3) return 'warn'
  return 'bad'
}

/* ---------------- naming -----------------
 * Queue-mode groups have a synthetic `__QUEUE_MODE__` name that's pure
 * noise to the operator — the meaningful identifier is the queue. Named
 * groups put the group name first with the queue as muted subtitle. */
const primaryName = (g) => g.name === '__QUEUE_MODE__' ? g.queueName : g.name

/* ---------------- formatters ---------------- */
function fmt(n) {
  if (n === null || n === undefined) return '—'
  if (n >= 1e6) return (n / 1e6).toFixed(n >= 1e7 ? 0 : 1) + 'M'
  if (n >= 1e3) return (n / 1e3).toFixed(n >= 1e4 ? 0 : 1) + 'k'
  return String(n)
}
function fmtLag(seconds) {
  if (!seconds || seconds < 1) return '0s'
  if (seconds < 60)    return Math.round(seconds) + 's'
  if (seconds < 3600)  {
    const m = Math.floor(seconds / 60)
    const s = Math.round(seconds % 60)
    return s ? `${m}m ${s}s` : `${m}m`
  }
  if (seconds < 86400) {
    const h = Math.floor(seconds / 3600)
    const m = Math.floor((seconds % 3600) / 60)
    return m ? `${h}h ${m}m` : `${h}h`
  }
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  return h ? `${d}d ${h}h` : `${d}d`
}

/* ---------------- sort / limit ---------------- */
const displayed = computed(() => {
  const sorted = [...props.consumers]

  if (props.sortBy === 'health') {
    sorted.sort((a, b) =>
      SEV_RANK[cardSev(b)] - SEV_RANK[cardSev(a)] ||
      (b.maxTimeLag || 0) - (a.maxTimeLag || 0)
    )
  } else if (props.sortBy === 'name') {
    sorted.sort((a, b) => primaryName(a).localeCompare(primaryName(b)))
  } else if (props.sortBy === 'lag') {
    sorted.sort((a, b) => (b.maxTimeLag || 0) - (a.maxTimeLag || 0))
  } else if (props.sortBy === 'members') {
    sorted.sort((a, b) => (b.members || 0) - (a.members || 0))
  }

  return props.limit ? sorted.slice(0, props.limit) : sorted
})
</script>

<style scoped>
.chg {
  background: var(--ink-2);
  border: 1px solid var(--bd);
  border-radius: var(--r-card);
  overflow: hidden;
}

/* column header */
.chead {
  display: grid;
  grid-template-columns: 14px minmax(180px, 1fr) 80px 80px 86px 100px;
  gap: 10px;
  align-items: center;
  padding: 0 12px 0 0;
  height: 28px;
  background: color-mix(in srgb, var(--text-hi) 1.2%, transparent);
  border-bottom: 1px solid var(--bd);
  font-size: 9.5px;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--text-low);
  font-weight: 500;
}
.chead .h-name { padding-left: 28px; }
.chead .h-c { text-align: center; }

/* row */
.crow {
  position: relative;
  display: grid;
  grid-template-columns: 14px minmax(180px, 1fr) 80px 80px 86px 100px;
  gap: 10px;
  align-items: center;
  padding: 0 12px 0 0;
  height: 26px;
  border-bottom: 1px solid var(--bd);
  cursor: pointer;
  transition: background .12s ease;
  font-size: 11.5px;
}
.crow:last-child { border-bottom: none; }
.crow:hover { background: color-mix(in srgb, var(--text-hi) 2.5%, transparent); }

/* left status stripe — same idiom as QueueHealthGrid so the two pages
   read as siblings in the same visual system. */
.crow::before {
  content: '';
  position: absolute;
  left: 0; top: 0; bottom: 0;
  width: 3px;
  background: var(--bd-hi);
}
.crow.sev-ok::before { background: var(--ok-500); }
.crow.sev-mute::before { background: var(--bd-hi); }
.crow.sev-warn::before { background: var(--warn-400); }
.crow.sev-bad::before {
  background: var(--ember-400);
  box-shadow: 1px 0 12px color-mix(in srgb, var(--ember-500) 55%, transparent);
}
.crow.sev-bad { background: color-mix(in srgb, var(--ember-500) 3%, transparent); }
.crow.sev-warn { background: color-mix(in srgb, var(--warn-400) 2%, transparent); }
.crow.sev-mute { opacity: 0.78; }

.crow-skeleton { cursor: default; }
.crow-skeleton::before { background: var(--bd-hi) !important; box-shadow: none !important; }
.crow-skeleton:hover { background: transparent; }

/* status dot */
.cdot {
  width: 6px; height: 6px;
  border-radius: var(--r-pill);
  background: var(--bd-hi);
  margin-left: 10px;
  position: relative;
  flex-shrink: 0;
}
.crow.sev-ok .cdot { background: var(--ok-500); }
.crow.sev-warn .cdot { background: var(--warn-400); }
.crow.sev-bad .cdot { background: var(--ember-400); }
.crow.sev-bad .cdot::after {
  content: '';
  position: absolute; inset: -2px;
  border-radius: var(--r-pill);
  background: var(--ember-400);
  opacity: .25;
  animation: chg-pulse 2.4s ease-out infinite;
}
@keyframes chg-pulse {
  0% { transform: scale(.6); opacity: .35; }
  100% { transform: scale(2.0); opacity: 0; }
}

/* name cell */
.cname {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-hi);
  letter-spacing: -0.005em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  display: inline-flex;
  align-items: baseline;
  gap: 6px;
}
.cname .nm { color: var(--text-hi); }
.cname .ns { color: var(--text-low); font-weight: 400; }
.cname-tag {
  display: inline-block;
  padding: 1px 5px;
  border-radius: var(--r-chip);
  border: 1px solid var(--bd);
  background: var(--ink-3);
  color: var(--text-low);
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 9px;
  font-weight: 600;
  letter-spacing: .08em;
  text-transform: uppercase;
  flex-shrink: 0;
}

/* cell wrapper to align chip in column center */
.cell-c { text-align: center; }

/* compact chip — borrowed verbatim from QueueHealthGrid so the two
   pages share one visual vocabulary for severity-toned numbers. */
.cc {
  display: inline-flex;
  align-items: baseline;
  justify-content: center;
  padding: 2px 7px;
  border-radius: var(--r-control);
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-variant-numeric: tabular-nums;
  font-size: 11px;
  font-weight: 600;
  line-height: 1.4;
  border: 1px solid transparent;
  white-space: nowrap;
  min-width: 52px;
  letter-spacing: 0;
}
.cc i {
  font-style: normal;
  opacity: .55;
  font-weight: 500;
  margin-left: 4px;
  font-size: 9.5px;
}
/* Fill + border are the status tokens, same as QueueHealthGrid — the two
   grids must never drift apart on a palette change. */
.cc.sev-mute { background: var(--ink-3); color: var(--text-mid); border-color: var(--bd); }
.cc.sev-ok   { background: var(--ok-glow); color: var(--ok-500); border-color: var(--ok-bd); }
.cc.sev-warn { background: var(--warn-glow); color: var(--warn-400); border-color: var(--warn-bd); }
.cc.sev-bad  { background: var(--ember-glow); color: var(--ember-400); border-color: var(--ember-bd); }
.cc-empty {
  font-family: 'JetBrains Mono', ui-monospace, monospace;
  font-size: 11px;
  color: var(--text-low);
}

/* row actions — hover-only, like QueueHealthGrid. Four buttons (view,
   move-to-now, seek, delete) instead of one, but same overall shape:
   small square buttons with subtle hover states; only the trash icon
   uses the danger tone. */
.cactions {
  display: inline-flex;
  align-items: center;
  justify-content: flex-end;
  gap: 2px;
  opacity: 0;
  transition: opacity .12s ease;
}
.crow:hover .cactions,
.crow:focus-within .cactions { opacity: 1; }
.caction {
  width: 22px; height: 22px;
  display: grid;
  place-items: center;
  background: transparent;
  border: 1px solid transparent;
  border-radius: var(--r-control);
  color: var(--text-low);
  cursor: pointer;
}
.caction:hover {
  color: var(--text-hi);
  border-color: var(--bd-hi);
  background: var(--ink-3);
}
.caction-danger:hover {
  color: var(--ember-400);
  border-color: var(--ember-bd);
  background: color-mix(in srgb, var(--ember-500) 6%, transparent);
}
.caction svg { width: 12px; height: 12px; }

/* Empty state: shared `.empty-state` block from style.css, supplied by the
   host view through the #empty slot. Nothing local — the wrapper must not
   pad, or the block pads twice. */

/* Responsive: drop the Partitions column first at narrow widths since
   total partition count is the least operationally urgent of the three
   metrics — the operator cares about lag count + lag duration first. */
@media (max-width: 720px) {
  .chead, .crow {
    grid-template-columns: 14px minmax(150px, 1fr) 80px 86px 96px;
  }
  .chead > :nth-child(3),
  .crow > :nth-child(3) { display: none; }
}
</style>
