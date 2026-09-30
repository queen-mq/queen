<template>
  <!--
    The tenant's partitions as the seeds of a sunflower. The golden angle
    packs any number of points evenly in a disc, which is the whole reason
    for the shape: the same drawing holds 3 partitions or 1,000,000.

    Up to MAX_SEEDS partitions get one seed each. Above that, every queue
    gets seeds in proportion to its partition count (at least one), and each
    seed stands for a slice of that queue — the caption says how many
    partitions one seed is.

    Each queue owns a wedge of the flower: the disc's points are dealt out by
    angle, clockwise from twelve o'clock in queue-name order, so a queue keeps
    its place across refreshes. Every wedge is drawn a little out from the
    centre along its middle, like an exploded pie, so the flower reads as its
    queues without a colour per queue (up to MAX_WEDGES queues). Inside a wedge the fullest seeds sit
    nearest the centre. Colour stays the queue's verdict from the page
    (warn / bad) and nothing else. Hovering a seed lights its queue and names
    it.

    Sizes: with `partitions` (/resources/partitions, every partition's own
    pending and lag) a seed is one real partition. Without it — an older
    broker, or more partitions than seeds — /resources/queues only gives a
    queue's partition count and pending total, so a seed's size is its queue's
    pending PER PARTITION, and the caption says so.
  -->
  <div class="sf">
    <div v-if="loading" class="sf-body"><span class="skeleton sf-skeleton" /></div>
    <div v-else-if="error" class="sf-body sf-msg">Partitions unavailable · {{ errorText }}</div>
    <div v-else-if="!plan.seeds.length" class="sf-body sf-msg">No partitions yet</div>
    <template v-else>
      <div ref="body" class="sf-body">
        <svg
          :viewBox="`0 0 ${SIZE} ${SIZE}`"
          role="img"
          :aria-label="ariaLabel"
          :class="{ focusing: focusQueue !== null }"
        >
          <circle
            v-for="s in plan.seeds"
            :key="s.key"
            class="sf-seed"
            :class="[s.sev === 'ok' ? '' : s.sev, { dim: focusQueue !== null && s.queue !== focusQueue, on: hover?.key === s.key }]"
            :cx="s.x" :cy="s.y" :r="s.r"
            @mouseenter="enter(s, $event)"
            @mouseleave="leave"
            @click="open(s)"
          />
        </svg>
        <div v-if="hover" class="sf-tip" :style="tipStyle" role="tooltip">
          <div class="sf-tip-head">{{ hover.head }}</div>
          <div v-if="hover.sub" class="sf-tip-sub">{{ hover.sub }}</div>
          <div class="sf-tip-line">{{ hover.line }}</div>
        </div>
      </div>
      <p class="sf-foot">{{ caption }}</p>
    </template>
  </div>
</template>

<script>
// A Fibonacci number, like the florets of the flower it is named after. It is
// also where seeds stop being legible at this size: 610 seeds are ~3px each.
// Exported so a page asks for per-partition rows only while each seed is one.
export const MAX_SEEDS = 610
</script>

<script setup>
import { computed, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { formatNumber, formatDuration } from '@/composables/useApi'
import { describeApiError } from '@/api/errors'

const props = defineProps({
  // [{ name, partitions, pending, sev: 'ok' | 'warn' | 'bad', lag? }]
  // In 'partitions' mode each entry IS a partition (partitions: 1) and its
  // pending is that partition's own — real per-partition data; `lag` is the
  // age in seconds of its oldest unconsumed message (null: caught up).
  queues: { type: Array, default: () => [] },
  // Queues mode only: /resources/partitions rows, [{ queue, partition,
  // pending, lagSeconds }] for every partition. Null draws seeds per queue.
  partitions: { type: Array, default: null },
  mode: { type: String, default: 'queues' }, // 'queues' | 'partitions'
  queueName: { type: String, default: '' },  // partitions mode: the queue they belong to
  loading: { type: Boolean, default: false },
  error: { type: [Object, String], default: null },
})
const perPartition = computed(() => props.mode === 'partitions')

const router = useRouter()
const SIZE = 300
const GOLDEN = Math.PI * (3 - Math.sqrt(5))
const TAU = Math.PI * 2
// Pending per partition that draws a seed at full size, unless a queue is
// fuller. Keeps a tenant holding a handful of messages from drawing giant
// seeds for nothing.
const FULL_AT = 50
// Seed geometry, in units of the spacing between seeds: a seed's radius runs
// from MIN_SIZE (empty) to MAX_SIZE (full), and the spacing itself stops
// growing at SEED_SPACING, the density of a flower with ~40 seeds.
const SEED_SPACING = 22
const MIN_SIZE = 0.3
const MAX_SIZE = 0.72
const RANK = { bad: 2, warn: 1, ok: 0 }
// How far each queue's wedge is drawn out from the centre, in seed spacings:
// enough that the ground between two wedges is wider than between two seeds.
// Past MAX_WEDGES a wedge is a sliver and the flower stays whole.
const EXPLODE = 1.6
const MAX_WEDGES = 24

const errorText = computed(() =>
  typeof props.error === 'string' ? props.error : describeApiError(props.error)
)

const plural = (n, one, many) => `${formatNumber(n)} ${n === 1 ? one : many}`

// Most pending first; the verdict breaks ties so a failing queue is never
// pushed to the rim by a busier healthy one.
const byWeight = (a, b) =>
  b.value - a.value || RANK[b.sev] - RANK[a.sev] || a.queue.localeCompare(b.queue) ||
  String(a.partition ?? '').localeCompare(String(b.partition ?? ''))

/** Seeds per queue when there are more partitions than seeds. */
function sliceUnits(shown) {
  const shownPartitions = shown.reduce((s, q) => s + q.partitions, 0)
  // One per partition while it fits; otherwise largest-remainder shares of
  // MAX_SEEDS, at least one each.
  let alloc
  if (shownPartitions <= MAX_SEEDS) {
    alloc = shown.map(q => ({ q, n: q.partitions }))
  } else {
    const exact = shown.map(q => (q.partitions / shownPartitions) * MAX_SEEDS)
    alloc = shown.map((q, i) => ({ q, n: Math.max(1, Math.floor(exact[i])), rem: exact[i] - Math.floor(exact[i]) }))
    let used = alloc.reduce((s, a) => s + a.n, 0)
    const byRem = [...alloc].sort((a, b) => b.rem - a.rem)
    for (let i = 0; used < MAX_SEEDS && i < byRem.length; i++, used++) byRem[i].n += 1
    const byN = [...alloc].sort((a, b) => b.n - a.n)
    for (let i = 0; used > MAX_SEEDS; i = (i + 1) % byN.length) {
      if (byN[i].n > 1) { byN[i].n -= 1; used -= 1 }
    }
  }
  const units = []
  for (const { q, n } of alloc) {
    for (let k = 0; k < n; k++) {
      units.push({ key: `${q.name}#${k}`, queue: q.name, partition: null, sev: q.sev || 'ok', value: q.pending / q.partitions, slice: q.partitions / n, q })
    }
  }
  return units
}

const plan = computed(() => {
  const qs = props.queues
    .map(q => ({ ...q, partitions: Math.max(0, Math.round(q.partitions || 0)), pending: Math.max(0, q.pending || 0) }))
    .filter(q => q.partitions > 0)
  const totalPartitions = qs.reduce((s, q) => s + q.partitions, 0)
  if (!totalPartitions) return { seeds: [], totalPartitions: 0, perSeed: 1, trimmedQueues: 0, real: false, wedges: 0, exploded: false }

  // Which queues get seeds: all of them, unless there are more queues than
  // seeds, in which case the ones holding the most work.
  let shown = qs
  let trimmedQueues = 0
  if (qs.length > MAX_SEEDS) {
    shown = [...qs].sort((a, b) => b.pending - a.pending || b.partitions - a.partitions).slice(0, MAX_SEEDS)
    trimmedQueues = qs.length - MAX_SEEDS
  }

  let units
  let real = false
  if (perPartition.value) {
    units = shown.map(q => ({
      key: q.name, queue: props.queueName, partition: q.name, sev: q.sev || 'ok',
      value: q.pending, pending: q.pending, lag: q.lag, slice: 1, q,
    }))
    real = true
  } else if (props.partitions && totalPartitions <= MAX_SEEDS) {
    // One seed per real partition. A queue the rows do not cover (created
    // between the two reads) keeps per-queue seeds until the next refresh.
    const byQueue = new Map(shown.map(q => [q.name, { q, rows: [] }]))
    for (const p of props.partitions) byQueue.get(p.queue)?.rows.push(p)
    units = []
    for (const { q, rows } of byQueue.values()) {
      if (!rows.length) { units.push(...sliceUnits([q])); continue }
      for (const p of rows) {
        const pending = Math.max(0, p.pending || 0)
        units.push({
          key: `${q.name}#${p.partition}`, queue: q.name, partition: String(p.partition), sev: q.sev || 'ok',
          value: pending, pending, lag: p.lagSeconds, slice: 1, q,
        })
      }
    }
    real = units.every(u => u.partition !== null)
  } else {
    units = sliceUnits(shown)
  }

  const n = units.length
  const names = [...new Set(units.map(u => u.queue))].sort((a, b) => a.localeCompare(b))
  const wedges = perPartition.value ? 1 : names.length
  const explode = wedges > 1 && wedges <= MAX_WEDGES ? EXPLODE : 0

  // The spacing that fills the disc, never wider than a seed's natural
  // spacing: three partitions are a small head of three seeds in the middle,
  // not three discs pushed out past the edge. The divisor keeps the outermost
  // seed, at its largest, inside the drawing.
  const c = Math.min(SEED_SPACING, (SIZE / 2 - 2) / (Math.sqrt(Math.max(n - 0.5, 0.5)) + MAX_SIZE + explode))
  const maxV = Math.max(FULL_AT, ...units.map(u => u.value))

  // Point i of the flower: the golden angle, and a radius that grows with
  // the square root so every seed has the same area around it.
  const point = (i) => ({ i, r: n === 1 ? 0 : c * Math.sqrt(i + 0.5), a: i * GOLDEN, dx: 0, dy: 0 })

  // One wedge per queue: deal the points out by angle, clockwise from twelve
  // o'clock (screen y points down, so a growing angle turns clockwise), in
  // queue-name order; inside a wedge the heaviest seeds take the innermost
  // points, and the whole wedge moves out along its middle. A single queue
  // is one flower, heaviest at the centre.
  const placed = []
  if (wedges > 1) {
    const clock = (a) => (((a + Math.PI / 2) % TAU) + TAU) % TAU
    const around = Array.from({ length: n }, (_, i) => point(i)).sort((p, q) => clock(p.a) - clock(q.a))
    const groups = new Map(names.map(name => [name, []]))
    for (const u of units) groups.get(u.queue).push(u)
    let at = 0
    for (const name of names) {
      const members = groups.get(name).sort(byWeight)
      const slice = around.slice(at, at + members.length)
      // The wedge's middle: halfway between its first and last point.
      const mid = (clock(slice[0].a) + clock(slice[slice.length - 1].a)) / 2 - Math.PI / 2
      const dx = explode * c * Math.cos(mid)
      const dy = explode * c * Math.sin(mid)
      const pts = slice.sort((p, q) => p.i - q.i)
      at += members.length
      members.forEach((u, k) => placed.push({ u, p: { ...pts[k], dx, dy } }))
    }
  } else {
    units.sort(byWeight).forEach((u, i) => placed.push({ u, p: point(i) }))
  }

  const seeds = placed.map(({ u, p }) => ({
    ...u,
    x: (SIZE / 2 + p.dx + p.r * Math.cos(p.a)).toFixed(2),
    y: (SIZE / 2 + p.dy + p.r * Math.sin(p.a)).toFixed(2),
    r: (c * (MIN_SIZE + (MAX_SIZE - MIN_SIZE) * Math.sqrt(u.value / maxV))).toFixed(2),
  }))
  return { seeds, totalPartitions, perSeed: totalPartitions / n, trimmedQueues, real, wedges, exploded: explode > 0 }
})

// ---------------------------------------------------------------------------
// Hover: the seed's queue lights up, the rest dim, and a tip names it.
// ---------------------------------------------------------------------------
const body = ref(null)
const hover = ref(null)
const focusQueue = computed(() => (hover.value && plan.value.wedges > 1 ? hover.value.queue : null))
// A refresh can drop the seed under the pointer.
watch(plan, (p) => {
  if (hover.value && !p.seeds.some(s => s.key === hover.value.key)) hover.value = null
})

// `lag` undefined: not known (an older broker); null: nothing unconsumed.
const lagText = (s) => {
  if (s === undefined) return ''
  if (s === null) return ' · caught up'
  return ` · lag ${s < 1 ? 'under 1s' : formatDuration(s * 1000)}`
}

function describe(s) {
  if (s.partition !== null) {
    const line = `${formatNumber(s.pending)} pending${lagText(s.lag)}`
    return perPartition.value
      ? { head: s.partition, sub: null, line }
      : { head: s.queue, sub: `partition ${s.partition}`, line }
  }
  // Per-queue seeds: what the queue listing knows, said as such.
  return {
    head: s.queue,
    sub: s.slice === 1 ? `one of ${plural(s.q.partitions, 'partition', 'partitions')}` : `one seed ≈ ${plural(Math.round(s.slice), 'partition', 'partitions')}`,
    line: `${formatNumber(s.q.pending)} pending in the queue`,
  }
}

// Leaving a seed clears the tip a beat later, so crossing the gap to the
// next seed does not flicker it.
let leaving = null
function leave() {
  clearTimeout(leaving)
  leaving = setTimeout(() => { hover.value = null }, 90)
}
onUnmounted(() => clearTimeout(leaving))

function enter(s, ev) {
  clearTimeout(leaving)
  const box = body.value?.getBoundingClientRect()
  const dot = ev.target.getBoundingClientRect()
  if (!box) return
  hover.value = {
    key: s.key,
    queue: s.queue,
    x: dot.left + dot.width / 2 - box.left,
    y: dot.top - box.top,
    below: dot.top - box.top < 72,
    bottom: dot.bottom - box.top,
    width: box.width,
    ...describe(s),
  }
}

// Centred over the seed, kept inside the card; under it near the top edge.
const TIP_W = 168
const tipStyle = computed(() => {
  const h = hover.value
  if (!h) return {}
  const left = Math.min(Math.max(h.x - TIP_W / 2, 4), Math.max(4, h.width - TIP_W - 4))
  return h.below
    ? { left: `${left}px`, top: `${h.bottom + 8}px`, width: `${TIP_W}px` }
    : { left: `${left}px`, top: `${h.y - 8}px`, width: `${TIP_W}px`, transform: 'translateY(-100%)' }
})

// ---------------------------------------------------------------------------
// Words
// ---------------------------------------------------------------------------
const hot = computed(() => plan.value.seeds.filter(s => s.sev === 'warn' || s.sev === 'bad').length)

const caption = computed(() => {
  const p = plan.value
  if (perPartition.value) {
    const all = props.queues.length
    const head = p.trimmedQueues
      ? `The ${formatNumber(MAX_SEEDS)} of ${formatNumber(all)} partitions holding the most pending, one seed each.`
      : `${plural(all, 'partition', 'partitions')}, one seed each.`
    return `${head} Size is pending messages in the partition; colour only when this queue needs you. Click a seed for its messages.`
  }
  const head = p.perSeed <= 1
    ? `${plural(p.totalPartitions, 'partition', 'partitions')}, one seed each`
    : `${formatNumber(p.totalPartitions)} partitions in ${formatNumber(p.seeds.length)} seeds, one seed ≈ ${formatNumber(Math.round(p.perSeed))} partitions`
  const wedge = p.exploded ? ', a wedge per queue.' : '.'
  const trimmed = p.trimmedQueues ? ` Showing the ${formatNumber(MAX_SEEDS)} queues with the most pending.` : ''
  const size = p.real ? 'Size is the partition\'s pending' : 'Size is pending per partition of its queue'
  return `${head}${wedge}${trimmed} ${size}; colour only on queues that need you.`
})

const ariaLabel = computed(() =>
  `${formatNumber(plan.value.totalPartitions)} partitions drawn as sunflower seeds; ${hot.value} seeds belong to queues that need attention`
)

const open = (s) => {
  if (perPartition.value) router.push({ path: '/messages', query: { queue: props.queueName, partition: s.partition } })
  else router.push(`/queues/${encodeURIComponent(s.queue)}`)
}
</script>

<style scoped>
.sf { display: flex; flex-direction: column; min-width: 0; height: 100%; }
.sf-body { position: relative; flex: 1; display: grid; place-items: center; padding: 12px 14px 4px; min-height: 220px; }
.sf-body svg { display: block; width: 100%; max-width: 290px; height: auto; }
.sf-seed { fill: var(--sf-seed, var(--text-faint)); cursor: pointer; transition: fill .15s var(--ease), opacity .15s var(--ease); }
.sf-seed.warn { fill: var(--warn-400); }
.sf-seed.bad { fill: var(--ember-400); }
/* The hovered queue keeps its seeds; the others step back. */
.focusing .sf-seed:not(.dim):not(.warn):not(.bad) { fill: var(--text-low); }
.sf-seed.dim { opacity: .28; }
.sf-seed.on { stroke: var(--text-hi); stroke-width: 1.2; }
.sf-tip {
  position: absolute;
  z-index: 2;
  pointer-events: none;
  box-sizing: border-box;
  padding: 6px 9px;
  background: var(--ink-3);
  border: 1px solid var(--bd-hi);
  border-radius: var(--r-chip);
  font-size: 12px;
  line-height: 1.45;
  color: var(--text-mid);
}
.sf-tip-head { color: var(--text-hi); font-weight: 500; overflow-wrap: anywhere; }
.sf-tip-sub { color: var(--text-low); overflow-wrap: anywhere; }
.sf-tip-line { margin-top: 2px; font-variant-numeric: tabular-nums; }
.sf-foot { margin: 0; padding: 0 16px 14px; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.sf-msg { font-size: 12px; color: var(--text-low); text-align: center; }
.sf-skeleton { display: block; width: 220px; height: 220px; border-radius: 50%; }
</style>
