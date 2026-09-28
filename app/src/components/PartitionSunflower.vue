<template>
  <!--
    The tenant's partitions as the seeds of a sunflower. The golden angle
    packs any number of points evenly in a disc, which is the whole reason
    for the shape: the same drawing holds 3 partitions or 1,000,000.

    Up to MAX_SEEDS partitions get one seed each. Above that, every queue
    gets seeds in proportion to its partition count (at least one), and each
    seed stands for a slice of that queue — the caption says how many
    partitions one seed is. Nothing here is per-partition data: /resources/
    queues gives a queue's partition count and pending total, so a seed's
    size is its queue's pending PER PARTITION, and the caption says that too.
    Colour is the queue's verdict from the page (warn / bad) and nothing else.
  -->
  <div class="sf">
    <div v-if="loading" class="sf-body"><span class="skeleton sf-skeleton" /></div>
    <div v-else-if="error" class="sf-body sf-msg">Partitions unavailable · {{ errorText }}</div>
    <div v-else-if="!plan.seeds.length" class="sf-body sf-msg">No partitions yet</div>
    <template v-else>
      <div class="sf-body">
        <svg :viewBox="`0 0 ${SIZE} ${SIZE}`" role="img" :aria-label="ariaLabel">
          <circle
            v-for="s in plan.seeds"
            :key="s.key"
            class="sf-seed"
            :class="s.sev === 'ok' ? '' : s.sev"
            :cx="s.x" :cy="s.y" :r="s.r"
            @click="open(s.queue)"
          ><title>{{ s.title }}</title></circle>
        </svg>
      </div>
      <p class="sf-foot">{{ caption }}</p>
    </template>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { useRouter } from 'vue-router'
import { formatNumber } from '@/composables/useApi'
import { describeApiError } from '@/api/errors'

const props = defineProps({
  // [{ name, partitions, pending, sev: 'ok' | 'warn' | 'bad' }]
  // In 'partitions' mode each entry IS a partition (partitions: 1) and its
  // pending is that partition's own — real per-partition data.
  queues: { type: Array, default: () => [] },
  mode: { type: String, default: 'queues' }, // 'queues' | 'partitions'
  queueName: { type: String, default: '' },  // partitions mode: the queue they belong to
  loading: { type: Boolean, default: false },
  error: { type: [Object, String], default: null },
})
const perPartition = computed(() => props.mode === 'partitions')

const router = useRouter()
const SIZE = 300
// A Fibonacci number, like the florets of the flower it is named after. It is
// also where seeds stop being legible at this size: 610 seeds are ~3px each.
const MAX_SEEDS = 610
const GOLDEN = Math.PI * (3 - Math.sqrt(5))
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

const errorText = computed(() =>
  typeof props.error === 'string' ? props.error : describeApiError(props.error)
)

const plan = computed(() => {
  const qs = props.queues
    .map(q => ({ ...q, partitions: Math.max(0, Math.round(q.partitions || 0)), pending: Math.max(0, q.pending || 0) }))
    .filter(q => q.partitions > 0)
  const totalPartitions = qs.reduce((s, q) => s + q.partitions, 0)
  if (!totalPartitions) return { seeds: [], totalPartitions: 0, perSeed: 1, trimmedQueues: 0 }

  // Which queues get seeds: all of them, unless there are more queues than
  // seeds, in which case the ones holding the most work.
  let shown = qs
  let trimmedQueues = 0
  if (qs.length > MAX_SEEDS) {
    shown = [...qs].sort((a, b) => b.pending - a.pending || b.partitions - a.partitions).slice(0, MAX_SEEDS)
    trimmedQueues = qs.length - MAX_SEEDS
  }
  const shownPartitions = shown.reduce((s, q) => s + q.partitions, 0)

  // Seeds per queue: one per partition while it fits; otherwise largest-
  // remainder shares of MAX_SEEDS, at least one each.
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

  const raw = []
  for (const { q, n } of alloc) {
    const perPartition = q.pending / q.partitions
    const slice = q.partitions / n
    for (let k = 0; k < n; k++) {
      raw.push({ queue: q.name, sev: q.sev || 'ok', value: perPartition, slice, q, k })
    }
  }
  // Most pending per partition at the centre; the verdict breaks ties so a
  // failing queue is never pushed to the rim by a busier healthy one.
  const rank = { bad: 2, warn: 1, ok: 0 }
  raw.sort((a, b) => b.value - a.value || rank[b.sev] - rank[a.sev] || a.queue.localeCompare(b.queue))

  const n = raw.length
  // The spacing that fills the disc, never wider than a seed's natural
  // spacing: three partitions are a small head of three seeds in the middle,
  // not three discs pushed out past the edge. The divisor keeps the outermost
  // seed, at its largest, inside the drawing.
  const c = Math.min(SEED_SPACING, (SIZE / 2 - 2) / (Math.sqrt(Math.max(n - 0.5, 0.5)) + MAX_SIZE))
  const maxV = Math.max(FULL_AT, ...raw.map(s => s.value))
  const seeds = raw.map((s, i) => {
    const r = n === 1 ? 0 : c * Math.sqrt(i + 0.5)
    const a = i * GOLDEN
    const size = c * (MIN_SIZE + (MAX_SIZE - MIN_SIZE) * Math.sqrt(s.value / maxV))
    const per = s.slice === 1 ? '1 partition' : `≈${formatNumber(Math.round(s.slice))} partitions`
    return {
      key: `${s.queue}#${s.k}`,
      queue: s.queue,
      sev: s.sev,
      x: (SIZE / 2 + r * Math.cos(a)).toFixed(2),
      y: (SIZE / 2 + r * Math.sin(a)).toFixed(2),
      r: size.toFixed(2),
      title: perPartition.value
        ? `partition ${s.queue} · ${formatNumber(s.q.pending)} pending`
        : `${s.queue} · this seed ${per} · ${formatNumber(s.q.pending)} pending in ${formatNumber(s.q.partitions)} ${s.q.partitions === 1 ? 'partition' : 'partitions'}`,
    }
  })
  return { seeds, totalPartitions, perSeed: totalPartitions / n, trimmedQueues }
})

const hot = computed(() => plan.value.seeds.filter(s => s.sev === 'warn' || s.sev === 'bad').length)

const caption = computed(() => {
  const p = plan.value
  if (perPartition.value) {
    const all = props.queues.length
    const head = p.trimmedQueues
      ? `The ${formatNumber(MAX_SEEDS)} of ${formatNumber(all)} partitions holding the most pending, one seed each.`
      : `${formatNumber(all)} ${all === 1 ? 'partition' : 'partitions'}, one seed each.`
    return `${head} Size is pending messages in the partition; colour only when this queue needs you. Click a seed for its messages.`
  }
  const head = p.perSeed <= 1
    ? `${formatNumber(p.totalPartitions)} ${p.totalPartitions === 1 ? 'partition' : 'partitions'}, one seed each.`
    : `${formatNumber(p.totalPartitions)} partitions in ${formatNumber(p.seeds.length)} seeds, one seed ≈ ${formatNumber(Math.round(p.perSeed))} partitions.`
  const trimmed = p.trimmedQueues ? ` Showing the ${formatNumber(MAX_SEEDS)} queues with the most pending.` : ''
  return `${head}${trimmed} Size is pending per partition of its queue; colour only on queues that need you.`
})

const ariaLabel = computed(() =>
  `${formatNumber(plan.value.totalPartitions)} partitions drawn as sunflower seeds; ${hot.value} seeds belong to queues that need attention`
)

const open = (name) => {
  if (perPartition.value) router.push({ path: '/messages', query: { queue: props.queueName, partition: name } })
  else router.push(`/queues/${encodeURIComponent(name)}`)
}
</script>

<style scoped>
.sf { display: flex; flex-direction: column; min-width: 0; height: 100%; }
.sf-body { flex: 1; display: grid; place-items: center; padding: 12px 14px 4px; min-height: 220px; }
.sf-body svg { display: block; width: 100%; max-width: 290px; height: auto; }
.sf-seed { fill: var(--sf-seed, var(--text-faint)); cursor: pointer; transition: fill .15s var(--ease); }
.sf-seed.warn { fill: var(--warn-400); }
.sf-seed.bad { fill: var(--ember-400); }
.sf-seed:hover { stroke: var(--text-hi); stroke-width: 1.2; }
.sf-foot { margin: 0; padding: 0 16px 14px; font-size: 12px; line-height: 1.5; color: var(--text-low); }
.sf-msg { font-size: 12px; color: var(--text-low); text-align: center; }
.sf-skeleton { display: block; width: 220px; height: 220px; border-radius: 50%; }
</style>
