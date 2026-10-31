<script setup>
// The band's cluster chip: the cluster's name and, only when something is
// wrong with the cell, one mark and a few words. A click opens each node now
// and, behind the proxy, the clusters to switch to.
import { computed, ref } from 'vue'
import QcPopover from './QcPopover.vue'
import QcIcon from './QcIcon.vue'

const props = defineProps({
  name: { type: String, default: '' },
  mark: { type: Object, default: null },        // { tone: amber|red, words }
  summary: { type: String, default: '' },        // "queen-mq-v2-1 leads, all 3 voting"
  nodes: { type: Array, default: () => [] },     // [{ id, name, leader, cpu, memory, disk, behind, tone }]
  clusters: { type: Array, default: () => [] },  // [{ id, label, on }]
  issues: { type: Array, default: () => [] },    // [{ tone, words }]
  metrics: { type: Array, default: () => [] },   // [{ id, label, value, tone, title }]
})
const emit = defineEmits(['switch'])
const open = ref(false)
const others = computed(() => props.clusters.filter((c) => !c.on))
</script>

<template>
  <QcPopover v-model:open="open" side="bottom-end" :width="560" label="Cluster" :focus-first="false">
    <template #trigger="{ toggle }">
      <button type="button" class="qn-chip" aria-haspopup="dialog" :aria-expanded="open" :aria-label="`Cluster ${name}: its nodes${clusters.length > 1 ? ', change cluster' : ''}`" @click="toggle">
        <span v-if="mark" class="qn-mark" :class="mark.tone" aria-hidden="true" />
        <span class="qn-mono" style="font-weight: 500">{{ name }}</span>
        <span v-if="mark && mark.words" :class="mark.tone" style="font-size: 12px">{{ mark.words }}</span>
        <span v-if="metrics.length" class="qn-cms">
          <span v-for="m in metrics" :key="m.id" class="qn-cm" :title="m.title"><span class="qn-t3">{{ m.label }}</span><span class="qn-num" :class="m.tone">{{ m.value }}</span></span>
        </span>
        <QcIcon name="chevron" :size="12" />
      </button>
    </template>
    <div style="padding: 8px 10px 6px">
      <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 8px 12px">
        <span class="qn-mono" style="font-size: 14px; font-weight: 500">{{ name }}</span>
        <span class="qn-t3" style="font-size: 11px; padding: 0 6px; border: 1px solid var(--line); border-radius: 3px">cell-level</span>
        <span class="qn-t2">{{ summary }}</span>
      </div>
      <ul v-if="issues.length" style="margin-top: 10px; display: grid; gap: 4px">
        <li v-for="(i, k) in issues" :key="k" style="display: flex; align-items: center; gap: 8px">
          <span class="qn-mark" :class="i.tone" aria-hidden="true" /><span>{{ i.words }}</span>
        </li>
      </ul>
      <div class="qn-num qn-nodes" role="table" aria-label="Each node now">
        <div class="qn-nodes-row qn-nodes-head" role="row">
          <span role="columnheader">Node</span><span role="columnheader" class="r">CPU</span><span role="columnheader" class="r">Memory</span><span role="columnheader" class="r">Disk</span><span role="columnheader" class="r">Behind</span>
        </div>
        <div v-for="n in nodes" :key="n.id" class="qn-nodes-row" role="row">
          <span role="cell" class="qn-mono" style="overflow: hidden; text-overflow: ellipsis">{{ n.name }}<span v-if="n.leader" class="qn-t3" style="font-family: 'IBM Plex Sans', sans-serif; margin-left: 6px">leader</span></span>
          <span role="cell" class="r" :class="n.cpuTone">{{ n.cpu }}</span>
          <span role="cell" class="r" :class="n.memoryTone">{{ n.memory }}</span>
          <span role="cell" class="r" :class="n.diskTone">{{ n.disk }}</span>
          <span role="cell" class="r" :class="n.behindTone || (n.leader ? 'qn-t3' : '')">{{ n.behind }}</span>
        </div>
        <p v-if="!nodes.length" class="qn-t2" style="padding: 10px 0">The broker did not say who its nodes are.</p>
      </div>
      <div style="display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--line2)">
        <RouterLink to="/cluster" style="font-weight: 500" @click="open = false">Open Cluster</RouterLink>
        <div style="flex: 1 1 0" />
        <template v-if="others.length">
          <span class="qn-t3" style="font-size: 12px">Switch to</span>
          <button v-for="c in others" :key="c.id" type="button" class="qn-chip" style="height: 28px" @click="emit('switch', c.id); open = false">
            <span class="qn-mono">{{ c.label }}</span>
          </button>
        </template>
      </div>
    </div>
  </QcPopover>
</template>

<style scoped>
.qn-cms { display: inline-flex; align-items: baseline; gap: 12px; padding-left: 10px; margin-left: 2px; border-left: 1px solid var(--line); font-size: 12.5px; }
.qn-cm { display: inline-flex; align-items: baseline; gap: 5px; }
:global(.qn.tone .qn-cms) { border-left-color: var(--bg3); }
@media (max-width: 1180px) { .qn-cms { display: none; } }
.qn-nodes { margin-top: 10px; font-size: 12.5px; }
.qn-nodes-row { display: grid; grid-template-columns: minmax(0, 1.8fr) 0.6fr 1.2fr 0.6fr 0.7fr; gap: 0 10px; align-items: center; height: 32px; border-bottom: 1px solid var(--line2); }
.qn-nodes-head { height: 26px; font-size: 11.5px; color: var(--text3); border-bottom-color: var(--line); }
.r { text-align: right; }
:global(.qn.tone .qn-nodes-head) { border-bottom-color: var(--bg3); }
</style>
