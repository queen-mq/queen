<script setup>
// Every node now, one row each in node-id order: a reading never moves a row.
// A click picks the node (its drawer opens on the right); a second click on
// the picked row closes it.
import { computed } from 'vue'

const props = defineProps({
  rows: { type: Array, default: () => [] },       // clusterViews nodeRows
  columns: { type: Object, default: () => ({}) },  // { version, network, raft }
  picked: { type: String, default: '' },
})
const emit = defineEmits(['pick'])

// The columns after Role, which a node nobody reached spans with its error.
const after = computed(() => 6 + (props.columns.network ? 1 : 0) + (props.columns.raft ? 1 : 0))
// Each column's least width fits its longest real reading ("Unreachable",
// "1,234,567", "0.27 / 1.29 MB/s"): a number is never cut. Under the sum the
// table scrolls inside itself.
const GAP = 12
const tracks = computed(() => [
  [112, 1.35],                              // node
  [80, 0.75],                               // role
  props.columns.version ? [100, 0.9] : null, // version, only when they differ
  [76, 0.85],                               // up for
  [40, 0.5],                                // cpu
  [100, 1.05],                              // memory held
  [82, 0.95],                               // disk
  [66, 0.6],                                // behind
  [70, 0.75],                               // last heard
  props.columns.network ? [106, 1.1] : null, // network
  props.columns.raft ? [106, 1.1] : null,    // raft port
].filter(Boolean))
const cols = computed(() => tracks.value.map(([min, fr]) => `minmax(${min}px, ${fr}fr)`).join(' '))
const minWidth = computed(() => tracks.value.reduce((s, [min]) => s + min, 0) + GAP * (tracks.value.length - 1))
</script>

<template>
  <div class="cn-scroll">
    <div class="cn-table" role="table" aria-label="The nodes now" :style="{ '--cols': cols, minWidth: minWidth + 'px' }">
      <div class="qn-tr head" role="row">
        <span role="columnheader">Node</span>
        <span role="columnheader">Role</span>
        <span v-if="columns.version" role="columnheader">Version</span>
        <span role="columnheader" class="r">Up for</span>
        <span role="columnheader" class="r" title="Of the cores the node may use, over the last minute">CPU</span>
        <span role="columnheader" class="r" title="What the process holds itself, of the memory it may use. Its resident memory, charted below, also holds the store's file pages, which the kernel drops first.">Memory held</span>
        <span role="columnheader" title="Used of the data disk; the tick is where the node stops taking writes">Disk</span>
        <span role="columnheader" class="r" title="Log entries the leader holds that this node has not matched">Behind</span>
        <span role="columnheader" class="r" title="Since the leader last heard from this node">Last heard</span>
        <span v-if="columns.network" role="columnheader" class="r" title="Bytes a second over its network interfaces, last minute">Network in / out</span>
        <span v-if="columns.raft" role="columnheader" class="r" title="Bytes a second over the Raft port, last minute">Raft port in / out</span>
      </div>
      <div
        v-for="r in rows" :key="r.key" class="qn-tr click" :class="{ on: picked === r.name }" role="row"
        tabindex="0" :aria-selected="picked === r.name" @click="emit('pick', r.name)" @keydown.enter.prevent="emit('pick', r.name)"
      >
        <span role="cell" class="qn-mono cn-name" :title="r.name">{{ r.name }}</span>
        <span role="cell" :class="r.roleTone || (r.leader ? '' : 'qn-t2')">{{ r.role }}</span>
        <span v-if="columns.version" role="cell" class="qn-mono qn-t2" :title="r.version">{{ r.version }}</span>
        <span v-if="r.unreachable" role="cell" class="red wrap cn-down" :style="{ gridColumn: `span ${after}` }">{{ r.error || 'No answer from this node.' }}</span>
        <template v-else>
          <span role="cell" class="r qn-num qn-t2">{{ r.up }}</span>
          <span role="cell" class="r qn-num" :class="r.cpuTone">{{ r.cpu }}</span>
          <span role="cell" class="r qn-num" :class="r.memoryTone">{{ r.memory }}</span>
          <span role="cell" class="cn-disk">
            <span class="cn-bar" aria-hidden="true">
              <span class="cn-fill" :class="r.disk.tone" :style="{ width: `${Math.max(r.disk.used ? 2 : 0, Math.min(100, r.disk.used || 0))}%` }" />
              <span v-if="r.disk.gate != null" class="cn-gate" :style="{ left: `${r.disk.gate}%` }" />
            </span>
            <span class="qn-num" :class="r.disk.tone">{{ r.disk.text }}</span>
          </span>
          <span role="cell" class="r qn-num" :class="r.behindTone || (r.leader ? 'qn-t3' : '')">{{ r.behind }}</span>
          <span role="cell" class="r qn-num" :class="r.heardTone || (r.leader ? 'qn-t3' : 'qn-t2')">{{ r.heard }}</span>
          <span v-if="columns.network" role="cell" class="r qn-num">{{ r.network || '–' }}</span>
          <span v-if="columns.raft" role="cell" class="r qn-num">{{ r.raft || '–' }}</span>
        </template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.cn-scroll { overflow-x: auto; }
.cn-table .qn-tr { gap: 0 12px; min-height: 40px; }
.cn-table .qn-tr.head { min-height: 32px; }
.cn-table .qn-tr:not(.head) { font-size: 13px; }
.cn-name { font-weight: 500; }
.cn-down { font-size: 12.5px; }
.cn-disk { display: inline-flex; align-items: center; gap: 8px; }
.cn-bar { position: relative; flex: 0 0 44px; height: 4px; border-radius: 2px; background: var(--line); }
.cn-fill { position: absolute; left: 0; top: 0; bottom: 0; border-radius: 2px; background: var(--text3); }
.cn-fill.amber { background: var(--amber); }
.cn-fill.red { background: var(--red); }
.cn-gate { position: absolute; top: -3px; bottom: -3px; width: 1px; background: var(--text2); }
:global(.qn.tone .cn-bar) { background: var(--ctl); }
</style>
