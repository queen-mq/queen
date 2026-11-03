<script setup>
// Each node's replicated log and its store (LMDB), one row each in node-id
// order (clusterViews storeRows): the last entry applied, the log on its
// disk, how much of the store's map is used, and its readers. The term is
// said once, in the line under the title. A click opens the node on the
// right, as in the table above.
defineProps({
  rows: { type: Array, default: () => [] },
  picked: { type: String, default: '' },
})
const emit = defineEmits(['pick'])
</script>

<template>
  <section class="cs" aria-labelledby="cs-title">
    <h2 id="cs-title" class="cs-title">Log and store, on each node</h2>
    <div class="cs-scroll">
      <div class="cs-table" role="table" aria-label="Each node's log and store">
        <div class="qn-tr head" role="row">
          <span role="columnheader">Node</span>
          <span role="columnheader" class="r" title="The last log entry the node applied to its state">Applied</span>
          <span role="columnheader" class="r" title="The replicated log on the node's disk">Log</span>
          <span role="columnheader" class="r" title="Its log files">Files</span>
          <span role="columnheader" class="r" title="Used of the store's map (LMDB). From 85% the node refuses new writes, until it is under 80% again.">Store map</span>
          <span role="columnheader" class="r" title="What the store holds">Store used</span>
          <span role="columnheader" class="r" title="Read transactions open on the store, of the most it allows">Readers</span>
        </div>
        <div
          v-for="r in rows" :key="r.key" class="qn-tr click" :class="{ on: picked === r.name }" role="row"
          tabindex="0" :aria-selected="picked === r.name" @click="emit('pick', r.name)" @keydown.enter.prevent="emit('pick', r.name)"
        >
          <span role="cell" class="qn-mono cs-name" :title="r.name">{{ r.name }}</span>
          <span v-if="r.unreachable" role="cell" class="red cs-down">{{ r.error || 'No answer from this node.' }}</span>
          <template v-else>
            <span role="cell" class="r qn-num" :title="r.appliedNote || undefined">{{ r.applied }}</span>
            <span role="cell" class="r qn-num">{{ r.log }}</span>
            <span role="cell" class="r qn-num qn-t2">{{ r.files || '–' }}</span>
            <span role="cell" class="r qn-num"><span :class="r.mapTone">{{ r.map }}</span><span v-if="r.mapOf" class="qn-t3 cs-of">{{ r.mapOf }}</span></span>
            <span role="cell" class="r qn-num">{{ r.used }}</span>
            <span role="cell" class="r qn-num qn-t2">{{ r.readers }}</span>
          </template>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.cs-title { font-size: 15px; font-weight: 500; }
.cs-scroll { overflow-x: auto; margin-top: 10px; }
.cs-table { --cols: minmax(112px, 1.35fr) minmax(100px, 1fr) minmax(64px, .7fr) minmax(72px, .7fr) minmax(112px, 1.1fr) minmax(76px, .8fr) minmax(84px, .8fr); min-width: 700px; }
.cs-table .qn-tr { gap: 0 12px; min-height: 40px; }
.cs-table .qn-tr.head { min-height: 32px; }
.cs-table .qn-tr:not(.head) { font-size: 13px; }
.cs-name { font-weight: 500; }
.cs-of { margin-left: 4px; }
/* a phone: the node, its log and its store's map; a tap opens the rest */
@media (max-width: 640px) {
  .cs-table { --cols: minmax(0, 1.2fr) minmax(0, .8fr) minmax(0, 1.2fr); min-width: 0; }
  .cs-table .qn-tr > :nth-child(2), .cs-table .qn-tr > :nth-child(4), .cs-table .qn-tr > :nth-child(n+6) { display: none; }
  .cs-table .qn-tr > .cs-down { display: block; }
}
.cs-down { grid-column: 2 / -1; font-size: 12.5px; white-space: normal; }
</style>
