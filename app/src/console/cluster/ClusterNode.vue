<script setup>
// One node, on the right of the page: who it is and where it answers, its
// replicated log and store, its disk by what fills it, its machine, and what
// crossed its Raft port. Everything from the members read (clusterViews nodeDetail).
defineProps({
  detail: { type: Object, default: null },  // nodeDetail(), or null for a name the read does not hold
  name: { type: String, default: '' },
  versions: { type: Boolean, default: false },  // the nodes run different versions: say this one's
})
const emit = defineEmits(['close'])
const ADDRESS = new Set(['HTTP', 'Raft'])
</script>

<template>
  <aside class="cnd qn-panel" :aria-label="`Node ${detail?.name || name}`">
    <div class="cnd-head">
      <div style="flex: 1 1 0; min-width: 0">
        <p class="qn-t3" style="font-size: 12px">{{ detail?.nodeId != null ? `Node ${detail.nodeId}` : 'Node' }}</p>
        <h3 class="qn-mono cnd-name">{{ detail?.name || name }}</h3>
      </div>
      <button type="button" class="qn-btn3" aria-label="Close the details" style="height: 28px" @click="emit('close')">✕</button>
    </div>

    <p v-if="!detail" class="qn-t2" style="margin-top: 12px">There is no node named <span class="qn-mono">{{ name }}</span> in the last members read.</p>
    <template v-else>
      <dl class="qn-dl cnd-dl">
        <template v-for="f in detail.facts.filter((x) => !ADDRESS.has(x.k) && (versions || x.k !== 'Version'))" :key="f.k">
          <dt>{{ f.k }}</dt><dd :class="[f.mono ? 'qn-mono' : '', f.tone || '']">{{ f.v }}</dd>
        </template>
      </dl>
      <div v-if="detail.facts.some((x) => ADDRESS.has(x.k))" class="cnd-addr">
        <p v-for="f in detail.facts.filter((x) => ADDRESS.has(x.k))" :key="f.k"><span class="qn-t3">{{ f.k }}</span> <span class="qn-mono">{{ f.v }}</span></p>
      </div>

      <template v-if="detail.unreachable">
        <p class="red qn-mono cnd-err">{{ detail.error || 'No answer from this node.' }}</p>
        <p class="qn-t2" style="margin-top: 8px">It does not answer the leader, so nothing else about it is known.</p>
      </template>
      <template v-else>
        <template v-if="detail.log.length">
          <h4 class="cnd-h">Its log and store</h4>
          <dl class="qn-dl cnd-dl">
            <template v-for="f in detail.log" :key="f.k">
              <dt>{{ f.k }}</dt>
              <dd><span class="qn-num" :class="f.tone || ''">{{ f.v }}</span><span v-if="f.sub" class="qn-t3 qn-num cnd-sub">{{ f.sub }}</span></dd>
            </template>
          </dl>
        </template>

        <template v-if="detail.disk">
          <h4 class="cnd-h">Its disk</h4>
          <p v-if="detail.disk.line" class="qn-num" style="font-size: 12.5px">{{ detail.disk.line }}. <span :class="detail.disk.gateTone || 'qn-t2'">{{ detail.disk.gate }}</span></p>
          <ul v-if="detail.disk.parts?.length" class="cnd-parts" aria-label="The disk, by what fills it">
            <li v-for="p in detail.disk.parts" :key="p.label">
              <span style="flex: 1 1 0; min-width: 0">{{ p.label }}</span>
              <span class="cnd-bar" aria-hidden="true"><span :style="{ width: `${Math.max(1, Math.round(p.share * 100))}%` }" /></span>
              <span class="qn-num cnd-size">{{ p.size }}</span>
            </li>
          </ul>
          <p v-if="detail.disk.io" class="qn-t2 qn-num" style="font-size: 12.5px; margin-top: 8px">{{ detail.disk.io }}, {{ detail.window }}.</p>
        </template>

        <template v-if="detail.machine.length">
          <h4 class="cnd-h">Its machine</h4>
          <dl class="qn-dl cnd-dl">
            <template v-for="f in detail.machine" :key="f.k">
              <dt>{{ f.k }}</dt>
              <dd><span class="qn-num">{{ f.v }}</span><span v-if="f.sub" class="qn-t3 cnd-sub">{{ f.sub }}</span></dd>
            </template>
          </dl>
        </template>

        <template v-if="detail.channels.length">
          <h4 class="cnd-h">Its Raft port, {{ detail.window }}</h4>
          <div role="table" aria-label="Bytes a second over the Raft port, by what they carry" class="cnd-ch" style="--cols: minmax(0, 1fr) 72px 72px">
            <div class="qn-tr head" role="row"><span role="columnheader">Carries</span><span role="columnheader" class="r">In</span><span role="columnheader" class="r">Out</span></div>
            <div v-for="c in detail.channels" :key="c.key" class="qn-tr" role="row" :title="c.note">
              <span role="cell" :class="c.idle ? 'qn-t3' : ''">{{ c.label }}</span>
              <span role="cell" class="r qn-num" :class="c.idle ? 'qn-t3' : ''">{{ c.in }}</span>
              <span role="cell" class="r qn-num" :class="c.idle ? 'qn-t3' : ''">{{ c.out }}</span>
            </div>
          </div>
        </template>
      </template>
    </template>
  </aside>
</template>

<style scoped>
.cnd { padding: 16px 18px 18px; }
.cnd-head { display: flex; align-items: flex-start; gap: 8px; }
.cnd-name { font-size: 14px; font-weight: 500; margin-top: 2px; overflow-wrap: anywhere; }
.cnd-dl { grid-template-columns: 76px minmax(0, 1fr); margin-top: 12px; }
.cnd-sub { display: block; font-size: 11.5px; margin-top: 1px; }
.cnd-addr { margin-top: 10px; display: grid; gap: 4px; font-size: 11.5px; overflow-wrap: anywhere; }
.cnd-addr .qn-mono { color: var(--text2); }
.cnd-err { margin-top: 14px; font-size: 12px; overflow-wrap: anywhere; }
.cnd-h { font-size: 12px; font-weight: 500; color: var(--text3); margin: 18px 0 0; padding-top: 14px; border-top: 1px solid var(--line2); }
.cnd-h + .cnd-dl { margin-top: 8px; }
.cnd-h + p, .cnd-h + .cnd-ch { margin-top: 8px; }
.cnd-parts { margin-top: 8px; display: grid; gap: 6px; font-size: 12.5px; }
.cnd-parts li { display: flex; align-items: center; gap: 10px; }
.cnd-bar { flex: 0 0 64px; height: 4px; border-radius: 2px; background: var(--line); overflow: hidden; }
.cnd-bar > span { display: block; height: 4px; border-radius: 2px; background: var(--text3); }
.cnd-size { flex: 0 0 56px; text-align: right; }
.cnd-ch .qn-tr { min-height: 30px; font-size: 12.5px; gap: 0 10px; }
.cnd-ch .qn-tr.head { min-height: 26px; font-size: 11.5px; }
:global(.qn.tone .cnd-bar) { background: var(--bg2); }
</style>
