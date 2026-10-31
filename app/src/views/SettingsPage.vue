<script setup>
// Settings, in the new console: a fixed list of four pages on the left
// (Access, Alerts, Users and tenants, Audit log), the picked one on the right.
// The pick lives in the address (?view=, Alerts when absent); a click lights
// it and changes the right side, never the list. What was typed on Alerts is
// kept while another page is open (useAlertLines lives here, not in the view).
import { computed, reactive } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import ConsoleShell from '@/console/ConsoleShell.vue'
import { useConsoleFrame } from '@/console/useConsoleFrame'
import { VIEWS, DEFAULT_VIEW, useAlertLines } from '@/console/useSettingsPage'
import SettingsAlerts from '@/console/settings/SettingsAlerts.vue'
import SettingsAccess from '@/console/settings/SettingsAccess.vue'
import SettingsUsers from '@/console/settings/SettingsUsers.vue'

const route = useRoute()
const router = useRouter()
const frame = useConsoleFrame()
const where = computed(() => frame.cluster.value.name)

const view = computed(() => (VIEWS.some((v) => v.id === route.query.view) ? route.query.view : DEFAULT_VIEW))
const current = computed(() => VIEWS.find((v) => v.id === view.value))
function pick(id) {
  if (id === view.value) return
  const query = { ...route.query, view: id === DEFAULT_VIEW ? undefined : id }
  if (id !== 'alerts') delete query.queue
  router.replace({ query })
}
const path = computed(() => [{ label: 'Settings', to: '/settings' }, { label: current.value.label }])

const alerts = reactive(useAlertLines(where))
</script>

<template>
  <ConsoleShell :path="path" :frame="frame">
    <div class="qs">
      <h1 class="qs-title">Settings</h1>
      <div class="qs-split">
        <nav class="qs-side" aria-label="Settings pages">
          <button
            v-for="v in VIEWS" :key="v.id" type="button" class="qn-opt qs-li" :class="{ on: view === v.id }"
            :aria-current="view === v.id ? 'page' : undefined" @click="pick(v.id)"
          >
            <span style="flex: 1 1 0; min-width: 0">{{ v.label }}</span>
            <span v-if="v.cell" class="qn-tag qs-cell" title="Cell-level: it covers every tenant on the cell">cell</span>
          </button>
        </nav>

        <div class="qs-main">
          <SettingsAlerts v-if="view === 'alerts'" :a="alerts" :where="where" />
          <SettingsAccess v-else-if="view === 'access'" :where="where" />
          <SettingsUsers v-else-if="view === 'users'" :where="where" />
          <section v-else aria-labelledby="qs-audit">
            <h2 id="qs-audit" class="qs-h">Every change on <span class="qn-mono">{{ where }}</span></h2>
            <p class="qn-t2 qs-calm">The proxy writes an audit row for every sign-in and every change to API keys, roles, users and tenants, and keeps each row 90 days. No route serves those rows yet, so there is nothing to list here.</p>
          </section>
        </div>
      </div>
    </div>
  </ConsoleShell>
</template>

<style scoped>
.qs { max-width: 1400px; margin: 0 auto; padding: 26px 32px 56px; }
.qs-title { font-size: 22px; font-weight: 500; }
.qs-split { display: flex; align-items: flex-start; gap: 24px 48px; margin-top: 22px; }
.qs-side { flex: 0 0 220px; display: flex; flex-direction: column; gap: 2px; position: sticky; top: 76px; }
.qs-li { height: 34px; }
.qs-cell { font-size: 11px; padding: 0 6px; color: var(--text3); }
.qs-main { flex: 1 1 0; min-width: 0; max-width: 1040px; }
.qs-h { font-size: 17px; font-weight: 500; }
.qs-calm { margin-top: 10px; font-size: 14px; max-width: 64ch; }

@media (max-width: 900px) {
  .qs-split { flex-direction: column; align-items: stretch; gap: 18px; }
  .qs-side { position: static; flex: 0 0 auto; flex-direction: row; flex-wrap: wrap; gap: 4px; }
  .qs-li { width: auto; }
}
@media (max-width: 640px) {
  .qs { padding: 18px 16px 40px; }
  .qs-title { font-size: 19px; }
}
</style>
