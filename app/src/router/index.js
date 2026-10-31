import { createRouter, createWebHistory } from 'vue-router'

import { can, ensureIdentity } from '@/stores/identity'
import { notifyError } from '@/stores/ui'

// Route meta is the declaration of what a page needs and what it is about.
// Nothing else may decide either: the guard and the console's shell read it.
//
//   requires : capability the identity store must grant. One of
//              'read' | 'produce' | 'consume' | 'queueAdmin' | 'operator'.
//              Mirrors proxy/src/routes.rs RouteClass one-for-one.
//   scope    : 'tenant' (numbers are for the acting tenant) or 'cell'
//              (numbers cover the whole cell, every tenant on it). A cell-level
//              page MUST say so on screen; a cell number read as a tenant
//              number is the lie class the console exists to prevent.
//   console  : the page draws the console's own rail and band (app/src/console).
//   section  : the rail item lit while you are here.
function performanceMoved(to) {
  const query = {}
  for (const key of ['range', 'from', 'to']) {
    if (typeof to.query[key] === 'string' && to.query[key]) query[key] = to.query[key]
  }
  const queue = typeof to.query.queue === 'string' ? to.query.queue : ''
  return queue ? { path: `/queues/${encodeURIComponent(queue)}`, query } : { path: '/workload', query }
}

const routes = [
  {
    // The new console's Overview (app/src/console): its own rail and band,
    // so App.vue draws no old shell around it (meta.console).
    path: '/',
    name: 'Overview',
    component: () => import('@/views/Overview.vue'),
    meta: {
      title: 'Overview', subtitle: 'Is anything waiting on you?',
      requires: 'read', scope: 'tenant', console: true, section: 'overview',
    }
  },

  {
    // The new console's Queues (app/src/console).
    path: '/queues',
    name: 'QueuesPage',
    component: () => import('@/views/QueuesPage.vue'),
    meta: {
      title: 'Queues', subtitle: 'Where is the work stuck?',
      requires: 'read', scope: 'tenant', console: true, section: 'queues',
    }
  },

  {
    // A queue in the new console (app/src/console): its groups, partitions,
    // messages, dead letters, timers and options as tabs (?tab=).
    path: '/queues/:queueName',
    name: 'QueuePage',
    component: () => import('@/views/QueuePage.vue'),
    meta: {
      title: 'Queue', subtitle: 'Is this queue all right?',
      requires: 'read', scope: 'tenant', console: true, section: 'queues',
    }
  },

  // A queue's alert lines live in Settings now (the new console).
  { path: '/queues/:queueName/alerts', redirect: (to) => ({ path: '/settings', query: { queue: to.params.queueName } }) },

  // Ephemeral queues are a filter of Queues in the new console (?name= picks one).
  { path: '/ephemeral', redirect: (to) => ({ path: '/queues', query: { show: 'eph', ...(typeof to.query.name === 'string' && to.query.name ? { name: to.query.name } : {}) } }) },
  // Consumer groups: one queue's are that queue's page; all of them, the
  // Queues section's Consumer groups view (?group= picks one).
  { path: '/consumers', redirect: (to) => (typeof to.query.queue === 'string' && to.query.queue
    ? { path: `/queues/${encodeURIComponent(to.query.queue)}` }
    : { path: '/queues', query: { view: 'groups', ...(typeof to.query.group === 'string' && to.query.group ? { group: to.query.group } : {}) } }) },
  // Messages: the Queues section's Messages view, with the old page's filters
  // (the Laravel dashboard links /messages?queue=X&status=pending|processing,
  // and the old Traces page one message by partitionId + transactionId).
  { path: '/messages', redirect: (to) => {
    const q = (k) => (typeof to.query[k] === 'string' && to.query[k] ? to.query[k] : '')
    const query = { view: 'messages' }
    for (const [from, into] of [['queue', 'mq'], ['status', 'mstatus'], ['partition', 'mpart'], ['partitionId', 'mpid'], ['transactionId', 'mtx']]) {
      if (q(from)) query[into] = q(from)
    }
    return { path: '/queues', query }
  } },
  // KV, locks and timers are State in the new console.
  { path: '/kv', redirect: (to) => ({ path: '/state', query: typeof to.query.ns === 'string' ? { ns: to.query.ns } : {} }) },

  // One queue's timers are its Scheduled tab; all of them, State's timers.
  { path: '/timers', redirect: (to) => (typeof to.query.queue === 'string' && to.query.queue
    ? { path: `/queues/${encodeURIComponent(to.query.queue)}`, query: { tab: 'scheduled' } }
    : { path: '/state', query: { view: 'timers' } }) },

  { path: '/locks', redirect: { path: '/state', query: { view: 'locks' } } },

  // Traces are Entities in the new console: one traced name's timeline.
  { path: '/traces', redirect: (to) => ({ path: '/entities', query: typeof to.query.trace === 'string' ? { name: to.query.trace } : {} }) },

  // Supervisors are the Queues section's Workers view (?queue= narrows it).
  { path: '/supervisors', redirect: (to) => ({ path: '/queues', query: { view: 'workers', ...(typeof to.query.queue === 'string' && to.query.queue ? { queue: to.query.queue } : {}) } }) },
  // The two Performance pages are gone: what they showed is on Workload, on a
  // queue's own page and on System. A link someone kept still lands somewhere
  // that answers it — the queue it named, else Workload — on the same window.
  { path: '/analytics', redirect: performanceMoved },
  { path: '/operations', redirect: performanceMoved },
  {
    // The new console's Workload: who does the work, how much, and what is
    // stuck. Tenant-scoped: /api/v1/analytics/workload counts only this
    // tenant's queues.
    path: '/workload',
    name: 'WorkloadPage',
    component: () => import('@/views/WorkloadPage.vue'),
    meta: {
      title: 'Workload', subtitle: 'Who is doing the work, how much, and what is stuck',
      requires: 'read', scope: 'tenant', console: true, section: 'workload',
    }
  },
  {
    // The new console's Dead letters, by cause (?queue= narrows to one queue).
    path: '/dlq',
    name: 'DeadLetters',
    component: () => import('@/views/DeadLettersPage.vue'),
    meta: {
      title: 'Dead letters', subtitle: 'Why messages failed, by cause',
      requires: 'read', scope: 'tenant', console: true, section: 'dlq',
    }
  },

  {
    // The new console's Settings: access, alert lines, users and tenants, the
    // audit log (?view=). 'read' because it states the lines in force to
    // anyone; saving them is offered to queueAdmin.
    path: '/settings',
    name: 'SettingsPage',
    component: () => import('@/views/SettingsPage.vue'),
    meta: {
      title: 'Settings', subtitle: 'Access, alert lines, users and the audit log',
      requires: 'read', scope: 'tenant', console: true, section: 'settings',
    }
  },

  // Members and API keys are Settings → Access in the new console.
  { path: '/access', redirect: { path: '/settings', query: { view: 'access' } } },

  // The two pages before 2026-10-30.
  { path: '/members', redirect: { path: '/settings', query: { view: 'access' } } },
  { path: '/keys', redirect: { path: '/settings', query: { view: 'access' } } },
  // The cell is Cluster in the new console.
  { path: '/system', redirect: '/cluster' },

  // Accounts across the cell: Settings › Users and tenants.
  { path: '/users', redirect: { path: '/settings', query: { view: 'users' } } },
  {
    // The new console's State: KV keys by namespace, locks, timers of every queue (?view=).
    path: '/state',
    name: 'StatePage',
    component: () => import('@/views/StatePage.vue'),
    meta: {
      title: 'State', subtitle: 'Keys, locks and timers',
      requires: 'read', scope: 'tenant', console: true, section: 'state',
    }
  },
  {
    // The new console's Cluster: the cell under every tenant (operators).
    path: '/cluster',
    name: 'ClusterPage',
    component: () => import('@/views/ClusterPage.vue'),
    meta: {
      title: 'Cluster', subtitle: 'The cell: nodes, the replicated log, traffic',
      requires: 'operator', scope: 'cell', console: true, section: 'cluster',
    }
  },
  {
    // The new console's Entities: one entity's timeline across queues, from the trace API (?name=).
    path: '/entities',
    name: 'EntitiesPage',
    component: () => import('@/views/EntitiesPage.vue'),
    meta: {
      title: 'Entities', subtitle: 'What happened to one of them?',
      requires: 'read', scope: 'tenant', console: true, section: 'entities',
    }
  },
  {
    // The new console's Streams: each stream, what it reads and writes, and
    // its windows past their end (GET /streams/v1/queries, ?name=).
    path: '/streams',
    name: 'StreamsPage',
    component: () => import('@/views/StreamsPage.vue'),
    meta: {
      title: 'Streams', subtitle: 'Does every stream keep up?',
      requires: 'read', scope: 'tenant', console: true, section: 'streams',
    }
  },
  // The old design is gone: its addresses open the new console.
  { path: '/classic/:rest(.*)*', redirect: (to) => ({ path: '/' + [].concat(to.params.rest || []).join('/'), query: to.query }) },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/views/NotFoundPage.vue'),
    meta: { title: 'Not found', subtitle: 'No such page', requires: 'read', console: true, section: 'none' }
  },
]

const router = createRouter({
  // BASE_URL so the same bundle can be served at '/' by the proxy and under a
  // path prefix if it is ever mounted like the console is.
  history: createWebHistory(import.meta.env.BASE_URL),
  routes
})

router.beforeEach(async (to) => {
  document.title = `${to.meta.title || 'Queen'} | Queen Dashboard`

  // Identity gates the whole app: no route renders before we know who this is
  // and what they may see. A 401 inside redirects to the proxy login and never
  // resolves, so navigation simply stops here.
  try {
    await ensureIdentity()
  } catch {
    // Boot failed (network / 5xx). App.vue renders the failure; let the
    // navigation through so it has something to render into.
    return true
  }

  const requires = to.meta.requires || 'read'
  if (can(requires)) return true

  // '/' is the fallback, so it can never be the thing we refuse — an account
  // with no cluster membership would loop forever. The shell renders why
  // there is nothing to show instead.
  if (to.path === '/') return true

  notifyError(
    requires === 'operator'
      ? 'That page is cell-level and only a live operator may open it'
      : `That page needs the "${requires}" capability on this cluster`,
    'Not permitted',
  )
  return { path: '/', replace: true }
})

export default router
