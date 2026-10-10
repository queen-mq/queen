import { createRouter, createWebHistory } from 'vue-router'

import { can, ensureIdentity, useIdentity } from '@/stores/identity'
import { notifyError } from '@/stores/ui'

const { standalone } = useIdentity()

// Route meta is the declaration of what a page needs and what it is about.
// Nothing else may decide either — the sidebar and the guard both read it.
//
//   requires : capability the identity store must grant. One of
//              'read' | 'produce' | 'consume' | 'queueAdmin' | 'operator'.
//              Mirrors proxy/src/routes.rs RouteClass one-for-one.
//   scope    : 'tenant' (numbers are for the acting tenant) or 'cell'
//              (numbers cover the whole cell, every tenant on it). A cell-level
//              page MUST say so on screen; a cell number read as a tenant
//              number is the lie class this shell exists to prevent.
//   nav      : { group, order } to appear in the sidebar. Omit to stay
//              reachable by URL but out of the nav.
//   navParent: the nav row that stands for this page when it has none of its
//              own: that row is the one lit while you are here.
//   proxyOnly: the broker-direct dashboard has no pxdb-backed account store.
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
    path: '/',
    name: 'Dashboard',
    component: () => import('@/views/Dashboard.vue'),
    meta: {
      title: 'Overview', subtitle: 'System overview and key metrics',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Overview', icon: 'dashboard', order: 1 },
    }
  },
  {
    path: '/queues',
    name: 'Queues',
    component: () => import('@/views/Queues.vue'),
    meta: {
      title: 'Queues', subtitle: 'Manage message queues and partitions',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'queues', order: 1 },
    }
  },
  {
    path: '/queues/:queueName',
    name: 'QueueDetail',
    component: () => import('@/views/QueueDetail.vue'),
    meta: {
      title: 'Queue', subtitle: 'Queue configuration and status',
      requires: 'read', scope: 'tenant',
    }
  },
  {
    // The RAM storage class (EPHEMERAL_QUEUES.md §5.3). Tenant-scoped and
    // 'read' like the durable list — the two status routes are classified
    // Gated(Ephemeral, Read) at the proxy, so anyone who may read queues may
    // read these. It is a SEPARATE page on purpose: an ephemeral queue has no
    // pending, no retained bytes and no DLQ, so its rows cannot share a table
    // with columns that would have to lie about them.
    path: '/ephemeral',
    name: 'Ephemeral',
    component: () => import('@/views/Ephemeral.vue'),
    meta: {
      title: 'Ephemeral queues', subtitle: 'RAM-class queues — contents survive nothing',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'ephemeral', order: 2 },
    }
  },
  {
    path: '/consumers',
    name: 'Consumers',
    component: () => import('@/views/Consumers.vue'),
    meta: {
      title: 'Consumer groups', subtitle: 'Monitor consumer lag and status',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Workers', icon: 'consumers', order: 1 },
    }
  },
  {
    path: '/messages',
    name: 'Messages',
    component: () => import('@/views/Messages.vue'),
    meta: {
      title: 'Messages', subtitle: 'Browse, inspect and push messages',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'messages', order: 4 },
    }
  },
  {
    // Tenant-scoped read access to the key-value store.
    path: '/kv',
    name: 'Kv',
    component: () => import('@/views/Kv.vue'),
    meta: {
      title: 'KV', subtitle: 'Browse the key-value store, namespace by namespace',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'kv', order: 7 },
    }
  },
  {
    // Scheduled messages that have not fired yet. 'read' because the list, the
    // count and the peek are Gated(Timers, Read) at the proxy — the CANCEL on
    // this page is gated separately, on the rule Ephemeral.vue already uses
    // for a gated write (produce ‖ consume, i.e. not a Viewer).
    path: '/timers',
    name: 'Timers',
    component: () => import('@/views/Timers.vue'),
    meta: {
      title: 'Timers', subtitle: 'Scheduled messages waiting to fire, per queue',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'timers', order: 6 },
    }
  },
  {
    // Who holds each lock and semaphore permit. 'read' because a permit is a
    // KV row and the page reads it through the console's KV listing (Read at
    // the proxy by prefix), not through POST /api/v1/locks, which is
    // read-write even for a `get`. The page writes nothing.
    path: '/locks',
    name: 'Locks',
    component: () => import('@/views/Locks.vue'),
    meta: {
      title: 'Locks', subtitle: 'Who holds each lock and semaphore, and until when',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'locks', order: 8 },
    }
  },
  {
    path: '/traces',
    name: 'Traces',
    component: () => import('@/views/Traces.vue'),
    meta: {
      title: 'Traces', subtitle: 'Track message flows across queues',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Analysis', icon: 'traces', order: 3 },
    }
  },
  {
    path: '/supervisors',
    name: 'Supervisors',
    component: () => import('@/views/Supervisors.vue'),
    meta: {
      title: 'Supervisors', subtitle: 'Published supervisor status and worker pool health',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Workers', icon: 'system', order: 2 },
    }
  },
  // The two Performance pages are gone: what they showed is on Workload, on a
  // queue's own page and on System. A link someone kept still lands somewhere
  // that answers it — the queue it named, else Workload — on the same window.
  { path: '/analytics', redirect: performanceMoved },
  { path: '/operations', redirect: performanceMoved },
  {
    // Who is doing the work. Tenant-scoped: /api/v1/analytics/workload counts
    // only this tenant's queues, and its `tenant` total is what every share on
    // the page is computed against.
    path: '/workload',
    name: 'Workload',
    component: () => import('@/views/Workload.vue'),
    meta: {
      title: 'Workload', subtitle: 'Who is doing the work, how much, and what is stuck',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Analysis', icon: 'workload', order: 2 },
    }
  },
  {
    path: '/dlq',
    name: 'DeadLetter',
    component: () => import('@/views/DeadLetter.vue'),
    meta: {
      title: 'Dead letter', subtitle: 'Inspect, replay and purge failed messages',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Messaging', icon: 'dlq', order: 5 },
    }
  },
  {
    // The lines this console judges by, for the acting tenant: one document in
    // its KV (stores/settingsStore.js). 'read' because the page states the
    // lines in force to anyone; saving is offered to queueAdmin, the capability
    // that configures a queue.
    path: '/settings',
    name: 'Settings',
    component: () => import('@/views/Settings.vue'),
    meta: {
      title: 'Settings', subtitle: 'The lines this console judges by',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Analysis', icon: 'settings', order: 4 },
    }
  },
  {
    // Tenant-level, cluster admins only: who may use the acting cluster and with
    // which credentials. The proxy owns these identities (proxy/src/console.rs),
    // so a broker-direct build has nothing to show.
    path: '/members',
    name: 'Members',
    component: () => import('@/views/Members.vue'),
    meta: {
      title: 'Members', subtitle: 'Who may use this cluster, and with which role',
      requires: 'clusterAdmin', scope: 'tenant', proxyOnly: true,
      nav: { group: 'Access', icon: 'members', order: 1 },
    }
  },
  {
    path: '/keys',
    name: 'ApiKeys',
    component: () => import('@/views/ApiKeys.vue'),
    meta: {
      title: 'API keys', subtitle: 'Credentials services use to call this cluster',
      requires: 'clusterAdmin', scope: 'tenant', proxyOnly: true,
      nav: { group: 'Access', icon: 'keys', order: 2 },
    }
  },
  {
    // Cell-level: host resources and the replicated log cover every tenant on
    // this cell, which is why the proxy answers it for live operators only.
    path: '/system',
    name: 'System',
    component: () => import('@/views/System.vue'),
    meta: {
      title: 'System', subtitle: 'Cell-level: server resources and the replicated log',
      requires: 'operator', scope: 'cell',
      nav: { group: 'Cell', icon: 'system', order: 1 },
    }
  },
  {
    // Accounts across the cell. No row of its own: it is reached from Members
    // through the scope switch both pages carry (components/AccessScope.vue).
    path: '/users',
    name: 'Users',
    component: () => import('@/views/Users.vue'),
    meta: {
      title: 'Users', subtitle: 'Cell-level: user accounts and cluster access',
      requires: 'operator', scope: 'cell', proxyOnly: true,
      navParent: '/members',
    }
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: () => import('@/components/NotFound.vue'),
    meta: { title: 'Not found', subtitle: 'No such page', requires: 'read' }
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

  if (to.meta.proxyOnly && standalone.value) {
    notifyError('User accounts are managed by queen-proxy and are unavailable in broker-direct mode', 'Not available')
    return { path: '/', replace: true }
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
