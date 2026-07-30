import { createRouter, createWebHistory } from 'vue-router'

import { can, ensureIdentity } from '@/stores/identity'
import { notifyError } from '@/stores/ui'

// Route meta is the declaration of what a page needs and what it is about.
// Nothing else may decide either — the sidebar and the guard both read it.
//
//   requires : capability the identity store must grant. One of
//              'read' | 'produce' | 'consume' | 'queueAdmin' | 'operator'.
//              Mirrors queen_proxy/src/routes.rs RouteClass one-for-one.
//   scope    : 'tenant' (numbers are for the acting tenant) or 'cell'
//              (numbers cover the whole cell, every tenant on it). A cell-level
//              page MUST say so on screen; a cell number read as a tenant
//              number is the lie class this shell exists to prevent.
//   nav      : { group, order } to appear in the sidebar. Omit to stay
//              reachable by URL but out of the nav.
const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: () => import('@/views/Dashboard.vue'),
    meta: {
      title: 'Dashboard', subtitle: 'System overview and key metrics',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Overview', icon: 'dashboard', order: 1 },
    }
  },
  {
    path: '/operations',
    name: 'QueueOperations',
    component: () => import('@/views/QueueOperations.vue'),
    meta: {
      title: 'Queue Operations', subtitle: 'Per-queue throughput, lag, and consumer health',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Overview', icon: 'operations', order: 2 },
    }
  },
  {
    path: '/queues',
    name: 'Queues',
    component: () => import('@/views/Queues.vue'),
    meta: {
      title: 'Queues', subtitle: 'Manage message queues and partitions',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Routing', icon: 'queues', order: 1 },
    }
  },
  {
    path: '/queues/:queueName',
    name: 'QueueDetail',
    component: () => import('@/views/QueueDetail.vue'),
    meta: {
      title: 'Queue Detail', subtitle: 'Queue configuration and status',
      requires: 'read', scope: 'tenant',
    }
  },
  {
    path: '/consumers',
    name: 'Consumers',
    component: () => import('@/views/Consumers.vue'),
    meta: {
      title: 'Consumer Groups', subtitle: 'Monitor consumer lag and status',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Routing', icon: 'consumers', order: 2 },
    }
  },
  {
    path: '/messages',
    name: 'Messages',
    component: () => import('@/views/Messages.vue'),
    meta: {
      title: 'Messages', subtitle: 'Browse and inspect messages',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Routing', icon: 'messages', order: 3 },
    }
  },
  {
    path: '/traces',
    name: 'Traces',
    component: () => import('@/views/Traces.vue'),
    meta: {
      title: 'Traces', subtitle: 'Track message flows across queues',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Observability', icon: 'traces', order: 1 },
    }
  },
  {
    path: '/analytics',
    name: 'Analytics',
    component: () => import('@/views/Analytics.vue'),
    meta: {
      title: 'Analytics', subtitle: 'Throughput and performance trends',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Observability', icon: 'analytics', order: 2 },
    }
  },
  {
    path: '/dlq',
    name: 'DeadLetter',
    component: () => import('@/views/DeadLetter.vue'),
    meta: {
      // No "replay": the broker has no re-push route for a DLQ snapshot, and a
      // subtitle promising one is the same lie as a button that cannot work.
      title: 'Dead Letter', subtitle: 'Inspect and purge failed messages',
      requires: 'read', scope: 'tenant',
      nav: { group: 'Observability', icon: 'dlq', order: 3 },
    }
  },
  {
    // Cell-level: host resources and Postgres internals cover every tenant on
    // this cell, which is why the proxy answers it for live operators only.
    path: '/system',
    name: 'System',
    component: () => import('@/views/System.vue'),
    meta: {
      title: 'System', subtitle: 'Cell-level: server resources and Postgres internals',
      requires: 'operator', scope: 'cell',
      nav: { group: 'Cell', icon: 'system', order: 1 },
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
