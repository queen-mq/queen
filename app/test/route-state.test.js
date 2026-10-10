import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createRenderer, h, nextTick, ref } from 'vue'
import { createMemoryHistory, createRouter, RouterView } from 'vue-router'
import { useRouteState } from '../src/composables/useRouteState.js'
import { useRouteRange } from '../src/composables/useRouteRange.js'

const renderer = createRenderer({
  createElement: () => ({}), createText: () => ({}), createComment: () => ({}),
  setText() {}, setElementText() {}, patchProp() {}, insert() {}, remove() {},
  parentNode: () => null, nextSibling: () => null,
})
const settle = async () => { await nextTick(); await new Promise(resolve => setTimeout(resolve, 0)); await nextTick() }

test('same-page navigation, filter edits and Back restore one consistent query', async () => {
  let state, setups = 0
  const Page = { setup() {
    setups++
    state = { queue: ref(''), namespace: ref(null), page: ref(1), queues: ref([]) }
    useRouteState(state)
    // A second state owner (for example a range picker) must merge its patch.
    state.range = ref('1h')
    useRouteState({ range: state.range })
    return () => h('div')
  } }
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/list', name: 'List', component: Page }, { path: '/other', name: 'Other', component: { render: () => h('div') } }] })
  await router.push('/list?queue=a&namespace=&page=3&returnTo=%2F')
  const app = renderer.createApp({ render: () => h(RouterView) })
  app.use(router); app.mount({})
  await settle()
  assert.equal(state.queue.value, 'a')
  assert.equal(state.namespace.value, '')
  assert.equal(state.page.value, 3)
  await router.push('/list?queue=b&page=2')
  await settle()
  assert.equal(setups, 1)
  assert.equal(state.queue.value, 'b')
  assert.equal(state.page.value, 2)
  assert.equal(state.namespace.value, null)
  state.queue.value = 'orders/#?'
  state.range.value = '24h'
  state.queues.value = ['orders/#?', 'billing']
  await settle()
  assert.equal(router.currentRoute.value.query.queue, 'orders/#?')
  assert.equal(router.currentRoute.value.query.range, '24h')
  assert.deepEqual(router.currentRoute.value.query.queues, ['orders/#?', 'billing'])
  const listUrl = router.currentRoute.value.fullPath
  await router.push('/other')
  router.back(); await settle()
  assert.equal(router.currentRoute.value.fullPath, listUrl)
  assert.equal(state.range.value, '24h')
  assert.deepEqual(state.queues.value, ['orders/#?', 'billing'])
  router.back(); await settle()
  assert.equal(state.queue.value, 'a')
  assert.equal(state.page.value, 3)
  assert.equal(state.namespace.value, '')
  assert.deepEqual(state.queues.value, [])
  app.unmount()
})

test('analysis periods restore across views without applying unfinished custom inputs', async () => {
  let state, reloads = 0
  const Page = { setup() {
    state = { range: ref(60), customMode: ref(false), customFrom: ref(''), customTo: ref(''), appliedCustom: ref(null) }
    useRouteRange({ ...state, reload: () => reloads++ })
    return () => h('div')
  } }
  const router = createRouter({ history: createMemoryHistory(), routes: [{ path: '/analysis', name: 'Analysis', component: Page }] })
  await router.push('/analysis?queue=orders&range=15m')
  const app = renderer.createApp({ render: () => h(RouterView) })
  app.use(router); app.mount({}); await settle()
  assert.equal(state.range.value, 15)
  assert.equal(reloads, 0)
  state.customMode.value = true
  state.customFrom.value = '2026-10-01T10:00'
  await settle()
  assert.equal(state.customMode.value, true)
  assert.equal(router.currentRoute.value.query.from, undefined)
  assert.equal(reloads, 0)
  const from = new Date('2026-10-01T00:00:12.345Z'), to = new Date('2026-10-02T00:00:12.345Z')
  state.appliedCustom.value = { from, to }
  await settle()
  assert.equal(router.currentRoute.value.query.from, from.toISOString())
  assert.equal(router.currentRoute.value.query.queue, 'orders')
  assert.equal(reloads, 0, 'the Apply handler already fetched this local change')
  const customPath = router.currentRoute.value.fullPath
  await router.push('/analysis?range=24h'); await settle()
  assert.equal(state.range.value, 1440)
  assert.equal(state.customMode.value, false)
  assert.equal(reloads, 1)
  router.back(); await settle()
  assert.equal(router.currentRoute.value.fullPath, customPath)
  assert.equal(state.appliedCustom.value.from.toISOString(), from.toISOString())
  assert.equal(state.customMode.value, true)
  assert.equal(reloads, 2)
  app.unmount()
})
