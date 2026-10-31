// Settings › Users and tenants (console/settings/usersActions.js): what each
// operator change sends (method, path, body), the proxy's checks before it,
// the last-admin guard, and a list that never moves under the hand.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import {
  MASK, PROVIDERS, ROLE_NAMES, USERS_PATH, accessRows, adminCount, callText, checkEmail, checkName,
  checkPassword, createPlan, doneWords, failWords, filterKey, grantsText, inRank, listingOf, onlyAdmin,
  patchCreated, patchName, patchRole, rankNew, removePlan, renamePlan, roleChoices, roleOn, rolePlan,
  roleSentence, shownSet, whoOf, withShown,
} from '../src/console/settings/usersActions.js'
import { NO_ACCESS, userGroups } from '../src/composables/accessGroups.js'

// The shape of GET /api/operator/users (proxy/src/operator.rs list_users).
const T_DEV = '7f1e8f8a-0000-4000-8000-000000000001'
const T_ACME = '7f1e8f8a-0000-4000-8000-000000000002'
const C_DEV = 'c0000000-0000-4000-8000-00000000000a'
const C_DEV2 = 'c0000000-0000-4000-8000-00000000000b'
const C_ACME = 'c0000000-0000-4000-8000-00000000000c'
const RAW = {
  cell: { id: 'cell-1', slug: 'local' },
  tenants: [{ id: T_ACME, slug: 'acme', name: 'Acme Inc' }, { id: T_DEV, slug: 'dev', name: 'dev' }],
  clusters: [
    { id: C_ACME, slug: 'acme-prod', tenant_id: T_ACME, status: 'active' },
    { id: C_DEV, slug: 'dev', tenant_id: T_DEV, status: 'active' },
    { id: C_DEV2, slug: 'dev-2', tenant_id: T_DEV, status: 'active' },
  ],
  users: [
    { id: 'u-carol', email: 'carol@acme.io', name: 'Carol', tenant_id: T_ACME, tenant_slug: 'acme', is_operator: false, has_local_password: false, roles: [{ cluster_id: C_ACME, cluster_slug: 'acme-prod', role: 'admin' }] },
    { id: 'u-admin', email: 'admin@localhost', name: null, tenant_id: T_DEV, tenant_slug: 'dev', is_operator: true, has_local_password: true, roles: [{ cluster_id: C_DEV, cluster_slug: 'dev', role: 'admin' }] },
    { id: 'u-ada', email: 'ada@example.com', name: 'Ada Lovelace', tenant_id: T_DEV, tenant_slug: 'dev', is_operator: false, has_local_password: false, roles: [{ cluster_id: C_DEV2, cluster_slug: 'dev-2', role: 'admin' }, { cluster_id: C_DEV, cluster_slug: 'dev', role: 'viewer' }] },
    { id: 'u-bob', email: 'bob@example.com', name: '', tenant_id: T_DEV, tenant_slug: 'dev', is_operator: false, has_local_password: true },
  ],
}
const L = listingOf(RAW)
const user = (id) => L.users.find((u) => u.id === id)
const cluster = (id) => L.clusters.find((c) => c.id === id)
const FORM = { tenant: T_DEV, cluster: C_DEV, name: '  Grace Hopper ', email: ' Grace@Example.COM ', provider: 'google', password: '', role: 'viewer' }

// api/index.js's `operator` object, run against a client that records what it
// would send: the dialog's call must be exactly what the API layer sends.
function apiOperator() {
  const src = readFileSync(new URL('../src/api/index.js', import.meta.url), 'utf8')
  const start = src.indexOf('export const operator = {')
  const end = src.indexOf('\n}\n', start)
  assert.ok(start >= 0 && end > start, 'api/index.js still declares `export const operator = {…}`')
  const sent = []
  const rec = (method) => (url, a, b) => {
    const body = method === 'GET' || method === 'DELETE' ? null : a
    sent.push({ method, url, body, config: method === 'GET' || method === 'DELETE' ? a : b })
    return Promise.resolve({ status: 200, data: {} })
  }
  const client = { get: rec('GET'), post: rec('POST'), patch: rec('PATCH'), put: rec('PUT'), delete: rec('DELETE') }
  // eslint-disable-next-line no-new-func
  const operator = new Function('client', `${src.slice(start, end + 2).replace('export const', 'const')}\nreturn operator`)(client)
  return { operator, sent }
}

test('the listing as kept: every user has a list of roles, every list is a list', () => {
  assert.deepEqual(user('u-bob').roles, [])
  assert.deepEqual(listingOf(null), { cell: null, tenants: [], clusters: [], users: [] })
  assert.deepEqual(listingOf({ users: [{ id: 'x', roles: 'oops' }] }).users[0].roles, [])
})

test('add: POST /api/operator/users with what the proxy keeps — trimmed name, lowercased email, no password for Google', () => {
  const p = createPlan(FORM, L)
  assert.equal(p.ok, true)
  assert.equal(p.say, '')
  assert.equal(p.request.method, 'POST')
  assert.equal(p.request.path, USERS_PATH)
  assert.deepEqual(p.request.body, {
    tenant_id: T_DEV, name: 'Grace Hopper', email: 'grace@example.com', provider: 'google', password: null, cluster_id: C_DEV, role: 'viewer',
  })
  assert.equal(p.call, `POST /api/operator/users ${JSON.stringify(p.request.body)}`)
  assert.equal(p.tenant.slug, 'dev')
  assert.equal(p.cluster.slug, 'dev')
  assert.deepEqual(p.clusters.map((c) => c.slug), ['dev', 'dev-2'])
})

test('add with a password: sent as typed, never shown — the call masks it, length too', () => {
  const p = createPlan({ ...FORM, provider: 'local', password: ' correct horse battery ' }, L)
  assert.equal(p.ok, true)
  assert.equal(p.request.body.password, ' correct horse battery ')
  assert.ok(!p.call.includes('horse'))
  assert.ok(p.call.includes(`"password":"${MASK}"`))
  assert.equal(callText({ method: 'POST', path: USERS_PATH, body: { password: 'x'.repeat(40) } }), callText({ method: 'POST', path: USERS_PATH, body: { password: 'y'.repeat(12) } }))
})

test('add: the proxy refusals, said before it refuses', () => {
  const say = (patch) => createPlan({ ...FORM, ...patch }, L)
  assert.equal(say({ name: '   ' }).ok, false)
  assert.equal(say({ name: '   ' }).say, '', 'an empty field blocks quietly')
  assert.equal(say({ name: 'n'.repeat(161) }).say, 'A name has at most 160 characters.')
  assert.equal(say({ name: '😀'.repeat(160) }).ok, true, '160 characters, whatever their bytes')
  assert.equal(say({ email: 'grace' }).say, 'Write the whole address, like name@example.com.')
  assert.equal(say({ email: 'a b@example.com' }).ok, false)
  assert.equal(say({ email: 'ops@localhost' }).ok, true, 'no dot needed: the bootstrap admin is admin@localhost')
  assert.equal(say({ email: 'ADA@example.com' }).say, 'ada@example.com already has an account here, in dev.')
  assert.equal(say({ provider: 'local', password: '' }).ok, false)
  assert.equal(say({ provider: 'local', password: 'short' }).say, 'A password has at least 12 characters.')
  assert.equal(say({ provider: 'local', password: 'é'.repeat(6) }).ok, true, '12 bytes, as the proxy counts')
  assert.equal(say({ provider: 'local', password: 'p'.repeat(129) }).say, 'A password has at most 128 characters.')
  assert.equal(say({ provider: 'saml' }).ok, false)
  assert.equal(say({ role: 'owner' }).say, 'Pick their role.')
  assert.equal(say({ tenant: '' }).say, 'Pick the tenant they belong to.')
  assert.equal(say({ cluster: C_ACME }).ok, false, 'a cluster of another tenant is not theirs')
  const bare = listingOf({ ...RAW, clusters: RAW.clusters.filter((c) => c.tenant_id !== T_DEV) })
  assert.equal(createPlan(FORM, bare).say, 'dev has no cluster on this cell, so they would have no access.')
  assert.equal(createPlan(FORM, listingOf({})).say, 'There is no tenant on this cell to add them to.')
})

test('the checks, one by one, mirror proxy/src/operator.rs', () => {
  assert.deepEqual(checkName(' Ada Lovelace '), { ok: true, say: '', value: 'Ada Lovelace' })
  assert.equal(checkEmail(' Dev@Example.com ').value, 'dev@example.com')
  assert.equal(checkEmail(`${'a'.repeat(310)}@example.com`).say, 'An address has at most 320 characters.')
  assert.deepEqual(checkPassword('google', 'anything'), { ok: true, say: '', value: null })
  assert.equal(checkPassword('local', 'twelve-chars').ok, true)
  assert.deepEqual(PROVIDERS.map((p) => p.value), ['google', 'github', 'local'])
  assert.deepEqual(ROLE_NAMES, ['admin', 'producer', 'consumer', 'viewer'])
  assert.deepEqual(ROLE_NAMES.map(roleSentence), [
    'An admin can do everything, members and API keys included.',
    'A producer can push messages, and read.',
    'A consumer can pop and acknowledge messages, and read.',
    'A viewer can only read.',
  ])
  assert.equal(roleSentence('owner'), '')
})

test('rename: PATCH /api/operator/users/{id} { name }; the same name is no change', () => {
  const p = renamePlan(user('u-ada'), '  Ada King ')
  assert.equal(p.ok, true)
  assert.deepEqual([p.request.method, p.request.path, p.request.body], ['PATCH', '/api/operator/users/u-ada', { name: 'Ada King' }])
  assert.equal(p.call, 'PATCH /api/operator/users/u-ada {"name":"Ada King"}')
  assert.equal(p.was, 'Ada Lovelace')
  assert.equal(renamePlan(user('u-ada'), 'Ada Lovelace ').ok, false)
  assert.equal(renamePlan(user('u-ada'), 'Ada Lovelace ').same, true)
  assert.equal(renamePlan(user('u-admin'), '').ok, false, 'no name to no name')
  assert.equal(renamePlan(user('u-admin'), 'Admin').ok, true, 'a first name for a user who had none')
  assert.equal(renamePlan({ id: 'a/b?c' }, 'X').request.path, '/api/operator/users/a%2Fb%3Fc', 'the id is encoded')
})

test('role: PUT …/roles/{cluster} { role }; the same role is no change', () => {
  const p = rolePlan(user('u-ada'), cluster(C_DEV), 'producer', L.users)
  assert.equal(p.ok, true)
  assert.deepEqual([p.from, p.to], ['viewer', 'producer'])
  assert.deepEqual([p.request.method, p.request.path, p.request.body], ['PUT', `/api/operator/users/u-ada/roles/${C_DEV}`, { role: 'producer' }])
  assert.equal(p.call, `PUT /api/operator/users/u-ada/roles/${C_DEV} {"role":"producer"}`)
  assert.equal(rolePlan(user('u-ada'), cluster(C_DEV), 'viewer', L.users).ok, false)
  assert.equal(rolePlan(user('u-ada'), cluster(C_DEV), 'owner', L.users).ok, false)
  const give = rolePlan(user('u-bob'), cluster(C_DEV2), 'viewer', L.users)
  assert.deepEqual([give.ok, give.from, give.to], [true, '', 'viewer'], 'a user with no access is given some the same way')
})

test('the last-admin guard: the only admin of a cluster can be neither demoted nor removed', () => {
  assert.equal(adminCount(L.users, C_DEV), 1)
  assert.equal(onlyAdmin(L.users, user('u-admin'), C_DEV), true)
  assert.equal(onlyAdmin(L.users, user('u-ada'), C_DEV), false, 'a viewer is not guarded')
  const demote = rolePlan(user('u-admin'), cluster(C_DEV), 'viewer', L.users)
  assert.equal(demote.ok, false)
  assert.equal(demote.guard, true)
  assert.equal(demote.say, 'admin@localhost is the only admin of dev, and a cluster keeps at least one: make someone else its admin first.')
  const rm = removePlan(user('u-ada'), cluster(C_DEV2), L.users)
  assert.equal(rm.ok, false)
  assert.equal(rm.say, 'Ada Lovelace is the only admin of dev-2, and a cluster keeps at least one: make someone else its admin first.')
  // A second admin lifts it.
  const two = patchRole(L, 'u-ada', cluster(C_DEV), 'admin').users
  assert.equal(adminCount(two, C_DEV), 2)
  assert.equal(rolePlan(two.find((u) => u.id === 'u-admin'), cluster(C_DEV), 'viewer', two).ok, true)
})

test('remove: DELETE …/roles/{cluster} with no body; nothing to remove is no change', () => {
  const p = removePlan(user('u-ada'), cluster(C_DEV), L.users)
  assert.equal(p.ok, true)
  assert.equal(p.from, 'viewer')
  assert.deepEqual([p.request.method, p.request.path, p.request.body], ['DELETE', `/api/operator/users/u-ada/roles/${C_DEV}`, null])
  assert.equal(p.call, `DELETE /api/operator/users/u-ada/roles/${C_DEV}`)
  const none = removePlan(user('u-bob'), cluster(C_DEV), L.users)
  assert.deepEqual([none.ok, none.say], [false, 'bob@example.com has no access to dev.'])
})

test('each request is exactly what api/index.js operator.* sends', () => {
  const { operator, sent } = apiOperator()
  const plans = [
    createPlan(FORM, L),
    createPlan({ ...FORM, provider: 'local', password: 'a long password' }, L),
    renamePlan(user('u-ada'), 'Ada King'),
    rolePlan(user('u-ada'), cluster(C_DEV), 'consumer', L.users),
    removePlan(user('u-ada'), cluster(C_DEV), L.users),
    renamePlan({ id: 'odd id/1' }, 'Odd'),
  ]
  for (const p of plans) {
    const { method, path, body, fn, args } = p.request
    sent.length = 0
    operator[fn](...args, { quiet: true })
    assert.equal(sent.length, 1, fn)
    assert.deepEqual({ method: sent[0].method, url: sent[0].url, body: sent[0].body }, { method, url: path, body }, fn)
    assert.deepEqual(sent[0].config, { quiet: true }, `${fn} passes the request config through`)
  }
})

test('the clusters a user may have access to: their tenant’s on this cell, by name', () => {
  assert.deepEqual(accessRows(user('u-ada'), L.clusters).map((r) => [r.cluster.slug, r.role]), [['dev', 'viewer'], ['dev-2', 'admin']])
  assert.deepEqual(accessRows(user('u-bob'), L.clusters).map((r) => [r.cluster.slug, r.role]), [['dev', ''], ['dev-2', '']])
  assert.deepEqual(accessRows(user('u-carol'), L.clusters).map((r) => r.cluster.slug), ['acme-prod'])
  assert.equal(grantsText(user('u-ada')), 'dev viewer · dev-2 admin')
  assert.equal(grantsText(user('u-bob')), '')
  assert.equal(roleOn(user('u-ada'), C_DEV2), 'admin')
  assert.equal(whoOf(user('u-ada')), 'Ada Lovelace')
  assert.equal(whoOf(user('u-bob')), 'bob@example.com', 'no name: the email')
})

test('the order: by tenant then email once, then a row keeps its place and a new user comes last', () => {
  const r1 = rankNew(new Map(), L.users)
  assert.deepEqual(inRank(L.users, r1).map((u) => u.email), ['carol@acme.io', 'ada@example.com', 'admin@localhost', 'bob@example.com'])
  // A rename, a role change and a new user: nobody moves, the new one is last.
  let next = patchName(L, 'u-bob', 'Aaron')
  next = patchRole(next, 'u-ada', cluster(C_DEV), null)
  next = { ...next, users: [{ id: 'u-abe', email: 'abe@example.com', tenant_slug: 'acme', roles: [] }, ...next.users] }
  const r2 = rankNew(r1, next.users)
  assert.deepEqual(inRank(next.users, r2).map((u) => u.email), ['carol@acme.io', 'ada@example.com', 'admin@localhost', 'bob@example.com', 'abe@example.com'])
  assert.equal(r2.get('u-carol'), 0)
})

test('a filter decides its rows when it changes; a change made under it keeps every row', () => {
  const admins = { q: '', tenant: '', role: 'admin' }
  const s1 = shownSet(L.users, admins, null)
  assert.deepEqual([...s1.ids].sort(), ['u-ada', 'u-admin', 'u-carol'])
  // Ada stops being an admin anywhere: her row stays until the filter changes.
  let next = patchRole(L, 'u-ada', cluster(C_DEV2), 'viewer')
  const s2 = shownSet(next.users, admins, s1)
  assert.deepEqual([...s2.ids].sort(), ['u-ada', 'u-admin', 'u-carol'])
  // A user the filter has not seen joins only if they match.
  next = { ...next, users: [...next.users, { id: 'u-new', email: 'new@x.io', tenant_slug: 'dev', roles: [{ cluster_id: C_DEV, role: 'admin' }] }, { id: 'u-quiet', email: 'q@x.io', tenant_slug: 'dev', roles: [] }] }
  const s3 = shownSet(next.users, admins, s2)
  assert.ok(s3.ids.has('u-new'))
  assert.ok(!s3.ids.has('u-quiet'))
  // A new filter decides afresh.
  const s4 = shownSet(next.users, { ...admins, role: NO_ACCESS }, s3)
  assert.deepEqual([...s4.ids].sort(), ['u-bob', 'u-quiet'])
  // Typing is a new filter at each letter; case is not.
  assert.equal(filterKey({ q: 'ADA ' }), filterKey({ q: 'ada' }))
  assert.deepEqual([...shownSet(L.users, { q: 'lovelace' }, s4).ids], ['u-ada'])
  assert.deepEqual([...shownSet(L.users, { tenant: 'acme' }, null).ids], ['u-carol'])
  // A user added here shows whatever the filter.
  assert.ok(withShown(s4, 'u-added').ids.has('u-added'))
  assert.equal(withShown(s4, 'u-added').key, s4.key)
})

test('the role filter: All, the roles held, No access, each with its count', () => {
  const choices = roleChoices(userGroups(L.users, L.tenants, {}))
  assert.deepEqual(choices.map((c) => [c.value, c.label, c.count]), [
    ['', 'All', undefined], ['admin', 'Admins', 3], ['viewer', 'Viewers', 1], [NO_ACCESS, 'No access', 1],
  ])
})

test('after an add, the new user is at the end, from the proxy’s answer', () => {
  const plan = createPlan(FORM, L)
  const answer = { id: 'u-grace', email: 'grace@example.com', name: 'Grace Hopper', tenant_id: T_DEV, cluster_id: C_DEV, role: 'viewer' }
  const next = patchCreated(L, answer, plan, '2026-10-31T10:00:00Z')
  const last = next.users.at(-1)
  assert.equal(next.users.length, L.users.length + 1)
  assert.deepEqual([last.id, last.email, last.name, last.tenant_slug, last.has_local_password, last.last_login_at], ['u-grace', 'grace@example.com', 'Grace Hopper', 'dev', false, null])
  assert.deepEqual(last.roles, [{ cluster_id: C_DEV, cluster_slug: 'dev', role: 'viewer' }])
  assert.equal(patchCreated(next, answer, plan).users.length, next.users.length, 'the same answer twice is one row')
})

test('the proxy’s answer as a toast, and its refusal', () => {
  const plan = createPlan(FORM, L)
  assert.deepEqual(doneWords('add', { status: 201, plan, answer: { email: 'grace@example.com', role: 'viewer' } }), {
    text: 'grace@example.com added to dev',
    sub: 'The proxy answered 201: a viewer on dev. Their first Google sign-in with this address links the account.',
  })
  const local = createPlan({ ...FORM, provider: 'local', password: 'twelve chars!' }, L)
  assert.match(doneWords('add', { status: 201, plan: local }).sub, /They sign in with the password you set\.$/)
  assert.deepEqual(doneWords('rename', { status: 200, user: user('u-ada'), answer: { name: 'Ada King' } }), {
    text: 'Renamed to Ada King', sub: 'The proxy answered 200. ada@example.com keeps their access as it was.',
  })
  const role = rolePlan(user('u-ada'), cluster(C_DEV), 'producer', L.users)
  assert.equal(doneWords('role', { status: 200, user: user('u-ada'), cluster: cluster(C_DEV), plan: role }).text, 'Ada Lovelace is now a producer on dev')
  assert.equal(doneWords('remove', { status: 200, user: user('u-bob'), cluster: cluster(C_DEV) }).text, 'bob@example.com no longer has access to dev')
  assert.deepEqual(failWords('remove', 400, 'cannot remove or demote the last admin of this cluster'), {
    text: 'Not removed', sub: 'The proxy answered 400: Cannot remove or demote the last admin of this cluster.',
  })
  assert.deepEqual(failWords('add', 409, 'a user with this email already exists'), { text: 'Not added', sub: 'The proxy answered 409: A user with this email already exists.' })
  assert.deepEqual(failWords('rename', 0, 'Cannot reach the API — the proxy or the broker is unreachable'), { text: 'Not renamed', sub: 'Cannot reach the API — the proxy or the broker is unreachable.' })
})
