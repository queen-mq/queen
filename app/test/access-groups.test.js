import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  NO_ACCESS, ROLES, ROLE_ORDER, SCOPES, filterKeys, filterMembers, filterUsers, inScopeOrder, keyGroups,
  keyPickTitle, keyState, memberGroups, memberPickTitle, pickKeyState, roleCan, roleLabel, userGroups,
  userHasRole, userMatches, userPickTitle,
} from '../src/composables/accessGroups.js'

test('a role reads as a plural noun on its row', () => {
  assert.deepEqual(['admin', 'producer', 'consumer', 'viewer'].map(roleLabel), ['Admins', 'Producers', 'Consumers', 'Viewers'])
  assert.equal(roleLabel(''), '')
})

// Shapes as proxy/src/console.rs list_members / list_keys answer them.
const members = [
  { user_id: 'u1', email: 'ada@x.io', role: 'viewer', granted_at: '2026-10-01T00:00:00Z' },
  { user_id: 'u2', email: 'bob@x.io', role: 'admin', granted_at: '2026-10-02T00:00:00Z' },
  { user_id: 'u3', email: 'cy@x.io', role: 'producer', granted_at: '2026-10-03T00:00:00Z' },
  { user_id: 'u4', email: 'di@x.io', role: 'viewer', granted_at: '2026-10-04T00:00:00Z' },
]

test('members: the roles the list has, most privileged first, with counts', () => {
  const g = memberGroups(members)
  assert.equal(g.all, 4)
  assert.deepEqual(g.roles, [
    { role: 'admin', count: 1 },
    { role: 'producer', count: 1 },
    { role: 'viewer', count: 2 },
  ])
  // consumer is nobody's role: no row for it.
  assert.ok(!g.roles.some(r => r.role === 'consumer'))
})

test('members: a picked role nobody has stays at 0; an unknown role sorts last', () => {
  assert.deepEqual(memberGroups(members, 'consumer').roles.find(r => r.role === 'consumer'), { role: 'consumer', count: 0 })
  const g = memberGroups([...members, { email: 'e@x.io', role: 'auditor' }])
  assert.equal(g.roles.at(-1).role, 'auditor')
})

test('members: the filter keeps the picked role only, all on no pick', () => {
  assert.deepEqual(filterMembers(members, 'viewer').map(m => m.email), ['ada@x.io', 'di@x.io'])
  assert.equal(filterMembers(members, '').length, 4)
  assert.deepEqual(filterMembers(members, 'consumer'), [])
  assert.deepEqual(memberGroups(null), { all: 0, roles: [] })
  assert.deepEqual(filterMembers(undefined, 'admin'), [])
})

const keys = [
  { id: 'k1', name: 'billing', scopes: ['read'], created_at: '2026-10-01', last_used_at: '2026-10-29', revoked_at: null },
  { id: 'k2', name: 'audit', scopes: ['read'], created_at: '2026-10-02', last_used_at: null, revoked_at: null },
  { id: 'k3', name: 'old', scopes: ['produce'], created_at: '2026-09-01', last_used_at: null, revoked_at: '2026-10-10' },
]

test('keys: active and revoked, from revoked_at, with counts and never-used', () => {
  assert.equal(keyState(keys[0]), 'active')
  assert.equal(keyState(keys[2]), 'revoked')
  const g = keyGroups(keys)
  assert.equal(g.all, 3)
  assert.deepEqual(g.states, [
    { state: 'active', name: 'Active', count: 2, neverUsed: 1 },
    { state: 'revoked', name: 'Revoked', count: 1, neverUsed: 1 },
  ])
  // Both states always have a row, even at 0.
  assert.deepEqual(keyGroups([]).states.map(s => s.count), [0, 0])
})

test('keys: the filter, and a state the payload cannot have reads as all keys', () => {
  assert.deepEqual(filterKeys(keys, 'active').map(k => k.id), ['k1', 'k2'])
  assert.deepEqual(filterKeys(keys, 'revoked').map(k => k.id), ['k3'])
  assert.equal(pickKeyState('expired'), '')
  assert.equal(filterKeys(keys, 'expired').length, 3)
  assert.equal(filterKeys(keys, '').length, 3)
})

// Shape as proxy/src/operator.rs list_users answers it.
const tenants = [
  { id: 't2', slug: 'globex', name: 'Globex Corp' },
  { id: 't1', slug: 'acme', name: 'acme' },
]
const users = [
  { id: 'a', email: 'ada@acme.io', name: 'Ada', tenant_id: 't1', tenant_slug: 'acme',
    roles: [{ cluster_id: 'c1', cluster_slug: 'acme-prod', role: 'admin' }, { cluster_id: 'c2', cluster_slug: 'acme-dev', role: 'viewer' }] },
  { id: 'b', email: 'bob@acme.io', name: 'Bob', tenant_id: 't1', tenant_slug: 'acme',
    roles: [{ cluster_id: 'c1', cluster_slug: 'acme-prod', role: 'viewer' }] },
  { id: 'c', email: 'cy@globex.io', name: '', tenant_id: 't2', tenant_slug: 'globex', roles: [] },
]

test('users: search matches name, email or tenant, case-blind', () => {
  assert.ok(userMatches(users[0], 'ADA'))
  assert.ok(userMatches(users[2], 'globex'))
  assert.ok(userMatches(users[1], '  '))
  assert.ok(!userMatches(users[1], 'globex'))
})

test('users: tenants by slug and roles held, with counts; no-access only when someone has none', () => {
  const g = userGroups(users, tenants)
  assert.equal(g.all, 3)
  assert.deepEqual(g.tenants, [
    { slug: 'acme', name: '', count: 2 }, // name equal to the slug is not repeated
    { slug: 'globex', name: 'Globex Corp', count: 1 },
  ])
  // Ada holds admin and viewer: she counts under both.
  assert.deepEqual(g.roles.map(r => [r.role, r.count]), [['admin', 1], ['viewer', 2], [NO_ACCESS, 1]])
  assert.equal(g.roles.at(-1).name, 'No access')
  assert.ok(!userGroups(users.slice(0, 2), tenants).roles.some(r => r.role === NO_ACCESS))
})

test('users: rows stay while the search narrows; counts follow it; a pick stays at 0', () => {
  const g = userGroups(users, tenants, { search: 'bob' })
  assert.equal(g.all, 1)
  assert.deepEqual(g.tenants.map(t => [t.slug, t.count]), [['acme', 1], ['globex', 0]])
  assert.deepEqual(g.roles.map(r => [r.role, r.count]), [['admin', 0], ['viewer', 1], [NO_ACCESS, 0]])
  const gone = userGroups(users.slice(0, 2), tenants, { tenant: 'initech', role: NO_ACCESS })
  assert.deepEqual(gone.tenants.find(t => t.slug === 'initech'), { slug: 'initech', name: '', count: 0 })
  assert.deepEqual(gone.roles.at(-1), { role: NO_ACCESS, name: 'No access', count: 0 })
  assert.deepEqual(userGroups(undefined, undefined), { all: 0, tenants: [], roles: [] })
})

test('users: the filter combines search, tenant and role', () => {
  assert.ok(userHasRole(users[0], 'viewer'))
  assert.ok(userHasRole(users[2], NO_ACCESS))
  assert.deepEqual(filterUsers(users, { role: 'viewer' }).map(u => u.id), ['a', 'b'])
  assert.deepEqual(filterUsers(users, { tenant: 'globex' }).map(u => u.id), ['c'])
  assert.deepEqual(filterUsers(users, { role: NO_ACCESS }).map(u => u.id), ['c'])
  assert.deepEqual(filterUsers(users, { tenant: 'acme', role: 'admin', search: 'ada' }).map(u => u.id), ['a'])
  assert.equal(filterUsers(users).length, 3)
})

test('roles and scopes: the proxy\'s lists, each with one short line of what it allows', () => {
  assert.deepEqual(ROLE_ORDER, ['admin', 'producer', 'consumer', 'viewer'])
  assert.ok(ROLES.every(r => r.can && !r.can.includes('\n')))
  assert.equal(roleCan('viewer'), 'Read only')
  assert.equal(roleCan('auditor'), '')
  assert.deepEqual(SCOPES.map(s => s.name).sort(), ['admin', 'consume', 'produce', 'read'])
  assert.deepEqual(inScopeOrder(['admin', 'read', 'produce']), ['read', 'produce', 'admin'])
  assert.deepEqual(inScopeOrder(['later', 'consume']), ['consume', 'later'])
  assert.deepEqual(inScopeOrder(null), [])
})

test('a pick names the page: all, a role, a state, a tenant, no access', () => {
  assert.equal(memberPickTitle(''), 'All members')
  assert.equal(memberPickTitle('admin'), 'Admins')
  assert.equal(keyPickTitle(''), 'All keys')
  assert.equal(keyPickTitle('active'), 'Active keys')
  assert.equal(keyPickTitle('revoked'), 'Revoked keys')
  assert.equal(keyPickTitle('expired'), 'All keys')
  assert.equal(userPickTitle(), 'All users')
  assert.equal(userPickTitle({ tenant: 'acme' }), 'acme')
  assert.equal(userPickTitle({ role: 'viewer' }), 'Viewers')
  assert.equal(userPickTitle({ role: NO_ACCESS }), 'No access')
})
