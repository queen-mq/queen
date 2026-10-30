// The sub sidebars of the Access pages (Members, API keys, Users): the groups
// each list falls into, with counts, and the filter a pick applies. Plain
// functions over the payloads the pages already read — proxy/src/console.rs
// list_members ({ user_id, email, role, granted_at }) and list_keys ({ id,
// name, scopes, created_at, last_used_at, revoked_at }), proxy/src/operator.rs
// list_users ({ cell, tenants, clusters, users[].roles[] }) — so the counts
// in the column and the rows on the page come from one rule. Pure:
// test/access-groups.test.js.

const list = value => (Array.isArray(value) ? value : [])

// proxy/src/console.rs VALID_ROLES, most privileged first, with what each
// may do (the proxy's RouteClass table, mirrored by stores/identity.js
// CAPABILITIES). A role the proxy adds later still shows, after these, by
// name.
export const ROLES = Object.freeze([
  { name: 'admin', can: 'Everything, including members and API keys' },
  { name: 'producer', can: 'Push messages, and read' },
  { name: 'consumer', can: 'Pop and acknowledge messages, and read' },
  { name: 'viewer', can: 'Read only' },
])
export const ROLE_ORDER = ROLES.map(r => r.name)

/** What a role may do, in one short line; '' for a role this file does not know. */
export const roleCan = role => ROLES.find(r => r.name === role)?.can || ''

// proxy/src/console.rs VALID_SCOPES, in the order a key's scopes are listed.
export const SCOPES = Object.freeze([
  { name: 'read', can: 'List queues, read messages and stats' },
  { name: 'produce', can: 'Push messages' },
  { name: 'consume', can: 'Pop and acknowledge messages' },
  { name: 'admin', can: 'Configure and delete queues' },
])

/** A key's scopes in that one order, whatever order it was created with. */
export function inScopeOrder(scopes) {
  const rank = s => {
    const i = SCOPES.findIndex(x => x.name === s)
    return i < 0 ? SCOPES.length : i
  }
  return [...list(scopes)].sort((a, b) => rank(a) - rank(b) || String(a).localeCompare(String(b)))
}

const roleRank = role => {
  const i = ROLE_ORDER.indexOf(role)
  return i < 0 ? ROLE_ORDER.length : i
}
const byRole = (a, b) => roleRank(a) - roleRank(b) || String(a).localeCompare(String(b))

/** A role as a row says it: "admin" -> "Admins". */
export const roleLabel = role => (role ? `${role[0].toUpperCase()}${role.slice(1)}s` : '')

// ---------------------------------------------------------------------------
// Members: one role per member on the acting cluster.
// ---------------------------------------------------------------------------

/** The roles the members really have, most privileged first, each with its
 * count. A picked role nobody has any more stays, at 0, so the column still
 * shows why the list is empty. */
export function memberGroups(members, picked = '') {
  const rows = list(members)
  const counts = new Map()
  for (const m of rows) if (m?.role) counts.set(m.role, (counts.get(m.role) || 0) + 1)
  if (picked && !counts.has(picked)) counts.set(picked, 0)
  return {
    all: rows.length,
    roles: [...counts.keys()].sort(byRole).map(role => ({ role, count: counts.get(role) })),
  }
}

export function filterMembers(members, role = '') {
  const rows = list(members)
  return role ? rows.filter(m => m?.role === role) : rows
}

/** The page's title for a pick: "All members", "Admins". */
export const memberPickTitle = role => (role ? roleLabel(role) : 'All members')

// ---------------------------------------------------------------------------
// API keys: list_keys answers revoked_at and nothing about expiry, so a key
// is active or revoked — there is no "expired" to show.
// ---------------------------------------------------------------------------

export const KEY_STATES = [
  { state: 'active', name: 'Active' },
  { state: 'revoked', name: 'Revoked' },
]

export const keyState = key => (key?.revoked_at ? 'revoked' : 'active')

/** A state from the address, or '' (all keys) for anything else. */
export const pickKeyState = raw => (KEY_STATES.some(s => s.state === raw) ? raw : '')

export function keyGroups(keys) {
  const rows = list(keys)
  return {
    all: rows.length,
    states: KEY_STATES.map(({ state, name }) => {
      const mine = rows.filter(k => keyState(k) === state)
      return { state, name, count: mine.length, neverUsed: mine.filter(k => !k?.last_used_at).length }
    }),
  }
}

export function filterKeys(keys, state = '') {
  const rows = list(keys)
  const pick = pickKeyState(state)
  return pick ? rows.filter(k => keyState(k) === pick) : rows
}

/** The page's title for a pick: "All keys", "Active keys", "Revoked keys". */
export function keyPickTitle(state) {
  const pick = KEY_STATES.find(s => s.state === pickKeyState(state))
  return pick ? `${pick.name} keys` : 'All keys'
}

// ---------------------------------------------------------------------------
// Users (operator, whole cell): one tenant per user; roles are per cluster,
// so a user holds none, one or several.
// ---------------------------------------------------------------------------

/** The pick for users who hold no role on any cluster of the cell. */
export const NO_ACCESS = 'none'

const grantsOf = user => list(user?.roles)

/** The page's search: name, email or tenant, case-blind. */
export function userMatches(user, search = '') {
  const query = String(search || '').trim().toLowerCase()
  if (!query) return true
  return [user?.name, user?.email, user?.tenant_slug].some(v => String(v || '').toLowerCase().includes(query))
}

/** Does the user hold this role on at least one cluster (or none at all, for NO_ACCESS)? */
export function userHasRole(user, role) {
  const grants = grantsOf(user)
  return role === NO_ACCESS ? grants.length === 0 : grants.some(g => g?.role === role)
}

export function filterUsers(users, { search = '', tenant = '', role = '' } = {}) {
  return list(users).filter(u =>
    userMatches(u, search) &&
    (!tenant || u?.tenant_slug === tenant) &&
    (!role || userHasRole(u, role)))
}

/** The page's title for a pick: a tenant's slug, "Admins", "No access", "All users". */
export function userPickTitle({ tenant = '', role = '' } = {}) {
  if (tenant) return tenant
  if (role === NO_ACCESS) return 'No access'
  if (role) return roleLabel(role)
  return 'All users'
}

/** The column's rows: tenants (by slug) and roles held on the cell. The rows
 * come from the whole list, so they stay put while the search narrows; the
 * counts follow the search. One pick at a time, so each count is what a
 * click on that row shows. */
export function userGroups(users, tenants, { search = '', tenant = '', role = '' } = {}) {
  const rows = list(users)
  const matching = rows.filter(u => userMatches(u, search))

  const names = new Map()
  for (const t of list(tenants)) if (t?.slug) names.set(t.slug, t.name || '')
  for (const u of rows) if (u?.tenant_slug && !names.has(u.tenant_slug)) names.set(u.tenant_slug, '')
  if (tenant && !names.has(tenant)) names.set(tenant, '')
  const tenantRows = [...names.keys()].sort((a, b) => a.localeCompare(b)).map(slug => ({
    slug,
    name: names.get(slug) && names.get(slug) !== slug ? names.get(slug) : '',
    count: matching.filter(u => u?.tenant_slug === slug).length,
  }))

  const held = new Set()
  for (const u of rows) for (const g of grantsOf(u)) if (g?.role) held.add(g.role)
  if (role && role !== NO_ACCESS) held.add(role)
  const roleRows = [...held].sort(byRole).map(r => ({
    role: r, name: r, count: matching.filter(u => userHasRole(u, r)).length,
  }))
  if (role === NO_ACCESS || rows.some(u => !grantsOf(u).length)) {
    roleRows.push({ role: NO_ACCESS, name: 'No access', count: matching.filter(u => userHasRole(u, NO_ACCESS)).length })
  }

  return { all: matching.length, tenants: tenantRows, roles: roleRows }
}
