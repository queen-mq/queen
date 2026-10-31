// Settings › Users and tenants: what an operator's change sends, the checks
// the proxy makes before it, and the rules the list keeps its rows by. Pure:
// no Vue, no API (test/settings-users.test.js).
//
//   add     POST   /api/operator/users                          { tenant_id, name, email, provider, password, cluster_id, role }
//   rename  PATCH  /api/operator/users/{user}                   { name }
//   role    PUT    /api/operator/users/{user}/roles/{cluster}   { role }
//   remove  DELETE /api/operator/users/{user}/roles/{cluster}
//
// The checks are proxy/src/operator.rs's own: an email with an @ (at most 320
// bytes), a name of 1 to 160 characters, a password of 12 to 128 bytes for a
// local account and none for Google or GitHub, one of the four roles, and the
// last-admin guard (a cluster keeps at least one admin). So the dialog says
// why before the proxy refuses. Each request also names the `operator.*`
// function of api/index.js that sends it (`fn`, `args`).
import { NO_ACCESS, ROLES, filterUsers, roleLabel } from '../../composables/accessGroups.js'

export const USERS_PATH = '/api/operator/users'
export const ROLE_NAMES = Object.freeze(ROLES.map((r) => r.name))
/** proxy/src/operator.rs VALID_PROVIDERS, as the add dialog names them. */
export const PROVIDERS = Object.freeze([
  { value: 'google', label: 'Google' },
  { value: 'github', label: 'GitHub' },
  { value: 'local', label: 'Password' },
])
/** What a call shows in place of a password: never the password, nor its length. */
export const MASK = '••••••••'

const list = (v) => (Array.isArray(v) ? v : [])
const enc = encodeURIComponent
const bytes = (s) => new TextEncoder().encode(String(s)).length
const chars = (s) => [...String(s)].length
const userPath = (userId) => `${USERS_PATH}/${enc(userId)}`
const rolePath = (userId, clusterId) => `${userPath(userId)}/roles/${enc(clusterId)}`

const WHAT = { admin: 'an admin', producer: 'a producer', consumer: 'a consumer', viewer: 'a viewer' }
/** "a viewer"; a role this file does not know, by its name. */
export const roleWord = (role) => WHAT[role] || role || 'no role'
/** What a role may do, starting low: "read only". */
export const roleCan = (role) => (ROLES.find((r) => r.name === role)?.can || '').replace(/^./, (c) => c.toLowerCase())
export const providerLabel = (p) => PROVIDERS.find((x) => x.value === p)?.label || p
const CAN = {
  admin: 'do everything, members and API keys included',
  producer: 'push messages, and read',
  consumer: 'pop and acknowledge messages, and read',
  viewer: 'only read',
}
/** "A viewer can only read."; '' for a role this file does not know. */
export const roleSentence = (role) => (CAN[role] ? `${roleWord(role).replace(/^./, (c) => c.toUpperCase())} can ${CAN[role]}.` : '')

/** A person as a sentence names them: their name, else their email. */
export function whoOf(user) {
  const name = String(user?.name ?? '').trim()
  return name || String(user?.email ?? '').trim() || 'this user'
}

/** The listing as the page keeps it: every list there, and every user's roles a list. */
export function listingOf(raw) {
  const d = raw && typeof raw === 'object' ? raw : {}
  return {
    cell: d.cell || null,
    tenants: list(d.tenants),
    clusters: list(d.clusters),
    users: list(d.users).map((u) => ({ ...u, roles: list(u?.roles) })),
  }
}

// ---- the proxy's checks --------------------------------------------------
// Each answers { ok, say, value }: `say` is the one line the dialog shows ('' to
// stay quiet, as for a field not typed yet), `value` what is sent.

/** validate_name: trimmed, 1 to 160 characters. */
export function checkName(raw) {
  const value = String(raw ?? '').trim()
  if (!value) return { ok: false, say: '', value }
  if (chars(value) > 160) return { ok: false, say: 'A name has at most 160 characters.', value }
  return { ok: true, say: '', value }
}

/** validate_email: trimmed and lowercased, with an @. Here also one @ with
 * something on each side and no space, and not an address already on this cell. */
export function checkEmail(raw, users = []) {
  const value = String(raw ?? '').trim().toLowerCase()
  if (!value) return { ok: false, say: '', value }
  if (!/^[^\s@]+@[^\s@]+$/.test(value)) return { ok: false, say: 'Write the whole address, like name@example.com.', value }
  if (bytes(value) > 320) return { ok: false, say: 'An address has at most 320 characters.', value }
  const dup = list(users).find((u) => String(u?.email ?? '').toLowerCase() === value)
  if (dup) return { ok: false, say: `${value} already has an account here, in ${dup.tenant_slug}.`, value }
  return { ok: true, say: '', value }
}

/** validate_password: a local account needs 12 to 128 bytes, as typed (no trim);
 * Google and GitHub accounts take none, and get null. */
export function checkPassword(provider, raw) {
  if (provider !== 'local') return { ok: true, say: '', value: null }
  const value = String(raw ?? '')
  if (!value) return { ok: false, say: '', value }
  if (bytes(value) < 12) return { ok: false, say: 'A password has at least 12 characters.', value }
  if (bytes(value) > 128) return { ok: false, say: 'A password has at most 128 characters.', value }
  return { ok: true, say: '', value }
}

// ---- roles and the last-admin guard ----------------------------------------

/** The user's role on a cluster, '' for none. */
export const roleOn = (user, clusterId) => list(user?.roles).find((g) => g?.cluster_id === clusterId)?.role || ''

/** How many users hold the admin role on the cluster. The listing has every
 * role on every cluster of the cell (store/web.rs kv_operator_listing). */
export const adminCount = (users, clusterId) => list(users).filter((u) => roleOn(u, clusterId) === 'admin').length

/** proxy would_orphan_admins: the cluster's only admin can be neither demoted nor removed. */
export const onlyAdmin = (users, user, clusterId) => roleOn(user, clusterId) === 'admin' && adminCount(users, clusterId) <= 1

/** The clusters of the user's tenant on this cell, by name, each with the user's role there. */
export function accessRows(user, clusters = []) {
  return list(clusters)
    .filter((c) => user && c?.tenant_id === user.tenant_id)
    .sort((a, b) => String(a.slug).localeCompare(String(b.slug)))
    .map((cluster) => ({ cluster, role: roleOn(user, cluster.id) }))
}

/** "dev admin · stage viewer", by cluster name; '' for no access. */
export const grantsText = (user) => [...list(user?.roles)]
  .sort((a, b) => String(a?.cluster_slug).localeCompare(String(b?.cluster_slug)))
  .map((g) => `${g.cluster_slug} ${g.role}`)
  .join(' · ')

// ---- requests ------------------------------------------------------------

/** "POST /api/operator/users {…}": the call as the dialog shows it, the password masked. */
export function callText(req) {
  if (!req) return ''
  let body = req.body
  if (body && typeof body === 'object' && typeof body.password === 'string' && body.password) body = { ...body, password: MASK }
  return body == null ? `${req.method} ${req.path}` : `${req.method} ${req.path} ${JSON.stringify(body)}`
}

/**
 * Add a user, from the dialog's form against the listing.
 *   form: { tenant, cluster (ids), name, email, provider, password, role }
 * → { ok, say, request, call, tenant, cluster, clusters (the tenant's), email, name }
 */
export function createPlan(form = {}, listing = {}) {
  const tenants = list(listing.tenants)
  const tenant = tenants.find((t) => t?.id === form.tenant) || null
  const clusters = tenant
    ? list(listing.clusters).filter((c) => c?.tenant_id === tenant.id).sort((a, b) => String(a.slug).localeCompare(String(b.slug)))
    : []
  const cluster = clusters.find((c) => c.id === form.cluster) || null
  const name = checkName(form.name)
  const email = checkEmail(form.email, listing.users)
  const provider = PROVIDERS.some((p) => p.value === form.provider) ? form.provider : ''
  const password = checkPassword(provider, form.password)
  const role = ROLE_NAMES.includes(form.role) ? form.role : ''

  let say = ''
  if (!tenants.length) say = 'There is no tenant on this cell to add them to.'
  else if (!tenant) say = 'Pick the tenant they belong to.'
  else if (!clusters.length) say = `${tenant.slug} has no cluster on this cell, so they would have no access.`
  else say = name.say || email.say || (provider ? password.say : 'Pick how they sign in.') || (role ? '' : 'Pick their role.')

  const body = {
    tenant_id: tenant?.id || '',
    name: name.value,
    email: email.value,
    provider,
    password: provider === 'local' ? String(form.password ?? '') : null,
    cluster_id: cluster?.id || '',
    role,
  }
  const request = { method: 'POST', path: USERS_PATH, body, fn: 'createUser', args: [body] }
  const ok = !!(tenant && cluster && name.ok && email.ok && provider && password.ok && role)
  return { ok, say, request, call: callText(request), tenant, cluster, clusters, email: email.value, name: name.value }
}

/** Rename a user. Unchanged is not a change. → { ok, say, same, was, name, request, call } */
export function renamePlan(user, raw) {
  const name = checkName(raw)
  const was = String(user?.name ?? '')
  const same = name.value === was.trim()
  const body = { name: name.value }
  const request = user ? { method: 'PATCH', path: userPath(user.id), body, fn: 'updateUser', args: [user.id, body] } : null
  return { ok: !!request && name.ok && !same, say: name.say, same, was, name: name.value, request, call: callText(request) }
}

/** Why the only admin of a cluster keeps the role. */
export const onlyAdminSay = (user, cluster) => `${whoOf(user)} is the only admin of ${cluster.slug}, and a cluster keeps at least one: make someone else its admin first.`

/** Give or change a user's role on a cluster. → { ok, say, from, to, guard, request, call } */
export function rolePlan(user, cluster, role, users = []) {
  const from = roleOn(user, cluster?.id)
  const to = ROLE_NAMES.includes(role) ? role : ''
  const guard = !!cluster && onlyAdmin(users, user, cluster.id)
  const request = user && cluster
    ? { method: 'PUT', path: rolePath(user.id, cluster.id), body: { role: to }, fn: 'setUserRole', args: [user.id, cluster.id, to] }
    : null
  const say = guard && to && to !== 'admin' ? onlyAdminSay(user, cluster) : ''
  return { ok: !!request && !!to && to !== from && !(guard && to !== 'admin'), say, from, to, guard, request, call: callText(request) }
}

/** Take a user's access to a cluster away. → { ok, say, from, guard, request, call } */
export function removePlan(user, cluster, users = []) {
  const from = roleOn(user, cluster?.id)
  const guard = !!cluster && onlyAdmin(users, user, cluster.id)
  const request = user && cluster
    ? { method: 'DELETE', path: rolePath(user.id, cluster.id), body: null, fn: 'removeUserRole', args: [user.id, cluster.id] }
    : null
  let say = ''
  if (cluster && !from) say = `${whoOf(user)} has no access to ${cluster.slug}.`
  else if (guard) say = onlyAdminSay(user, cluster)
  return { ok: !!request && !!from && !guard, say, from, guard, request, call: callText(request) }
}

// ---- the list: its order and its rows --------------------------------------

const byTenantEmail = (a, b) => String(a?.tenant_slug ?? '').localeCompare(String(b?.tenant_slug ?? ''))
  || String(a?.email ?? '').localeCompare(String(b?.email ?? ''))

/**
 * Ranks for the users not ranked yet, after the ones that are, by tenant then
 * email. Ranked once, a user keeps their place while the page is open: a
 * refresh or a change made here never moves a row, and a new user comes in at
 * the end.
 */
export function rankNew(ranks, users) {
  const next = new Map(ranks || [])
  let top = next.size ? Math.max(...next.values()) + 1 : 0
  for (const u of list(users).filter((x) => x?.id && !next.has(x.id)).sort(byTenantEmail)) next.set(u.id, top++)
  return next
}

/** The users in their ranks; one not ranked yet goes last. */
export function inRank(users, ranks) {
  const at = (u) => (ranks?.has(u.id) ? ranks.get(u.id) : Infinity)
  return [...list(users)].sort((a, b) => at(a) - at(b) || byTenantEmail(a, b))
}

/** The address of a filter: typing case does not change it. */
export const filterKey = ({ q = '', tenant = '', role = '' } = {}) => JSON.stringify([String(q).trim().toLowerCase(), tenant, role])

/**
 * The users a filter shows (search q, tenant slug, role or NO_ACCESS),
 * decided when the filter changes. After that, a refresh or a change made
 * here neither hides a row nor adds one: a user who stops matching (demoted
 * under "Admins") keeps their row until the filter changes. Only users the
 * filter has not seen yet (new ones) join, if they match.
 *   prev: the last answer, or null → { key, ids, seen }
 */
export function shownSet(users, filter = {}, prev = null) {
  const key = filterKey(filter)
  const rows = list(users)
  const same = !!prev && prev.key === key
  const ids = same ? new Set(prev.ids) : new Set()
  const seen = same ? new Set(prev.seen) : new Set()
  const match = new Set(filterUsers(rows, { search: filter.q, tenant: filter.tenant, role: filter.role }).map((u) => u.id))
  for (const u of rows) {
    if (seen.has(u.id)) continue
    seen.add(u.id)
    if (match.has(u.id)) ids.add(u.id)
  }
  return { key, ids, seen }
}

/** The same set with a user in it whatever the filter: one just added here. */
export function withShown(set, userId) {
  if (!set) return set
  return { key: set.key, ids: new Set([...set.ids, userId]), seen: new Set([...set.seen, userId]) }
}

/** The role filter's choices: All, then the roles held on the cell (counted
 * from `groups`, accessGroups.userGroups), then No access. */
export function roleChoices(groups) {
  return [
    { value: '', label: 'All' },
    ...list(groups?.roles).map((r) => ({ value: r.role, label: r.role === NO_ACCESS ? 'No access' : roleLabel(r.role), count: r.count })),
  ]
}

// ---- the list after a change, before the next read --------------------------

const withUsers = (listing, users) => ({ ...listing, users })

/** After a rename: the row's name, in place. */
export function patchName(listing, userId, name) {
  return withUsers(listing, list(listing?.users).map((u) => (u.id === userId ? { ...u, name } : u)))
}

/** After a role change (role) or a removal (''): the row's roles, in place. */
export function patchRole(listing, userId, cluster, role) {
  return withUsers(listing, list(listing?.users).map((u) => {
    if (u.id !== userId) return u
    const rest = list(u.roles).filter((g) => g.cluster_id !== cluster.id)
    return { ...u, roles: role ? [...rest, { cluster_id: cluster.id, cluster_slug: cluster.slug, role }] : rest }
  }))
}

/** After an add: the new user, from the proxy's answer, at the end. */
export function patchCreated(listing, answer, plan, now = new Date().toISOString()) {
  const a = answer || {}
  const row = {
    id: String(a.id),
    email: a.email ?? plan.email,
    name: a.name ?? plan.name,
    tenant_id: plan.tenant.id,
    tenant_slug: plan.tenant.slug,
    is_operator: false,
    has_local_password: plan.request.body.provider === 'local',
    created_at: now,
    last_login_at: null,
    roles: [{ cluster_id: plan.cluster.id, cluster_slug: plan.cluster.slug, role: a.role ?? plan.request.body.role }],
  }
  return withUsers(listing, [...list(listing?.users).filter((u) => u.id !== row.id), row])
}

// ---- the proxy's answer, as a toast ------------------------------------------

/** After a change the proxy took. → { text, sub } */
export function doneWords(kind, { status = 200, user, cluster, plan, answer } = {}) {
  const said = `The proxy answered ${status}`
  if (kind === 'add') {
    const email = answer?.email ?? plan?.email
    const p = plan?.request?.body?.provider
    return {
      text: `${email} added to ${plan?.tenant?.slug}`,
      sub: `${said}: ${roleWord(answer?.role ?? plan?.request?.body?.role)} on ${plan?.cluster?.slug}. ${p === 'local' ? 'They sign in with the password you set.' : `Their first ${providerLabel(p)} sign-in with this address links the account.`}`,
    }
  }
  if (kind === 'rename') {
    return { text: `Renamed to ${answer?.name ?? plan?.name}`, sub: `${said}. ${user?.email} keeps their access as it was.` }
  }
  if (kind === 'role') {
    return { text: `${whoOf(user)} is now ${roleWord(plan?.to)} on ${cluster?.slug}`, sub: `${said}. Their access to other clusters stays as it was.` }
  }
  return { text: `${whoOf(user)} no longer has access to ${cluster?.slug}`, sub: `${said}. Their next request to ${cluster?.slug} is refused; the account stays.` }
}

const sentence = (s) => {
  const t = String(s || '').trim().replace(/[.\s]+$/, '')
  return t ? `${t[0].toUpperCase()}${t.slice(1)}.` : ''
}

/** After a refusal. `message` is describeApiError's. → { text, sub } */
export function failWords(kind, status, message) {
  const text = { add: 'Not added', rename: 'Not renamed', role: 'Not changed', remove: 'Not removed' }[kind] || 'Not changed'
  const said = sentence(message) || 'No reason given.'
  return { text, sub: status ? `The proxy answered ${status}: ${said}` : said }
}
