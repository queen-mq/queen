// Auth-mode tests for the console's fetch helpers.
//
// The runner is node's own (`node --test`, Node 24+): the console carries no
// UI or HTTP library beyond Vue, and a test runner is not the place to break
// that. `fetch` and `window` are the only globals api.js touches, and both are
// stubbed here, so nothing boots — no vite, no jsdom, no browser.
//
// What these pin is the AUTH MODE the console picks per host, which is not a
// preference but a property of the cell it was served by:
//
//   mint-capable host  ->  Bearer from /auth/session-token   (the auth host)
//   verify-only host   ->  the session cookie, no Bearer     (a fleet cell)
//
// The second row is the bug this file was written for. A fleet cell holds the
// auth host's public key and no signer, so /auth/session-token cannot mint and
// answers 503 not_configured. api.js treated that as fatal and every cell's
// /console/ died on load with "could not start a session (HTTP 503)", while
// the session it needed was sitting in the cookie the handoff had just set.

import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  apiFetch,
  bootstrapSession,
  setActCluster,
  __resetSessionForTests,
} from '../src/api.js';

// --- harness ---------------------------------------------------------------

/** One canned HTTP response, shaped like the parts of `Response` api.js uses.
 * Body is re-serialised per call so a route can answer more than once. */
function reply(status, body) {
  const text = body === undefined ? '' : JSON.stringify(body);
  return () => ({
    status,
    ok: status >= 200 && status < 300,
    json: async () => JSON.parse(text), // throws on '' exactly like fetch does
    text: async () => text,
  });
}

/** Install a fetch stub over `routes` (path -> reply()) and return the call
 * log. An unrouted path throws rather than defaulting to 200: a test that
 * silently passes because the console called something else is worthless. */
function stubFetch(routes) {
  const calls = [];
  globalThis.fetch = async (path, options = {}) => {
    calls.push({ path, options });
    const route = routes[path];
    if (!route) throw new Error(`unexpected fetch: ${path}`);
    return route();
  };
  return calls;
}

/** Fresh module state + a fresh fake location for each test. */
function reset() {
  __resetSessionForTests();
  globalThis.window = { location: { href: '' } };
}

const NO_SIGNER = reply(503, {
  code: 'not_configured',
  error: 'this proxy has no JWT signer, so it cannot issue sessions: set ...',
});
const MINTED = reply(200, { token: 'minted.jwt.value', expires_in: 900 });
const OVERVIEW = reply(200, { clusters: 1 });

/** The one data call, plus the bootstrap that precedes it. */
function callsFor(routes) {
  return stubFetch({ '/auth/session-token': routes.bootstrap, '/api/console/overview': OVERVIEW });
}

const dataCall = (calls) => calls.find((c) => c.path === '/api/console/overview');

// --- verify-only cells: the regression --------------------------------------

test('a verify-only cell boots the console on the cookie instead of failing', async () => {
  reset();
  const calls = callsFor({ bootstrap: NO_SIGNER });

  const body = await apiFetch('/api/console/overview');

  assert.deepEqual(body, { clusters: 1 }, 'the data call must succeed, not throw');
  assert.equal(window.location.href, '', 'no-signer is not a login problem: no redirect');
});

test('a cookie-only session sends no Authorization header at all', async () => {
  reset();
  const calls = callsFor({ bootstrap: NO_SIGNER });

  await apiFetch('/api/console/overview');

  const headers = dataCall(calls).options.headers;
  // Not `Bearer null`, not `Bearer `: read_credential treats ANY non-empty
  // Authorization value as the chosen credential and stops before the cookie
  // fallback, so a placeholder header would 401 every call.
  assert.ok(!('Authorization' in headers), `unexpected Authorization: ${headers.Authorization}`);
});

test('a cookie-only session sends the cookie', async () => {
  reset();
  const calls = callsFor({ bootstrap: NO_SIGNER });

  await apiFetch('/api/console/overview');

  assert.equal(dataCall(calls).options.credentials, 'include');
});

test('the no-signer answer is learned once, not re-probed per call', async () => {
  reset();
  const calls = callsFor({ bootstrap: NO_SIGNER });

  await apiFetch('/api/console/overview');
  await apiFetch('/api/console/overview');

  const probes = calls.filter((c) => c.path === '/auth/session-token');
  assert.equal(probes.length, 1, 'every view would otherwise re-ask a host that already said no');
});

test('a cookie-only session still names its cluster on a shared host', async () => {
  reset();
  const calls = callsFor({ bootstrap: NO_SIGNER });
  setActCluster('11111111-2222-3333-4444-555555555555', 'x-queen-act-cluster');

  await apiFetch('/api/console/overview');

  // Dropping the Bearer must not drop the act-as header with it: on a shared
  // host that header is what names the cluster, and without it the call 403s.
  assert.equal(
    dataCall(calls).options.headers['x-queen-act-cluster'],
    '11111111-2222-3333-4444-555555555555',
  );
});

// --- the auth host must be untouched ----------------------------------------

test('a mint-capable host still sends the minted Bearer', async () => {
  reset();
  const calls = callsFor({ bootstrap: MINTED });

  await apiFetch('/api/console/overview');

  assert.equal(dataCall(calls).options.headers.Authorization, 'Bearer minted.jwt.value');
});

test('concurrent callers share one bootstrap request', async () => {
  reset();
  const calls = callsFor({ bootstrap: MINTED });

  await Promise.all([bootstrapSession(), bootstrapSession(), bootstrapSession()]);

  assert.equal(calls.filter((c) => c.path === '/auth/session-token').length, 1);
});

// --- everything that must STAY an error -------------------------------------

test('a 503 without not_configured stays an error', async () => {
  reset();
  // A proxy in front of a restarting instance. Transient, and nothing about it
  // says this host cannot mint — degrading to cookie-only here would strip the
  // Bearer off a perfectly capable host for the rest of the session.
  callsFor({ bootstrap: reply(503, { code: 'unavailable', error: 'upstream restarting' }) });

  await assert.rejects(
    () => apiFetch('/api/console/overview'),
    /could not start a session \(HTTP 503\)/,
  );
});

test('a 503 with an unreadable body stays an error', async () => {
  reset();
  callsFor({ bootstrap: reply(503) }); // empty body: res.json() throws

  await assert.rejects(
    () => apiFetch('/api/console/overview'),
    /could not start a session \(HTTP 503\)/,
  );
});

test('a 200 carrying no token stays an error', async () => {
  reset();
  callsFor({ bootstrap: reply(200, { expires_in: 900 }) });

  await assert.rejects(() => apiFetch('/api/console/overview'), /no token in the response/);
});

test('no session still redirects to login', async () => {
  reset();
  callsFor({ bootstrap: reply(401, { code: 'unauthorized', error: 'no session' }) });

  // By design this promise never settles — the page is navigating away — so it
  // is started and not awaited; one macrotask is enough for the redirect.
  apiFetch('/api/console/overview');
  await new Promise((r) => setTimeout(r, 0));

  // On a verify-only cell /auth/login is the "sign in at the portal" notice
  // (oauth.rs `SignIn::Elsewhere`), which is the right destination for a cell
  // whose cookie expired.
  assert.equal(window.location.href, '/auth/login?next=%2Fconsole%2F');
});
