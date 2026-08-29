// Minimal fetch helpers for the console SPA. Deliberately dependency-free
// (no axios) — the console carries zero UI/HTTP libraries beyond Vue itself,
// see ../README.md.
//
// Auth model (matches src/console.rs + src/oauth.rs):
//   1. On load, exchange the httpOnly `queen_session` cookie for a
//      short-lived Bearer session token via GET /auth/session-token.
//   2. Send that Bearer on every /api/console/* and /api/v1/* call (both are
//      same-origin — the console is served by the same queen-proxy instance
//      that serves the data-plane gateway and /auth).
//   3. A 401 at any point (missing/expired cookie, expired session token)
//      redirects to /auth/login?next=/console/, per the task spec.
//   4. Signing out is POST /auth/logout, which clears the cookie server-side
//      and deny-lists the session — without it the httpOnly cookie stays
//      valid until its own exp, which on a shared machine is the whole point.

let sessionToken = null;
let bootstrapping = null;

// Shared-host cluster naming (acting.rs, decision z). On a host listed in
// QUEEN_PROXY_SHARED_HOSTS the Host label names no cluster, so a session must
// name one with the act-as header on EVERY call — /api/console/* and
// /api/v1/* alike. On a per-cluster hostname this stays null and no header is
// sent: the console routes deliberately ignore it there, and the data plane
// HONOURS it there (it would retarget the session), so attaching it
// unconditionally is not an option. App.vue decides from /auth/me whether a
// cluster must be named, and with which one.
let actCluster = null;
let actClusterHeader = 'x-queen-act-cluster';

/** Name the cluster every subsequent apiFetch acts on (a cluster uuid or
 * slug), or clear it with null. `headerName` is /auth/me's
 * `act_cluster_header`, so a renamed header cannot strand the console. */
export function setActCluster(reference, headerName) {
  actCluster = reference || null;
  if (headerName) actClusterHeader = headerName;
}

/** GET /auth/me — the session's identity, clusters, and (crucially) which
 * cluster this very request resolved to. Cookie-authenticated like
 * /auth/session-token: /auth/me describes the browser's session (oauth.rs)
 * and rejects a Bearer. A 401 redirects to login and never resolves. */
export function fetchMe() {
  return fetch('/auth/me', { credentials: 'include' }).then((res) => {
    if (res.status === 401) {
      redirectToLogin();
      return new Promise(() => {}); // navigating away; never settles
    }
    if (!res.ok) {
      throw new Error(`could not read the session identity (HTTP ${res.status})`);
    }
    return res.json();
  });
}

export function redirectToLogin() {
  window.location.href = `/auth/login?next=${encodeURIComponent('/console/')}`;
}

/** Resolves once a session Bearer token is held in memory. Concurrent
 * callers share one in-flight request (bootstrapping). A 401 redirects the
 * page and never resolves (navigation is already underway). */
export function bootstrapSession() {
  if (sessionToken) return Promise.resolve(sessionToken);
  if (bootstrapping) return bootstrapping;
  bootstrapping = fetch('/auth/session-token', { credentials: 'include' })
    .then((res) => {
      if (res.status === 401) {
        redirectToLogin();
        return new Promise(() => {}); // navigating away; never settles
      }
      if (!res.ok) {
        throw new Error(`could not start a session (HTTP ${res.status})`);
      }
      return res.json();
    })
    .then((body) => {
      sessionToken = body.token;
      bootstrapping = null;
      return sessionToken;
    });
  return bootstrapping;
}

/** Authenticated fetch: attaches the Bearer session token, parses the JSON
 * response (console.rs and the broker gateway both always return JSON, error
 * or not), and throws Error(message) with `.status`/`.body` attached on any
 * non-2xx. A 401 mid-session (token outlived its 900s TTL) also redirects to
 * login rather than surfacing a confusing error in the UI. */
export async function apiFetch(path, options = {}) {
  const token = await bootstrapSession();
  const headers = { ...(options.headers || {}), Authorization: `Bearer ${token}` };
  if (actCluster) headers[actClusterHeader] = actCluster;
  const res = await fetch(path, { ...options, headers });
  if (res.status === 401) {
    redirectToLogin();
    return new Promise(() => {});
  }
  const text = await res.text();
  let body = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }
  if (!res.ok) {
    const message = (body && (body.error || body.code)) || `HTTP ${res.status}`;
    const err = new Error(message);
    err.status = res.status;
    err.body = body;
    throw err;
  }
  return body;
}

/** Ends the session: POST /auth/logout (cookie-authenticated — it reads the
 * httpOnly `queen_session` cookie, not the Bearer, so no Authorization header
 * is sent), drop the in-memory Bearer, then back to the login page. The
 * redirect happens even if the call fails: a client that keeps showing the
 * console because logout 502'd is worse than one that sends the user to a
 * login screen they may have to use twice. */
export async function logout() {
  try {
    await fetch('/auth/logout', { method: 'POST', credentials: 'include' });
  } catch {
    // network error — fall through to the redirect below
  }
  sessionToken = null;
  bootstrapping = null;
  redirectToLogin();
}
