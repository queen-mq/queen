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
//   5. On a VERIFY-ONLY host step 1 cannot happen, and the console runs on the
//      cookie alone instead. See `cookieOnly`.

let sessionToken = null;
let bootstrapping = null;

// Verify-only hosts (a fleet cell: the auth host's public key and no signer,
// config::JwtMode::VerifyOnly) cannot mint, and step 1 above is a mint. The
// session is real — it is the cookie the control-plane handoff just adopted,
// `establish_handoff` — so the console drops the Bearer and rides that cookie
// the way the broker dashboard always has. `auth::read_credential` accepts a
// cookie on every route the console calls, so the header is the whole change.
//
// That cookie is NOT `queen_session` on a cell, which is why nothing here names
// one. The handoff writes a host-only, cell-scoped cookie instead
// (`auth::cell_cookie_name_of`: `__Host-queen_session_cell` when Secure), so
// that opening one cell's console cannot overwrite the browser's fleet-wide
// session and sign it out of its siblings. `auth::read_session_cookie` prefers
// the cell cookie and falls back to the fleet one; `credentials: 'include'`
// below offers whichever exist and lets the host choose between them.
//
// Not solved by handing the SPA the cookie's own token instead: the cookie is
// httpOnly so that a scripting bug cannot read a session out of the browser,
// and a mint that echoed it would trade a 900s bearer for one living the
// control plane's full TTL. A host that cannot mint should hold no bearer.
let cookieOnly = false;

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

/** Is this the verify-only host's "I hold no signer" answer?
 *
 * Both halves are required. A bare 503 is what a load balancer says while the
 * instance behind it restarts, and that one is transient and must stay an
 * error; `not_configured` is oauth.rs's `err_no_signer`, which is a permanent
 * fact about the host. Treating every 503 as cookie-only would answer a blip
 * by silently stripping the Authorization header off the whole session. */
function isNoSigner(status, body) {
  return status === 503 && !!body && body.code === 'not_configured';
}

/** Resolves once the session is usable: with a Bearer token on a mint-capable
 * host, or with null on a verify-only one, where the cookie IS the session and
 * `cookieOnly` is now set. Concurrent callers share one in-flight request
 * (bootstrapping). A 401 redirects the page and never resolves (navigation is
 * already underway). */
export function bootstrapSession() {
  if (sessionToken || cookieOnly) return Promise.resolve(sessionToken);
  if (bootstrapping) return bootstrapping;
  bootstrapping = fetch('/auth/session-token', { credentials: 'include' })
    .then(async (res) => {
      if (res.status === 401) {
        redirectToLogin();
        return new Promise(() => {}); // navigating away; never settles
      }
      // Read the body before branching: the no-signer answer is told from a
      // transient 503 by the `code` inside it.
      const body = await res.json().catch(() => null);
      if (isNoSigner(res.status, body)) {
        cookieOnly = true;
        bootstrapping = null;
        return null;
      }
      if (!res.ok) {
        throw new Error(`could not start a session (HTTP ${res.status})`);
      }
      // A 2xx carrying no token is a broken host, not a cookie-only one:
      // falling back here would drop the Bearer on a host that does mint.
      if (!body || !body.token) {
        throw new Error('could not start a session (no token in the response)');
      }
      sessionToken = body.token;
      bootstrapping = null;
      return sessionToken;
    });
  return bootstrapping;
}

/** Authenticated fetch: attaches the Bearer session token (or, in cookieOnly
 * mode, no Authorization header at all, leaving the request to authenticate on
 * the session cookie), parses the JSON response (console.rs and the broker
 * gateway both always return JSON, error or not), and throws Error(message)
 * with `.status`/`.body` attached on any non-2xx. A 401 mid-session (token
 * outlived its 900s TTL, or the adopted cookie reached its own exp) also
 * redirects to login rather than surfacing a confusing error in the UI. */
export async function apiFetch(path, options = {}) {
  const token = await bootstrapSession();
  const headers = { ...(options.headers || {}) };
  // An empty Bearer is not a weaker credential, it is a malformed one: it
  // suppresses the cookie fallback in read_credential and 401s the console.
  if (token) headers.Authorization = `Bearer ${token}`;
  if (actCluster) headers[actClusterHeader] = actCluster;
  // Same-origin already sends the cookie by default; saying so keeps the one
  // credential a cookieOnly session has from depending on that default.
  const res = await fetch(path, { ...options, headers, credentials: 'include' });
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
  cookieOnly = false;
  redirectToLogin();
}

/** Test seam: drop every scrap of session state this module holds, so a test
 * can boot the console twice in one process against two different hosts. */
export function __resetSessionForTests() {
  sessionToken = null;
  bootstrapping = null;
  cookieOnly = false;
  actCluster = null;
  actClusterHeader = 'x-queen-act-cluster';
}
