/**
 * Pure helpers for the Traefik ForwardAuth handler.
 *
 * None of these functions touch the network or write to res, which keeps the
 * route handler small and the helpers trivial to unit-test in the future.
 */

const FORWARD_AUTH_ENABLED = (process.env.FORWARD_AUTH_ENABLED || 'false').toLowerCase() === 'true';
const AUTH_HOST = process.env.AUTH_HOST || '';

const FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS = (process.env.FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS || '')
  .split(',')
  .map((h) => h.trim().toLowerCase())
  .filter(Boolean);

const SUPPORTED_EMIT_HEADERS = new Set([
  'Authorization',
  'X-Auth-User',
  'X-Auth-Email',
  'X-Auth-Sub',
  'X-Auth-Role',
  'X-Auth-Groups',
]);

const FORWARD_AUTH_EMIT_HEADERS = (() => {
  const raw = process.env.FORWARD_AUTH_EMIT_HEADERS || 'Authorization';
  const set = new Set();
  for (const name of raw.split(',')) {
    const trimmed = name.trim();
    if (!trimmed) continue;
    // Case-insensitive match against the canonical names.
    const canonical = [...SUPPORTED_EMIT_HEADERS].find(
      (h) => h.toLowerCase() === trimmed.toLowerCase()
    );
    if (canonical) set.add(canonical);
  }
  return set;
})();

export function isForwardAuthEnabled() {
  return FORWARD_AUTH_ENABLED;
}

export function getForwardAuthConfig() {
  return {
    enabled: FORWARD_AUTH_ENABLED,
    authHost: AUTH_HOST,
    allowedRedirectHosts: [...FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS],
    emitHeaders: [...FORWARD_AUTH_EMIT_HEADERS],
  };
}

/**
 * Reconstruct the original client-facing request from the X-Forwarded-* headers
 * Traefik sets when calling a ForwardAuth middleware. The handler MUST use this
 * — never `req.url` / `req.method` — because the proxy is being called by
 * Traefik on behalf of a different upstream request.
 *
 * Falls back to the local request fields when the headers are missing (useful
 * for direct curl tests during development).
 */
export function parseForwardedRequest(req) {
  const h = req.headers || {};
  const method = String(h['x-forwarded-method'] || req.method || 'GET').toUpperCase();
  const proto = String(h['x-forwarded-proto'] || (req.secure ? 'https' : 'http'));
  const host = String(h['x-forwarded-host'] || h['host'] || '');
  const uri = String(h['x-forwarded-uri'] || '/');
  const originalUrl = host ? `${proto}://${host}${uri.startsWith('/') ? uri : '/' + uri}` : '';
  return { method, proto, host, uri, originalUrl };
}

/**
 * Per-route options injected by Traefik labels as request headers. We use
 * custom headers (not query params) because Traefik's ForwardAuth middleware
 * passes through arbitrary `customRequestHeaders` cleanly without leaking them
 * to upstream after auth resolution.
 *
 *  - `X-Forward-Auth-RBAC: method`     → enforce checkMethodAccess-style RBAC
 *  - `X-Forward-Auth-Headers: name,…`  → narrow the global emit allowlist
 */
export function parseRouteOpts(req) {
  const h = req.headers || {};
  const enforceMethodRBAC =
    String(h['x-forward-auth-rbac'] || '').toLowerCase() === 'method';

  let emitHeaders = FORWARD_AUTH_EMIT_HEADERS;
  const overrideRaw = h['x-forward-auth-headers'];
  if (overrideRaw) {
    const requested = new Set();
    for (const name of String(overrideRaw).split(',')) {
      const trimmed = name.trim();
      if (!trimmed) continue;
      const canonical = [...SUPPORTED_EMIT_HEADERS].find(
        (n) => n.toLowerCase() === trimmed.toLowerCase()
      );
      if (canonical && FORWARD_AUTH_EMIT_HEADERS.has(canonical)) {
        requested.add(canonical);
      }
    }
    emitHeaders = requested;
  }

  return { enforceMethodRBAC, emitHeaders };
}

/**
 * Heuristic: did this look like a browser navigation, or an API call?
 * Used to decide between a 302 redirect and a 401+location-header response.
 */
export function wantsHtml(req) {
  const accept = String(req.headers?.['accept'] || '').toLowerCase();
  const xrw = req.headers?.['x-requested-with'];
  if (xrw) return false;
  if (accept.includes('application/json')) return false;
  // Default to HTML when Accept is missing or includes text/html — covers the
  // common browser-navigation case Traefik forwards us.
  return accept === '' || accept.includes('text/html') || accept.includes('*/*');
}

/**
 * Validate a host against the redirect allowlist. Empty allowlist denies
 * everything (fail-closed) — operators MUST opt-in to cross-host redirects.
 */
export function isAllowedRedirect(host) {
  if (!host) return false;
  const lower = String(host).toLowerCase();
  return FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS.includes(lower);
}

/**
 * Build the absolute login URL for the bounce. Returns null if AUTH_HOST is
 * not configured (caller should 401 in that case rather than emit a relative
 * Location that Traefik would silently rewrite).
 */
export function buildLoginRedirect(originalUrl) {
  if (!AUTH_HOST) return null;
  const base = `https://${AUTH_HOST}/login`;
  if (!originalUrl) return base;
  // Only round-trip the redirect_uri if its host is in the allowlist;
  // otherwise drop it so we never send a user back to an unknown origin.
  let host = '';
  try {
    host = new URL(originalUrl).host;
  } catch {
    /* malformed — fall through with no redirect_uri */
  }
  if (host && isAllowedRedirect(host)) {
    return `${base}?redirect_uri=${encodeURIComponent(originalUrl)}`;
  }
  return base;
}

/**
 * Compute the X-Auth-* response headers Traefik will copy to the upstream
 * request when the protected router opts-in via authResponseHeaders. The
 * `Authorization` header carries the (already minted or passthrough) bearer
 * token — caller passes it in to keep this function side-effect-free.
 */
export function selectResponseHeaders(claims, opts, bearerToken) {
  const headers = {};
  const emit = opts?.emitHeaders || FORWARD_AUTH_EMIT_HEADERS;

  if (emit.has('Authorization') && bearerToken) {
    headers['Authorization'] = `Bearer ${bearerToken}`;
  }
  if (emit.has('X-Auth-User') && claims?.username) {
    headers['X-Auth-User'] = String(claims.username);
  }
  if (emit.has('X-Auth-Email') && claims?.email) {
    headers['X-Auth-Email'] = String(claims.email);
  }
  if (emit.has('X-Auth-Sub') && claims?.sub) {
    headers['X-Auth-Sub'] = String(claims.sub);
  }
  if (emit.has('X-Auth-Role') && claims?.role) {
    headers['X-Auth-Role'] = String(claims.role);
  }
  if (emit.has('X-Auth-Groups')) {
    const groups = claims?.groups || claims?.roles;
    if (Array.isArray(groups) && groups.length > 0) {
      headers['X-Auth-Groups'] = groups.join(',');
    }
  }

  return headers;
}
