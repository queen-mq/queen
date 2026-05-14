import express from 'express';
import cookieParser from 'cookie-parser';
import { createProxyMiddleware } from 'http-proxy-middleware';
import {
  authenticateUser,
  generateToken,
  verifyToken,
  verifyExternalToken,
  isExternalAuthEnabled,
  extractClaimsFromAnyToken,
} from './auth.js';
import { requireAuth, checkMethodAccess, enforceMethodRBACFromClaims } from './middleware.js';
import { initDatabase } from './db.js';
import {
  isGoogleAuthEnabled,
  getGoogleConfig,
  buildAuthorizeUrl,
  verifyOauthState,
  handleGoogleCallback,
} from './google-auth.js';
import { mintInternalToken } from './internal-jwt.js';
import {
  isForwardAuthEnabled,
  getForwardAuthConfig,
  parseForwardedRequest,
  parseRouteOpts,
  wantsHtml,
  isAllowedRedirect,
  buildLoginRedirect,
  selectResponseHeaders,
} from './forward-auth.js';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;
const QUEEN_SERVER_URL = process.env.QUEEN_SERVER_URL || 'http://localhost:8080';

// Optional: parent-domain cookie scope so the session cookie is shared between
// auth.example.com and the protected upstreams (grafana.example.com etc.).
// Empty = legacy host-only behavior, preserves existing single-host installs.
const COOKIE_DOMAIN = process.env.COOKIE_DOMAIN || '';

// Allowlist of hosts that may appear in `?redirect_uri=` on /login and friends.
// Required to be set in cross-host mode; in legacy mode we still allow same-host
// relative paths (the only thing the existing /login UI ever round-trips).
const ALLOWED_REDIRECT_HOSTS = (process.env.FORWARD_AUTH_ALLOWED_REDIRECT_HOSTS || '')
  .split(',')
  .map((h) => h.trim().toLowerCase())
  .filter(Boolean);

app.use(express.json());
app.use(cookieParser());

/**
 * Build the cookie options for the long-lived session token. Honors
 * COOKIE_DOMAIN when set so the cookie is shared across subdomains. `lax`
 * (not `strict`) is required so the cookie survives the cross-site redirect
 * back from the IdP.
 */
function sessionCookieOpts() {
  const opts = {
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    maxAge: 24 * 60 * 60 * 1000,
  };
  if (COOKIE_DOMAIN) opts.domain = COOKIE_DOMAIN;
  return opts;
}

/**
 * Validate a `?redirect_uri=` value supplied by the user. Returns:
 *  - the original string if it is a same-origin relative path (`/...`), or
 *  - the original string if it is an absolute URL whose host is in the
 *    cross-host allowlist, or
 *  - null if it is anything else (including malformed URLs / `//host` style
 *    protocol-relative URLs that browsers treat as cross-origin).
 *
 * Fail-closed: when in doubt, drop it. The caller falls back to `/`.
 */
function sanitizeRedirectUri(raw) {
  if (typeof raw !== 'string' || !raw) return null;
  // Reject protocol-relative URLs like `//evil.example.com/path`.
  if (raw.startsWith('//')) return null;
  if (raw.startsWith('/')) return raw;
  try {
    const u = new URL(raw);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    if (ALLOWED_REDIRECT_HOSTS.includes(u.host.toLowerCase())) return raw;
    return null;
  } catch {
    return null;
  }
}

// Serve static files from public directory
app.use(express.static(path.join(__dirname, '..', 'public')));

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.get('/health/ready', (req, res) => {
  res.json({ status: 'ok' });
});

// Login page - serve only if not authenticated
app.get('/login', (req, res) => {
  res.sendFile(path.join(__dirname, '..', 'public', 'login.html'));
});

// Login API endpoint
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;


  if (!username || !password) {
    return res.status(400).json({ error: 'Username and password required' });
  }

  try {
    const user = await authenticateUser(username, password);

    if (!user) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const token = generateToken(user);

    res.cookie('token', token, sessionCookieOpts());

    // Round-trip a redirect target so the login UI can navigate after success.
    // We sanitize here so the client never sees a value that would later be
    // refused; null means "no safe target, go home".
    const redirectUri = sanitizeRedirectUri(
      typeof req.body?.redirect_uri === 'string' ? req.body.redirect_uri : req.query?.redirect_uri
    );

    res.json({
      success: true,
      user: {
        username: user.username,
        role: user.role
      },
      redirect_uri: redirectUri
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Logout endpoint
app.post('/api/logout', (req, res) => {
  // Clear with explicit domain so the cross-host cookie is actually invalidated.
  const opts = COOKIE_DOMAIN ? { domain: COOKIE_DOMAIN } : {};
  res.clearCookie('token', opts);
  res.json({ success: true });
});

// Public auth-config probe — lets the login page decide which buttons to show
// and which post-login redirect targets are safe to navigate to.
app.get('/api/auth/config', (req, res) => {
  res.json({
    google: { enabled: isGoogleAuthEnabled() },
    forwardAuth: {
      enabled: isForwardAuthEnabled(),
      allowedRedirectHosts: ALLOWED_REDIRECT_HOSTS,
    },
  });
});

const COOKIE_SECURE = process.env.NODE_ENV === 'production';
// `lax` (not `strict`) is required so the cookie survives the cross-site
// redirect back from accounts.google.com. Used only in legacy cookie mode.
const OAUTH_COOKIE_OPTS = {
  httpOnly: true,
  secure: COOKIE_SECURE,
  sameSite: 'lax',
  path: '/api/auth/google',
  maxAge: 5 * 60 * 1000,
};

// In cross-host mode (COOKIE_DOMAIN set) we cannot rely on cookies set on the
// auth host for state — the user might have come in via a totally different
// origin and the upstream router doesn't share path scope. Encode state in a
// short-lived signed JWT inside the `state` query param instead.
const OAUTH_STATE_MODE = COOKIE_DOMAIN ? 'signed' : 'cookie';

// Step 1: kick off the Google OAuth Authorization Code flow.
app.get('/api/auth/google', (req, res) => {
  if (!isGoogleAuthEnabled()) {
    return res.status(503).json({ error: 'Google auth is not configured' });
  }
  try {
    // `next` accepts either a same-origin path (always allowed) or an absolute
    // URL whose host is in the cross-host allowlist.
    const next = sanitizeRedirectUri(req.query.next) || '/';

    if (OAUTH_STATE_MODE === 'signed') {
      const { url } = buildAuthorizeUrl({ redirectAfterLogin: next, signed: true });
      return res.redirect(url);
    }

    const { url, state, nonce } = buildAuthorizeUrl({ redirectAfterLogin: next });
    res.cookie('g_state', state, OAUTH_COOKIE_OPTS);
    res.cookie('g_nonce', nonce, OAUTH_COOKIE_OPTS);
    res.cookie('g_next', next, OAUTH_COOKIE_OPTS);
    res.redirect(url);
  } catch (error) {
    console.error('[GoogleAuth] failed to start flow:', error);
    res.status(500).json({ error: 'Failed to start Google sign-in' });
  }
});

// Step 2: handle the redirect back from Google.
app.get('/api/auth/google/callback', async (req, res) => {
  const clearOAuthCookies = () => {
    res.clearCookie('g_state', { path: '/api/auth/google' });
    res.clearCookie('g_nonce', { path: '/api/auth/google' });
    res.clearCookie('g_next', { path: '/api/auth/google' });
  };

  if (!isGoogleAuthEnabled()) {
    clearOAuthCookies();
    return res.status(503).send('Google auth is not configured');
  }

  if (req.query.error) {
    clearOAuthCookies();
    return res.redirect(`/login?error=${encodeURIComponent(String(req.query.error))}`);
  }

  const code = typeof req.query.code === 'string' ? req.query.code : '';
  const state = typeof req.query.state === 'string' ? req.query.state : '';

  // Resolve { expectedNonce, next } from either the signed state (cross-host)
  // or the legacy state/nonce/next cookies (single-host).
  let expectedNonce = '';
  let next = '/';

  if (OAUTH_STATE_MODE === 'signed') {
    const decoded = verifyOauthState(state);
    if (!code || !decoded) {
      clearOAuthCookies();
      return res.redirect('/login?error=invalid_state');
    }
    expectedNonce = decoded.nonce;
    next = sanitizeRedirectUri(decoded.next) || '/';
  } else {
    const expectedState = req.cookies.g_state;
    expectedNonce = req.cookies.g_nonce;
    const cookieNext = sanitizeRedirectUri(req.cookies.g_next);
    next = cookieNext || '/';
    if (!code || !state || !expectedState || state !== expectedState) {
      clearOAuthCookies();
      return res.redirect('/login?error=invalid_state');
    }
  }

  try {
    const user = await handleGoogleCallback({ code, expectedNonce });
    const token = generateToken(user);

    res.cookie('token', token, sessionCookieOpts());

    clearOAuthCookies();
    res.redirect(next);
  } catch (error) {
    clearOAuthCookies();
    if (error.code === 'NOT_PROVISIONED') {
      console.warn('[GoogleAuth] sign-in denied (not provisioned):', error.message);
      return res.redirect('/login?error=not_provisioned');
    }
    console.error('[GoogleAuth] callback error:', error);
    res.redirect('/login?error=google_failed');
  }
});

// Get current user info
app.get('/api/me', requireAuth, (req, res) => {
  res.json({
    username: req.user.username,
    role: req.user.role
  });
});

/**
 * Traefik ForwardAuth endpoint (issue #30).
 *
 * Stateless: never opens a DB connection on the bearer path, never proxies
 * upstream. Returns only status + headers; Traefik discards the body.
 *
 * Mounted BEFORE the global cookie-auth middleware so an unauthenticated call
 * here returns the bounce response (302/401) instead of the proxy's own
 * /login redirect.
 *
 * Wire-up:
 *   traefik.http.middlewares.queen-fa.forwardauth.address: \
 *     http://queen-proxy:3000/api/auth/forward-auth
 *   traefik.http.middlewares.queen-fa.forwardauth.authResponseHeaders: \
 *     Authorization,X-Auth-User,X-Auth-Email,X-Auth-Role,X-Auth-Sub
 *
 * See proxy/README.md for the full deployment example.
 */
if (isForwardAuthEnabled()) {
  app.get('/api/auth/forward-auth', async (req, res) => {
    const fwd = parseForwardedRequest(req);
    const opts = parseRouteOpts(req);

    /** Build the unauthenticated response — 302 for browsers, 401 for APIs. */
    const respondUnauth = () => {
      const loginUrl = buildLoginRedirect(fwd.originalUrl);
      if (wantsHtml(req) && loginUrl) {
        return res.redirect(302, loginUrl);
      }
      if (loginUrl) {
        res.setHeader('X-Forward-Auth-Location', loginUrl);
      }
      return res.status(401).end();
    };

    try {
      // 1) Bearer token wins (API clients carrying central-IdP JWTs).
      const authHeader = req.headers.authorization || '';
      const bearer = authHeader.startsWith('Bearer ')
        ? authHeader.slice('Bearer '.length).trim()
        : '';

      if (bearer) {
        const claims = await extractClaimsFromAnyToken(bearer);
        if (!claims) return respondUnauth();
        if (opts.enforceMethodRBAC && !enforceMethodRBACFromClaims(fwd.method, claims)) {
          return res.status(403).end();
        }
        // Pass the bearer through untouched per issue answer #3 — central IdP
        // identity is the source of truth for protected upstreams.
        const headers = selectResponseHeaders(claims, opts, bearer);
        for (const [name, value] of Object.entries(headers)) res.setHeader(name, value);
        return res.status(200).end();
      }

      // 2) Cookie-based session (browser SSO users). Only valid in cross-host
      //    mode (otherwise the cookie wouldn't reach us anyway), but we still
      //    accept it in single-host mode for direct testing.
      const cookieToken = req.cookies?.token;
      if (cookieToken) {
        const claims = await extractClaimsFromAnyToken(cookieToken);
        if (claims) {
          if (opts.enforceMethodRBAC && !enforceMethodRBACFromClaims(fwd.method, claims)) {
            return res.status(403).end();
          }
          // Mint a fresh short-lived internal JWT for the upstream so we never
          // hand the long-lived session cookie value to other services.
          const upstreamToken = mintInternalToken(claims);
          const headers = selectResponseHeaders(claims, opts, upstreamToken);
          for (const [name, value] of Object.entries(headers)) res.setHeader(name, value);
          return res.status(200).end();
        }
      }

      // 3) No credentials (or invalid) → bounce.
      return respondUnauth();
    } catch (err) {
      console.error('[ForwardAuth] handler error:', err);
      return res.status(500).end();
    }
  });
}

// Middleware to check if user is authenticated, redirect to login if not
// Supports both internal (proxy-generated) and external (SSO/IDP) tokens
app.use(async (req, res, next) => {
  // Skip auth check for login page, login API, OAuth flow, the public
  // auth-config probe used by the login UI, and the ForwardAuth endpoint
  // (which has its own bouncer baked in).
  if (
    req.path === '/login' ||
    req.path.startsWith('/api/login') ||
    req.path.startsWith('/api/auth/google') ||
    req.path === '/api/auth/config' ||
    req.path === '/api/auth/forward-auth'
  ) {
    return next();
  }

  const token = req.cookies.token || req.headers.authorization?.replace('Bearer ', '');

  if (!token) {
    // If it's an API call, return 401
    if (req.path.startsWith('/api/') || req.xhr || req.headers.accept?.includes('application/json')) {
      return res.status(401).json({ error: 'Authentication required' });
    }
    // Otherwise redirect to login page
    return res.redirect('/login');
  }

  // Try external token verification first (SSO passthrough)
  if (isExternalAuthEnabled()) {
    const externalUser = await verifyExternalToken(token);
    if (externalUser) {
      req.user = externalUser;
      req.originalToken = token;  // Keep original token for passthrough
      return next();
    }
  }

  // Fall back to internal (proxy-generated) token verification
  const user = verifyToken(token);

  if (!user) {
    if (req.path.startsWith('/api/') || req.xhr || req.headers.accept?.includes('application/json')) {
      return res.status(401).json({ error: 'Invalid or expired token' });
    }
    return res.redirect('/login');
  }

  req.user = user;
  next();
});

// Proxy all other requests to Queen server with RBAC
app.use('/', 
  requireAuth,
  checkMethodAccess,
  createProxyMiddleware({
    target: QUEEN_SERVER_URL,
    changeOrigin: true,
    ws: true,
    logLevel: 'silent',
    onProxyReq: (proxyReq, req, res) => {
      // Remove large/unnecessary headers that can cause "Request Header Fields Too Large" errors
      proxyReq.removeHeader('cookie');
      proxyReq.removeHeader('referer');
      
      // Forward JWT token to Queen server for authentication
      // For external tokens (SSO), pass through the original token unchanged
      // For internal tokens, get from cookie
      let token;
      if (req.originalToken) {
        // External SSO token - pass through as-is
        token = req.originalToken;
      } else {
        // Internal proxy token
        token = req.cookies.token || req.headers.authorization?.replace('Bearer ', '');
      }
      
      if (token) {
        proxyReq.setHeader('Authorization', `Bearer ${token}`);
      }
      
      // Also add user info headers for backward compatibility / logging
      proxyReq.setHeader('X-Proxy-User', req.user.username || req.user.subject || 'unknown');
      proxyReq.setHeader('X-Proxy-Role', req.user.role || 'read-only');
      if (req.user.isExternal) {
        proxyReq.setHeader('X-Proxy-External', 'true');
      }
      
      // Re-stream body if it was consumed by express.json()
      if (req.body && Object.keys(req.body).length > 0) {
        const bodyData = JSON.stringify(req.body);
        proxyReq.setHeader('Content-Type', 'application/json');
        proxyReq.setHeader('Content-Length', Buffer.byteLength(bodyData));
        proxyReq.write(bodyData);
      }
    },
    onError: (err, req, res) => {
      console.error('Proxy error:', err);
      res.status(502).json({ error: 'Bad gateway - Queen server unreachable' });
    }
  })
);

async function startServer() {
  try {
    await initDatabase();
    
    app.listen(PORT, () => {
      console.log(`Queen Proxy listening on port ${PORT}`);
      console.log(`  Target: ${QUEEN_SERVER_URL}`);
      if (isExternalAuthEnabled()) {
        console.log(`  External SSO: enabled (JWKS passthrough)`);
        console.log(`    JWKS URL: ${process.env.EXTERNAL_JWKS_URL || process.env.JWT_JWKS_URL}`);
      } else {
        console.log(`  External SSO: disabled (internal auth only)`);
      }
      if (isGoogleAuthEnabled()) {
        const cfg = getGoogleConfig();
        console.log(`  Google OAuth: enabled`);
        console.log(`    Redirect URI: ${cfg.redirectUri}`);
        if (cfg.allowedDomains.length) {
          console.log(`    Allowed domains: ${cfg.allowedDomains.join(', ')}`);
        }
        console.log(`    Auto-provision: ${cfg.autoProvision} (default role: ${cfg.defaultRole})`);
      } else {
        console.log(`  Google OAuth: disabled`);
      }
      if (isForwardAuthEnabled()) {
        const fa = getForwardAuthConfig();
        console.log(`  ForwardAuth: enabled at GET /api/auth/forward-auth`);
        console.log(`    Auth host: ${fa.authHost || '(not set — will 401 instead of 302)'}`);
        console.log(`    Cookie domain: ${COOKIE_DOMAIN || '(host-only)'}`);
        console.log(`    Allowed redirect hosts: ${fa.allowedRedirectHosts.join(', ') || '(none)'}`);
        console.log(`    Emit headers: ${fa.emitHeaders.join(', ')}`);
        console.log(`    OAuth state mode: ${COOKIE_DOMAIN ? 'signed' : 'cookie'}`);
      } else {
        console.log(`  ForwardAuth: disabled`);
      }
    });
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
