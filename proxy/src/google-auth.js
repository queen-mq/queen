import crypto from 'crypto';
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import {
  createGoogleUser,
  getUserByEmail,
  getUserByGoogleSub,
  linkGoogleAccount,
} from './db.js';

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || '';
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || '';
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || '';

// When COOKIE_DOMAIN is set we operate in cross-host mode (auth.example.com
// kicks the OAuth flow on behalf of grafana.example.com etc.). In that mode we
// CANNOT round-trip state via a cookie set on the auth host before the bounce,
// so we encode it as a short-lived signed JWT in the `state` query param
// instead. The HS256 secret is the same JWT_SECRET used for session tokens.
const OAUTH_STATE_SECRET = process.env.JWT_SECRET || 'change-me-in-production';
const OAUTH_STATE_TTL_SECONDS = 5 * 60;

// Comma-separated list of allowed Google Workspace domains (matched against the
// `hd` claim). Empty = allow any verified email.
const GOOGLE_ALLOWED_DOMAINS = (process.env.GOOGLE_ALLOWED_DOMAINS || '')
  .split(',')
  .map((d) => d.trim().toLowerCase())
  .filter(Boolean);

const GOOGLE_AUTO_PROVISION = (process.env.GOOGLE_AUTO_PROVISION || 'false').toLowerCase() === 'true';
const GOOGLE_DEFAULT_ROLE = process.env.GOOGLE_DEFAULT_ROLE || 'read-only';

// Per-email allowlist for the Google sign-in flow. Each entry is either:
//   - a literal email (case-insensitive exact match), e.g. `admin@example.com`
//   - a regex wrapped in slashes, e.g. `/.*@example\.com$/` or `/^admin\+/`
// Comma-separated. Empty = no email-level restriction (relies on
// GOOGLE_ALLOWED_DOMAINS alone). This gate applies ONLY to the Google login
// flow — API clients carrying central-IdP JWTs (the FA bearer path) are
// trusted as-is per issue #30 answer #3 and the issue #30 follow-up where
// xmlking confirmed API client emails can't be enumerated up front.
//
// FORWARD_AUTH_ALLOWED_EMAILS is the previous (broader-scoped) name and
// remains supported for one release as a deprecation alias.
const GOOGLE_ALLOWED_EMAILS_RAW =
  process.env.GOOGLE_ALLOWED_EMAILS ||
  process.env.FORWARD_AUTH_ALLOWED_EMAILS ||
  '';

if (process.env.FORWARD_AUTH_ALLOWED_EMAILS && !process.env.GOOGLE_ALLOWED_EMAILS) {
  console.warn('[GoogleAuth] FORWARD_AUTH_ALLOWED_EMAILS is deprecated — use GOOGLE_ALLOWED_EMAILS instead');
}

/**
 * Parse the comma-separated allowlist into a mix of literal lower-case strings
 * and RegExp objects. Malformed regexes are skipped (with a console warning)
 * rather than killing the process so a single bad entry can't take auth down.
 */
function parseAllowedEmails(spec) {
  if (!spec) return [];
  const out = [];
  for (const raw of spec.split(',')) {
    const entry = raw.trim();
    if (!entry) continue;
    if (entry.length >= 2 && entry.startsWith('/') && entry.endsWith('/')) {
      try {
        out.push(new RegExp(entry.slice(1, -1), 'i'));
      } catch (err) {
        console.warn(`[GoogleAuth] skipping invalid GOOGLE_ALLOWED_EMAILS regex "${entry}": ${err.message}`);
      }
    } else {
      out.push(entry.toLowerCase());
    }
  }
  return out;
}

const GOOGLE_ALLOWED_EMAILS = parseAllowedEmails(GOOGLE_ALLOWED_EMAILS_RAW);

/**
 * Returns true when an email passes the Google sign-in allowlist. With no
 * allowlist configured everyone whose domain passes GOOGLE_ALLOWED_DOMAINS is
 * allowed (back-compat). With an allowlist, a missing email always denies —
 * we have no safe way to apply an allowlist to an unknown identity.
 */
export function isGoogleEmailAllowed(email) {
  if (GOOGLE_ALLOWED_EMAILS.length === 0) return true;
  if (!email || typeof email !== 'string') return false;
  const lower = email.toLowerCase();
  for (const rule of GOOGLE_ALLOWED_EMAILS) {
    if (rule instanceof RegExp) {
      if (rule.test(email)) return true;
    } else if (rule === lower) {
      return true;
    }
  }
  return false;
}

export function hasGoogleEmailAllowlist() {
  return GOOGLE_ALLOWED_EMAILS.length > 0;
}

const ALLOWED_ROLES = new Set(['admin', 'read-write', 'read-only']);

const GOOGLE_AUTHORIZE_URL = 'https://accounts.google.com/o/oauth2/v2/auth';
const GOOGLE_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const GOOGLE_JWKS_URL = 'https://www.googleapis.com/oauth2/v3/certs';
const GOOGLE_VALID_ISSUERS = ['https://accounts.google.com', 'accounts.google.com'];

let jwksCache = null;
function getJWKS() {
  if (!jwksCache) {
    jwksCache = jose.createRemoteJWKSet(new URL(GOOGLE_JWKS_URL));
  }
  return jwksCache;
}

export function isGoogleAuthEnabled() {
  return !!(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET && GOOGLE_REDIRECT_URI);
}

export function getGoogleConfig() {
  return {
    enabled: isGoogleAuthEnabled(),
    clientId: GOOGLE_CLIENT_ID,
    redirectUri: GOOGLE_REDIRECT_URI,
    allowedDomains: GOOGLE_ALLOWED_DOMAINS,
    allowedEmails: GOOGLE_ALLOWED_EMAILS.map((r) => (r instanceof RegExp ? `/${r.source}/` : r)),
    autoProvision: GOOGLE_AUTO_PROVISION,
    defaultRole: ALLOWED_ROLES.has(GOOGLE_DEFAULT_ROLE) ? GOOGLE_DEFAULT_ROLE : 'read-only',
  };
}

function randomToken(bytes = 32) {
  return crypto.randomBytes(bytes).toString('hex');
}

/**
 * Build the URL the browser should be redirected to in order to start the
 * Google OAuth 2.0 Authorization Code flow.
 *
 * Two modes:
 *  - Cookie mode (legacy single-host): returns plain `state` + `nonce` random
 *    tokens that the caller MUST persist in short-lived httpOnly cookies and
 *    cross-check in the callback.
 *  - Signed mode (cross-host, opt-in via `signed: true`): encodes nonce +
 *    redirect_uri inside the `state` query param itself as a short-lived HS256
 *    JWT. No cookie write required before the bounce, which is what cross-host
 *    SSO needs (the auth host issues the redirect on behalf of a different
 *    upstream host that has no cookie yet).
 */
export function buildAuthorizeUrl({ redirectAfterLogin, signed = false } = {}) {
  if (!isGoogleAuthEnabled()) {
    throw new Error('Google auth is not configured');
  }

  const nonce = randomToken();
  const next = redirectAfterLogin || '/';

  let state;
  if (signed) {
    state = jwt.sign(
      { nonce, next },
      OAUTH_STATE_SECRET,
      { algorithm: 'HS256', expiresIn: OAUTH_STATE_TTL_SECONDS }
    );
  } else {
    state = randomToken();
  }

  const params = new URLSearchParams({
    client_id: GOOGLE_CLIENT_ID,
    redirect_uri: GOOGLE_REDIRECT_URI,
    response_type: 'code',
    scope: 'openid email profile',
    access_type: 'online',
    include_granted_scopes: 'true',
    prompt: 'select_account',
    state,
    nonce,
  });

  // If a single domain is configured, hint Google's account chooser to it.
  if (GOOGLE_ALLOWED_DOMAINS.length === 1) {
    params.set('hd', GOOGLE_ALLOWED_DOMAINS[0]);
  }

  return {
    url: `${GOOGLE_AUTHORIZE_URL}?${params.toString()}`,
    state,
    nonce,
    redirectAfterLogin: next,
    signed,
  };
}

/**
 * Verify a signed OAuth `state` JWT produced by `buildAuthorizeUrl({signed:true})`.
 * Returns the decoded `{ nonce, next }` payload, or null if verification fails
 * (expired, tampered, or just not a JWT — e.g. a legacy cookie-mode random
 * token routed through this helper by mistake).
 */
export function verifyOauthState(state) {
  if (!state || typeof state !== 'string') return null;
  try {
    const payload = jwt.verify(state, OAUTH_STATE_SECRET, { algorithms: ['HS256'] });
    if (!payload || typeof payload !== 'object') return null;
    if (typeof payload.nonce !== 'string' || !payload.nonce) return null;
    return { nonce: payload.nonce, next: typeof payload.next === 'string' ? payload.next : '/' };
  } catch {
    return null;
  }
}

async function exchangeCodeForTokens(code) {
  const body = new URLSearchParams({
    code,
    client_id: GOOGLE_CLIENT_ID,
    client_secret: GOOGLE_CLIENT_SECRET,
    redirect_uri: GOOGLE_REDIRECT_URI,
    grant_type: 'authorization_code',
  });

  const response = await fetch(GOOGLE_TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: body.toString(),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Google token exchange failed (${response.status}): ${text}`);
  }

  const tokens = await response.json();
  if (!tokens.id_token) {
    throw new Error('Google token response missing id_token');
  }
  return tokens;
}

async function verifyIdToken(idToken, expectedNonce) {
  const { payload } = await jose.jwtVerify(idToken, getJWKS(), {
    issuer: GOOGLE_VALID_ISSUERS,
    audience: GOOGLE_CLIENT_ID,
  });

  if (expectedNonce && payload.nonce !== expectedNonce) {
    throw new Error('Google id_token nonce mismatch');
  }
  if (!payload.sub) {
    throw new Error('Google id_token missing sub');
  }
  if (!payload.email) {
    throw new Error('Google id_token missing email (request the "email" scope)');
  }
  if (payload.email_verified === false) {
    throw new Error('Google email is not verified');
  }

  if (GOOGLE_ALLOWED_DOMAINS.length > 0) {
    const hd = (payload.hd || '').toLowerCase();
    const emailDomain = String(payload.email).split('@')[1]?.toLowerCase() || '';
    const domainOk = GOOGLE_ALLOWED_DOMAINS.includes(hd) || GOOGLE_ALLOWED_DOMAINS.includes(emailDomain);
    if (!domainOk) {
      throw new Error(`Google account domain not allowed: ${hd || emailDomain || '<unknown>'}`);
    }
  }

  return payload;
}

/**
 * Resolve a verified Google identity to a local user record. Linking by email
 * is gated on `email_verified=true` to prevent account takeover by an attacker
 * who registers a Google account with someone else's address.
 */
async function resolveUser(claims) {
  const sub = claims.sub;
  const email = String(claims.email).toLowerCase();

  let user = await getUserByGoogleSub(sub);
  if (user) return user;

  if (claims.email_verified) {
    const existing = await getUserByEmail(email);
    if (existing) {
      return await linkGoogleAccount(existing.id, sub, email);
    }
  }

  if (!GOOGLE_AUTO_PROVISION) {
    const err = new Error('User is not provisioned for this proxy');
    err.code = 'NOT_PROVISIONED';
    throw err;
  }

  const role = ALLOWED_ROLES.has(GOOGLE_DEFAULT_ROLE) ? GOOGLE_DEFAULT_ROLE : 'read-only';
  return await createGoogleUser({ email, googleSub: sub, role, username: email });
}

/**
 * Run the full callback half of the OAuth flow: exchange the code, verify the
 * id_token, then look up / link / provision the local user.
 *
 * @param {object} args
 * @param {string} args.code - The `code` query param Google returned.
 * @param {string} args.expectedNonce - The nonce that was placed in the authorize request.
 * @returns {Promise<{ id, username, role, email }>} - Local user suitable for `generateToken`.
 */
export async function handleGoogleCallback({ code, expectedNonce }) {
  if (!isGoogleAuthEnabled()) {
    throw new Error('Google auth is not configured');
  }
  if (!code) {
    throw new Error('Missing authorization code');
  }

  const tokens = await exchangeCodeForTokens(code);
  const claims = await verifyIdToken(tokens.id_token, expectedNonce);
  const user = await resolveUser(claims);

  return {
    id: user.id,
    username: user.username,
    role: user.role,
    email: user.email,
  };
}
