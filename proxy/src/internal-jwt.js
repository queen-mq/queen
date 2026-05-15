import crypto from 'crypto';
import * as jose from 'jose';

// We use `jose` for signing (not `jsonwebtoken`) because the latter doesn't
// accept `EdDSA` as an `algorithm` enum value, which would block Ed25519 /
// Ed448 keys — the most compact and currently-recommended choice for new
// asymmetric JWT setups. `jose` supports HS256/HS384/HS512/RS256/EdDSA/ES256
// uniformly and is already a dep (used by verifyExternalToken).

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const INTERNAL_JWT_ISSUER = process.env.INTERNAL_JWT_ISSUER || 'queen-proxy';
const INTERNAL_JWT_AUDIENCE = process.env.INTERNAL_JWT_AUDIENCE || '';
const INTERNAL_JWT_EXPIRES_IN = process.env.INTERNAL_JWT_EXPIRES_IN || '15m';
const INTERNAL_JWT_CLAIM_MAPPING = process.env.INTERNAL_JWT_CLAIM_MAPPING ||
  'sub:sub,role:role,email:email,preferred_username:username';

// Optional asymmetric private key (PEM). When set:
//   - The signing algorithm is auto-detected from the key (RS256 for RSA,
//     EdDSA for Ed25519/Ed448, ES256 for prime256v1).
//   - The corresponding public key is exported as a JWK and served on the
//     proxy's JWKS endpoint so the broker can verify proxy-minted tokens
//     without sharing JWT_SECRET.
// When unset, we keep the legacy HS256 + JWT_SECRET behavior so existing
// deployments don't break.
const INTERNAL_JWT_PRIVATE_KEY_PEM = process.env.INTERNAL_JWT_PRIVATE_KEY_PEM || '';

// Optional explicit kid for the JWKS entry. When unset we derive a stable
// SHA-256 thumbprint from the public key so rotations naturally get a new id.
const INTERNAL_JWT_KEY_ID = process.env.INTERNAL_JWT_KEY_ID || '';

// HS256 fallback wiring (legacy / default).
const INTERNAL_JWT_SIGNING_KEY = process.env.INTERNAL_JWT_SIGNING_KEY || process.env.JWT_SECRET || 'change-me-in-production';

// ---------------------------------------------------------------------------
// Algorithm + key initialization
// ---------------------------------------------------------------------------

/**
 * Resolve the asymmetric key material from INTERNAL_JWT_PRIVATE_KEY_PEM at
 * module-load time so we fail loudly on bad PEM rather than on the first
 * mint. Returns `null` when the env is unset (legacy HS256 mode).
 *
 * The asymmetric branch picks the JWS algorithm from the key type; we don't
 * accept INTERNAL_JWT_ALG overrides for asymmetric keys to avoid the
 * footgun of pairing an Ed25519 key with `RS256`.
 */
function initAsymmetric() {
  if (!INTERNAL_JWT_PRIVATE_KEY_PEM) return null;
  let priv;
  try {
    priv = crypto.createPrivateKey({ key: INTERNAL_JWT_PRIVATE_KEY_PEM, format: 'pem' });
  } catch (err) {
    throw new Error(`INTERNAL_JWT_PRIVATE_KEY_PEM is not valid PEM: ${err.message}`);
  }
  const pub = crypto.createPublicKey(priv);
  let alg;
  switch (priv.asymmetricKeyType) {
    case 'rsa':
      alg = 'RS256';
      break;
    case 'ed25519':
    case 'ed448':
      alg = 'EdDSA';
      break;
    case 'ec': {
      // Only P-256 is supported here; jsonwebtoken needs an explicit alg name.
      const curve = priv.asymmetricKeyDetails?.namedCurve;
      if (curve === 'prime256v1' || curve === 'P-256') alg = 'ES256';
      else if (curve === 'secp384r1' || curve === 'P-384') alg = 'ES384';
      else throw new Error(`Unsupported EC curve for INTERNAL_JWT_PRIVATE_KEY_PEM: ${curve}`);
      break;
    }
    default:
      throw new Error(`Unsupported key type for INTERNAL_JWT_PRIVATE_KEY_PEM: ${priv.asymmetricKeyType}`);
  }
  return { priv, pub, alg };
}

const ASYMMETRIC = initAsymmetric();

// HS256 mode honors INTERNAL_JWT_ALG so users can opt into HS384/HS512 if they
// want; asymmetric mode ignores it because the alg is dictated by the key.
const EFFECTIVE_ALG = ASYMMETRIC ? ASYMMETRIC.alg : (process.env.INTERNAL_JWT_ALG || 'HS256');

// JWK + kid (asymmetric mode only). Computed once at startup.
let CACHED_JWK = null;
let CACHED_KID = '';

async function ensureJwk() {
  if (!ASYMMETRIC) return null;
  if (CACHED_JWK) return CACHED_JWK;
  const jwk = await jose.exportJWK(ASYMMETRIC.pub);
  jwk.alg = ASYMMETRIC.alg;
  jwk.use = 'sig';
  CACHED_KID = INTERNAL_JWT_KEY_ID || (await jose.calculateJwkThumbprint(jwk));
  jwk.kid = CACHED_KID;
  CACHED_JWK = jwk;
  return jwk;
}

// Eagerly compute on startup so the JWKS endpoint is ready on the first
// request and any export error surfaces in the startup log, not later.
if (ASYMMETRIC) {
  ensureJwk().catch((err) => {
    console.error('[InternalJwt] failed to export JWK from private key:', err);
  });
}

// ---------------------------------------------------------------------------
// Claim mapping + payload projection
// ---------------------------------------------------------------------------

/**
 * Parse a `<dst>:<src>,<dst>:<src>` claim mapping spec into [{dst, src}] pairs.
 * Whitespace around tokens is tolerated; malformed pairs are skipped.
 */
export function parseClaimMapping(spec) {
  if (!spec || typeof spec !== 'string') return [];
  const out = [];
  for (const pair of spec.split(',')) {
    const trimmed = pair.trim();
    if (!trimmed) continue;
    const idx = trimmed.indexOf(':');
    if (idx <= 0 || idx === trimmed.length - 1) continue;
    const dst = trimmed.slice(0, idx).trim();
    const src = trimmed.slice(idx + 1).trim();
    if (!dst || !src) continue;
    out.push({ dst, src });
  }
  return out;
}

/**
 * Build the payload for the internal token by projecting `srcClaims` through
 * `mapping`. Missing source values are dropped (we don't emit `null`s the broker
 * would have to defend against).
 */
function projectClaims(srcClaims, mapping) {
  const payload = {};
  for (const { dst, src } of mapping) {
    const value = srcClaims?.[src];
    if (value === undefined || value === null || value === '') continue;
    payload[dst] = value;
  }
  return payload;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Parse a duration spec like `'15m'`, `'24h'`, `'7d'`, `'600s'`, or a bare
 * number-of-seconds. Returns the number of seconds, or null when input is
 * `'never'` or empty (no expiry should be set).
 *
 * Supports the same suffixes the previous `jsonwebtoken`-backed version
 * understood (s/m/h/d), so existing INTERNAL_JWT_EXPIRES_IN env values keep
 * working without changes.
 */
function parseExpirySeconds(spec) {
  if (!spec || spec === 'never') return null;
  if (typeof spec === 'number') return spec > 0 ? Math.floor(spec) : null;
  const s = String(spec).trim();
  const m = /^(\d+)\s*(s|m|h|d)?$/.exec(s);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  switch (m[2] || 's') {
    case 's': return n;
    case 'm': return n * 60;
    case 'h': return n * 3600;
    case 'd': return n * 86400;
    default: return null;
  }
}

/**
 * Mint a short-lived JWT to forward to upstream services through the
 * `Authorization: Bearer …` header on the ForwardAuth response.
 *
 * Two modes:
 *  - Asymmetric (INTERNAL_JWT_PRIVATE_KEY_PEM set): RS256/EdDSA/ES256/ES384.
 *    The corresponding public key is published on the proxy's JWKS endpoint
 *    so the broker can verify with `JWT_JWKS_URL` instead of sharing a HS
 *    secret. Tokens carry a `kid` header so the broker picks the right key
 *    during rotation.
 *  - Symmetric (default): HS256 signed with INTERNAL_JWT_SIGNING_KEY (which
 *    falls back to JWT_SECRET). Existing deployments keep working unchanged.
 *
 * @param {object} srcClaims - Normalized claim bag (from `extractClaimsFromAnyToken`
 *                             or the local DB user record).
 * @param {object} [opts]    - Per-call overrides (mostly used in tests).
 * @returns {Promise<string>} Signed compact JWT (jose's SignJWT is async).
 */
export async function mintInternalToken(srcClaims, opts = {}) {
  const issuer = opts.issuer ?? INTERNAL_JWT_ISSUER;
  const audience = opts.audience ?? INTERNAL_JWT_AUDIENCE;
  const expiresIn = opts.expiresIn || INTERNAL_JWT_EXPIRES_IN;
  const mapping = opts.claimMapping || parseClaimMapping(INTERNAL_JWT_CLAIM_MAPPING);
  const algorithm = opts.algorithm || EFFECTIVE_ALG;

  const payload = projectClaims(srcClaims || {}, mapping);

  const protectedHeader = { alg: algorithm, typ: 'JWT' };
  if (ASYMMETRIC && CACHED_KID) protectedHeader.kid = CACHED_KID;

  const builder = new jose.SignJWT(payload).setProtectedHeader(protectedHeader);
  if (issuer) builder.setIssuer(issuer);
  if (audience) builder.setAudience(audience);
  builder.setIssuedAt();
  const expSeconds = parseExpirySeconds(expiresIn);
  if (expSeconds !== null) builder.setExpirationTime(`${expSeconds}s`);

  const key = ASYMMETRIC
    ? ASYMMETRIC.priv
    : new TextEncoder().encode(opts.signingKey || INTERNAL_JWT_SIGNING_KEY);

  return await builder.sign(key);
}

/**
 * Returns the JWKS document the proxy publishes. Only populated when
 * INTERNAL_JWT_PRIVATE_KEY_PEM is configured — symmetric installs return an
 * empty `{ keys: [] }` because there is no public key to share.
 */
export async function getInternalJwks() {
  if (!ASYMMETRIC) return { keys: [] };
  const jwk = await ensureJwk();
  return { keys: jwk ? [jwk] : [] };
}

export function isAsymmetricInternalJwtEnabled() {
  return !!ASYMMETRIC;
}

export function getInternalJwtConfig() {
  return {
    algorithm: EFFECTIVE_ALG,
    issuer: INTERNAL_JWT_ISSUER,
    audience: INTERNAL_JWT_AUDIENCE,
    expiresIn: INTERNAL_JWT_EXPIRES_IN,
    claimMapping: parseClaimMapping(INTERNAL_JWT_CLAIM_MAPPING),
    asymmetric: !!ASYMMETRIC,
    keyId: CACHED_KID || null,
  };
}
