import jwt from 'jsonwebtoken';

const INTERNAL_JWT_SIGNING_KEY = process.env.INTERNAL_JWT_SIGNING_KEY || process.env.JWT_SECRET || 'change-me-in-production';
const INTERNAL_JWT_ALG = process.env.INTERNAL_JWT_ALG || 'HS256';
const INTERNAL_JWT_ISSUER = process.env.INTERNAL_JWT_ISSUER || 'queen-proxy';
const INTERNAL_JWT_AUDIENCE = process.env.INTERNAL_JWT_AUDIENCE || '';
const INTERNAL_JWT_EXPIRES_IN = process.env.INTERNAL_JWT_EXPIRES_IN || '15m';
const INTERNAL_JWT_CLAIM_MAPPING = process.env.INTERNAL_JWT_CLAIM_MAPPING ||
  'sub:sub,role:role,email:email,preferred_username:username';

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

/**
 * Mint a short-lived JWT to forward to upstream services through the
 * `Authorization: Bearer …` header on the ForwardAuth response. Defaults are
 * chosen so the broker keeps verifying it unchanged: HS256 signed with
 * `JWT_SECRET`, `role` and `sub` claims preserved.
 *
 * @param {object} srcClaims - Normalized claim bag (from `extractClaimsFromAnyToken`
 *                             or the local DB user record).
 * @param {object} [opts]    - Per-call overrides (mostly used in tests).
 * @returns {string} Signed compact JWT.
 */
export function mintInternalToken(srcClaims, opts = {}) {
  const signingKey = opts.signingKey || INTERNAL_JWT_SIGNING_KEY;
  const algorithm = opts.algorithm || INTERNAL_JWT_ALG;
  const issuer = opts.issuer ?? INTERNAL_JWT_ISSUER;
  const audience = opts.audience ?? INTERNAL_JWT_AUDIENCE;
  const expiresIn = opts.expiresIn || INTERNAL_JWT_EXPIRES_IN;
  const mapping = opts.claimMapping || parseClaimMapping(INTERNAL_JWT_CLAIM_MAPPING);

  const payload = projectClaims(srcClaims || {}, mapping);

  const signOpts = { algorithm };
  if (issuer) signOpts.issuer = issuer;
  if (audience) signOpts.audience = audience;
  if (expiresIn && expiresIn !== 'never') signOpts.expiresIn = expiresIn;

  return jwt.sign(payload, signingKey, signOpts);
}

export function getInternalJwtConfig() {
  return {
    algorithm: INTERNAL_JWT_ALG,
    issuer: INTERNAL_JWT_ISSUER,
    audience: INTERNAL_JWT_AUDIENCE,
    expiresIn: INTERNAL_JWT_EXPIRES_IN,
    claimMapping: parseClaimMapping(INTERNAL_JWT_CLAIM_MAPPING),
  };
}
