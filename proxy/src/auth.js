import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import * as jose from 'jose';
import { getUserByUsername } from './db.js';

const JWT_SECRET = process.env.JWT_SECRET || 'change-me-in-production';
const JWT_EXPIRES_IN = process.env.JWT_EXPIRES_IN || '24h';

// External IDP configuration for SSO passthrough
const EXTERNAL_JWKS_URL = process.env.EXTERNAL_JWKS_URL || process.env.JWT_JWKS_URL || '';
const EXTERNAL_ISSUER = process.env.EXTERNAL_ISSUER || process.env.JWT_ISSUER || '';
const EXTERNAL_AUDIENCE = process.env.EXTERNAL_AUDIENCE || process.env.JWT_AUDIENCE || '';

// Cache for JWKS
let jwksCache = null;
let jwksCacheTime = 0;
const JWKS_CACHE_TTL = 3600 * 1000; // 1 hour

/**
 * Get or refresh JWKS from external IDP
 */
async function getJWKS() {
  if (!EXTERNAL_JWKS_URL) {
    return null;
  }

  const now = Date.now();
  if (jwksCache && (now - jwksCacheTime) < JWKS_CACHE_TTL) {
    return jwksCache;
  }

  try {
    jwksCache = jose.createRemoteJWKSet(new URL(EXTERNAL_JWKS_URL));
    jwksCacheTime = now;
    console.log('[Auth] JWKS loaded from:', EXTERNAL_JWKS_URL);
    return jwksCache;
  } catch (error) {
    console.error('[Auth] Failed to load JWKS:', error.message);
    return null;
  }
}

/**
 * Verify token from external IDP (SSO passthrough)
 * Supports RS256, EdDSA (Ed25519), and other algorithms via JWKS
 * 
 * @param {string} token - JWT token to verify
 * @returns {object|null} - Decoded claims or null if invalid
 */
export async function verifyExternalToken(token) {
  if (!EXTERNAL_JWKS_URL) {
    return null;
  }

  try {
    const jwks = await getJWKS();
    if (!jwks) {
      return null;
    }

    const options = {};
    
    // Add issuer validation if configured
    if (EXTERNAL_ISSUER) {
      options.issuer = EXTERNAL_ISSUER;
    }
    
    // Add audience validation if configured
    if (EXTERNAL_AUDIENCE) {
      options.audience = EXTERNAL_AUDIENCE;
    }

    const { payload, protectedHeader } = await jose.jwtVerify(token, jwks, options);
    
    console.log('[Auth] External token verified, alg:', protectedHeader.alg, 'sub:', payload.sub);
    
    // Normalize claims to match internal token format
    return {
      id: payload.sub || payload.id,
      username: payload.preferred_username || payload.username || payload.name || payload.sub,
      email: payload.email,
      role: payload.role || (payload.roles && payload.roles[0]) || 'read-only',
      roles: payload.roles || (payload.role ? [payload.role] : ['read-only']),
      subject: payload.sub,
      issuer: payload.iss,
      isExternal: true,  // Flag to indicate this is an external token
      raw: payload       // Full payload for callers that need non-standard claims
    };
  } catch (error) {
    // Don't log expected failures. The first three are routine: bad signature,
    // expired token, or wrong audience — happens whenever the global cookie
    // middleware probes a token that wasn't actually minted by this IdP. The
    // last two cover the "an internal HS256 cookie was handed to an external
    // EdDSA/RS256 JWKS verifier" case which fires on every cookie request when
    // both EXTERNAL_JWKS_URL and the internal cookie auth are in use — that
    // produces ERR_JOSE_NOT_SUPPORTED / ERR_JOSE_ALG_NOT_ALLOWED depending on
    // jose internals and is not actionable.
    const expected = new Set([
      'ERR_JWT_EXPIRED',
      'ERR_JWS_SIGNATURE_VERIFICATION_FAILED',
      'ERR_JWT_CLAIM_VALIDATION_FAILED',
      'ERR_JOSE_NOT_SUPPORTED',
      'ERR_JOSE_ALG_NOT_ALLOWED',
      'ERR_JWS_INVALID',
    ]);
    if (!expected.has(error.code)) {
      console.debug('[Auth] External token verification failed:', error.message);
    }
    return null;
  }
}

export async function authenticateUser(username, password) {
  const user = await getUserByUsername(username);
  
  if (!user) {
    return null;
  }

  const isValid = await bcrypt.compare(password, user.password_hash);
  
  if (!isValid) {
    return null;
  }

  return {
    id: user.id,
    username: user.username,
    role: user.role
  };
}

export function generateToken(user, expiresIn = JWT_EXPIRES_IN) {
  const options = {};
  if (expiresIn && expiresIn !== 'never') {
    options.expiresIn = expiresIn;
  }
  return jwt.sign(
    {
      id: user.id,
      username: user.username,
      role: user.role
    },
    JWT_SECRET,
    options
  );
}

export function verifyToken(token) {
  try {
    return jwt.verify(token, JWT_SECRET);
  } catch (error) {
    return null;
  }
}

export async function hashPassword(password) {
  return bcrypt.hash(password, 10);
}

/**
 * Check if external SSO is configured
 */
export function isExternalAuthEnabled() {
  return !!EXTERNAL_JWKS_URL;
}

/**
 * Normalize an arbitrary token (external IDP-minted or internal HS256) into a
 * single claim bag that downstream code can pass to `mintInternalToken` or
 * `selectResponseHeaders`. External tokens are tried first when JWKS is
 * configured (matches the precedence in the global cookie auth middleware).
 *
 * Returns null if neither path validates the token. The `raw` field carries
 * the original verified payload so callers can pull non-standard claims
 * (e.g. `groups`) without us having to enumerate them here.
 *
 * Used by the ForwardAuth handler only — existing flows keep using
 * `verifyExternalToken` / `verifyToken` directly so their behavior is unchanged.
 */
export async function extractClaimsFromAnyToken(token) {
  if (!token) return null;

  if (isExternalAuthEnabled()) {
    const ext = await verifyExternalToken(token);
    if (ext) {
      return {
        sub: ext.subject || ext.id,
        username: ext.username,
        email: ext.email,
        role: ext.role,
        roles: ext.roles,
        groups: ext.raw?.groups,
        isExternal: true,
        raw: ext.raw,
      };
    }
  }

  const internal = verifyToken(token);
  if (internal) {
    return {
      sub: internal.id != null ? String(internal.id) : internal.username,
      username: internal.username,
      email: internal.email,
      role: internal.role,
      roles: internal.role ? [internal.role] : [],
      isExternal: false,
      raw: internal,
    };
  }

  return null;
}

