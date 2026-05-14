import { verifyToken } from './auth.js';

// Authentication middleware - checks if user has valid JWT
// Supports both internal (proxy-generated) and external (SSO) tokens
// Note: If req.user is already set by global middleware, skip re-verification
export function requireAuth(req, res, next) {
  // Already authenticated by global middleware (handles both internal and external tokens)
  if (req.user) {
    return next();
  }

  const token = req.cookies.token || req.headers.authorization?.replace('Bearer ', '');

  if (!token) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  const user = verifyToken(token);

  if (!user) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }

  req.user = user;
  next();
}

// RBAC middleware - checks if user has required role
export function requireRole(allowedRoles) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Authentication required' });
    }

    if (!allowedRoles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Insufficient permissions' });
    }

    next();
  };
}

// Method-based access control
export function checkMethodAccess(req, res, next) {
  const method = req.method;
  const role = req.user?.role;

  if (!role) {
    return res.status(401).json({ error: 'Authentication required' });
  }

  // Admin can do everything
  if (role === 'admin') {
    return next();
  }

  // Read-write can do GET, POST, PUT, DELETE
  if (role === 'read-write') {
    return next();
  }

  // Read-only can only do GET
  if (role === 'read-only' && method === 'GET') {
    return next();
  }

  return res.status(403).json({ 
    error: 'Insufficient permissions',
    detail: `Role '${role}' cannot perform ${method} operations`
  });
}

/**
 * Pure-function variant of `checkMethodAccess` for the ForwardAuth handler.
 * Returns true when the method is allowed for the role embedded in `claims`,
 * false otherwise. No HTTP I/O so it is trivial to unit test and cannot
 * accidentally write a body — Traefik discards ForwardAuth response bodies
 * anyway.
 *
 * Mirrors `checkMethodAccess`:
 *  - admin       → all methods
 *  - read-write  → all methods
 *  - read-only   → GET only
 *  - missing role → denied
 *
 * Also accepts a `roles` array (external IdPs typically emit one); any
 * matching role wins.
 */
export function enforceMethodRBACFromClaims(method, claims) {
  const m = String(method || '').toUpperCase();
  const roles = new Set();
  if (claims?.role) roles.add(claims.role);
  if (Array.isArray(claims?.roles)) {
    for (const r of claims.roles) if (r) roles.add(r);
  }
  if (roles.size === 0) return false;
  if (roles.has('admin')) return true;
  if (roles.has('read-write')) return true;
  if (roles.has('read-only')) return m === 'GET' || m === 'HEAD' || m === 'OPTIONS';
  return false;
}

