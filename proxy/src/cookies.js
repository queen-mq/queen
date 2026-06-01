/**
 * Centralized cookie naming for the proxy.
 *
 * The session cookie name is configurable via COOKIE_NAME and defaults to the
 * prefixed `queen_token` rather than the bare `token`. A bare `token` cookie
 * collides with other apps sitting behind the same Traefik/parent-domain — most
 * notably MinIO, which also issues a cookie literally named `token`. When both
 * share a COOKIE_DOMAIN the browser sends both and the wrong one can win,
 * breaking auth in confusing ways (issue #30).
 *
 * Operators upgrading from a build that used `token` can either:
 *   - set COOKIE_NAME=token to keep the old name (no re-login), or
 *   - accept the new default, which simply forces one re-login as the old
 *     `token` cookie is ignored.
 */
export const SESSION_COOKIE_NAME = process.env.COOKIE_NAME || 'queen_token';
