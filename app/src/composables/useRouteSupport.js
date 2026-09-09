// Which optional routes a cluster's broker lacks: the ephemeralStore rule,
// generalised. Nothing negotiates a version, so the first call to a route that
// is new in some release is also the probe, and three answers are STABLE,
// asking again cannot change them:
//
//     404                  the broker predates the route
//     404 route_blocked    the proxy does not classify the family
//     not_an_api_response  broker-direct: the SPA fallback answered the GET
//
// (the predicate itself is api/errors.js isMissingRouteError, shared with the
// HTTP client's `probe` option, which keeps those answers off the toast
// surface for a call that declared itself a probe.)
//
// Every call the shell makes and loses is a toast (stores/ui.js
// reportApiFailure), so a page that re-asks on each mount, pill click or
// refresh tick turns an older broker into a stream of them. A remembered
// verdict makes the second ask free: the wrapped call rejects at once with
// the error that established the verdict, no request leaves, and the panel
// renders the same quiet "not available" state it would have rendered anyway.
//
// Verdicts are keyed by the cluster epoch the caller supplies: a switch can
// land on a different cell running a different version, so they reset with
// the rows, exactly as the tenant-keyed stores do. Pure so it is testable;
// stores/routeSupport.js binds it to identity's currentEpoch.

import { isMissingRouteError } from '../api/errors.js'

export const isMissingRoute = isMissingRouteError

export function createRouteSupport(getEpoch) {
  const absent = new Map() // `${epoch}|${route}` -> the error that said so
  const key = (route) => `${getEpoch()}|${route}`

  /** The error that established a missing-route verdict in this epoch, or null. */
  const missing = (route) => absent.get(key(route)) || null
  const remember = (route, err) => { absent.set(key(route), err) }
  /** Drop a verdict so an operator can re-ask after upgrading the broker. */
  const forget = (route) => { absent.delete(key(route)) }

  /**
   * Wrap an API call. A known-absent route rejects immediately with the
   * remembered error (no request, no toast); a fresh stable verdict is
   * remembered; every other outcome passes through untouched.
   */
  const guard = (route, call) => async (...args) => {
    const known = missing(route)
    if (known) throw known
    try {
      return await call(...args)
    } catch (err) {
      if (isMissingRoute(err)) remember(route, err)
      throw err
    }
  }

  return { missing, remember, forget, guard }
}
