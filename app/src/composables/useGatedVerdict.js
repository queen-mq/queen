// What a failed call to an OPTIONAL, GATED route family actually means — the
// four answers a page has to be able to tell apart, and the copy for each.
//
// stores/routeSupport.js already remembers one of them ("this route is not on
// this cell") so it is asked once per cluster epoch. It is not enough for the
// KV and timer families, because those live behind a plan feature AND behind a
// pair of operator switches, and the three refusals look identical to a view
// that only reads the status code:
//
//   404 / route_blocked / not_an_api_response   the route is NOT HERE
//   403 feature_gated                           here, NOT IN THE PLAN
//   503 kv_disabled / timers_disabled           here, granted, PAUSED by an
//       / kv_unavailable / timers_unavailable   operator, or the cell cannot
//       / ephemeral_*                           reach its database right now
//   anything else                               TRANSIENT
//
// The first three are states to RENDER — one quiet card, no poll, no toast
// storm. The fourth keeps the last-good rows and lets the global surface carry
// the failure, exactly as views/Ephemeral.vue does for its own family.
//
// WHERE THE CODE LIVES IS NOT ONE PLACE — the proxy names it in `code`, the
// broker's own handlers put it in `error` — and api/errors.js `refusalCode`
// hides that, along with the closed sets of codes each of the three states is
// made of (`isFeatureGatedError`, `isPausedFamilyError`, `isMissingRouteError`).
// They live there rather than here because api/httpClient.js needs the same
// predicates to keep a page that RENDERS these verdicts off the global toast
// surface, and a transport that imported a composable to learn them would be
// the wrong way round.
//
// What is left here is the part that is about the SCREEN: the order of
// precedence between the three stable answers, and the copy for each.
//
// Pure: no Vue, no store, no HTTP. The verdict is a function of the error.
import {
  isFeatureGatedError,
  isMissingRouteError,
  isPausedFamilyError,
  refusalCode,
} from '../api/errors.js'

// Re-exported so a page reading a code off a refusal has one import, and so
// test/gated-verdict.test.js holds the reader against the envelopes it is for.
export { refusalCode }

/**
 * 'absent' | 'gated' | 'paused' | 'transient' — in that order of precedence,
 * which is also the order of how permanent the answer is.
 *
 * The three answers stores/routeSupport.js remembers per epoch come first: the
 * route is not on this cell, and asking again cannot change it. Then the plan
 * gate, then the cell's own switch; anything else is a failure to retry.
 */
export function gatedVerdict(err) {
  if (!err) return 'transient'
  if (isMissingRouteError(err)) return 'absent'
  if (isFeatureGatedError(err)) return 'gated'
  if (isPausedFamilyError(err)) return 'paused'
  return 'transient'
}

/**
 * One family's name, declined so the copy below reads as English rather than
 * as a template. Unknown keys become the label itself, so a page can pass a
 * plain noun phrase without registering it here first.
 */
const FAMILIES = {
  timers: { subject: 'Timers', lower: 'timers', be: 'are' },
  kv: { subject: 'The KV browser', lower: 'the KV browser', be: 'is' },
  ephemeral: { subject: 'Ephemeral queues', lower: 'ephemeral queues', be: 'are' },
}

const familyOf = (family) =>
  FAMILIES[family] || { subject: String(family || 'This feature'), lower: String(family || 'this feature'), be: 'is' }

/**
 * The quiet-state copy for a verdict: `{title, detail}`.
 *
 * Deliberately generic — what is true of every gated family and nothing more.
 * A page that knows something sharper (which release added its routes, what an
 * operator should do next) says it in its own template underneath; this is the
 * sentence that must be identical everywhere so the wording of a 403 or a 503
 * does not drift between two pages describing the same cell.
 */
export function describeVerdict(verdict, family) {
  const f = familyOf(family)
  switch (verdict) {
    case 'absent':
      return {
        title: `${f.subject} ${f.be} not available on this broker`,
        detail:
          `The route family is not served here — the broker on this cell predates it, or the proxy ` +
          `in front does not classify it. Asking again cannot change that, and nothing else on the ` +
          `dashboard is affected.`,
      }
    case 'gated':
      return {
        title: `${f.subject} ${f.be} not enabled for this cluster`,
        detail:
          `The routes exist, but this cluster's plan does not grant the feature, so the proxy refuses ` +
          `before the broker sees the call. An administrator has to enable it.`,
      }
    case 'paused':
      return {
        title: `${f.subject} ${f.be} not being served right now`,
        detail:
          `The cell answered 503 — an operator's switch, or a broker that cannot reach its database. ` +
          `It is neither your permissions nor your plan, and a retry is the only cure.`,
      }
    default:
      return {
        title: `Cannot load ${f.lower}`,
        detail: 'The call failed, so anything on screen predates it.',
      }
  }
}
