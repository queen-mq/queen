// Module singleton: the optional-route verdicts of this session, keyed by the
// cluster epoch so a switch re-probes. The rule and the reasons live in
// composables/useRouteSupport.js; this file only binds it to the identity
// store, which cannot be imported by the node test suite.
import { createRouteSupport } from '@/composables/useRouteSupport'
import { currentEpoch } from '@/stores/identity'

export { isMissingRoute } from '@/composables/useRouteSupport'
export const routeSupport = createRouteSupport(currentEpoch)
