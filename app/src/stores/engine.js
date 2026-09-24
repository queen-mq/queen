// Module singleton: the storage engine of the cell behind the acting cluster.
// The rule and the reasons live in composables/useEngine.js; this file only
// binds it to the API client, the identity epoch and localStorage, none of
// which the node test suite can import.
import { system } from '@/api'
import { createEngineStore } from '@/composables/useEngine'
import { currentEpoch, onClusterChange, useIdentity } from '@/stores/identity'

const STORAGE_PREFIX = 'queen.engine.'
const { actingCluster } = useIdentity()

const storageKey = () => {
  const id = actingCluster.value?.id
  return id ? `${STORAGE_PREFIX}${id}` : null
}

const engineStore = createEngineStore({
  fetchHealth: (config) => system.getHealth(config),
  getEpoch: currentEpoch,
  recall: () => {
    const key = storageKey()
    try { return key ? localStorage.getItem(key) : null } catch { return null }
  },
  remember: (engine) => {
    const key = storageKey()
    try { if (key) localStorage.setItem(key, engine) } catch { /* private mode */ }
  },
})

// A switch can land on a different cell running the other engine.
onClusterChange(() => engineStore.reset())

/** GET /health through the store, so the one call the shell polls also decides the engine. */
export const loadHealth = (config) => engineStore.loadHealth(config)

/**
 * The verdict for a view. Asking is also the probe: the first view to need it
 * starts one /health call, shared with the shell's own poll.
 */
export function useEngine() {
  engineStore.ensure()
  return { engine: engineStore.engine, isRaft: engineStore.isRaft }
}
