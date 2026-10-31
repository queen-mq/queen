// Who may use the acting cluster: its members and its API keys, for the
// Access page (views/Access.vue). Both lists are read together, because the
// page's list counts both whichever table is open, and each table changes its
// own list after a grant, a removal, a new key or a revocation.
//
// Module-level like the other stores, and keyed by tenant the same way: a
// cluster switch empties both lists (they were another cluster's).
//
//   const { members, keys, loadMembers, loadKeys, loadAll } = useAccessStore()
import { ref } from 'vue'

import { access } from '@/api'
import { onClusterChange } from '@/stores/identity'

function list(fetch) {
  const items = ref([])
  const loading = ref(false)
  const loaded = ref(false)
  const error = ref(null)
  const load = async () => {
    if (loading.value) return
    loading.value = true
    error.value = null
    try {
      const { data } = await fetch()
      items.value = Array.isArray(data) ? data : []
      loaded.value = true
    } catch (err) {
      error.value = err
    } finally {
      loading.value = false
    }
  }
  const reset = () => {
    items.value = []
    loaded.value = false
    error.value = null
  }
  return { items, loading, loaded, error, load, reset }
}

const members = list(() => access.listMembers())
const keys = list(() => access.listKeys())

onClusterChange(() => {
  members.reset()
  keys.reset()
})

export function useAccessStore() {
  return {
    members: members.items,
    membersLoading: members.loading,
    membersLoaded: members.loaded,
    membersError: members.error,
    loadMembers: members.load,
    keys: keys.items,
    keysLoading: keys.loading,
    keysLoaded: keys.loaded,
    keysError: keys.error,
    loadKeys: keys.load,
    loadAll: () => Promise.all([members.load(), keys.load()]),
  }
}
