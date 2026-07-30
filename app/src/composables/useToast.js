// View-facing handle on the global notification surface.
//
// API failures are ALREADY reported by the axios interceptor — do not
// re-announce one you caught, or the same failure shows up twice. Use this for
// what the shell cannot see: the outcome of a local action, a refusal you
// decided yourself, a validation message.
//
//   const { notifySuccess, notifyError } = useToast()
//   try { await queues.delete(name); notifySuccess(`Deleted ${name}`) }
//   catch (e) { /* already on screen; render the inline state */ }
import { useUiStore } from '@/stores/ui'

export function useToast() {
  const { notifySuccess, notifyInfo, notifyWarn, notifyError, dismiss } = useUiStore()
  return { notifySuccess, notifyInfo, notifyWarn, notifyError, dismiss }
}
