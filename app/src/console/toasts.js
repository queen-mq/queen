// After an action, the broker's own answer, for 6 s (errors stay 12 s).
import { reactive } from 'vue'

export const toasts = reactive([])
let seq = 0

export function toast(text, sub = '', tone = '') {
  const id = ++seq
  toasts.push({ id, text, sub, tone })
  const ms = tone === 'red' ? 12_000 : 6_000
  setTimeout(() => dismiss(id), ms)
  return id
}

export function dismiss(id) {
  const i = toasts.findIndex((t) => t.id === id)
  if (i >= 0) toasts.splice(i, 1)
}
