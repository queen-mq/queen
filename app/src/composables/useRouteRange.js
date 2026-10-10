import { computed, ref, watch } from 'vue'
import { useRouteState } from './useRouteState.js'
import { validWindow } from './navigation.js'
import { formatDateTimeLocal } from './useFormat.js'

export const rangeMinutes = value => {
  const match = /^(\d+)(m|h|d)$/.exec(String(value))
  return match ? Number(match[1]) * ({ m: 1, h: 60, d: 1440 }[match[2]]) : 60
}

// Share the APPLIED period, not a half-edited custom-range form.
export function useRouteRange({ range, customMode, customFrom, customTo, appliedCustom, reload }) {
  const numeric = typeof range.value === 'number'
  const model = computed({
    get: () => numeric ? (range.value % 60 === 0 ? `${range.value / 60}h` : `${range.value}m`) : range.value,
    set: value => { range.value = numeric ? rangeMinutes(value) : value },
  })
  const from = ref(''), to = ref('')
  const { restoring } = useRouteState({ range: model, from, to })
  const apply = () => {
    const window = validWindow(from.value, to.value)
    customMode.value = Boolean(window)
    appliedCustom.value = window
    if (window) {
      customFrom.value = formatDateTimeLocal(window.from)
      customTo.value = formatDateTimeLocal(window.to)
    }
  }
  apply()
  watch([from, to, model], () => { if (restoring.value) { apply(); reload() } })
  watch([customMode, appliedCustom], () => {
    const window = customMode.value && appliedCustom.value
    from.value = window ? window.from.toISOString() : ''
    to.value = window ? window.to.toISOString() : ''
  })
}
