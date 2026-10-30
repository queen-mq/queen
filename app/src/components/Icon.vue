<script>
// One icon from the dashboard's set (composables/icons.js, Lucide): one grid,
// one stroke weight, round caps, the text's colour. `<Icon name="layers" />`.
// Decorative by default (aria-hidden); give it a `label` when the icon is the
// only thing that says what a control does.
import { h } from 'vue'

import { ICONS } from '@/composables/icons'

export default {
  name: 'Icon',
  props: {
    name: { type: String, required: true },
    size: { type: [Number, String], default: 16 },
    stroke: { type: [Number, String], default: 1.75 },
    label: { type: String, default: '' },
  },
  setup(props) {
    return () => {
      const nodes = ICONS[props.name] || []
      return h('svg', {
        class: 'icon',
        width: props.size,
        height: props.size,
        viewBox: '0 0 24 24',
        fill: 'none',
        stroke: 'currentColor',
        'stroke-width': props.stroke,
        'stroke-linecap': 'round',
        'stroke-linejoin': 'round',
        'aria-hidden': props.label ? undefined : 'true',
        'aria-label': props.label || undefined,
        role: props.label ? 'img' : undefined,
        focusable: 'false',
      }, nodes.map(([tag, attrs]) => h(tag, attrs)))
    }
  },
}
</script>
