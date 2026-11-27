// A tool's answer as text an agent can hold: bounded, and saying when it was
// cut and how to read the rest.

export const MAX_CHARS = 40_000

function arraysIn(value, path = '', out = []) {
  if (Array.isArray(value)) {
    if (value.length > 1) out.push({ path, holder: value })
    value.forEach((v, i) => {
      if (v && typeof v === 'object') arraysIn(v, `${path}[${i}]`, out)
    })
  } else if (value && typeof value === 'object') {
    for (const [k, v] of Object.entries(value)) arraysIn(v, path ? `${path}.${k}` : k, out)
  }
  return out
}

/** The answer as JSON, its largest lists shortened until it fits. */
export function render(answer, maxChars = MAX_CHARS) {
  const out = structuredClone(answer)
  let text = JSON.stringify(out)
  if (text.length <= maxChars) return text

  const cut = []
  for (let round = 0; round < 24 && text.length > maxChars; round += 1) {
    const lists = arraysIn(out)
      .map((a) => ({ ...a, size: JSON.stringify(a.holder).length }))
      .sort((a, b) => b.size - a.size)
    const biggest = lists[0]
    if (!biggest) break
    const before = biggest.holder.length
    const keep = Math.max(1, Math.floor(before * Math.min(0.75, (maxChars * 0.9) / text.length)))
    biggest.holder.length = keep
    const seen = cut.find((c) => c.path === biggest.path)
    if (seen) seen.shown = keep
    else cut.push({ path: biggest.path, shown: keep, had: before })
    text = JSON.stringify(out)
  }
  if (text.length > maxChars) {
    return JSON.stringify({
      error: 'answer_too_large',
      message: 'This answer does not fit even when shortened. Ask with a lower limit or a narrower filter.',
    })
  }
  if (cut.length) {
    out.cut = { lists: cut, hint: 'Shortened to fit. Ask again with a lower limit, a later offset or a narrower filter.' }
    text = JSON.stringify(out)
  }
  return text
}

/** Spread evenly: at most `max` points of a long series, always with the last. */
export function thin(points, max = 30) {
  if (!Array.isArray(points) || points.length <= max) return points
  const out = []
  const step = (points.length - 1) / (max - 1)
  for (let i = 0; i < max; i += 1) out.push(points[Math.round(i * step)])
  return out
}
