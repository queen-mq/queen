import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'

import { resolveTheme, THEME_STORAGE_KEY } from '../src/composables/useTheme.js'

// The whole selection policy, with both environment reads passed in.
test('a stored choice always wins', () => {
  assert.equal(resolveTheme('light', false), 'light')
  assert.equal(resolveTheme('light', true), 'light')
  assert.equal(resolveTheme('dark', true), 'dark')
  assert.equal(resolveTheme('dark', false), 'dark')
})

test('with nothing stored the OS is followed only when it asks for light', () => {
  assert.equal(resolveTheme(null, true), 'light')
  // The regression that matters: an OS on dark, or on no-preference, must
  // land every existing user exactly where they already were.
  assert.equal(resolveTheme(null, false), 'dark')
  assert.equal(resolveTheme(undefined, false), 'dark')
})

test('a junk stored value falls back rather than throwing', () => {
  assert.equal(resolveTheme('', false), 'dark')
  assert.equal(resolveTheme('solarized', false), 'dark')
  assert.equal(resolveTheme('solarized', true), 'light')
})

// The pre-paint script in index.html duplicates resolveTheme() on purpose —
// it has to run before any module loads or the first frame flashes the wrong
// scheme. Duplicated logic drifts, so pin the three things that would break
// it silently: the storage key, the media query, and the one-sided default.
test('the pre-paint script in index.html agrees with resolveTheme', () => {
  const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8')
  assert.match(html, new RegExp(`getItem\\('${THEME_STORAGE_KEY}'\\)`))
  assert.match(html, /prefers-color-scheme: light/)
  assert.match(html, /var t = 'dark'/)          // default when nothing matches
  assert.match(html, /classList\.toggle\('light'/)
  assert.match(html, /colorScheme = t/)
})

// Both schemes must define the same token surface, or a component styled
// against a token the light set forgot renders with the dark value on a white
// card. Structural, so it holds for tokens added later.
test('the light token set covers every colour token in :root', () => {
  const css = readFileSync(new URL('../src/style.css', import.meta.url), 'utf8')
  const block = (marker) => {
    const start = css.indexOf(marker)
    assert.notEqual(start, -1, `missing block: ${marker}`)
    let i = css.indexOf('{', start), depth = 0
    for (let j = i; j < css.length; j++) {
      if (css[j] === '{') depth++
      else if (css[j] === '}' && --depth === 0) return css.slice(i, j)
    }
    throw new Error(`unterminated block: ${marker}`)
  }
  const names = (b) => new Set([...b.matchAll(/(--[\w-]+)\s*:/g)].map((m) => m[1]))

  const dark = names(block('  :root {'))
  const light = names(block('  html.light {'))

  // Scheme-independent by design: a radius, a font and an easing curve do not
  // change with the colour scheme and must NOT be restated.
  const INVARIANT = new Set(['--r-chip', '--r-control', '--r-card', '--r-pill', '--font-mono', '--ease'])

  const missing = [...dark].filter((n) => !light.has(n) && !INVARIANT.has(n))
  assert.deepEqual(missing, [], `light set is missing: ${missing.join(', ')}`)

  const stray = [...light].filter((n) => !dark.has(n))
  assert.deepEqual(stray, [], `light set defines tokens :root does not: ${stray.join(', ')}`)

  assert.match(block('  html.light {'), /color-scheme:\s*light/)
  assert.match(block('  :root {'), /color-scheme:\s*dark/)
})
