import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { runInNewContext } from 'node:vm'

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

  // Scheme-independent by design: radii, fonts and motion settings do not
  // change with the colour scheme and must NOT be restated.
  const INVARIANT = new Set(['--r-chip', '--r-control', '--r-card', '--r-pill', '--font-mono', '--ease', '--drawer-duration', '--drawer-ease'])

  const missing = [...dark].filter((n) => !light.has(n) && !INVARIANT.has(n))
  assert.deepEqual(missing, [], `light set is missing: ${missing.join(', ')}`)

  const stray = [...light].filter((n) => !dark.has(n))
  assert.deepEqual(stray, [], `light set defines tokens :root does not: ${stray.join(', ')}`)

  assert.match(block('  html.light {'), /color-scheme:\s*light/)
  assert.match(block('  :root {'), /color-scheme:\s*dark/)
})


test('System resolves to the device scheme', () => {
  assert.equal(resolveTheme('system', true), 'light')
  assert.equal(resolveTheme('system', false), 'dark')
})

// Exercise the real state and listeners with browser boundaries supplied here.
let themeInstance = 0
async function themeBrowser(t, { stored = null, light = false, blocked = false } = {}) {
  const mediaListeners = [], storageListeners = []
  const mq = { matches: light, addEventListener: (_, fn) => mediaListeners.push(fn) }
  const classes = new Set()
  const root = {
    style: {},
    classList: { toggle: (name, on) => on ? classes.add(name) : classes.delete(name) },
  }
  const storage = {
    getItem: () => { if (blocked) throw new Error('denied'); return stored },
    setItem: (_, value) => { if (blocked) throw new Error('denied'); stored = value },
    removeItem: () => { if (blocked) throw new Error('denied'); stored = null },
  }
  for (const [key, value] of Object.entries({
    localStorage: storage,
    matchMedia: () => mq,
    document: { documentElement: root },
    window: { addEventListener: (_, fn) => storageListeners.push(fn) },
  })) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, key)
    Object.defineProperty(globalThis, key, { value, configurable: true, writable: true })
    t.after(() => descriptor
      ? Object.defineProperty(globalThis, key, descriptor)
      : delete globalThis[key])
  }
  const api = await import(`../src/composables/useTheme.js?test=${++themeInstance}`)
  return {
    ...api, root, classes,
    stored: () => stored,
    mediaListeners,
    changeDevice: (light) => {
      mq.matches = light
      mediaListeners.forEach(fn => fn({ matches: light }))
    },
    changeStorage: (value) => {
      stored = value
      storageListeners.forEach(fn => fn({ key: THEME_STORAGE_KEY }))
    },
  }
}

test('System follows device changes live and initialization attaches one listener', async (t) => {
  const env = await themeBrowser(t)
  env.initTheme()
  env.initTheme()
  assert.equal(env.themePreference.value, 'system')
  assert.equal(env.theme.value, 'dark')
  assert.equal(env.mediaListeners.length, 1)
  env.changeDevice(true)
  assert.equal(env.theme.value, 'light')
  assert.equal(env.root.style.colorScheme, 'light')
  assert.deepEqual([...env.classes], ['light'])
  env.changeDevice(false)
  assert.equal(env.theme.value, 'dark')
  assert.equal(env.stored(), null)
})

test('a fixed preference persists; selecting System releases the override immediately', async (t) => {
  const env = await themeBrowser(t, { stored: 'light' })
  env.initTheme()
  assert.equal(env.themePreference.value, 'light')
  env.changeDevice(false)
  assert.equal(env.theme.value, 'light')
  env.setTheme('dark')
  assert.equal(env.stored(), 'dark')
  env.changeDevice(true)
  assert.equal(env.theme.value, 'dark')
  env.setTheme('system')
  assert.equal(env.stored(), null)
  assert.equal(env.themePreference.value, 'system')
  assert.equal(env.theme.value, 'light')
  env.initTheme()
  assert.equal(env.themePreference.value, 'system')
})

test('a fixed choice remains fixed even when storage is blocked', async (t) => {
  const env = await themeBrowser(t, { blocked: true })
  env.initTheme()
  env.setTheme('light')
  env.changeDevice(false)
  assert.equal(env.theme.value, 'light')
  env.setTheme('system')
  assert.equal(env.theme.value, 'dark')
  env.changeDevice(true)
  assert.equal(env.theme.value, 'light')
})

test('other tabs can select a fixed scheme or restore System', async (t) => {
  const env = await themeBrowser(t)
  env.initTheme()
  env.changeStorage('light')
  assert.equal(env.themePreference.value, 'light')
  assert.equal(env.theme.value, 'light')
  env.changeStorage(null)
  assert.equal(env.themePreference.value, 'system')
  assert.equal(env.theme.value, 'dark')
})

test('pre-paint matches System and fixed preferences, including blocked storage', () => {
  const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8')
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1]
  for (const stored of [null, 'system', 'light', 'dark', 'invalid']) {
    for (const prefersLight of [true, false]) {
      for (const blocked of [true, false]) {
        const root = { style: {}, classList: { toggle() {} } }
        runInNewContext(script, {
          localStorage: { getItem() { if (blocked) throw new Error('denied'); return stored } },
          window: { matchMedia: () => ({ matches: prefersLight }) },
          document: { documentElement: root },
        })
        assert.equal(root.style.colorScheme, resolveTheme(blocked ? null : stored, prefersLight))
      }
    }
  }
})
