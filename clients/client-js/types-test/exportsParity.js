/**
 * Export parity between the JS modules and their sibling declarations.
 *
 * For every `client-v2/**\/*.d.ts` that sits next to a `.js` module, the
 * module is imported at runtime and its export names are written into a
 * generated TypeScript file:
 *
 *   import * as api from '<module>.js'
 *   const keys: Record<keyof typeof api, true> = { <runtime export>: true, ... }
 *
 * `tsc` then fails on either kind of drift: a runtime export the declaration
 * lacks is an excess property, and a declared value the module does not
 * export is a missing one. Only the `tsc` CLI is used, so this works on
 * TypeScript 7, which has no stable JS compiler API.
 */

import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const sourceDir = join(root, 'client-v2')
const tsc = join(root, 'node_modules', '.bin', 'tsc')

function declarations(dir) {
  const out = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) out.push(...declarations(path))
    else if (entry.name.endsWith('.d.ts')) out.push(path)
  }
  return out
}

const pairs = declarations(sourceDir)
  .map((dts) => ({ dts, js: dts.replace(/\.d\.ts$/, '.js') }))
  .filter(({ js }) => existsSync(js))

const work = mkdtempSync(join(tmpdir(), 'queen-exports-parity-'))
let failed = false
try {
  const files = []
  for (const [index, { js }] of pairs.entries()) {
    const names = Object.keys(await import(pathToFileURL(js).href)).sort()
    const body = names.map((name) => `  ${JSON.stringify(name)}: true,`).join('\n')
    const file = join(work, `parity${index}.mts`)
    writeFileSync(file, [
      `// ${relative(root, js)}`,
      `import * as api from ${JSON.stringify(js)}`,
      `export const keys: Record<keyof typeof api, true> = {`,
      body,
      `}`,
      ''
    ].join('\n'))
    files.push(file)
  }
  writeFileSync(join(work, 'tsconfig.json'), JSON.stringify({
    compilerOptions: {
      module: 'nodenext',
      moduleResolution: 'nodenext',
      target: 'esnext',
      lib: ['ESNext'],
      types: ['node'],
      typeRoots: [join(root, 'node_modules', '@types')],
      strict: true,
      skipLibCheck: false,
      noEmit: true
    },
    files
  }, null, 2))

  const result = spawnSync(tsc, ['-p', join(work, 'tsconfig.json')], { encoding: 'utf8' })
  if (result.status !== 0) {
    failed = true
    process.stderr.write(result.stdout + result.stderr)
    process.stderr.write('\nexport parity: the declarations and the modules disagree (see above)\n')
  } else {
    console.log(`export parity: ${pairs.length} modules match their declarations`)
  }
} finally {
  rmSync(work, { recursive: true, force: true })
}
process.exit(failed ? 1 : 0)
