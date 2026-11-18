/**
 * parity between the JS modules and their sibling declarations, checked by
 * `tsc` on generated TypeScript (the CLI only: TypeScript 7 has no stable JS
 * compiler API).
 *
 * for every `client-v2/**\/*.d.ts` next to a `.js` module, the module is
 * imported and compared with its declaration:
 * - module exports: the runtime export names must be exactly the declared
 *   values (`Record<keyof typeof api, true>`: an undeclared export is an
 *   excess property, a declared one the module lacks is a missing one)
 * - every exported class, read from its prototype and from the class itself:
 *   - its runtime methods must be exactly the declared methods
 *   - its runtime accessors must be exactly the declared accessors and
 *     fields, plus the instance fields listed in INSTANCE_FIELDS (a field
 *     exists only on an instance, and no class is constructed here)
 *   - its statics must be exactly the declared statics
 *
 * members whose name starts with `_` are internal and are not compared.
 *
 * the builders that public methods return but no module exports (`push()`,
 * `create()`, `consume()`, `dlq()`, `Stream.view()`, and a transaction's
 * `queue()` and `kv`) are compared the same way with their declared
 * interfaces: REACHABLE builds one of each from a client that sends no
 * request.
 */

import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { basename, dirname, join, relative, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const sourceDir = join(root, 'client-v2')
const tsc = join(root, 'node_modules', '.bin', 'tsc')

// instance fields the declarations name: set in the constructor, so on no prototype
const INSTANCE_FIELDS = {
  StreamBuilder: ['config'],
  Window: ['allMessages', 'messages']
}

// builders the public API returns and no module exports: declared interface,
// and how to get one from a client that sends no request
const REACHABLE = [
  { module: 'builders/QueueBuilder.js', type: 'PushBuilder<any>', make: ({ queen }) => queen.queue('parity').push([{ data: null }]) },
  { module: 'builders/QueueBuilder.js', type: 'OperationBuilder<any>', make: ({ queen }) => queen.queue('parity').create() },
  { module: 'builders/QueueBuilder.js', type: 'ConsumeBuilder<any, any>', make: ({ queen }) => queen.queue('parity').consume(async () => {}) },
  { module: 'builders/QueueBuilder.js', type: 'DLQBuilder', make: ({ queen }) => queen.queue('parity').dlq() },
  { module: 'builders/TransactionBuilder.js', type: 'TransactionQueue', make: ({ queen }) => queen.transaction().queue('parity') },
  { module: 'builders/TransactionBuilder.js', type: 'TransactionKv', make: ({ queen }) => queen.transaction().kv },
  { module: 'streams/Stream.js', type: 'FeedView', make: ({ queen, Stream }) => Stream.view(queen, 'parity') }
]

// the type helpers every generated file starts with
const HELPERS = [
  '// the named members of T, without its index signatures',
  'type Known<T> = { [K in keyof T as string extends K ? never : number extends K ? never : K]: T[K] }',
  '// the members of T declared as functions: its methods',
  'type MethodKeys<T> = { [K in keyof Known<T>]-?: NonNullable<Known<T>[K]> extends (...args: any[]) => any ? K : never }[keyof Known<T>]',
  '// the other members of T: its accessors and fields',
  'type PropKeys<T> = Exclude<keyof Known<T>, MethodKeys<T>>',
  '// the statics of a class',
  "type Statics<C> = Exclude<keyof Known<C>, 'prototype'>",
  ''
]

const SYMBOLS = ['asyncIterator', 'iterator']

function declarations (dir) {
  const out = []
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name)
    if (entry.isDirectory()) out.push(...declarations(path))
    else if (entry.name.endsWith('.d.ts')) out.push(path)
  }
  return out
}

function isClass (value) {
  return typeof value === 'function' && Function.prototype.toString.call(value).startsWith('class')
}

// a member as the generated code names it: null for internal and unknown symbol keys
function member (key) {
  if (typeof key === 'symbol') {
    const known = SYMBOLS.find((name) => Symbol[name] === key)
    return known ? `[Symbol.${known}]` : null
  }
  return key.startsWith('_') ? null : JSON.stringify(key)
}

// the public methods, accessors and fields of an object and of its prototypes
function membersOf (target) {
  const methods = []
  const accessors = []
  const fields = []
  const seen = new Set(['"constructor"'])
  for (let o = target; o && o !== Object.prototype && o !== Function.prototype; o = Object.getPrototypeOf(o)) {
    for (const key of Reflect.ownKeys(o)) {
      const name = member(key)
      if (name === null || seen.has(name)) continue
      seen.add(name)
      const descriptor = Object.getOwnPropertyDescriptor(o, key)
      if (descriptor.get || descriptor.set) accessors.push(name)
      else if (typeof descriptor.value === 'function') methods.push(name)
      else fields.push(name)
    }
  }
  return { methods: methods.sort(), accessors: accessors.sort(), fields: fields.sort() }
}

// the statics of a class, without the ones every function has
function staticsOf (cls) {
  return Reflect.ownKeys(cls)
    .filter((key) => !['length', 'name', 'prototype'].includes(key))
    .map(member)
    .filter((name) => name !== null)
    .sort()
}

function record (variable, type, names) {
  return [`export const ${variable}: ${type} = {`, ...names.map((name) => `  ${name}: true,`), '}']
}

// the checks of one declared type against the members found at runtime
function memberChecks (label, type, found, extraFields = []) {
  return [
    `// ${label}`,
    ...record(`${label}_methods`, `Record<MethodKeys<${type}>, true>`, found.methods),
    ...record(`${label}_properties`, `Record<PropKeys<${type}>, true>`,
      [...found.accessors, ...found.fields, ...extraFields.map((f) => JSON.stringify(f))].sort()),
    ''
  ]
}

// defining modules first, so each class is checked under the module that defines it
const pairs = declarations(sourceDir)
  .map((dts) => ({ dts, js: dts.replace(/\.d\.ts$/, '.js') }))
  .filter(({ js }) => existsSync(js))
  .sort((a, b) => Number(basename(a.js) === 'index.js') - Number(basename(b.js) === 'index.js'))

const work = mkdtempSync(join(tmpdir(), 'queen-exports-parity-'))
let failed = false
let classes = 0
try {
  const files = []
  const checked = new Set()

  for (const [index, { js }] of pairs.entries()) {
    const mod = await import(pathToFileURL(js).href)
    const lines = [
      `// ${relative(root, js)}`,
      `import * as api from ${JSON.stringify(js)}`,
      '',
      ...HELPERS,
      ...record('moduleExports', 'Record<keyof typeof api, true>', Object.keys(mod).sort().map((n) => JSON.stringify(n))),
      ''
    ]
    for (const [name, value] of Object.entries(mod)) {
      if (!isClass(value) || checked.has(value)) continue
      checked.add(value)
      classes++
      lines.push(
        `type ${name}Instance = InstanceType<typeof api.${name}>`,
        ...memberChecks(name, `${name}Instance`, membersOf(value.prototype), INSTANCE_FIELDS[name]),
        ...record(`${name}_statics`, `Record<Statics<typeof api.${name}>, true>`, staticsOf(value)),
        ''
      )
    }
    const file = join(work, `parity${index}.mts`)
    writeFileSync(file, lines.join('\n'))
    files.push(file)
  }

  // the builders: made from a client whose URL nothing listens on, never awaited
  const { Queen, Stream } = await import(pathToFileURL(join(sourceDir, 'index.js')).href)
  const queen = new Queen({ url: 'http://127.0.0.1:9', handleSignals: false })
  try {
    const lines = ['// builders the public API returns', '', ...HELPERS]
    for (const [index, { module, type, make }] of REACHABLE.entries()) {
      const name = type.replace(/<.*$/, '')
      lines.push(
        `import type { ${name} } from ${JSON.stringify(join(sourceDir, module))}`,
        `type Reachable${index} = ${type}`,
        ...memberChecks(name, `Reachable${index}`, membersOf(make({ queen, Stream })))
      )
    }
    const file = join(work, 'reachable.mts')
    writeFileSync(file, lines.join('\n'))
    files.push(file)
  } finally {
    await queen.close()
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

  const result = spawnSync(tsc, ['-p', join(work, 'tsconfig.json'), '--pretty', 'true'], { encoding: 'utf8' })
  if (result.status !== 0) {
    failed = true
    process.stderr.write(result.stdout + result.stderr)
    process.stderr.write(`\nexport parity: the declarations and the modules disagree (see above; generated files kept in ${work})\n`)
  } else {
    console.log(`export parity: ${pairs.length} modules, ${classes} classes and ${REACHABLE.length} builders match their declarations`)
  }
} finally {
  if (!failed) rmSync(work, { recursive: true, force: true })
}
process.exit(failed ? 1 : 0)
