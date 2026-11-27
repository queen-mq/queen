// Flags and environment into one frozen object. The token is read from the
// environment only: a flag would show it in the process list.

import { readFileSync } from 'node:fs'

export const SERVICE_ACCOUNT_DIR = '/var/run/secrets/kubernetes.io/serviceaccount'

export const USAGE = `queen-debug: a read-only MCP server to debug a QueenMQ cluster

  queen-debug --brokers http://127.0.0.1:6632        over stdio, for one machine
  queen-debug --transport http                       inside Kubernetes

Flags (each has the environment variable in brackets):
  --brokers <urls>     Comma-separated broker addresses [QUEEN_DEBUG_BROKERS]
  --namespace <name>   Kubernetes namespace of the brokers [QUEEN_DEBUG_NAMESPACE]
  --selector <labels>  Label selector of the broker pods [QUEEN_DEBUG_SELECTOR]
  --broker-port <n>    Broker port on a discovered pod, default 6632 [QUEEN_DEBUG_BROKER_PORT]
  --transport <name>   stdio (default) or http [QUEEN_DEBUG_TRANSPORT]
  --listen <addr>      HTTP address, loopback only, default 127.0.0.1:7700 [QUEEN_DEBUG_LISTEN]
  --read-messages      Show message bodies and headers [QUEEN_DEBUG_READ_MESSAGES]
  --mask <paths>       Hide these fields, e.g. data.guest.email,headers.auth [QUEEN_DEBUG_MASK]
  --allow-probe        Turn on write_probe, the one tool that writes [QUEEN_DEBUG_ALLOW_PROBE]
  --help               This text

A broker with authentication on takes a token from QUEEN_DEBUG_TOKEN.`

const FLAGS = {
  '--brokers': 'brokers',
  '--namespace': 'namespace',
  '--selector': 'selector',
  '--broker-port': 'brokerPort',
  '--transport': 'transport',
  '--listen': 'listen',
  '--mask': 'mask',
}
const SWITCHES = { '--read-messages': 'readMessages', '--allow-probe': 'allowProbe' }
const LOOPBACK = new Set(['127.0.0.1', '::1', 'localhost'])

export class ConfigError extends Error {}

const truthy = (v) => ['1', 'true', 'yes', 'on'].includes(String(v ?? '').trim().toLowerCase())
const list = (v) => String(v ?? '').split(',').map((s) => s.trim()).filter(Boolean)

function parseArgs(argv) {
  const out = {}
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i]
    if (arg === '--help' || arg === '-h') {
      out.help = true
    } else if (SWITCHES[arg]) {
      out[SWITCHES[arg]] = true
    } else if (FLAGS[arg]) {
      if (i + 1 >= argv.length) throw new ConfigError(`${arg} needs a value`)
      out[FLAGS[arg]] = argv[(i += 1)]
    } else {
      throw new ConfigError(`unknown argument: ${arg}`)
    }
  }
  return out
}

function brokerUrl(text) {
  const withScheme = /^[a-z]+:\/\//i.test(text) ? text : `http://${text}`
  let url
  try {
    url = new URL(withScheme)
  } catch {
    throw new ConfigError(`not a broker address: ${text}`)
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new ConfigError(`not a broker address: ${text}`)
  if (url.pathname !== '/' || url.search || url.username || url.password) {
    throw new ConfigError(`a broker address is scheme, host and port only: ${text}`)
  }
  return url.origin
}

function parseListen(text) {
  const m = /^(?:\[([^\]]+)\]|([^:]+)):(\d{1,5})$/.exec(text)
  if (!m) throw new ConfigError(`--listen takes host:port, got ${text}`)
  const host = m[1] ?? m[2]
  const port = Number(m[3])
  if (port > 65535) throw new ConfigError(`--listen port out of range: ${port}`)
  if (!LOOPBACK.has(host)) {
    throw new ConfigError(`--listen must be a loopback address, got ${host}: this server has no login, and can read every message`)
  }
  return { host, port }
}

function detectNamespace(env, read) {
  if (env.QUEEN_DEBUG_NAMESPACE) return env.QUEEN_DEBUG_NAMESPACE
  if (!env.KUBERNETES_SERVICE_HOST) return null
  try {
    return read(`${SERVICE_ACCOUNT_DIR}/namespace`).trim()
  } catch {
    return null
  }
}

export function loadConfig(argv = process.argv.slice(2), env = process.env, read = (p) => readFileSync(p, 'utf8')) {
  const a = parseArgs(argv)
  if (a.help) return Object.freeze({ help: true })

  const transport = a.transport ?? env.QUEEN_DEBUG_TRANSPORT ?? 'stdio'
  if (transport !== 'stdio' && transport !== 'http') throw new ConfigError(`--transport is stdio or http, got ${transport}`)

  const brokerPort = Number(a.brokerPort ?? env.QUEEN_DEBUG_BROKER_PORT ?? 6632)
  if (!Number.isInteger(brokerPort) || brokerPort < 1 || brokerPort > 65535) {
    throw new ConfigError('--broker-port must be a port number')
  }

  const brokers = list(a.brokers ?? env.QUEEN_DEBUG_BROKERS).map(brokerUrl)
  const selector = a.selector ?? env.QUEEN_DEBUG_SELECTOR ?? null
  const namespace = a.namespace ?? detectNamespace(env, read)
  const inCluster = Boolean(env.KUBERNETES_SERVICE_HOST)
  if (!brokers.length && !(inCluster && selector && namespace)) {
    throw new ConfigError('no brokers: pass --brokers, or run in Kubernetes with --namespace and --selector')
  }

  const mask = list(a.mask ?? env.QUEEN_DEBUG_MASK)
  for (const path of mask) {
    if (!/^(data|headers)(\.[^.\s]+)+$/.test(path)) {
      throw new ConfigError(`a mask path starts with data. or headers., got ${path}`)
    }
  }

  return Object.freeze({
    help: false,
    transport,
    listen: parseListen(a.listen ?? env.QUEEN_DEBUG_LISTEN ?? '127.0.0.1:7700'),
    brokers: Object.freeze(brokers),
    brokerPort,
    namespace,
    selector,
    inCluster,
    token: env.QUEEN_DEBUG_TOKEN || null,
    readMessages: Boolean(a.readMessages) || truthy(env.QUEEN_DEBUG_READ_MESSAGES),
    mask: Object.freeze(mask),
    allowProbe: Boolean(a.allowProbe) || truthy(env.QUEEN_DEBUG_ALLOW_PROBE),
  })
}
