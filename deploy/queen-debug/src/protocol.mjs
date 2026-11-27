// The Model Context Protocol: JSON-RPC, stateless, tools and prompts. Every
// message is answered on its own, so the same dispatch serves stdio and HTTP.

import { render } from './answer.mjs'
import { BrokerError } from './broker.mjs'
import { KubeError } from './kube.mjs'

export const PROTOCOL_VERSIONS = ['2025-11-25', '2025-06-18', '2025-03-26', '2024-11-05']

const result = (id, value) => ({ jsonrpc: '2.0', id, result: value })
const failure = (id, code, message) => ({ jsonrpc: '2.0', id, error: { code, message } })

class InvalidParams extends Error {}

/** The small part of JSON Schema the tools declare, strict about unknown fields. */
export function validateArgs(schema, args) {
  const given = args ?? {}
  if (typeof given !== 'object' || Array.isArray(given)) throw new InvalidParams('arguments must be an object')
  const props = schema.properties || {}
  for (const name of Object.keys(given)) {
    if (!(name in props)) throw new InvalidParams(`unknown argument "${name}". This tool takes: ${Object.keys(props).join(', ') || 'nothing'}`)
  }
  for (const name of schema.required || []) {
    if (given[name] === undefined || given[name] === null || given[name] === '') throw new InvalidParams(`"${name}" is required`)
  }
  for (const [name, value] of Object.entries(given)) {
    if (value === undefined || value === null) continue
    const p = props[name]
    if (p.type === 'string' && typeof value !== 'string') throw new InvalidParams(`"${name}" must be a string`)
    if (p.type === 'boolean' && typeof value !== 'boolean') throw new InvalidParams(`"${name}" must be true or false`)
    if (p.type === 'integer') {
      if (!Number.isInteger(value)) throw new InvalidParams(`"${name}" must be a whole number`)
      if (p.minimum !== undefined && value < p.minimum) throw new InvalidParams(`"${name}" must be at least ${p.minimum}`)
      if (p.maximum !== undefined && value > p.maximum) throw new InvalidParams(`"${name}" must be at most ${p.maximum}`)
    }
    if (p.type === 'array' && !Array.isArray(value)) throw new InvalidParams(`"${name}" must be a list`)
    if (p.enum && !p.enum.includes(value)) throw new InvalidParams(`"${name}" must be one of: ${p.enum.join(', ')}`)
  }
  return given
}

/** A failed call as an answer the agent can act on, never a stack trace. */
function errorAnswer(e) {
  if (e instanceof BrokerError) {
    const hints = {
      no_access: 'This broker has authentication on and the token cannot read this. Set QUEEN_DEBUG_TOKEN.',
      not_found: 'Check the name with list_queues or list_groups.',
      unavailable: 'The broker cannot answer now, usually because the cluster has no quorum. Run diagnose.',
      timeout: 'The node did not answer in time. Run diagnose.',
      unreachable: 'No broker could be reached. Run diagnose.',
      too_large: 'Ask with a lower limit or a narrower filter.',
    }
    return { error: e.kind, message: e.message, code: e.code ?? undefined, node: e.node ?? undefined, hint: hints[e.kind] }
  }
  if (e instanceof KubeError) return { error: 'kubernetes', message: e.message }
  if (e instanceof RangeError) return { error: 'bad_argument', message: e.message }
  return { error: 'internal', message: 'This tool failed. The server log has the details.' }
}

/** Only the names that say what was looked at: never a value someone searched for. */
const LOGGED_ARGS = ['queue', 'group', 'node', 'status', 'namespace', 'name']

export function createProtocol({ tools, prompts = [], info, instructions, log = () => {} }) {
  const byName = new Map(tools.map((t) => [t.name, t]))
  const promptsByName = new Map(prompts.map((p) => [p.name, p]))

  async function callTool(name, args) {
    const tool = byName.get(name)
    if (!tool) throw new InvalidParams(`unknown tool: ${name}`)
    const started = Date.now()
    const entry = { at: new Date(started).toISOString(), event: 'tool_call', tool: name }
    for (const k of LOGGED_ARGS) if (typeof args?.[k] === 'string') entry[k] = args[k]
    let parsed
    try {
      parsed = validateArgs(tool.inputSchema, args)
    } catch (e) {
      log({ ...entry, ok: false, error: 'bad_argument' })
      throw e
    }
    try {
      const answer = await tool.run(parsed)
      log({ ...entry, ok: true, ms: Date.now() - started })
      return { content: [{ type: 'text', text: render(answer) }] }
    } catch (e) {
      const out = errorAnswer(e)
      log({ ...entry, ok: false, ms: Date.now() - started, error: out.error, detail: out.error === 'internal' ? String(e?.stack ?? e) : undefined })
      return { content: [{ type: 'text', text: render(out) }], isError: true }
    }
  }

  async function dispatch(message) {
    if (!message || typeof message !== 'object' || message.jsonrpc !== '2.0') {
      return failure(message?.id ?? null, -32600, 'Invalid request.')
    }
    if (typeof message.method !== 'string') return null // a response to us: we never ask
    if (!('id' in message)) return null // a notification
    const { id, method } = message
    const params = message.params ?? {}
    try {
      switch (method) {
        case 'initialize':
          return result(id, {
            protocolVersion: PROTOCOL_VERSIONS.includes(params.protocolVersion) ? params.protocolVersion : PROTOCOL_VERSIONS[0],
            capabilities: { tools: { listChanged: false }, prompts: { listChanged: false } },
            serverInfo: info,
            instructions,
          })
        case 'ping':
          return result(id, {})
        case 'tools/list':
          return result(id, {
            tools: tools.map((t) => ({
              name: t.name,
              title: t.title,
              description: t.description,
              inputSchema: t.inputSchema,
              annotations: { title: t.title, readOnlyHint: t.writes !== true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
            })),
          })
        case 'tools/call':
          return result(id, await callTool(params.name, params.arguments))
        case 'prompts/list':
          return result(id, {
            prompts: prompts.map((p) => ({ name: p.name, title: p.title, description: p.description, arguments: p.arguments ?? [] })),
          })
        case 'prompts/get': {
          const p = promptsByName.get(params.name)
          if (!p) throw new InvalidParams(`unknown prompt: ${params.name}`)
          return result(id, {
            description: p.description,
            messages: [{ role: 'user', content: { type: 'text', text: p.render(params.arguments ?? {}) } }],
          })
        }
        default:
          return failure(id, -32601, `Method not found: ${method}`)
      }
    } catch (e) {
      if (e instanceof InvalidParams) return failure(id, -32602, e.message)
      log({ at: new Date().toISOString(), event: 'internal_error', method, detail: String(e?.stack ?? e) })
      return failure(id, -32603, 'Internal error.')
    }
  }

  return { dispatch }
}
