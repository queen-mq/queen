#!/usr/bin/env node
// Logs go to stderr, as JSON lines: on stdio, stdout belongs to the protocol.

import { ConfigError, USAGE, loadConfig } from '../src/config.mjs'
import { buildServer } from '../src/server.mjs'
import { serveHttp, serveStdio } from '../src/transports.mjs'

const log = (entry) => process.stderr.write(`${JSON.stringify(entry)}\n`)

let config
try {
  config = loadConfig()
} catch (e) {
  if (!(e instanceof ConfigError)) throw e
  process.stderr.write(`queen-debug: ${e.message}\n\n${USAGE}\n`)
  process.exit(2)
}
if (config.help) {
  process.stdout.write(`${USAGE}\n`)
  process.exit(0)
}

const { protocol, tools } = buildServer(config, { log })
const started = {
  at: new Date().toISOString(),
  event: 'started',
  transport: config.transport,
  tools: tools.length,
  readMessages: config.readMessages,
  masked: config.mask.length,
  probe: config.allowProbe,
}

if (config.transport === 'http') {
  const server = await serveHttp(protocol, config.listen, log)
  log({ ...started, listen: `${config.listen.host}:${config.listen.port}` })
  const stop = () => server.close(() => process.exit(0))
  process.on('SIGTERM', stop)
  process.on('SIGINT', stop)
} else {
  log(started)
  await serveStdio(protocol)
  process.exit(0)
}
