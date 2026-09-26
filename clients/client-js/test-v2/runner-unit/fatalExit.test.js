import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import net from 'node:net'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { describe, it } from 'node:test'

const suiteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..')

// A loopback port with nothing listening on it: bind an ephemeral port, then
// release it. Not port 1: fetch() rejects the Fetch-spec "bad ports" (1 is one
// of them) before it ever connects, so the preflight would fail with "bad port"
// and never exercise a refused connection.
function closedLoopbackPort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address()
      server.close(error => (error ? reject(error) : resolve(port)))
    })
  })
}

function runWithUnreachableBroker(brokerUrl) {
  return new Promise((resolve, reject) => {
    const env = { ...process.env, QUEEN_SERVER_URL: brokerUrl }
    // TEST_CONFIG=multiple would point run.js at its fixed localhost URLs
    // instead of QUEEN_SERVER_URL.
    delete env.TEST_CONFIG
    const child = spawn(process.execPath, ['test-v2/run.js', 'human'], {
      cwd: suiteRoot,
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let output = ''
    const deadline = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`integration runner did not exit within 5s:\n${output}`))
    }, 5000)
    child.stdout.on('data', chunk => { output += chunk })
    child.stderr.on('data', chunk => { output += chunk })
    child.once('error', error => {
      clearTimeout(deadline)
      reject(error)
    })
    child.once('close', (code, signal) => {
      clearTimeout(deadline)
      resolve({ code, signal, output })
    })
  })
}

describe('integration runner lifecycle', () => {
  it('returns a failure status when the broker preflight fails', async () => {
    const port = await closedLoopbackPort()
    const result = await runWithUnreachableBroker(`http://127.0.0.1:${port}`)

    assert.equal(result.signal, null)
    assert.equal(result.code, 1, result.output)
    assert.match(result.output, /Main error: broker preflight .*ECONNREFUSED/)
  })
})
