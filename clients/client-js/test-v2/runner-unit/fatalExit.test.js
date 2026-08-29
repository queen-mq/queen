import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { describe, it } from 'node:test'

const suiteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..')

function runWithUnavailableDatabase() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['test-v2/run.js', 'human'], {
      cwd: suiteRoot,
      env: {
        ...process.env,
        PG_HOST: '127.0.0.1',
        PG_PORT: '1',
        PG_DB: 'postgres',
        PG_USER: 'postgres',
        PG_PASSWORD: 'postgres',
        QUEEN_SERVER_URL: 'http://127.0.0.1:1'
      },
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
  it('returns a failure status when database initialization fails', async () => {
    const result = await runWithUnavailableDatabase()

    assert.equal(result.signal, null)
    assert.equal(result.code, 1, result.output)
    assert.match(result.output, /Main error: connect ECONNREFUSED/)
  })
})
