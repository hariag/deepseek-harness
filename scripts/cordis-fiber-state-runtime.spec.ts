import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { resolve } from 'node:path'
import { expect, it } from 'vitest'

const execFileAsync = promisify(execFile)

it('loads source Agents against built Cordis with runtime FiberState values', async () => {
  const fixture = resolve(import.meta.dirname, 'fixtures/cordis-fiber-state-runtime.ts')
  const { stderr } = await execFileAsync(process.execPath, ['--import', 'tsx/esm', fixture], {
    cwd: resolve(import.meta.dirname, '..'),
    env: { ...process.env, TSX_TSCONFIG_PATH: resolve(import.meta.dirname, 'fixtures/tsconfig.built-cordis.json') },
    timeout: 30000,
  })
  expect(stderr).toBe('')
})
