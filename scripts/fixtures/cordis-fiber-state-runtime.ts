import assert from 'node:assert/strict'
import { realpath } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { FiberState } from '@deepseek-ai/cordis'

const actual = await realpath(fileURLToPath(import.meta.resolve('@deepseek-ai/cordis')))
const expected = await realpath(resolve(import.meta.dirname, '../../vendor/cordis/lib/index.js'))
assert.equal(actual, expected, 'the fixture must load the built Cordis artifact from this checkout')
assert.deepEqual(
  [FiberState.PENDING, FiberState.LOADING, FiberState.ACTIVE, FiberState.FAILED, FiberState.DISPOSED, FiberState.UNLOADING],
  [0, 1, 2, 3, 4, 5],
)
await import('../../packages/core/agent/src/index.ts')
await import('../../packages/core/agent-loop/src/index.ts')
