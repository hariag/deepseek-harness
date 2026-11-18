/** Per-session catalog demand, invalidation, generation and cancellation behavior. */
import { describe, expect, it } from 'vitest'
import type { SessionId } from '@deepseek-ai/dsh-api-remotes/client'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { CommandDefinitionId } from '@deepseek-ai/dsh-commands/types'
import type { CommandDescriptor } from '../src/client/directory.ts'
import { CommandDirectory } from '../src/client/directory.ts'

const sid = (k: string): SessionId => k as SessionId
const S1 = sid('s1')
const S2 = sid('s2')

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
  return { promise, resolve, reject }
}

const CMDS: CommandDescriptor[] = [
  { name: 'plan', description: 'plan mode' },
  { name: 'goal', description: 'set goal', input: { hint: 'goal text' } },
]

const S2_CMDS: CommandDescriptor[] = [
  ...CMDS,
  { name: 'attach', description: 'attach a file', input: { hint: 'path' } },
]

/** Directory over per-key pull queues: each fetch appends a hand-settled deferred. */
function bench() {
  const pulls = new Map<SessionId, Array<ReturnType<typeof deferred<readonly CommandDescriptor[]>>>>()
  const calls: SessionId[] = []
  const dir = new CommandDirectory((key) => {
    calls.push(key)
    const d = deferred<readonly CommandDescriptor[]>()
    const queue = pulls.get(key) ?? []
    queue.push(d)
    pulls.set(key, queue)
    return d.promise
  })
  const pull = (key: SessionId, i: number) => {
    const d = pulls.get(key)?.[i]
    if (d === undefined) throw new Error(`no pull #${i} for ${key}`)
    return d
  }
  return { dir, pull, calls, countOf: (key: SessionId) => pulls.get(key)?.length ?? 0 }
}

describe('status and resolve (per key)', () => {
  it('resolves bilingual aliases by definition identity and retains exact-name priority', async () => {
    const goal = {
      definitionId: brandString<CommandDefinitionId>('@deepseek-ai/dsh-command-goal'),
      name: 'objective',
      description: 'Reworded description.',
    }
    const exact = { name: 'goal', description: 'independent exact-name entry' }
    const { dir, pull } = bench()
    const refreshed = dir.refresh(S1)
    pull(S1, 0).resolve([goal])
    await refreshed
    expect(dir.resolve(S1, '目标')).toEqual(goal)
    expect(dir.resolve(S1, 'goal')).toEqual(goal)
    expect(dir.resolve(S1, '计划')).toBeUndefined()
    expect(dir.resolve(S1, 'unregistered')).toBeUndefined()
    const next = dir.refresh(S1)
    pull(S1, 1).resolve([goal, exact])
    await next
    expect(dir.resolve(S1, 'goal')).toEqual(exact)
    expect(dir.resolve(S1, '目标')).toEqual(goal)
  })

  it('starts cold and resolves nothing', () => {
    const { dir } = bench()
    expect(dir.status(S1)).toBe('cold')
    expect(dir.resolve(S1, 'plan')).toBeUndefined()
  })

  it('serves exact-name lookups once ready, undefined for unknown names', async () => {
    const { dir, pull } = bench()
    const refreshed = dir.refresh(S1)
    expect(dir.status(S1)).toBe('pending')
    pull(S1, 0).resolve(CMDS)
    await refreshed
    expect(dir.status(S1)).toBe('ready')
    expect(dir.resolve(S1, 'goal')).toEqual(CMDS[1])
    expect(dir.resolve(S1, 'nope')).toBeUndefined()
  })

  it('drops the snapshot and records failure on a failed pull', async () => {
    const { dir, pull } = bench()
    const refreshed = dir.refresh(S1)
    pull(S1, 0).reject(new Error('boom'))
    await refreshed
    expect(dir.status(S1)).toBe('failed')
    expect(dir.resolve(S1, 'plan')).toBeUndefined()
  })

  it('keys are isolated: one session catalog landing leaves another cold', async () => {
    const { dir, pull } = bench()
    const refreshed = dir.refresh(S1)
    pull(S1, 0).resolve(CMDS)
    await refreshed
    expect(dir.status(S2)).toBe('cold')
    expect(dir.resolve(S2, 'plan')).toBeUndefined()

    const other = dir.refresh(S2)
    pull(S2, 0).resolve(S2_CMDS)
    await other
    expect(dir.resolve(S2, 'attach')).toBeDefined()
    expect(dir.resolve(S1, 'attach')).toBeUndefined()
  })
})

describe('invalidation and single-flight reads', () => {
  it('does not pull historical sessions during a registry storm', async () => {
    const { dir, pull, calls } = bench()
    const a = dir.refresh(S1)
    const b = dir.refresh(S2)
    pull(S1, 0).resolve(CMDS)
    pull(S2, 0).resolve(S2_CMDS)
    await Promise.all([a, b])

    for (let i = 0; i < 100; i++) dir.invalidateAll()
    expect(calls).toEqual([S1, S2])
    expect(dir.resolve(S1, 'goal')).toBeDefined()
    const reader = dir.ensureReady(S1, new AbortController().signal)
    dir.warm(S1)
    const second = dir.ensureReady(S1, new AbortController().signal)
    expect(calls).toEqual([S1, S2, S1])
    pull(S1, 1).resolve([])
    await expect(reader).resolves.toEqual([])
    await expect(second).resolves.toEqual([])
    expect(dir.resolve(S1, 'goal')).toBeUndefined()
  })

  it('only a new reader after invalidation needs a later request', async () => {
    const { dir, pull, calls } = bench()
    const signal = new AbortController().signal
    const first = dir.ensureReady(S1, signal)
    dir.invalidateAll()
    const second = dir.ensureReady(S1, signal)
    expect(calls).toEqual([S1])
    pull(S1, 0).resolve(CMDS)
    await expect(first).resolves.toEqual(CMDS)
    expect(calls).toEqual([S1, S1])
    dir.invalidateAll()
    pull(S1, 1).resolve(S2_CMDS)
    await expect(second).resolves.toEqual(S2_CMDS)
    expect(calls).toEqual([S1, S1])
  })

  it('reconnect forgets idle sessions without fetching them', async () => {
    const { dir, pull, calls } = bench()
    const a = dir.refresh(S1)
    const b = dir.refresh(S2)
    pull(S1, 0).resolve(CMDS)
    pull(S2, 0).resolve(S2_CMDS)
    await Promise.all([a, b])
    dir.resetConnected()
    expect(dir.status(S1)).toBe('cold')
    expect(dir.resolve(S2, 'attach')).toBeUndefined()
    expect(calls).toEqual([S1, S2])
  })

  it.each(['preset', 'connection'])('a %s reset wakes readers and fences the old in-flight catalog', async (reset) => {
    const { dir, pull, calls } = bench()
    const signal = new AbortController().signal
    const wait = dir.ensureReady(S1, signal)
    if (reset === 'preset') dir.resetSession(S1)
    else dir.resetConnected()
    expect(dir.status(S1)).toBe('cold')
    const second = dir.ensureReady(S1, signal)
    expect(calls).toEqual([S1, S1])
    pull(S1, 1).resolve(S2_CMDS)
    await expect(wait).resolves.toEqual(S2_CMDS)
    await expect(second).resolves.toEqual(S2_CMDS)
    pull(S1, 0).reject(new Error('old generation'))
    await Promise.resolve()
    expect(dir.resolve(S1, 'attach')).toBeDefined()
  })
})

describe('warm', () => {
  it('launches a pull from cold, again after failure, and never over pending/ready', async () => {
    const { dir, pull, countOf } = bench()
    dir.warm(S1)
    expect(countOf(S1)).toBe(1)
    dir.warm(S1) // pending → no second pull
    expect(countOf(S1)).toBe(1)

    pull(S1, 0).reject(new Error('boom'))
    await Promise.resolve()
    await Promise.resolve()
    expect(dir.status(S1)).toBe('failed')
    dir.warm(S1) // failed → retry
    expect(countOf(S1)).toBe(2)

    pull(S1, 1).resolve(CMDS)
    await Promise.resolve()
    await Promise.resolve()
    dir.warm(S1) // ready → no-op
    expect(countOf(S1)).toBe(2)
  })

  it('warms keys independently', () => {
    const { dir, countOf } = bench()
    dir.warm(S2)
    expect(countOf(S2)).toBe(1)
    expect(countOf(S1)).toBe(0)
  })
})

describe('ensureReady (per key)', () => {
  const signal = () => new AbortController().signal

  it('returns the hot snapshot at once when ready', async () => {
    const { dir, pull, countOf } = bench()
    const warm = dir.refresh(S1)
    pull(S1, 0).resolve(CMDS)
    await warm
    await expect(dir.ensureReady(S1, signal())).resolves.toEqual(CMDS)
    expect(countOf(S1)).toBe(1)
  })

  it('launches a pull from cold and resolves on arrival, without touching other keys', async () => {
    const { dir, pull, countOf } = bench()
    const wait = dir.ensureReady(S2, signal())
    expect(dir.status(S2)).toBe('pending')
    pull(S2, 0).resolve(S2_CMDS)
    await expect(wait).resolves.toEqual(S2_CMDS)
    expect(countOf(S1)).toBe(0)
  })

  it('joins a flying pull instead of starting a second one', async () => {
    const { dir, pull, countOf } = bench()
    void dir.refresh(S1)
    const wait = dir.ensureReady(S1, signal())
    expect(countOf(S1)).toBe(1)
    pull(S1, 0).resolve(CMDS)
    await expect(wait).resolves.toEqual(CMDS)
  })

  it('rejects when the awaited pull fails (no silent downgrade)', async () => {
    const { dir, pull } = bench()
    const wait = dir.ensureReady(S1, signal())
    pull(S1, 0).reject(new Error('warmup boom'))
    await expect(wait).rejects.toThrow('command directory warmup failed: warmup boom')
  })

  it('retries from failed state with a fresh pull', async () => {
    const { dir, pull } = bench()
    const first = dir.ensureReady(S1, signal())
    pull(S1, 0).reject(new Error('boom'))
    await expect(first).rejects.toThrow()
    const second = dir.ensureReady(S1, signal())
    pull(S1, 1).resolve(CMDS)
    await expect(second).resolves.toEqual(CMDS)
  })

  it('rejects on abort while waiting', async () => {
    const { dir } = bench()
    const ac = new AbortController()
    const wait = dir.ensureReady(S1, ac.signal)
    ac.abort(new Error('attempt superseded'))
    await expect(wait).rejects.toThrow('attempt superseded')
  })

  it('rejects immediately on an already-aborted signal', async () => {
    const { dir, pull } = bench()
    const warm = dir.refresh(S1)
    pull(S1, 0).reject(new Error('irrelevant'))
    await warm
    const ac = new AbortController()
    ac.abort() // bare abort: the DOMException reason is itself an Error and travels as-is
    await expect(dir.ensureReady(S1, ac.signal)).rejects.toThrow(/aborted/)
  })
})
