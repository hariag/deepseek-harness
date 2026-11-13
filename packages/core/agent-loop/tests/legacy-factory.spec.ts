import { Context } from '@deepseek-ai/cordis'
import type { Agent, AgentFactory } from '@deepseek-ai/dsh-agent'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import { SessionId, SessionSeq, type SessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionHandle } from '@deepseek-ai/dsh-session-persistence'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { expect, it, onTestFinished } from 'vitest'
import { generationLogPath } from '../../../session/session-persistence-jsonl/src/format.ts'

async function setup(protectedLog: boolean) {
  const root = await mkdtemp(join(tmpdir(), 'dsh-legacy-factory-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  const id = SessionId('legacy-factory'), session = ctx.sessions.create(id)
  const writer = await ctx.sessionPersistence.create(session.header)
  if (protectedLog) session.append('agent/admission/hold', { messageId: 'm', admissionId: 'a' })
  else session.append('turn/start', { turn: 1 })
  await ctx.sessions.flush(session)
  const path = (await ctx.sessionPersistence.stat(id))?.location?.path
  if (path === undefined) throw new Error('missing artifact')
  await writer.close()
  let opened: SessionHandle | undefined
  const factory: AgentFactory = {
    async createAgent() { throw new Error('not used') },
    async resume(ownerCtx, options) {
      const persistence = ownerCtx.get('sessionPersistence')
      if (persistence === undefined) throw new Error('session persistence unavailable')
      const handle = await persistence.open(options.resumeSessionId, 'write')
      opened = handle
      return { agent: {} as Agent, dispose: () => handle.close() }
    },
  }
  ctx.agents.setFactory(factory)
  return { ctx, id, path, opened: () => opened }
}

it('allows a legacy factory to resume an ordinary log using its same exclusive writer', async () => {
  const { ctx, id, opened } = await setup(false)
  const result = await ctx.agents.resume({ resumeSessionId: id })
  try {
    const handle = opened()
    expect(handle).toBeDefined()
    await expect(ctx.sessionPersistence.open(id, 'write')).rejects.toThrow()
    const forbidden = { type: 'agent/admission/hold', seq: SessionSeq(1), time: Date.now(), data: { messageId: 'm', admissionId: 'a' } } as SessionEvent
    await expect(handle!.append([forbidden])).rejects.toThrow('required admission')
    expect((await handle!.read()).events.map(event => event.type)).toEqual(['turn/start'])
  } finally { await result.dispose() }
})

it('rejects protected history before returning the writer or changing the artifact', async () => {
  const { ctx, id, path, opened } = await setup(true)
  const before = await readFile(path)
  await expect(ctx.agents.resume({ resumeSessionId: id })).rejects.toThrow('cannot resume required admission')
  expect(opened()).toBeUndefined()
  expect(await readFile(path)).toEqual(before)
})

it('rejects legacy persistence without scoped validation before opening a writer', async () => {
  const { ctx, id, path, opened } = await setup(false)
  const before = await readFile(path)
  Reflect.set(ctx.sessionPersistence, 'withWriteOpenValidation', undefined)
  await expect(ctx.agents.resume({ resumeSessionId: id })).rejects.toThrow('requires a persistence backend with scoped write-open validation')
  expect(opened()).toBeUndefined()
  expect(await readFile(path)).toEqual(before)
})

it('rejects and disposes a legacy result that never obtained a validated writer', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-legacy-no-open-')), ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  let disposed = false
  ctx.agents.setFactory({
    async createAgent() { throw new Error('not used') },
    async resume() { return { agent: {} as Agent, async dispose() { disposed = true } } },
  })
  await expect(ctx.agents.resume({ resumeSessionId: SessionId('never-opened') })).rejects.toThrow('did not validate a write handle')
  expect(disposed).toBe(true)
})

it('rejects an unawaited write open after its validation scope has ended and releases the lease', async () => {
  const { ctx, id } = await setup(false)
  const persistence = ctx.sessionPersistence
  const entered = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const result = await persistence.withWriteOpenValidation(async () => {
    entered.resolve(undefined)
    await release.promise
  }, async () => {
    const pending = persistence.open(id, 'write')
    const settled = pending.then(handle => ({ handle }), (error: unknown) => ({ error }))
    await entered.promise
    return { settled }
  })
  // The operation intentionally returns a wrapper, not the still-pending open.
  expect(result.validated).toBe(false)
  release.resolve(undefined)
  const settled = await result.value.settled
  expect('error' in settled).toBe(true)
  if ('error' in settled) {
    expect(settled.error).toBeInstanceOf(Error)
    if (settled.error instanceof Error) expect(settled.error.message).toContain('scope ended')
  }
  const next = await persistence.open(id, 'write')
  await next.close()
})

it('does not return a writer when validation scope ends while historical migration is publishing', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-legacy-migration-scope-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  const id = SessionId('migration-scope')
  const path = generationLogPath(root, undefined, id, 0, 'none')
  const source = Buffer.from(`${JSON.stringify({ type: 'session', version: 0, id, createdAt: 1000, delegationDepth: 0 })}\n`)
  await mkdir(dirname(path), { recursive: true })
  await writeFile(path, source)
  const persistence = ctx.sessionPersistence
  const prototype = Reflect.getPrototypeOf(persistence)
  if (prototype === null) throw new Error('missing persistence prototype')
  const original: unknown = Reflect.get(prototype, 'publishStoredMigration')
  if (typeof original !== 'function') throw new Error('missing migration publish method')
  const entered = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  Reflect.set(prototype, 'publishStoredMigration', async function(this: object, ...args: unknown[]) {
    entered.resolve(undefined)
    await release.promise
    const result: unknown = await Reflect.apply(original, this, args)
    return result
  })
  onTestFinished(() => { Reflect.set(prototype, 'publishStoredMigration', original); release.resolve(undefined) })
  const result = await persistence.withWriteOpenValidation(() => {}, async () => {
    const settled = persistence.open(id, 'write').then(handle => ({ handle }), (error: unknown) => ({ error }))
    await entered.promise
    return { settled }
  })
  expect(result.validated).toBe(false)
  release.resolve(undefined)
  const settled = await result.value.settled
  expect('error' in settled).toBe(true)
  if ('error' in settled) {
    expect(settled.error).toBeInstanceOf(Error)
    if (settled.error instanceof Error) expect(settled.error.message).toContain('scope ended')
  }
  expect(await readFile(path)).toEqual(source)
  Reflect.set(prototype, 'publishStoredMigration', original)
  const next = await persistence.open(id, 'write')
  await next.close()
})
