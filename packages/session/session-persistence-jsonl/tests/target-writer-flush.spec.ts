import { Context } from '@deepseek-ai/cordis'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import SessionPersistence, { SessionHandleClosedError } from '@deepseek-ai/dsh-session-persistence'
import { JsonlSessionHandle } from '../src/storage.ts'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it, vi } from 'vitest'

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'dsh-target-writer-'))
  const ctx = new Context()
  await ctx.plugin(SessionStore)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  return { ctx, async close() { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) } }
}

it('fails closed for a persistence backend without target writer support', async () => {
  await expect(SessionPersistence.prototype.flushLiveSession(SessionId('unsupported'))).rejects.toThrow(/unsupported/)
})

it('flushes only the target live writer and its routed prefix', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const a = SessionId('target-a'), b = SessionId('target-b')
    const session = ctx.sessions.create(a), other = ctx.sessions.create(b)
    const writer = await ctx.sessionPersistence.create(session.header)
    const otherWriter = await ctx.sessionPersistence.create(other.header)
    const otherFlush = vi.spyOn(otherWriter, 'flush')
    const event = session.append('turn/start', { turn: 1 })
    other.append('turn/start', { turn: 1 })
    const through = Number(session.seq) - 1, identity = ctx.sessions.get(a)
    await ctx.sessionPersistence.flushLiveSession(a)
    expect(ctx.sessions.get(a)).toBe(identity)
    expect(Number(session.seq) - 1).toBe(through)
    expect(otherFlush).not.toHaveBeenCalled()
    const reader = await ctx.sessionPersistence.open(a, 'read')
    try { expect((await reader.read(event.seq, 1)).events).toEqual([event]) } finally { await reader.close() }
    await writer.close()
    await expect(ctx.sessionPersistence.flushLiveSession(a)).rejects.toThrow(/no live writer/)
  } finally { await f.close() }
})

it('refuses a missing writer and propagates its flush failure', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-missing'), session = ctx.sessions.create(id)
    await expect(ctx.sessionPersistence.flushLiveSession(id)).rejects.toThrow(/no live writer/)
    const writer = await ctx.sessionPersistence.create(session.header)
    session.append('turn/start', { turn: 1 })
    const failure = new Error('injected target flush failure')
    vi.spyOn(writer, 'flush').mockRejectedValueOnce(failure)
    await expect(ctx.sessionPersistence.flushLiveSession(id)).rejects.toBe(failure)
    await expect(ctx.sessionPersistence.flushLiveSession(id)).resolves.toBeUndefined()
  } finally { await f.close() }
})

it('refuses to confirm when the target writer closes or is replaced while waiting', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-replaced'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    session.append('turn/start', { turn: 1 })
    let release!: () => void, entered!: () => void
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    // Model a flush that has already succeeded but whose resolution is held.
    // Only the post-flush writer identity check can reject its replacement.
    vi.spyOn(writer, 'flush').mockImplementationOnce(async () => { entered(); await hold })
    const barrier = ctx.sessionPersistence.flushLiveSession(id)
    await ready
    await writer.close()
    const replacement = await ctx.sessionPersistence.open(id, 'write')
    release()
    await expect(barrier).rejects.toThrow(/writer changed/)
    await replacement.close()
  } finally { await f.close() }
})

it('refuses a writer that starts closing before the barrier confirms', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-closing'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    if (!(writer instanceof JsonlSessionHandle)) throw new Error('Expected JSONL writer')
    session.append('turn/start', { turn: 1 })
    await ctx.sessionPersistence.flushLiveSession(id)
    let release!: () => void, entered!: () => void, closing: Promise<void> | undefined
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    const drain = writer.drainLive.bind(writer), flush = writer.flush.bind(writer)
    vi.spyOn(writer, 'drainLive').mockImplementationOnce(drain).mockImplementationOnce(async () => { entered(); await hold; await drain() })
    vi.spyOn(writer, 'flush').mockImplementationOnce(async options => {
      await flush(options)
      closing = writer.close()
      await ready
    })
    try { await expect(ctx.sessionPersistence.flushLiveSession(id)).rejects.toBeInstanceOf(SessionHandleClosedError) }
    finally { release(); await closing }
  } finally { await f.close() }
})

it('rejects a writer already closing without waiting for its blocked drain', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-already-closing'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    if (!(writer instanceof JsonlSessionHandle)) throw new Error('Expected JSONL writer')
    let release!: () => void, entered!: () => void
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    const drain = writer.drainLive.bind(writer)
    vi.spyOn(writer, 'drainLive').mockImplementationOnce(async () => { entered(); await hold; await drain() })
    const closing = writer.close()
    try {
      await ready
      const attempt = ctx.sessionPersistence.flushLiveSession(id)
      const result = await Promise.race([
        attempt.then(() => 'resolved', error => error),
        new Promise<'waiting'>(resolve => setImmediate(() => resolve('waiting'))),
      ])
      expect(result).toBeInstanceOf(SessionHandleClosedError)
    } finally { release(); await closing }
  } finally { await f.close() }
})

it('refuses a replaced live Session during a fixed-prefix read', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-session-identity')
    const originalSession = ctx.sessions.prepare(id)
    const detach = ctx.sessions.enter(originalSession); ctx.sessions.announce(originalSession)
    const writer = await ctx.sessionPersistence.create(originalSession.header)
    originalSession.append('turn/start', { turn: 1 })
    const fixedSession = ctx.sessions.get(id), through = Number(originalSession.seq) - 1
    let release!: () => void, entered!: () => void
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    const flush = writer.flush.bind(writer)
    vi.spyOn(writer, 'flush').mockImplementationOnce(async options => { entered(); await hold; await flush(options) })
    const readFixedPrefix = async () => {
      await ctx.sessionPersistence.flushLiveSession(id)
      if (ctx.sessions.get(id) !== fixedSession || Number(originalSession.seq) - 1 !== through) throw new Error('live Session changed')
    }
    const barrier = readFixedPrefix()
    await ready
    detach()
    const replacement = ctx.sessions.prepare(id), detachReplacement = ctx.sessions.enter(replacement)
    ctx.sessions.announce(replacement)
    release()
    await expect(barrier).rejects.toThrow()
    detachReplacement()
  } finally { await f.close() }
})

it('rejects a fixed-prefix read if the live sequence advances while flushing', async () => {
  const f = await fixture(), { ctx } = f
  try {
    const id = SessionId('target-sequence-change'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    session.append('turn/start', { turn: 1 })
    const fixedSession = ctx.sessions.get(id), through = Number(session.seq) - 1
    let release!: () => void, entered!: () => void
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    const flush = writer.flush.bind(writer)
    vi.spyOn(writer, 'flush').mockImplementationOnce(async options => { entered(); await hold; await flush(options) })
    const fixed = async () => {
      await ctx.sessionPersistence.flushLiveSession(id)
      if (ctx.sessions.get(id) !== fixedSession || Number(session.seq) - 1 !== through) throw new Error('live Session changed')
    }
    const barrier = fixed()
    await ready
    session.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
    release()
    await expect(barrier).rejects.toThrow('live Session changed')
  } finally { await f.close() }
})
