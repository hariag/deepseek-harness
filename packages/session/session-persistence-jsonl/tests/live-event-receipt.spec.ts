import { Context } from '@deepseek-ai/cordis'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it, vi } from 'vitest'

it('confirms only an event observed by the current live writer', async () => {
  const root=await mkdtemp(join(tmpdir(),'dsh-e8-live-receipt-'))
  const ctx=new Context()
  try {
    await ctx.plugin(SessionStore)
    await ctx.plugin(Jsonl,{ root, compression:'none' })
    const id=SessionId('receipt-session'),session=ctx.sessions.create(id)
    expect(()=>ctx.sessionPersistence.beginLiveEventReceipt(id,'turn/start')).toThrow(/no live writer/)
    const writer=await ctx.sessionPersistence.create(session.header)
    const before=session.append('turn/start',{ turn:1 })
    const receipt=ctx.sessionPersistence.beginLiveEventReceipt(id,'turn/start')
    await expect(receipt.confirm(before)).rejects.toThrow(/not observed/)
    session.append('turn/end',{ turn:1, reason:{ kind:'completed' } })
    const event=session.append('turn/start',{ turn:2 })
    const changed={ ...event, data:{ turn:3 } }
    await expect(receipt.confirm(changed)).rejects.toThrow(/not observed/)
    await receipt.confirm(event)
    const reader=await ctx.sessionPersistence.open(id,'read')
    try {expect((await reader.read(event.seq,1)).events).toEqual([event])} finally {await reader.close()}
    receipt.dispose()
    await expect(receipt.confirm(event)).rejects.toThrow(/closed/)
    const after=ctx.sessionPersistence.beginLiveEventReceipt(id,'turn/end')
    const ended=session.append('turn/end',{ turn:2, reason:{ kind:'completed' } })
    await writer.close()
    await expect(after.confirm(ended)).rejects.toThrow(/closed/)
    after.dispose()
  } finally {await ctx.fiber.dispose();await rm(root,{ recursive:true, force:true })}
})

it('never confirms on a failed flush and can retry the same observed event', async () => {
  const root=await mkdtemp(join(tmpdir(),'dsh-e8-live-receipt-retry-'))
  const ctx=new Context()
  try {
    await ctx.plugin(SessionStore)
    await ctx.plugin(Jsonl,{ root, compression:'none' })
    const id=SessionId('receipt-retry'),session=ctx.sessions.create(id)
    const writer=await ctx.sessionPersistence.create(session.header)
    const receipt=ctx.sessionPersistence.beginLiveEventReceipt(id,'turn/end')
    session.append('turn/start',{ turn:1 })
    const event=session.append('turn/end',{ turn:1, reason:{ kind:'completed' } })
    const original=writer.flush.bind(writer)
    vi.spyOn(writer,'flush').mockRejectedValueOnce(new Error('injected flush failure'))
    await expect(receipt.confirm(event)).rejects.toThrow('injected flush failure')
    await receipt.confirm(event)
    await original()
    receipt.dispose()
  } finally {await ctx.fiber.dispose();await rm(root,{ recursive:true, force:true })}
})

it('refuses confirmation if the receipt is disposed while a flush is pending', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-e8-live-receipt-dispose-'))
  const ctx = new Context()
  try {
    await ctx.plugin(SessionStore)
    await ctx.plugin(Jsonl, { root, compression: 'none' })
    const id = SessionId('receipt-dispose'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    const receipt = ctx.sessionPersistence.beginLiveEventReceipt(id, 'turn/end')
    session.append('turn/start', { turn: 1 })
    const event = session.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
    let release!: () => void
    let entered!: () => void
    const ready = new Promise<void>((resolve) => { entered = resolve })
    const hold = new Promise<void>((resolve) => { release = resolve })
    const original = writer.flush.bind(writer)
    vi.spyOn(writer, 'flush').mockImplementationOnce(async (options) => { entered(); await hold; await original(options) })
    const confirmation = receipt.confirm(event)
    await ready
    receipt.dispose()
    release()
    await expect(confirmation).rejects.toThrow(/uncertain/)
  } finally { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) }
})

it('flushes the selected live writer and refuses a missing writer', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-live-prefix-'))
  const ctx = new Context()
  try {
    await ctx.plugin(SessionStore)
    await ctx.plugin(Jsonl, { root, compression: 'none' })
    const id = SessionId('live-prefix'), session = ctx.sessions.create(id)
    await expect(ctx.sessionPersistence.flushLiveSession(id)).rejects.toThrow(/no live writer/)
    const writer = await ctx.sessionPersistence.create(session.header)
    const event = session.append('turn/start', { turn: 1 })
    await ctx.sessionPersistence.flushLiveSession(id)
    const reader = await ctx.sessionPersistence.open(id, 'read')
    try { expect((await reader.read(event.seq, 1)).events).toEqual([event]) } finally { await reader.close() }
    await writer.close()
    await expect(ctx.sessionPersistence.flushLiveSession(id)).rejects.toThrow(/no live writer/)
  } finally { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) }
})

it('does not confirm a live writer which closes while the flush is waiting', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-live-prefix-close-'))
  const ctx = new Context()
  try {
    await ctx.plugin(SessionStore)
    await ctx.plugin(Jsonl, { root, compression: 'none' })
    const id = SessionId('live-prefix-close'), session = ctx.sessions.create(id)
    const writer = await ctx.sessionPersistence.create(session.header)
    session.append('turn/start', { turn: 1 })
    let release!: () => void
    let entered!: () => void
    const ready = new Promise<void>(resolve => { entered = resolve })
    const hold = new Promise<void>(resolve => { release = resolve })
    const original = writer.flush.bind(writer)
    vi.spyOn(writer, 'flush').mockImplementationOnce(async options => { entered(); await hold; await original(options) })
    const flushing = ctx.sessionPersistence.flushLiveSession(id)
    await ready
    await writer.close()
    release()
    await expect(flushing).rejects.toThrow()
  } finally { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) }
})
