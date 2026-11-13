import { Context } from '@deepseek-ai/cordis'
import { mountAgentLoopTestDependencies, mountAgentLoopTestHarness } from '@deepseek-ai/dsh-agent-loop-testkit'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it, onTestFinished } from 'vitest'
import { MockAdapter, textResponse } from './mock-adapter.ts'

async function setup() {
  const root = await mkdtemp(join(tmpdir(), 'dsh-live-event-receipt-'))
  const ctx = new Context()
  onTestFinished(async () => {
    await ctx.fiber.dispose()
    await rm(root, { recursive: true, force: true })
  })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  ctx.llm.registerAdapter(['receipt-test'], new MockAdapter([textResponse('done')]))
  await mountAgentLoopTestHarness(ctx)
  const agent = await ctx.agentLoop.create(SessionId('live-event-receipt'), { provider: 'receipt-test', model: 'mock' }, { cwd: root })
  return { ctx, agent }
}

it('confirms only an exact event observed by the live writer after receipt creation', async () => {
  const { ctx, agent } = await setup()
  const receipt = ctx.sessionPersistence.beginLiveEventReceipt(agent.session.id, 'agent/inbox/spliced')
  onTestFinished(() => { receipt.dispose() })
  const message = createUserMessage({ content: [{ type: 'text', text: 'original' }], source: { kind: 'user' } })
  agent.followup(message)
  const insertion = agent.session.snapshotEvents().find(event => event.type === 'agent/inbox/spliced' && event.data.inserted.some(item => item.id === message.id))
  if (insertion === undefined) throw new Error('missing live insertion')
  await receipt.confirm(insertion)
  const lossy = structuredClone(insertion)
  if (lossy.type !== 'agent/inbox/spliced') throw new Error('unexpected insertion type')
  Reflect.set(lossy.data, 'start', -0)
  await expect(receipt.confirm(lossy)).rejects.toThrow('losslessly JSON')
  Reflect.set(lossy.data, 'start', 0)
  Reflect.set(lossy.data, 'extra', undefined)
  await expect(receipt.confirm(lossy)).rejects.toThrow('losslessly JSON')
  const handle = await ctx.sessionPersistence.open(agent.session.id, 'read')
  try { expect((await handle.read(insertion.seq, 1)).events).toEqual([insertion]) } finally { await handle.close() }
  const newer = ctx.sessionPersistence.beginLiveEventReceipt(agent.session.id, 'agent/inbox/spliced')
  try { await expect(newer.confirm(insertion)).rejects.toThrow('not observed') } finally { newer.dispose() }
  await agent.whenIdle()
})

it('observes generic event types and keeps concurrent receipts independent', async () => {
  const { ctx, agent } = await setup()
  const first = ctx.sessionPersistence.beginLiveEventReceipt(agent.session.id, 'turn/start')
  const second = ctx.sessionPersistence.beginLiveEventReceipt(agent.session.id, 'turn/start')
  const observed = Promise.withResolvers<SessionEvent>()
  ctx.on('session/event', (session, event) => {
    if (session === agent.session && event.type === 'turn/start') observed.resolve(event)
  })
  onTestFinished(() => { first.dispose(); second.dispose() })
  agent.followup(createUserMessage({ content: [{ type: 'text', text: 'go' }], source: { kind: 'user' } }))
  const event = await observed.promise
  await Promise.all([first.confirm(event), second.confirm(event)])
  first.dispose()
  await second.confirm(event)
  await expect(first.confirm(event)).rejects.toThrow('closed')
  await agent.whenIdle()
})

it('refuses a missing writer even when an unrelated session flush listener exists', async () => {
  const { ctx } = await setup()
  const session = ctx.sessions.create(SessionId('unrouted'))
  ctx.on('session/flush', () => {})
  expect(await ctx.sessions.flush(session)).toBe(true)
  expect(() => ctx.sessionPersistence.beginLiveEventReceipt(session.id, 'turn/start')).toThrow('no live writer')
})
