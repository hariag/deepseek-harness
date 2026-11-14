import { Context } from '@deepseek-ai/cordis'
import { mountAgentLoopTestDependencies, mountAgentLoopTestHarness } from '@deepseek-ai/dsh-agent-loop-testkit'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import type { Agent, GuardedClaimCoordinator, GuardedClaimTurn } from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { Session, SessionId, SessionLogOffset, SessionSeq } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it, onTestFinished, vi } from 'vitest'
import { z } from 'zod'
import { MockAdapter, textResponse } from './mock-adapter.ts'
import AgentLoop from '../src/index.ts'

async function setup() {
  const root = await mkdtemp(join(tmpdir(), 'dsh-guarded-provider-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  const adapter = new MockAdapter([textResponse('done'), textResponse('done'), textResponse('done')])
  ctx.llm.registerAdapter(['guarded-provider'], adapter)
  await mountAgentLoopTestHarness(ctx)
  return { ctx, adapter }
}

const coordinator: GuardedClaimCoordinator = (_session, _inputs, claim) => { claim(); return 'committed' }
const options = { provider: 'guarded-provider', model: 'mock' }
function trackedCoordinator() {
  const starts = new WeakMap<Session, { input: Parameters<NonNullable<GuardedClaimCoordinator['beforeTurnStart']>>[1], turn: Readonly<GuardedClaimTurn> }>()
  const tracked: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: (session: Session, input: Parameters<NonNullable<GuardedClaimCoordinator['beforeTurnStart']>>[1], turn: Readonly<GuardedClaimTurn>) => { starts.set(session, { input, turn }) },
      cancelTurnStart: (session: Session) => { starts.delete(session) } },
  )
  return { starts, tracked }
}

it('registers one guarded start before its exact Session event', async () => {
  const { ctx } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-reservation'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded start unavailable')
  const message = createUserMessage({ content: [{ type:'text', text:'task' }], source: { kind:'user' } })
  let seen = false, ordinaryStarts = 0
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session || event.type !== 'turn/start') return
    const start = starts.get(agent.session)
    if (start?.turn.turnStartSeq !== event.seq) { ordinaryStarts += 1; return }
    expect(start.input.message.id).toBe(message.id)
    expect(start.input.admissionId).toBe('reserved-start')
    seen = true
  })
  await agent.guardedSend(message, 'next-turn', 'reserved-start')
  await agent.whenIdle()
  expect(seen).toBe(true)
  const start = agent.session.snapshotEvents().find(event => event.type === 'turn/start')
  expect(start).toBeDefined()
  agent.followup(createUserMessage({ content:[{ type:'text', text:'ordinary' }], source:{ kind:'user' } }))
  await agent.whenIdle()
  expect(ordinaryStarts).toBe(1)
  dispose()
  await handle.dispose()
})

it('illustrates consumer deduplication of two observations by Session and sequence', async () => {
  const { ctx } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-republish'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded start unavailable')
  let republished = false, observations = 0, effects = 0
  const seen = new WeakMap<Session, Set<number>>()
  const observe = (session: Session, seq: number): void => {
    if (session !== agent.session || starts.get(agent.session)?.turn.turnStartSeq !== seq) return
    observations += 1
    const seqs = seen.get(session) ?? new Set<number>()
    if (seqs.has(seq)) return
    seqs.add(seq); seen.set(session, seqs); effects += 1
  }
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session || event.type !== 'turn/start' || republished) return
    republished = true
    observe(session, event.seq)
    observe(session, event.seq)
  })
  await agent.guardedSend(createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } }), 'next-turn', 'republished-start')
  await agent.whenIdle()
  expect(republished).toBe(true)
  expect(observations).toBe(2)
  expect(effects).toBe(1)
  await ctx.sessionPersistence.flushLiveSession(agent.id)
  const reader = await ctx.sessionPersistence.open(agent.id, 'read')
  try {
    const { events } = await reader.read()
    expect(events.every((event, index) => event.seq === index)).toBe(true)
    expect(events.filter(event => event.type === 'turn/start')).toHaveLength(1)
  } finally { await reader.close() }
  dispose()
  await handle.dispose()
})

it('does not register an ordinary start as a guarded start', async () => {
  const { ctx } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-fabricated'), agentOptions: options })
  const { agent } = handle
  let observed: unknown
  ctx.on('session/event', (session, event) => {
    if (session === agent.session && event.type === 'turn/start') observed = starts.get(agent.session)
  })
  agent.session.append('turn/start', { turn:1 })
  expect(observed).toBeUndefined()
  agent.session.append('turn/end', { turn:1, reason:{ kind:'completed' } })
  dispose()
  await handle.dispose()
})

it('cleans up a failed provisional registration without starting or claiming a turn', async () => {
  const { ctx, adapter } = await setup()
  const cleaned = Promise.withResolvers<void>()
  let registered = false
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: () => { registered = true; throw new Error('host registration failed') },
      cancelTurnStart: () => { registered = false; cleaned.resolve() } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-registration-failure'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded send unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(input, 'next-turn', 'registration-failure')
  await cleaned.promise
  expect(registered).toBe(false)
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  dispose()
  await handle.dispose()
})

it('prevents Session append from reentering provisional registration', async () => {
  const { ctx, adapter } = await setup()
  const cleaned = Promise.withResolvers<void>()
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: (session: Session, _input: unknown, boundary: Readonly<GuardedClaimTurn>) => { session.append('turn/start', { turn:boundary.turn }) },
      cancelTurnStart: () => { cleaned.resolve() } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-reentrant'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded send unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(input, 'next-turn', 'reentrant-registration')
  await cleaned.promise
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  dispose()
  await handle.dispose()
})

it('rejects replacing a coordinator with the same function during provisional registration', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-generation'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined || agent.setGuardedClaimCoordinator === undefined) throw new Error('guarded send unavailable')
  const cleaned = Promise.withResolvers<void>()
  let release = () => {}
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: () => { release(); release = agent.setGuardedClaimCoordinator!(authority) },
      cancelTurnStart: () => { cleaned.resolve() } },
  )
  release = agent.setGuardedClaimCoordinator(authority)
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }); release() })
  await agent.guardedSend(input, 'next-turn', 'changed-generation')
  await cleaned.promise
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  release()
  await handle.dispose()
})

it('parks the input if the same coordinator is reinstalled after start but before claim', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-generation-after'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined || agent.setGuardedClaimCoordinator === undefined) throw new Error('guarded send unavailable')
  const { starts, tracked } = trackedCoordinator()
  let release = agent.setGuardedClaimCoordinator(tracked)
  const ended = Promise.withResolvers<void>()
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session) return
    if (event.type === 'turn/start' && starts.get(agent.session)?.turn.turnStartSeq === event.seq) {
      queueMicrotask(() => { release(); release = agent.setGuardedClaimCoordinator!(tracked) })
    }
    if (event.type === 'turn/end' && event.data.reason.kind === 'error') ended.resolve()
  })
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }); release() })
  await agent.guardedSend(input, 'next-turn', 'changed-generation-after')
  await ended.promise
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(adapter.requests).toHaveLength(0)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  release()
  await handle.dispose()
})

it('rejects an unpaired start callback before installing a coordinator', async () => {
  const { ctx } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-unpaired'), agentOptions: options })
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: () => {} },
  )
  expect(() => handle.agent.setGuardedClaimCoordinator!(authority)).toThrow(/callbacks must be installed together/)
  await handle.dispose()
})

it('does not start or claim when a provisional callback returns a Promise', async () => {
  const { ctx, adapter } = await setup()
  const cleaned = Promise.withResolvers<void>()
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: async () => {}, cancelTurnStart: () => { cleaned.resolve() } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-async-callback'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded send unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(input, 'next-turn', 'async-registration')
  await cleaned.promise
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  dispose()
  await handle.dispose()
})

it('fences the Agent if a provisional callback returns a non-Promise thenable', async () => {
  const { ctx, adapter } = await setup()
  const cleaned = Promise.withResolvers<void>()
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeTurnStart: () => ({ then: () => {} }), cancelTurnStart: () => { cleaned.resolve() } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-thenable'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded send unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  await agent.guardedSend(input, 'next-turn', 'thenable-registration')
  await cleaned.promise
  await agent.whenIdle()
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(adapter.requests).toHaveLength(0)
  dispose()
  await handle.dispose()
})

it('keeps a provisional route unresolved when pre-step rejects after guarded claim', async () => {
  const { ctx, adapter } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-after-claim-reject'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded send unavailable')
  ctx.on('agent/pre-step', async (_payload, next) => {
    await next()
    return { kind:'reject' }
  })
  const input = createUserMessage({ content:[{ type:'text', text:'task' }], source:{ kind:'user' } })
  await agent.guardedSend(input, 'next-turn', 'post-claim-reject')
  await agent.whenIdle()
  expect(starts.get(agent.session)?.input.message.id).toBe(input.id)
  expect(agent.inbox.nextTurn).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === input.id)).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  dispose()
  await handle.dispose()
})

it('preserves a maintenance wake when cancel retains its queued input', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-maintenance-wake'), agentOptions: options })
  const { agent } = handle
  const release = Promise.withResolvers<void>()
  onTestFinished(() => { release.resolve() })
  const maintenance = agent.runMaintenance(async () => { await release.promise })
  agent.followup(createUserMessage({ content:[{ type:'text', text:'ordinary' }], source:{ kind:'user' } }))
  agent.cancel({ kind:'user' }, { keepInbox:true })
  release.resolve()
  await maintenance
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  expect(agent.inbox.nextTurn).toHaveLength(0)
  await handle.dispose()
})

it('leaves all inputs queued if the guarded start candidate changes before claim', async () => {
  const { ctx, adapter } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-changed'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded start unavailable')
  const guarded = createUserMessage({ content:[{ type:'text', text:'guarded' }], source:{ kind:'user' } })
  const added = createUserMessage({ content:[{ type:'text', text:'new ordinary input' }], source:{ kind:'user' } })
  const direct = createUserMessage({ content:[{ type:'text', text:'direct inbox input' }], source:{ kind:'user' } })
  let injected = false
  const entered = Promise.withResolvers<void>()
  const directDone = Promise.withResolvers<void>()
  const parked = Promise.withResolvers<void>()
  const admissions = Reflect.get(agent, 'admissions') as { waitForChange(signal:AbortSignal):Promise<void> }
  const waitForChange = admissions.waitForChange.bind(admissions)
  const waitSpy = vi.spyOn(admissions, 'waitForChange').mockImplementation((signal) => { parked.resolve(); return waitForChange(signal) })
  onTestFinished(() => { waitSpy.mockRestore() })
  let ordinaryTurn: number | undefined
  let directTurn: number | undefined
  ctx.on('agent/pre-step', async (payload, next) => {
    const decision = await next()
    if (decision.kind === 'enter' && decision.messages.some(message => message.id === added.id)) ordinaryTurn = payload.turn
    if (decision.kind === 'enter' && decision.messages.some(message => message.id === direct.id)) directTurn = payload.turn
    return decision
  })
  ctx.on('session/event', (session, event) => {
    if (session === agent.session && event.type === 'turn/end' && event.data.turn === ordinaryTurn && event.data.reason.kind === 'completed') entered.resolve()
    if (session === agent.session && event.type === 'turn/end' && event.data.turn === directTurn && event.data.reason.kind === 'completed') directDone.resolve()
    if (session !== agent.session || event.type !== 'turn/start' || starts.get(agent.session)?.turn.turnStartSeq !== event.seq) return
    queueMicrotask(() => { injected = true; agent.steer(added) })
  })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(guarded, 'next-turn', 'changed-start')
  await entered.promise
  expect(injected).toBe(true)
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([guarded.id])
  expect(agent.inbox.nextStep).toHaveLength(0)
  await parked.promise
  agent.inbox.prepend('next-step', direct)
  await directDone.promise
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([guarded.id])
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(2)
  dispose()
  await handle.dispose()
})

it('does not auto-wake retained inputs after disposal at the failed turn ending', async () => {
  const { ctx, adapter } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-dispose'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded start unavailable')
  const guarded = createUserMessage({ content:[{ type:'text', text:'guarded' }], source:{ kind:'user' } })
  const added = createUserMessage({ content:[{ type:'text', text:'ordinary' }], source:{ kind:'user' } })
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session) return
    if (event.type === 'turn/start' && starts.get(agent.session)?.turn.turnStartSeq === event.seq) queueMicrotask(() => agent.steer(added))
    if (event.type === 'turn/end' && event.data.reason.kind === 'error') agent.cancel({ kind:'disposed' }, { keepInbox:true })
  })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(guarded, 'next-turn', 'dispose-at-end')
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([guarded.id])
  expect(agent.inbox.nextStep.map(item => item.id)).toEqual([added.id])
  dispose()
  await handle.dispose()
})

it('does not mutate admission state when synchronous cancellation cannot append its event', async () => {
  const { ctx } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-cancel'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded start unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'guarded' }], source:{ kind:'user' } })
  let failed = false
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session || event.type !== 'turn/start' || starts.get(agent.session)?.turn.turnStartSeq !== event.seq) return
    try { agent.cancel({ kind:'user' }) } catch { failed = true }
  })
  await agent.guardedSend(input, 'next-turn', 'cancel-at-start')
  await agent.whenIdle()
  expect(failed).toBe(true)
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'agent/admission/cancel' && event.data.messageId === input.id)).toBe(false)
  await ctx.sessionPersistence.flushLiveSession(agent.id)
  expect(await agent.inspectGuardedAdmission!(agent.id, input, 'next-turn', 'cancel-at-start'))
    .toEqual({ decision:'unknown', observed:{ pending:true, admission:'released' } })
  dispose()
  await handle.dispose()
})

it('parks the input when guarded cancellation is attempted during start publication', async () => {
  const { ctx } = await setup()
  const { starts, tracked } = trackedCoordinator()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => tracked)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-start-async-cancel'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined || agent.cancelGuardedAdmission === undefined) throw new Error('guarded start unavailable')
  const input = createUserMessage({ content:[{ type:'text', text:'guarded' }], source:{ kind:'user' } })
  let cancellation: Promise<void> | undefined
  const started = Promise.withResolvers<void>()
  ctx.on('session/event', (session, event) => {
    if (session !== agent.session || event.type !== 'turn/start' || starts.get(agent.session)?.turn.turnStartSeq !== event.seq) return
    cancellation = agent.cancelGuardedAdmission!(agent.id, input, 'next-turn', 'cancel-during-start')
    void cancellation.catch(() => {})
    started.resolve()
  })
  onTestFinished(() => { agent.cancel({ kind:'disposed' }, { keepInbox:true }) })
  await agent.guardedSend(input, 'next-turn', 'cancel-during-start')
  await started.promise
  expect(cancellation).toBeDefined()
  await expect(cancellation).rejects.toThrow()
  expect(agent.inbox.nextTurn.map(item => item.id)).toEqual([input.id])
  expect(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === input.id)).toBe(false)
  agent.cancel({ kind:'disposed' }, { keepInbox:true })
  await agent.whenIdle()
  dispose()
  await handle.dispose()
})

it('binds each guarded claim to its actual turn-start event without changing the pre-step payload', async () => {
  const { ctx } = await setup()
  const seen: Array<Readonly<GuardedClaimTurn>> = []
  const preStepKeys: string[][] = []
  ctx.on('agent/pre-step', async (payload, next) => {
    preStepKeys.push(Object.keys(payload).sort())
    return next()
  })
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => (_sessionId, _inputs, claim, turn) => {
    if (turn === undefined) throw new Error('guarded turn identity unavailable')
    expect(Object.isFrozen(turn)).toBe(true)
    seen.push(turn)
    claim()
    return 'committed'
  })
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-turn-identity'), agentOptions: options })
  expect(handle.agent.guardedClaimTurnVersion).toBe(1)
  if (handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  for (const admissionId of ['turn-proof-1', 'turn-proof-2']) {
    await handle.agent.guardedSend(createUserMessage({ content: [{ type: 'text', text: admissionId }], source: { kind: 'user' } }), 'next-turn', admissionId)
    await handle.agent.whenIdle()
  }
  const starts = handle.agent.session.snapshotEvents().filter(event => event.type === 'turn/start')
  expect(seen).toEqual(starts.map(event => ({ turn: event.data.turn, turnStartSeq: event.seq })))
  expect(preStepKeys.every(keys => !keys.includes('turnStartSeq'))).toBe(true)
  dispose()
  await handle.dispose()
})

it('keeps later normal steer and queued work out of a guarded turn and runs both afterwards', async () => {
  const { ctx } = await setup()
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-redo-isolation'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const target = createUserMessage({ content:[{ type:'text', text:'specific interrupted work' }], source:{ kind:'user' } })
  const steer = createUserMessage({ content:[{ type:'text', text:'later steering input' }], source:{ kind:'user' } })
  const queued = createUserMessage({ content:[{ type:'text', text:'later queued input' }], source:{ kind:'user' } })
  let injected = false
  ctx.on('agent/pre-step', async (_payload, next) => {
    const decision = await next()
    if (decision.kind === 'enter' && !injected && decision.messages.some(message => message.id === target.id)) {
      injected = true
      agent.steer(steer)
      agent.followup(queued)
    }
    return decision
  })
  await agent.guardedSend(target,'next-turn','redo-isolation')
  await agent.whenIdle()
  expect(injected).toBe(true)
  const byTurn = new Map<number,string[]>()
  let turn = -1
  for (const event of agent.session.snapshotEvents()) {
    if (event.type === 'turn/start') turn = event.data.turn
    if (event.type === 'user/message' && [target.id,steer.id,queued.id].includes(event.data.id)) {
      byTurn.set(turn,[...(byTurn.get(turn)??[]),event.data.id])
    }
  }
  const batches=[...byTurn.values()]
  expect(batches).toEqual([[target.id],[steer.id,queued.id]])
  expect(agent.inbox.nextStep).toHaveLength(0)
  expect(agent.inbox.nextTurn).toHaveLength(0)
  dispose()
  await handle.dispose()
})

it('keeps a guarded input and later normal work in their inbox when authority refuses before claim', async () => {
  const { ctx } = await setup()
  let calls = 0
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => () => { calls += 1; throw new Error('authority refused') })
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-refusal-keeps-queue'), agentOptions: options })
  const { agent } = handle
  if (agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  let release: () => void = () => {}
  const gate = new Promise<void>(resolve => { release = resolve })
  const maintenance = agent.runMaintenance(async () => { await gate })
  const target = createUserMessage({ content:[{ type:'text',text:'specific interrupted work' }],source:{kind:'user'} })
  const queued = createUserMessage({ content:[{ type:'text',text:'later normal work' }],source:{kind:'user'} })
  try {
    await agent.guardedSend(target,'next-turn','redo-refused')
    agent.followup(queued)
    release()
    await maintenance
    await agent.whenIdle()
    expect(calls).toBeGreaterThan(0)
    expect(agent.inbox.nextTurn.map(message=>message.id)).toEqual([target.id,queued.id])
    expect(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === target.id)).toBe(false)
  } finally {
    release()
    dispose()
    await handle.dispose()
  }
})

it('rejects malformed admission history before writing interrupted-turn repair to JSONL', async () => {
  const { ctx, adapter } = await setup()
  const id = SessionId('malformed-admission-before-repair')
  const session = Session.create(id)
  session.append('turn/start', { turn: 1 })
  session.append('agent/admission/release', { messageId: 'missing', admissionId: 'missing' })
  const writer = await ctx.sessionPersistence.create(session.header)
  await writer.append(session.snapshotEvents())
  await writer.close()
  const path = (await ctx.sessionPersistence.stat(id))?.location?.path
  if (path === undefined) throw new Error('missing JSONL artifact')
  const before = await readFile(path)
  await expect(ctx.agents.resume({ resumeSessionId: id, agentOptions: options })).rejects.toThrow('admission')
  expect(await readFile(path)).toEqual(before)
  expect(adapter.requests).toHaveLength(0)
})

it.each([
  ['messageId', 'm', '123'], ['messageId', 'm', 'null'], ['messageId', 'm', '{}'],
  ['admissionId', 'a', 'null'], ['admissionId', 'a', '{}'],
])('rejects non-string historical %s before repair writes', async (field, original, invalid) => {
  const { ctx, adapter } = await setup()
  const id = SessionId('non-string-admission')
  const session = Session.create(id)
  session.append('turn/start', { turn: 1 })
  session.append('agent/admission/hold', { messageId: 'm', admissionId: 'a' })
  const writer = await ctx.sessionPersistence.create(session.header)
  await writer.append(session.snapshotEvents())
  await writer.close()
  const path = (await ctx.sessionPersistence.stat(id))?.location?.path
  if (path === undefined) throw new Error('missing JSONL artifact')
  const contents = await readFile(path, 'utf8')
  const damaged = contents.replace(`"${field}":"${original}"`, `"${field}":${invalid}`)
  if (damaged === contents) throw new Error('fixture was not altered')
  await writeFile(path, damaged)
  const before = await readFile(path)
  await expect(ctx.agents.resume({ resumeSessionId: id, agentOptions: options })).rejects.toThrow('invalid admission identity')
  expect(await readFile(path)).toEqual(before)
  expect(adapter.requests).toHaveLength(0)
})

it('repairs a valid interrupted turn without a provider but keeps its guarded input pending', async () => {
  const { ctx, adapter } = await setup()
  const id = SessionId('valid-interrupted-no-provider')
  const session = Session.create(id)
  const input = createUserMessage({ content: [{ type: 'text', text: 'still pending' }], source: { kind: 'user' } })
  session.append('agent/admission/hold', { messageId: input.id, admissionId: 'no-provider' })
  session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, inserted: [input] })
  session.append('agent/admission/release', { messageId: input.id, admissionId: 'no-provider' })
  session.append('turn/start', { turn: 1 })
  const writer = await ctx.sessionPersistence.create(session.header)
  await writer.append(session.snapshotEvents())
  await writer.close()
  const path = (await ctx.sessionPersistence.stat(id))?.location?.path
  if (path === undefined) throw new Error('missing JSONL artifact')
  const before = await readFile(path)
  const restored = await ctx.agents.resume({ resumeSessionId: id, agentOptions: options })
  expect(await readFile(path)).not.toEqual(before)
  expect(restored.agent.session.snapshotEvents().some(event => event.type === 'turn/end')).toBe(true)
  expect(restored.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  expect(ctx.agents.guardedAdmissionsOf(restored.agent.session).root).not.toBeNull()
  expect(adapter.requests).toHaveLength(0)
  await restored.dispose()
})

it('rejects resume before opening a writer when scoped validation is unsupported', async () => {
  const { ctx } = await setup()
  const id = SessionId('unsupported-scoped-validation')
  const session = Session.create(id)
  session.append('turn/start', { turn: 1 })
  const writer = await ctx.sessionPersistence.create(session.header)
  await writer.append(session.snapshotEvents())
  await writer.close()
  const path = (await ctx.sessionPersistence.stat(id))?.location?.path
  if (path === undefined) throw new Error('missing JSONL artifact')
  const before = await readFile(path)
  const open = vi.spyOn(ctx.sessionPersistence, 'open')
  const unsupported = vi.spyOn(ctx.sessionPersistence, 'withWriteOpenValidation')
    .mockRejectedValue(new Error('Scoped write-open validation is unsupported'))
  onTestFinished(() => { open.mockRestore(); unsupported.mockRestore() })
  await expect(ctx.agents.resume({ resumeSessionId: id, agentOptions: options })).rejects.toThrow('validation is unsupported')
  expect(open).not.toHaveBeenCalled()
  expect(await readFile(path)).toEqual(before)
})

it('rejects a pre-registered competing authority projection before the real Agent registry starts', async () => {
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose() })
  await ctx.plugin(SessionStore)
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.sessionProjections.register({
    key: 'guardedAdmission', stateVersion: 3, replayOnHydrate: true,
    stateSchema: z.object({ root: z.null() }), init: () => ({ root: null }), apply: state => state,
  })
  let failure: unknown
  try { await ctx.plugin(AgentRegistry) } catch (error: unknown) { failure = error }
  if (!(failure instanceof Error)) throw new Error('Agent registry accepted a competing authority projection')
  expect(failure.message).toContain('exclusive owner')
})

it('awaits the provider before publishing a restored Agent or releasing a waking message', async () => {
  const { ctx, adapter } = await setup()
  const id = SessionId('provider-restored')
  const original = await ctx.agents.create({ sessionId: id, agentOptions: options })
  original.agent.session.append('turn/start', { turn: 1 })
  original.agent.session.append('turn/end', { turn: 1, reason: { kind: 'completed' } })
  await ctx.sessions.flush(original.agent.session)
  await original.dispose()
  const entered = Promise.withResolvers<Agent>()
  const release = Promise.withResolvers<undefined>()
  const disposeProvider = await ctx.agents.registerGuardedClaimProvider(async (agent) => {
    entered.resolve(agent)
    await release.promise
    return coordinator
  })
  const restored = ctx.agents.resume({ resumeSessionId: id, agentOptions: options })
  const agent = await Promise.race([
    entered.promise,
    restored.then(() => { throw new Error('restore returned before provider') }, (error: unknown) => { throw error }),
  ])
  agent.followup(createUserMessage({ content: [{ type: 'text', text: 'wake during setup' }], source: { kind: 'user' } }))
  expect(ctx.agents.get(id)).toBeUndefined()
  expect(adapter.requests).toHaveLength(0)
  release.resolve(undefined)
  const handle = await restored
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  await handle.dispose()
  disposeProvider()
})

it('keeps an actual recovered guarded input parked until provider binding and exact reconciliation', async () => {
  const { ctx, adapter } = await setup()
  const id = SessionId('provider-recovered-guarded')
  const original = await ctx.agents.create({ sessionId: id, agentOptions: options })
  if (original.agent.setGuardedClaimCoordinator === undefined || original.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const old = original.agent.setGuardedClaimCoordinator(coordinator)
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  const maintenance = original.agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'recover exactly' }], source: { kind: 'user' } })
  await original.agent.guardedSend(input, 'next-turn', 'recovery-1')
  old()
  unpark.resolve(undefined)
  await maintenance
  await original.agent.whenIdle()
  await original.dispose()

  const entered = Promise.withResolvers<Agent>()
  const release = Promise.withResolvers<undefined>()
  const dispose = await ctx.agents.registerGuardedClaimProvider(async (agent) => {
    entered.resolve(agent)
    await release.promise
    return coordinator
  })
  const pending = ctx.agents.resume({ resumeSessionId: id, agentOptions: options })
  const agent = await Promise.race([
    entered.promise,
    pending.then(() => { throw new Error('restore returned before provider') }, (error: unknown) => { throw error }),
  ])
  const ordinary = createUserMessage({ content: [{ type: 'text', text: 'wake recovered' }], source: { kind: 'user' } })
  agent.send(ordinary, 'next-step', true)
  expect(ctx.agents.get(id)).toBeUndefined()
  expect(adapter.requests).toHaveLength(0)
  release.resolve(undefined)
  const restored = await pending
  await vi.waitFor(() => { expect(adapter.requests).toHaveLength(1) })
  expect(restored.agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === ordinary.id)).toBe(true)
  expect(restored.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  if (restored.agent.reconcileGuardedAdmission === undefined) throw new Error('reconciliation unavailable')
  const flush = ctx.sessionPersistence.flushLiveSession.bind(ctx.sessionPersistence)
  vi.spyOn(ctx.sessionPersistence, 'flushLiveSession').mockImplementationOnce(async (sessionId) => {
    await flush(sessionId)
    restored.agent.session.append('agent/inbox/spliced', {
      target: 'next-turn', start: restored.agent.inbox.nextTurn.length, removedCount: 0, inserted: [],
    })
  })
  await expect(restored.agent.reconcileGuardedAdmission(id, input, 'next-turn', 'recovery-1')).rejects.toThrow('session changed during flush')
  expect(restored.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  expect(adapter.requests).toHaveLength(1)
  await restored.agent.reconcileGuardedAdmission(id, input, 'next-turn', 'recovery-1')
  await restored.agent.whenIdle()
  expect(adapter.requests).toHaveLength(2)
  expect(restored.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(false)
  dispose()
  await restored.dispose()
})

it.each([3, 2])('refolds persisted admissions despite a forged empty projection cache version %i', async (cacheVersion) => {
  const { ctx, adapter } = await setup()
  const id = SessionId('provider-forged-cache')
  const original = await ctx.agents.create({ sessionId: id, agentOptions: options })
  if (!original.agent.setGuardedClaimCoordinator || !original.agent.guardedSend) throw new Error('guarded admission unavailable')
  const detach = original.agent.setGuardedClaimCoordinator(coordinator)
  const parked = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const maintenance = original.agent.runMaintenance(async () => { parked.resolve(undefined); await release.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'must stay guarded' }], source: { kind: 'user' } })
  await original.agent.guardedSend(input, 'next-turn', 'forged-cache-action')
  detach()
  release.resolve(undefined)
  await maintenance
  await original.agent.whenIdle()
  const checkpoint = ctx.sessionProjections.checkpoint(original.agent.session)
  expect(checkpoint.guardedAdmission).toBeUndefined()
  await original.dispose()

  const prepare = ctx.sessions.prepare.bind(ctx.sessions)
  let forged = false
  const spy = vi.spyOn(ctx.sessions, 'prepare').mockImplementation((sessionId, preparationOptions) => {
    const session = prepare(sessionId, preparationOptions)
    if (sessionId === id) {
      const events = session.snapshotEvents()
      ctx.sessionProjections.hydrate(session, {
        ...checkpoint,
        guardedAdmission: { ver: cacheVersion, seq: SessionSeq(events.length - 1), val: { root: null } },
      }, events, SessionLogOffset(0))
      forged = true
    }
    return session
  })
  onTestFinished(() => { spy.mockRestore() })
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  const restored = await ctx.agents.resume({ resumeSessionId: id, agentOptions: options })
  expect(forged).toBe(true)
  const projection = ctx.agents.guardedAdmissionsOf(restored.agent.session)
  expect(projection.root).not.toBeNull()
  expect(Reflect.set(projection.root!, 'kind', 'leaf')).toBe(false)
  expect(Reflect.set(ctx.agents.guardedAdmissionRecordsOf(restored.agent.session)[0]!, 'messageId', 'forged')).toBe(false)
  expect(restored.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  expect(adapter.requests).toHaveLength(0)
  dispose()
  await restored.dispose()
})

it('binds an already-live idle Agent and permits a later provider after disposal', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-live'), agentOptions: options })
  const first = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  await expect(ctx.agents.registerGuardedClaimProvider(() => coordinator)).rejects.toThrow('already registered')
  first()
  const second = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  first()
  if (handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  await handle.agent.guardedSend(createUserMessage({ content: [{ type: 'text', text: 'guarded' }], source: { kind: 'user' } }), 'next-turn', 'live-1')
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  second()
  await handle.dispose()
})

it('keeps queued guarded work frozen when a provider unloads before the queued wake', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-hot-unload'), agentOptions: options })
  const first = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  const maintenance = handle.agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  if (handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const input = createUserMessage({ content: [{ type: 'text', text: 'unload preserves work' }], source: { kind: 'user' } })
  await handle.agent.guardedSend(input, 'next-turn', 'unload-1')
  first()
  unpark.resolve(undefined)
  await maintenance
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(handle.agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  const second = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(false)
  second()
  await handle.dispose()
})

it('wakes queued guarded work when the provider returns without injecting another message', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-rebind-wake'), agentOptions: options })
  const first = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  onTestFinished(() => { unpark.resolve(undefined) })
  const maintenance = handle.agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  if (handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const input = createUserMessage({ content: [{ type: 'text', text: 'resume when authority returns' }], source: { kind: 'user' } })
  await handle.agent.guardedSend(input, 'next-turn', 'provider-rebind-wake')
  first()
  unpark.resolve(undefined)
  await maintenance
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  const second = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(false)
  const userInputIds = handle.agent.session.snapshotEvents().flatMap(event => event.type === 'user/message' && event.data.source.kind === 'user' ? [event.data.id] : [])
  expect(userInputIds).toEqual([input.id])
  second()
  await handle.dispose()
})

it('does not wake ordinary non-waking context when a provider binds', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-no-ordinary-wake'), agentOptions: options })
  const input = createUserMessage({ content: [{ type: 'text', text: 'context only' }], source: { kind: 'user' } })
  handle.agent.inject(input)
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  await handle.agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(handle.agent.inbox.nextStep.some(message => message.id === input.id)).toBe(true)
  dispose()
  await handle.dispose()
})

it('stops waiting on a held guarded input when its provider is unloaded', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-held-unload'), agentOptions: options })
  onTestFinished(() => { handle.agent.cancel({ kind: 'disposed' }, { keepInbox: true }) })
  const first = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  onTestFinished(() => { unpark.resolve(undefined) })
  const maintenance = handle.agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  if (handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const input = createUserMessage({ content: [{ type: 'text', text: 'canceled pending' }], source: { kind: 'user' } })
  await handle.agent.guardedSend(input, 'next-turn', 'held-unload')
  handle.agent.cancel({ kind: 'user' })
  unpark.resolve(undefined)
  await maintenance
  await handle.agent.whenIdle()
  first()
  const second = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  expect(handle.agent.status).toBe('running')
  second()
  const timeout = Promise.withResolvers<never>()
  const timer = setTimeout(() => { timeout.reject(new Error('held driver did not stop after provider unload')) }, 1000)
  try { await Promise.race([handle.agent.whenIdle(), timeout.promise]) } finally { clearTimeout(timer) }
  expect(adapter.requests).toHaveLength(0)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  await handle.dispose()
})

it('rolls back a direct coordinator binding if the initiator scope closes before wake', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-closed-direct'), agentOptions: options })
  if (!handle.agent.setGuardedClaimCoordinator || !handle.agent.guardedSend) throw new Error('guarded admission unavailable')
  const old = handle.agent.setGuardedClaimCoordinator(coordinator)
  const parked = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const maintenance = handle.agent.runMaintenance(async () => { parked.resolve(undefined); await release.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'must remain pending' }], source: { kind: 'user' } })
  await handle.agent.guardedSend(input, 'next-turn', 'closed-direct')
  old()
  release.resolve(undefined)
  await maintenance
  await handle.agent.whenIdle()
  const closeInitiators: unknown = Reflect.get(ctx.agents, 'closeInitiators')
  if (typeof closeInitiators !== 'function') throw new Error('missing initiator close method')
  await Reflect.apply(closeInitiators, ctx.agents, [])
  expect(() => handle.agent.setGuardedClaimCoordinator!(coordinator)).toThrow('agent initiator scope is disposed')
  expect(handle.agent.status).toBe('idle')
  await handle.agent.whenIdle()
  // A second call must fail at the initiator boundary too, not at a leaked binding.
  expect(() => handle.agent.setGuardedClaimCoordinator!(coordinator)).toThrow('agent initiator scope is disposed')
  expect(adapter.requests).toHaveLength(0)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  await handle.dispose()
})

it('settles maintenance and rolls back provider registration when wake meets a closed initiator scope', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-closed-maintenance'), agentOptions: options })
  if (!handle.agent.setGuardedClaimCoordinator || !handle.agent.guardedSend) throw new Error('guarded admission unavailable')
  const old = handle.agent.setGuardedClaimCoordinator(coordinator)
  const parked = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const maintenance = handle.agent.runMaintenance(async () => { parked.resolve(undefined); await release.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'survive close' }], source: { kind: 'user' } })
  await handle.agent.guardedSend(input, 'next-turn', 'closed-maintenance')
  old()
  release.resolve(undefined)
  await maintenance
  await handle.agent.whenIdle()
  const closeInitiators: unknown = Reflect.get(ctx.agents, 'closeInitiators')
  if (typeof closeInitiators !== 'function') throw new Error('missing initiator close method')
  await Reflect.apply(closeInitiators, ctx.agents, [])
  await expect(ctx.agents.registerGuardedClaimProvider(() => coordinator)).rejects.toThrow('agent initiator scope is disposed')
  expect(handle.agent.status).toBe('idle')
  await handle.agent.whenIdle()
  await expect(ctx.agents.registerGuardedClaimProvider(() => coordinator)).rejects.toThrow('agent initiator scope is disposed')
  expect(adapter.requests).toHaveLength(0)
  expect(handle.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
  await handle.dispose()
})

it('rolls back an unpublished Agent when the provider is disposed during preparation', async () => {
  const { ctx, adapter } = await setup()
  const entered = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const disposeProvider = await ctx.agents.registerGuardedClaimProvider(async () => {
    entered.resolve(undefined)
    await release.promise
    return coordinator
  })
  const id = SessionId('provider-disposed')
  const pending = ctx.agents.create({ sessionId: id, agentOptions: options })
  const outcome = pending.then(() => undefined, (error: unknown) => error)
  await entered.promise
  disposeProvider()
  expect(await outcome).toBeInstanceOf(Error)
  release.resolve(undefined)
  expect(ctx.agents.get(id)).toBeUndefined()
  expect(adapter.requests).toHaveLength(0)
})

it('rejects late registration while a live Agent is busy and leaves no provider installed', async () => {
  const { ctx } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-busy'), agentOptions: options })
  const entered = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const busy = handle.agent.runMaintenance(async () => { entered.resolve(undefined); await release.promise })
  await entered.promise
  await expect(ctx.agents.registerGuardedClaimProvider(() => coordinator)).rejects.toThrow('already has active work')
  release.resolve(undefined)
  await busy
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => coordinator)
  dispose()
  await handle.dispose()
})

it.each([true, false])('holds guarded work until all live bindings settle (registration succeeds: %s)', async (succeed) => {
  const { ctx, adapter } = await setup()
  const first = await ctx.agents.create({ sessionId: SessionId('provider-ready-first'), agentOptions: options })
  const second = await ctx.agents.create({ sessionId: SessionId('provider-ready-second'), agentOptions: options })
  if (first.agent.setGuardedClaimCoordinator === undefined || first.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const old = first.agent.setGuardedClaimCoordinator(coordinator)
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  const maintenance = first.agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'wait for all owners' }], source: { kind: 'user' } })
  await first.agent.guardedSend(input, 'next-turn', 'ready-1')
  old()
  unpark.resolve(undefined)
  await maintenance
  await first.agent.whenIdle()
  expect(first.agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)

  const enteredSecond = Promise.withResolvers<undefined>()
  const releaseSecond = Promise.withResolvers<undefined>()
  const registration = ctx.agents.registerGuardedClaimProvider(async (agent) => {
    if (agent.id === second.agent.id) {
      enteredSecond.resolve(undefined)
      await releaseSecond.promise
      if (!succeed) throw new Error('second binding refused')
    }
    return coordinator
  })
  const outcome = registration.then(dispose => ({ dispose }), (error: unknown) => ({ error }))
  await enteredSecond.promise
  await Promise.resolve()
  expect(first.agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(adapter.requests).toHaveLength(0)
  releaseSecond.resolve(undefined)
  const result = await outcome
  await first.agent.whenIdle()
  if (succeed) {
    expect('dispose' in result).toBe(true)
    expect(adapter.requests).toHaveLength(1)
    if ('dispose' in result) result.dispose()
  } else {
    expect('error' in result).toBe(true)
    expect(adapter.requests).toHaveLength(0)
    expect(first.agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)
    expect(first.agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  }
  await first.dispose()
  await second.dispose()
})

it('refuses to skip an Agent whose durable history contains guarded admissions', async () => {
  const { ctx, adapter } = await setup()
  const handle = await ctx.agents.create({ sessionId: SessionId('provider-history'), agentOptions: options })
  if (handle.agent.setGuardedClaimCoordinator === undefined || handle.agent.guardedSend === undefined) throw new Error('guarded admission unavailable')
  const uninstall = handle.agent.setGuardedClaimCoordinator(() => { throw new Error('claim deliberately refused') })
  await handle.agent.guardedSend(createUserMessage({ content: [{ type: 'text', text: 'preserve' }], source: { kind: 'user' } }), 'next-turn', 'history-1')
  await handle.agent.whenIdle()
  uninstall()
  const before = handle.agent.session.snapshotEvents()
  await expect(ctx.agents.registerGuardedClaimProvider(() => undefined)).rejects.toThrow('guarded history')
  expect(handle.agent.session.snapshotEvents()).toEqual(before)
  expect(adapter.requests).toHaveLength(0)
  await handle.dispose()
})

it('binds configured startup Agents before their created notification', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-configured-provider-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  const id = SessionId('provider-configured')
  const prepared: string[] = []
  const created = Promise.withResolvers<Agent>()
  const dispose = await ctx.agents.registerGuardedClaimProvider((agent) => { prepared.push(agent.id); return coordinator })
  ctx.on('agent/created', ({ agent }) => { if (agent.id === id) created.resolve(agent) })
  await ctx.plugin(AgentLoop, { agents: [{ id: 'configured', sessionId: id, ...options }] })
  const agent = await created.promise
  expect(prepared).toContain(agent.id)
  expect(() => agent.setGuardedClaimCoordinator?.(coordinator)).toThrow('already installed')
  dispose()
})

it('checks the final frozen request before a guarded model call', async () => {
  const { ctx, adapter } = await setup()
  const seen: unknown[] = []
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeModelRequest: (_session: Session, request: unknown) => { seen.push(request); return true } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-request-authorized'), agentOptions: options })
  const { agent } = handle
  expect(agent.guardedRequestBoundaryVersion).toBe(1)
  if (!agent.guardedSend) throw new Error('guarded send unavailable')
  await agent.guardedSend(createUserMessage({ content:[{ type:'text', text:'authorized' }], source:{ kind:'user' } }), 'next-turn', 'authorized-request')
  await agent.whenIdle()
  expect(seen).toHaveLength(1)
  expect(Object.isFrozen(seen[0])).toBe(true)
  expect(adapter.requests).toHaveLength(1)
  expect(adapter.requests[0]).toBe(seen[0])
  dispose()
  await handle.dispose()
})

it('does not start the model when a guarded final request is refused', async () => {
  const { ctx, adapter } = await setup()
  const checked = Promise.withResolvers<void>()
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeModelRequest: () => { checked.resolve(); return false } },
  )
  const dispose = await ctx.agents.registerGuardedClaimProvider(() => authority)
  const handle = await ctx.agents.create({ sessionId: SessionId('guarded-request-denied'), agentOptions: options })
  const { agent } = handle
  if (!agent.guardedSend) throw new Error('guarded send unavailable')
  await agent.guardedSend(createUserMessage({ content:[{ type:'text', text:'denied' }], source:{ kind:'user' } }), 'next-turn', 'denied-request')
  await checked.promise
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/end' && event.data.reason.kind === 'error')).toBe(true)
  dispose()
  await handle.dispose()
})

it.each(['async', 'append', 'authority-change'] as const)('fails closed on guarded model guard %s', async mode => {
  const { ctx, adapter } = await setup()
  const checked = Promise.withResolvers<void>()
  let release = () => {}
  const authority: GuardedClaimCoordinator = Object.assign(
    (_session: string, _inputs: readonly unknown[], claim: () => void) => { claim(); return 'committed' as const },
    { beforeModelRequest: (session: Session) => {
      checked.resolve()
      if (mode === 'async') return Promise.resolve(true) as unknown as boolean
      if (mode === 'append') session.append('turn/start', { turn:999 })
      if (mode === 'authority-change') release()
      return true
    } },
  )
  const handle = await ctx.agents.create({ sessionId: SessionId(`guarded-model-${mode}`), agentOptions: options })
  const { agent } = handle
  if (!agent.guardedSend || !agent.setGuardedClaimCoordinator) throw new Error('guarded send unavailable')
  release = agent.setGuardedClaimCoordinator(authority)
  await agent.guardedSend(createUserMessage({ content:[{ type:'text', text:'restricted' }], source:{ kind:'user' } }), 'next-turn', `restricted-${mode}`)
  await checked.promise
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/end' && event.data.reason.kind === 'error')).toBe(true)
  await handle.dispose()
})
