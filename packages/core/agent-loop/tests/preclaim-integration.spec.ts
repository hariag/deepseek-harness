import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { mountAgentLoopTestDependencies, mountAgentLoopTestHarness } from '@deepseek-ai/dsh-agent-loop-testkit'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import { SessionId, type Session } from '@deepseek-ai/dsh-session'
import { defineContentToolFixture } from '@deepseek-ai/dsh-tool-testkit'
import * as Jsonl from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { expect, it, onTestFinished, vi } from 'vitest'
import { MockAdapter, textResponse, toolCallResponse } from './mock-adapter.ts'

async function setup(installCoordinator = true, script: ConstructorParameters<typeof MockAdapter>[0] = [textResponse('done'), textResponse('done'), textResponse('done')]) {
  const root = await mkdtemp(join(tmpdir(), 'dsh-preclaim-integration-'))
  const ctx = new Context()
  onTestFinished(async () => { await ctx.fiber.dispose(); await rm(root, { recursive: true, force: true }) })
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(Jsonl, { root, compression: 'none' })
  const adapter = new MockAdapter(script)
  ctx.llm.registerAdapter(['preclaim'], adapter)
  await mountAgentLoopTestHarness(ctx)
  const agent = await ctx.agentLoop.create(SessionId('preclaim-agent'), { provider: 'preclaim', model: 'mock' }, { cwd: root })
  if (installCoordinator) agent.setGuardedClaimCoordinator?.((_id, _inputs, claim) => { claim(); return 'committed' })
  const entered = Promise.withResolvers<undefined>()
  const release = Promise.withResolvers<undefined>()
  const persistence = ctx.sessionPersistence
  const begin = persistence.beginLiveEventReceipt.bind(persistence)
  vi.spyOn(persistence, 'beginLiveEventReceipt').mockImplementation((id, type) => {
    const receipt = begin(id, type)
    if (type !== 'agent/inbox/spliced') return receipt
    return {
      async confirm(event) { entered.resolve(undefined); await release.promise; await receipt.confirm(event) },
      dispose() { receipt.dispose() },
    }
  })
  onTestFinished(() => { release.resolve(undefined); vi.restoreAllMocks() })
  return { ctx, agent, adapter, entered, release }
}

function unsupportedAgent(session: Session, template: Agent): Agent {
  return {
    id: session.id, session, ctx: template.ctx, options: template.options,
    model: template.model, inbox: template.inbox, status: 'idle',
    send() {}, followup() {}, steer() {}, inject() {}, cancel() {},
    whenIdle: async () => {},
    runMaintenance: task => task(new AbortController().signal),
  }
}

it('blocks model entry and direct Session splice until exact insertion is durable', async () => {
  const { agent, adapter, entered, release } = await setup()
  const input = createUserMessage({ content: [{ type: 'text', text: 'guarded' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined) throw new Error('missing guardedSend')
  const admitted = agent.guardedSend(input, 'next-turn', 'action-1')
  await entered.promise
  expect(adapter.requests).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  const length = agent.session.snapshotEvents().length
  expect(() => agent.session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] })).toThrow('unresolved admission')
  expect(agent.session.snapshotEvents()).toHaveLength(length)
  expect(() => agent.session.append([
    ['agent/admission/release', { messageId: input.id, admissionId: 'action-1' }],
    ['agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] }],
  ])).toThrow('unauthorized')
  expect(agent.session.snapshotEvents()).toHaveLength(length)
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
})

it('does not report a pending admission as safe when its message already appeared in the durable transcript', async () => {
  const { ctx, agent, entered, release } = await setup()
  if (!agent.guardedSend || !agent.inspectGuardedAdmission) throw new Error('missing guarded admission capability')
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  onTestFinished(() => { agent.cancel({ kind: 'disposed' }, { keepInbox: true }); unpark.resolve(undefined) })
  const maintenance = agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'already observed' }], source: { kind: 'user' } })
  const admitted = agent.guardedSend(input, 'next-turn', 'previously-observed')
  await entered.promise
  release.resolve(undefined)
  await admitted
  agent.session.append('user/message', input, { surfaceOp: 'append' })
  await ctx.sessions.flush(agent.session)
  expect(await agent.inspectGuardedAdmission(agent.id, input, 'next-turn', 'previously-observed')).toEqual({ decision: 'unknown' })
  agent.cancel({ kind: 'disposed' }, { keepInbox: true })
  unpark.resolve(undefined)
  await maintenance
})

it('runs an earlier ordinary next-step while a later guarded next-turn waits for durability', async () => {
  const { agent, adapter, entered, release } = await setup()
  const ordinary = createUserMessage({ content: [{ type: 'text', text: 'earlier ordinary step' }], source: { kind: 'user' } })
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'later guarded turn' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined) throw new Error('missing guardedSend')
  agent.inject(ordinary)
  const admitted = agent.guardedSend(guarded, 'next-turn', 'ordinary-before-held')
  const outcome = admitted.then(() => undefined, (error: unknown) => error)
  await entered.promise
  await vi.waitFor(() => { expect(adapter.requests).toHaveLength(1) }, { timeout: 1000 })
  expect(agent.inbox.nextTurn.some(message => message.id === guarded.id)).toBe(true)
  release.resolve(undefined)
  expect(await outcome).toBeUndefined()
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(2)
})

it('keeps later ordinary next-step behind a held guarded next-step in the same lane', async () => {
  const { agent, adapter, entered, release } = await setup()
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'first guarded step' }], source: { kind: 'user' } })
  const ordinary = createUserMessage({ content: [{ type: 'text', text: 'later ordinary step' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined) throw new Error('missing guardedSend')
  const admitted = agent.guardedSend(guarded, 'next-step', 'held-before-ordinary')
  const outcome = admitted.then(() => undefined, (error: unknown) => error)
  await entered.promise
  agent.inject(ordinary)
  await new Promise(resolve => setTimeout(resolve, 30))
  expect(adapter.requests).toHaveLength(0)
  expect(agent.inbox.nextStep.map(message => message.id)).toEqual([guarded.id, ordinary.id])
  release.resolve(undefined)
  expect(await outcome).toBeUndefined()
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(2)
})

it('user cancel during persistence retains the guarded input and never later wakes it', async () => {
  const { agent, adapter, entered, release } = await setup()
  const input = createUserMessage({ content: [{ type: 'text', text: 'cancel me' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined) throw new Error('missing guardedSend')
  const admitted = agent.guardedSend(input, 'next-turn', 'action-2')
  const outcome = admitted.then(() => undefined, (error: unknown) => error)
  await entered.promise
  agent.cancel({ kind: 'user' })
  release.resolve(undefined)
  const error = await outcome
  expect(error).toBeInstanceOf(Error)
  if (error instanceof Error) expect(error.message).toContain('canceled')
  expect(agent.inbox.nextTurn.some(item => item.id === input.id)).toBe(true)
  agent.followup(createUserMessage({ content: [{ type: 'text', text: 'later' }], source: { kind: 'user' } }))
  await new Promise(resolve => setTimeout(resolve, 25))
  expect(adapter.requests).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  agent.cancel({ kind: 'disposed' }, { keepInbox: true })
  await agent.whenIdle()
})

it('rejects an unsupported provider before publishing a session with required records', async () => {
  const { ctx, agent } = await setup()
  const session = ctx.sessions.create(SessionId('unsupported-restore'))
  session.append('agent/admission/hold', { messageId: 'm', admissionId: 'a' })
  const original = session.snapshotEvents()
  const provider = unsupportedAgent(session, agent)
  expect(() => ctx.agents.enter(provider)).toThrow('cannot resume required')
  expect(session.snapshotEvents()).toEqual(original)
  expect(ctx.agents.get(session.id)).toBeUndefined()
})

it('rejects new required records before they enter an unsupported registered provider log', async () => {
  const { ctx, agent } = await setup()
  const session = ctx.sessions.create(SessionId('unsupported-live'))
  const provider = unsupportedAgent(session, agent)
  const detach = ctx.agents.enter(provider)
  try {
    const before = session.snapshotEvents()
    expect(() => session.append('agent/admission/hold', { messageId: 'm', admissionId: 'a' })).toThrow('does not support required')
    expect(session.snapshotEvents()).toEqual(before)
  } finally { detach() }
})

it('inspects only an exact stored pending admission and never authorizes a retry', async () => {
  const { ctx, agent, entered, release } = await setup()
  const input = createUserMessage({ content: [{ type: 'text', text: 'inspect me' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined || agent.inspectGuardedAdmission === undefined) throw new Error('missing admission capability')
  const admitted = agent.guardedSend(input, 'next-turn', 'inspect-action')
  await entered.promise
  await ctx.sessions.flush(agent.session)
  expect(await agent.inspectGuardedAdmission(agent.session.id, input, 'next-turn', 'inspect-action'))
    .toEqual({ decision: 'unknown', observed: { pending: true, admission: 'held' } })
  expect(await agent.inspectGuardedAdmission('different-session', input, 'next-turn', 'inspect-action')).toEqual({ decision: 'unknown' })
  expect(await agent.inspectGuardedAdmission(agent.session.id, input, 'next-step', 'inspect-action')).toEqual({ decision: 'unknown' })
  expect(await agent.inspectGuardedAdmission(agent.session.id, input, 'next-turn', 'different-action')).toEqual({ decision: 'unknown' })
  const changed = { ...input, content: [{ type: 'text' as const, text: 'changed' }] }
  expect(await agent.inspectGuardedAdmission(agent.session.id, changed, 'next-turn', 'inspect-action')).toEqual({ decision: 'unknown' })
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(await agent.inspectGuardedAdmission(agent.session.id, input, 'next-turn', 'inspect-action')).toEqual({ decision: 'unknown' })
})

it('refuses guarded delivery without an installed claim authority', async () => {
  const { agent, adapter } = await setup(false)
  const input = createUserMessage({ content: [{ type: 'text', text: 'no authority' }], source: { kind: 'user' } })
  if (agent.guardedSend === undefined) throw new Error('missing guardedSend')
  await expect(agent.guardedSend(input, 'next-turn', 'no-authority')).rejects.toThrow('coordinator is unavailable')
  expect(agent.inbox.nextTurn).toHaveLength(0)
  expect(adapter.requests).toHaveLength(0)
})

it('does not start a turn if the coordinator is revoked while a guarded insertion is becoming durable', async () => {
  const { agent, adapter, entered, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  const coordinator = ((_session, _inputs, claim) => { claim(); return 'committed' }) satisfies Parameters<NonNullable<Agent['setGuardedClaimCoordinator']>>[0]
  const uninstall = agent.setGuardedClaimCoordinator(coordinator)
  const input = createUserMessage({ content: [{ type: 'text', text: 'hold until coordinator' }], source: { kind: 'user' } })
  const admitted = agent.guardedSend(input, 'next-turn', 'revoked-before-turn')
  await entered.promise
  uninstall()
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  expect(agent.session.snapshotEvents().some(event => event.type === 'turn/start')).toBe(false)
  expect(agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(true)

  agent.setGuardedClaimCoordinator(coordinator)
  uninstall() // A stale disposer cannot remove the replacement.
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  expect(agent.inbox.nextTurn.some(message => message.id === input.id)).toBe(false)
})

it('does not start a model or retry after a fenced claim transaction throws', async () => {
  const { agent, adapter, entered, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  let calls = 0
  agent.setGuardedClaimCoordinator((_session, inputs, claim) => {
    calls++
    expect(inputs).toHaveLength(1)
    expect(inputs[0]?.admissionId).toBe('fail-after-claim')
    claim()
    throw new Error('commit outcome unknown')
  })
  const input = createUserMessage({ content: [{ type: 'text', text: 'fenced' }], source: { kind: 'user' } })
  const admitted = agent.guardedSend(input, 'next-turn', 'fail-after-claim')
  await entered.promise
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(calls).toBe(1)
  expect(adapter.requests).toHaveLength(0)
  agent.followup(createUserMessage({ content: [{ type: 'text', text: 'unrelated' }], source: { kind: 'user' } }))
  await agent.whenIdle()
  expect(calls).toBe(1)
  expect(adapter.requests).toHaveLength(0)
})

it.each(['before claim', 'during claim', 'replacement before claim'] as const)('does not enter the model when its coordinator is revoked %s', async (when) => {
  const { ctx, agent, adapter, entered, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  let uninstall: () => void = () => {}
  uninstall = agent.setGuardedClaimCoordinator((_session, _inputs, claim) => {
    if (when === 'before claim') uninstall()
    if (when === 'replacement before claim') {
      uninstall()
      agent.setGuardedClaimCoordinator?.((_id, _inputs, next) => { next(); return 'committed' })
    }
    claim()
    return 'committed'
  })
  if (when === 'during claim') ctx.on('session/event', (session, event) => {
    if (session === agent.session && event.type === 'agent/inbox/spliced' && (event.data.removedCount ?? 0) > 0) uninstall()
  })
  const input = createUserMessage({ content: [{ type: 'text', text: 'authority revoked' }], source: { kind: 'user' } })
  const admitted = agent.guardedSend(input, 'next-turn', `revoked-${when}`)
  await entered.promise
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(0)
  if (when !== 'during claim') expect(agent.inbox.nextTurn.some(item => item.id === input.id)).toBe(true)
})

it('rejects a guarded next-step send while an aborted activity would redirect it to next-turn', async () => {
  const { agent, adapter, release } = await setup(false)
  release.resolve(undefined)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const entered = Promise.withResolvers<undefined>()
  const leave = Promise.withResolvers<undefined>()
  onTestFinished(() => { leave.resolve(undefined) })
  const maintenance = agent.runMaintenance(async () => { entered.resolve(undefined); await leave.promise })
  await entered.promise
  agent.cancel({ kind: 'user' }, { keepInbox: true })
  const input = createUserMessage({ content: [{ type: 'text', text: 'exact target' }], source: { kind: 'user' } })
  await expect(agent.guardedSend(input, 'next-step', 'exact-target')).rejects.toThrow('guarded admission target changed')
  expect(agent.inbox.nextTurn).toHaveLength(0)
  expect(agent.inbox.nextStep).toHaveLength(0)
  expect(adapter.requests).toHaveLength(0)
  leave.resolve(undefined)
  await maintenance
})

it('does not redirect a guarded input if the active activity aborts during its hold event', async () => {
  const { ctx, agent, adapter, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const entered = Promise.withResolvers<undefined>()
  const leave = Promise.withResolvers<undefined>()
  onTestFinished(() => { leave.resolve(undefined) })
  const maintenance = agent.runMaintenance(async () => { entered.resolve(undefined); await leave.promise })
  await entered.promise
  ctx.on('session/event', (session, event) => {
    if (session === agent.session && event.type === 'agent/admission/hold') agent.cancel({ kind: 'user' }, { keepInbox: true })
  })
  const input = createUserMessage({ content: [{ type: 'text', text: 'hold event abort' }], source: { kind: 'user' } })
  await expect(agent.guardedSend(input, 'next-step', 'hold-abort')).rejects.toThrow('guarded admission target changed')
  expect(agent.session.snapshotEvents().some(event => event.type === 'agent/admission/hold' && event.data.messageId === input.id)).toBe(true)
  expect(agent.inbox.nextTurn).toHaveLength(0)
  expect(agent.inbox.nextStep).toHaveLength(0)
  expect(adapter.requests).toHaveLength(0)
  leave.resolve(undefined)
  await maintenance
})

it('removes a canceled guarded input only after the host durably reconciles cancellation', async () => {
  const { agent, adapter, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined || agent.cancelGuardedAdmission === undefined) {
    throw new Error('missing guarded admission capability')
  }
  release.resolve(undefined)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const entered = Promise.withResolvers<undefined>()
  const leave = Promise.withResolvers<undefined>()
  onTestFinished(() => { leave.resolve(undefined) })
  const maintenance = agent.runMaintenance(async () => { entered.resolve(undefined); await leave.promise })
  await entered.promise
  const input = createUserMessage({ content: [{ type: 'text', text: 'cancel pending' }], source: { kind: 'user' } })
  await agent.guardedSend(input, 'next-turn', 'cancel-reconciled')
  agent.cancel({ kind: 'user' })
  expect(agent.inbox.nextTurn.some(item => item.id === input.id)).toBe(true)
  await agent.cancelGuardedAdmission(agent.session.id, input, 'next-turn', 'cancel-reconciled')
  expect(agent.inbox.nextTurn.some(item => item.id === input.id)).toBe(false)
  leave.resolve(undefined)
  await maintenance
  const next = createUserMessage({ content: [{ type: 'text', text: 'later work' }], source: { kind: 'user' } })
  agent.followup(next)
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(1)
  const userMessageIds = agent.session.snapshotEvents().flatMap(event => event.type === 'user/message' && event.data.source.kind === 'user' ? [event.data.id] : [])
  expect(userMessageIds).toEqual([next.id])
})

it.each(['ordinary step before guarded turn', 'guarded step before ordinary turn'] as const)(
  'processes %s in separate model steps without changing order', async (order) => {
    const { agent, adapter, release } = await setup(false)
    if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
    release.resolve(undefined)
    const claimed: string[] = []
    agent.setGuardedClaimCoordinator((_session, inputs, claim) => { claimed.push(...inputs.map(item => item.message.id)); claim(); return 'committed' })
    const parked = Promise.withResolvers<undefined>()
    const unpark = Promise.withResolvers<undefined>()
    onTestFinished(() => { unpark.resolve(undefined) })
    const maintenance = agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
    await parked.promise
    const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded' }], source: { kind: 'user' } })
    const ordinary = createUserMessage({ content: [{ type: 'text', text: 'ordinary' }], source: { kind: 'user' } })
    if (order === 'ordinary step before guarded turn') {
      await agent.guardedSend(guarded, 'next-turn', 'serial-guarded')
      agent.send(ordinary, 'next-step', true)
    } else {
      await agent.guardedSend(guarded, 'next-step', 'serial-guarded')
      agent.send(ordinary, 'next-turn', true)
    }
    unpark.resolve(undefined)
    await maintenance
    await agent.whenIdle()
    expect(adapter.requests).toHaveLength(2)
    expect(claimed).toEqual([guarded.id])
    const events = agent.session.snapshotEvents()
    const first = order === 'ordinary step before guarded turn' ? ordinary : guarded
    const second = first === ordinary ? guarded : ordinary
    const firstAt = events.findIndex(event => event.type === 'user/message' && event.data.id === first.id)
    const secondAt = events.findIndex(event => event.type === 'user/message' && event.data.id === second.id)
    expect(firstAt).toBeGreaterThanOrEqual(0)
    expect(secondAt).toBeGreaterThan(firstAt)
    expect(events.slice(firstAt + 1, secondAt).some(event => event.type === 'step/end')).toBe(true)
  },
)

it('serializes independent guarded next-step inputs', async () => {
  const { agent, adapter, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  const batches: number[] = []
  agent.setGuardedClaimCoordinator((_session, inputs, claim) => { batches.push(inputs.length); claim(); return 'committed' })
  const parked = Promise.withResolvers<undefined>()
  const unpark = Promise.withResolvers<undefined>()
  onTestFinished(() => { unpark.resolve(undefined) })
  const maintenance = agent.runMaintenance(async () => { parked.resolve(undefined); await unpark.promise })
  await parked.promise
  for (const id of ['first', 'second']) {
    await agent.guardedSend(createUserMessage({ content: [{ type: 'text', text: id }], source: { kind: 'user' } }), 'next-step', id)
  }
  unpark.resolve(undefined)
  await maintenance
  await agent.whenIdle()
  expect(batches).toEqual([1, 1])
  expect(adapter.requests).toHaveLength(2)
  expect(agent.session.snapshotEvents().filter(event => event.type === 'turn/start')).toHaveLength(2)
})

it.each(['ordinary first', 'guarded first'] as const)(
  'finishes a tool-using %s turn before consuming input of the other kind', async (order) => {
    const { ctx, agent, adapter, release } = await setup(false, [
      toolCallResponse('enqueue', 'enqueue_other', {}), textResponse('first complete'), textResponse('second complete'),
    ])
    if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
    release.resolve(undefined)
    const guardedSend = agent.guardedSend.bind(agent)
    agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
    const ordinary = createUserMessage({ content: [{ type: 'text', text: 'ordinary' }], source: { kind: 'user' } })
    const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded' }], source: { kind: 'user' } })
    ctx.tools.register(defineContentToolFixture({
      name: 'enqueue_other', description: '', parameters: {},
      async execute() {
        if (order === 'ordinary first') await guardedSend(guarded, 'next-step', 'tool-guarded')
        else agent.steer(ordinary)
        return []
      },
    }))
    if (order === 'ordinary first') agent.followup(ordinary)
    else await guardedSend(guarded, 'next-turn', 'tool-guarded')
    await agent.whenIdle()
    expect(adapter.requests).toHaveLength(3)
    const events = agent.session.snapshotEvents()
    const first = order === 'ordinary first' ? ordinary : guarded
    const second = first === ordinary ? guarded : ordinary
    const firstAt = events.findIndex(event => event.type === 'user/message' && event.data.id === first.id)
    const secondAt = events.findIndex(event => event.type === 'user/message' && event.data.id === second.id)
    expect(firstAt).toBeGreaterThanOrEqual(0)
    expect(secondAt).toBeGreaterThan(firstAt)
    const between = events.slice(firstAt + 1, secondAt)
    expect(between.some(event => event.type === 'tool/result')).toBe(true)
    expect(between.some(event => event.type === 'turn/end')).toBe(true)
  },
)

it('keeps a stop-hook continuation in its guarded turn while deferring unrelated steering', async () => {
  let enqueueOrdinary: () => void = () => {}
  const { ctx, agent, adapter, release } = await setup(false, [
    () => { enqueueOrdinary(); return textResponse('first step') }, textResponse('continued'), textResponse('other work'),
  ])
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded task' }], source: { kind: 'user' } })
  const ordinary = createUserMessage({ content: [{ type: 'text', text: 'unrelated steering' }], source: { kind: 'user' } })
  const hook = createUserMessage({ content: [{ type: 'text', text: 'required stop hook continuation' }], source: { kind: 'user' } })
  enqueueOrdinary = () => { agent.steer(ordinary) }
  let continued = false
  ctx.on('agent/turn-stopping', ({ agent: subject }) => {
    if (subject === agent && !continued) { continued = true; subject.steer(hook) }
  })
  await agent.guardedSend(guarded, 'next-turn', 'hook-continuation')
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(3)
  const events = agent.session.snapshotEvents()
  const guardAt = events.findIndex(event => event.type === 'user/message' && event.data.id === guarded.id)
  const hookAt = events.findIndex(event => event.type === 'user/message' && event.data.id === hook.id)
  const ordinaryAt = events.findIndex(event => event.type === 'user/message' && event.data.id === ordinary.id)
  expect(guardAt).toBeGreaterThanOrEqual(0)
  expect(hookAt).toBeGreaterThan(guardAt)
  expect(ordinaryAt).toBeGreaterThan(hookAt)
  expect(events.slice(guardAt + 1, hookAt).some(event => event.type === 'turn/end')).toBe(false)
  expect(events.slice(hookAt + 1, ordinaryAt).some(event => event.type === 'turn/end')).toBe(true)
})

it('does not treat a delayed callback from an already returned stop hook as current-turn input', async () => {
  const callbackGate = Promise.withResolvers<undefined>()
  const { ctx, agent, adapter, release } = await setup(false, [
    textResponse('first step'), () => { callbackGate.resolve(undefined); return textResponse('continued') }, textResponse('later'),
  ])
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded task' }], source: { kind: 'user' } })
  const hook = createUserMessage({ content: [{ type: 'text', text: 'in-scope continuation' }], source: { kind: 'user' } })
  const late = createUserMessage({ content: [{ type: 'text', text: 'late callback' }], source: { kind: 'user' } })
  let scheduled = false
  ctx.on('agent/turn-stopping', ({ agent: subject }) => {
    if (subject !== agent || scheduled) return
    scheduled = true
    subject.steer(hook)
    void callbackGate.promise.then(() => { subject.steer(late) })
  })
  await agent.guardedSend(guarded, 'next-turn', 'late-hook')
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(3)
  const events = agent.session.snapshotEvents()
  const hookAt = events.findIndex(event => event.type === 'user/message' && event.data.id === hook.id)
  const lateAt = events.findIndex(event => event.type === 'user/message' && event.data.id === late.id)
  expect(hookAt).toBeGreaterThanOrEqual(0)
  expect(lateAt).toBeGreaterThan(hookAt)
  expect(events.slice(hookAt + 1, lateAt).some(event => event.type === 'turn/end')).toBe(true)
})

it('does not treat concurrent steering as stop-hook input while an async hook is waiting', async () => {
  const { ctx, agent, adapter, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded task' }], source: { kind: 'user' } })
  const hook = createUserMessage({ content: [{ type: 'text', text: 'hook continuation' }], source: { kind: 'user' } })
  const outside = createUserMessage({ content: [{ type: 'text', text: 'concurrent input' }], source: { kind: 'user' } })
  const hookEntered = Promise.withResolvers<undefined>()
  const releaseHook = Promise.withResolvers<undefined>()
  onTestFinished(() => { releaseHook.resolve(undefined) })
  let entered = false
  ctx.on('agent/turn-stopping', async ({ agent: subject }) => {
    if (subject !== agent || entered) return
    entered = true
    hookEntered.resolve(undefined)
    await releaseHook.promise
    subject.steer(hook)
  })
  await agent.guardedSend(guarded, 'next-turn', 'async-hook')
  await hookEntered.promise
  agent.steer(outside)
  releaseHook.resolve(undefined)
  await agent.whenIdle()
  expect(adapter.requests).toHaveLength(3)
  const events = agent.session.snapshotEvents()
  const hookAt = events.findIndex(event => event.type === 'user/message' && event.data.id === hook.id)
  const outsideAt = events.findIndex(event => event.type === 'user/message' && event.data.id === outside.id)
  expect(hookAt).toBeGreaterThanOrEqual(0)
  expect(outsideAt).toBeGreaterThan(hookAt)
  expect(events.slice(hookAt + 1, outsideAt).some(event => event.type === 'turn/end')).toBe(true)
})

it('keeps an ordinary turn stop hook ahead of a guarded input that arrived mid-turn', async () => {
  let enqueueGuarded: () => void = () => {}
  const { ctx, agent, adapter, release } = await setup(false, [
    () => { enqueueGuarded(); return textResponse('first step') }, textResponse('continued'), textResponse('guarded work'),
  ])
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  release.resolve(undefined)
  const guardedSend = agent.guardedSend.bind(agent)
  agent.setGuardedClaimCoordinator((_session, _inputs, claim) => { claim(); return 'committed' })
  const ordinary = createUserMessage({ content: [{ type: 'text', text: 'ordinary task' }], source: { kind: 'user' } })
  const guarded = createUserMessage({ content: [{ type: 'text', text: 'guarded later' }], source: { kind: 'user' } })
  const hook = createUserMessage({ content: [{ type: 'text', text: 'required continuation' }], source: { kind: 'user' } })
  let admission: Promise<void> = Promise.resolve()
  enqueueGuarded = () => { admission = guardedSend(guarded, 'next-step', 'guarded-after-ordinary') }
  let continued = false
  ctx.on('agent/turn-stopping', ({ agent: subject }) => {
    if (subject === agent && !continued) { continued = true; subject.steer(hook) }
  })
  agent.followup(ordinary)
  await agent.whenIdle()
  await admission
  expect(adapter.requests).toHaveLength(3)
  const events = agent.session.snapshotEvents()
  const ordinaryAt = events.findIndex(event => event.type === 'user/message' && event.data.id === ordinary.id)
  const hookAt = events.findIndex(event => event.type === 'user/message' && event.data.id === hook.id)
  const guardAt = events.findIndex(event => event.type === 'user/message' && event.data.id === guarded.id)
  expect(ordinaryAt).toBeGreaterThanOrEqual(0)
  expect(hookAt).toBeGreaterThan(ordinaryAt)
  expect(guardAt).toBeGreaterThan(hookAt)
  expect(events.slice(ordinaryAt + 1, hookAt).some(event => event.type === 'turn/end')).toBe(false)
  expect(events.slice(hookAt + 1, guardAt).some(event => event.type === 'turn/end')).toBe(true)
})

it('rejects direct removal of a released guarded input outside a fenced claim', async () => {
  const { agent, entered, release } = await setup(false)
  if (agent.setGuardedClaimCoordinator === undefined || agent.guardedSend === undefined) throw new Error('missing guarded claim capability')
  agent.setGuardedClaimCoordinator(() => { throw new Error('reservation refused') })
  const input = createUserMessage({ content: [{ type: 'text', text: 'stay pending' }], source: { kind: 'user' } })
  const admitted = agent.guardedSend(input, 'next-turn', 'must-not-remove')
  await entered.promise
  release.resolve(undefined)
  await admitted
  await agent.whenIdle()
  expect(agent.inbox.nextTurn.some(item => item.id === input.id)).toBe(true)
  expect(() => agent.inbox.remove(input.id)).toThrow('unresolved admission')
  expect(() => agent.session.append('agent/inbox/spliced', { target: 'next-turn', start: 0, removedCount: 1, inserted: [] })).toThrow('unresolved admission')
})
