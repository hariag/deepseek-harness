/**
 * Default Agent driver over queued turns and step-boundary input. Every request
 * is derived from the session log.
 * @module dsh-agent-loop/agent
 */

import type {
  Agent,
  AgentCancelCause,
  AgentEventDispatch,
  AgentOptions,
  AgentStatus,
  CancelOptions,
  ModelSelection,
  ModelSelectionProjectionState,
  InboxTarget,
  GuardedClaimCoordinator,
  GuardedClaimInput,
  InboxState,
  PreStepDecision,
  RequestErrorAction,
} from '@deepseek-ai/dsh-agent'
import { AgentModel, agentEvents, assembleContextFor } from '@deepseek-ai/dsh-agent'
import type { GenerateOptions, LlmCallConfig, Message, PreparedLlmCall } from '@deepseek-ai/dsh-llm'
import {
  LlmError,
  createAssistantMessage,
  createDeveloperMessage,
  errorChain,
  markAgentLoopRequest,
} from '@deepseek-ai/dsh-llm'
import { assertNever, deepFreeze, snapshotJsonValue } from '@deepseek-ai/dsh-util-values'
import { isDeepStrictEqual } from 'node:util'
import { AsyncLocalStorage } from 'node:async_hooks'
import type { Scope } from '@deepseek-ai/dsh-scope'
import { createScope } from '@deepseek-ai/dsh-scope'
import type { EpochHeader, RequestContext, Session, SessionEvent, SessionId, SessionSeq, TurnEndReason, UserMessage } from '@deepseek-ai/dsh-session'
import { canonicalHeader, headerEquals, ToolCallRecovery } from '@deepseek-ai/dsh-session'
import { renderContextSections, renderPrompt } from '@deepseek-ai/dsh-system-prompt'
import type { PromptAssembly } from '@deepseek-ai/dsh-system-prompt'
import { ToolActivationError, ToolBatchExecutionError } from '@deepseek-ai/dsh-tools'
import type { ResolvedToolSet } from '@deepseek-ai/dsh-tools'
import type { Context } from '@deepseek-ai/cordis'
import { inboxProjectionDefinition, ReactLoopInbox } from './inbox.ts'
import { AdmissionState } from './admission.ts'
import { RuntimeContextProjection } from './runtime-context.ts'
import { AssistantStreamAttempt } from './assistant-stream.ts'
import { SystemPromptProjection } from './runtime-context.ts'

/** Projected facts consumed by this driver; registration stays with the factory. */
interface LoopProjections {
  /** Current model selection, including pending changes. */
  readonly modelSelection: () => ModelSelectionProjectionState
  /** Current queued and claimed input. */
  readonly inbox: () => InboxState
}

type Phase =
  | { kind: 'idle'; lastTurn: number }
  | {
    kind: 'maintenance'
    abort: AbortController
    lastTurn: number
    wakeRequested: boolean
  }
  | { kind: 'running'; abort: AbortController; turn: number; step: number; wakeRequested: boolean }

type StepEndReason = Extract<TurnEndReason, { kind: 'completed' | 'max-tokens' }>
type TurnKind = 'initial' | 'ordinary' | 'guarded'
interface StoppingInputs { readonly turn: number; readonly ids: Set<string>; open: boolean }
interface ProposedInput { message: UserMessage; target: InboxTarget; index: number }

type PreparedStep =
  | { kind: 'reject' }
  | {
    kind: 'enter'
    messages: UserMessage[]
    startsRequestSeries?: true
    /** The claimed input belongs to a guarded task with its own turn. */
    claimedGuarded: boolean
    assembly: PromptAssembly
    selection: Readonly<ModelSelection>
    /** Admission shared by the step's declarations and dispatch; the turn disposes it after the step. */
    toolSet: ResolvedToolSet
  }

/** Remove adapter-derived values before plugins propose the next request config. */
function requestProposal(header: EpochHeader): LlmCallConfig {
  if (header.adapterDefaults === undefined) return header.config
  const proposal = { ...header.config }
  if (header.adapterDefaults.reasoningEffort === true) delete proposal.reasoningEffort
  if (header.adapterDefaults.maxTokens === true) delete proposal.maxTokens
  return proposal
}

/**
 * Read the cause `cancel()` passed when aborting a loop-owned signal, copying
 * only the fields `turn/end` records. The live reason stays the caller's
 * object, and Node's fetch assigns a `stack` onto it that `Session.append`
 * would either log or reject as data JSON cannot hold.
 * @param signal - a turn or maintenance signal this loop owns.
 * @returns the copied cause, or undefined while the signal is still live.
 */
function abortedCancelCause(signal: AbortSignal): AgentCancelCause | undefined {
  if (!signal.aborted) return undefined
  // `cancel()` is the only aborter of the signals this loop owns.
  const cause = signal.reason as AgentCancelCause
  switch (cause.kind) {
    case 'user':
    case 'parent':
    case 'disposed':
      return { kind: cause.kind }
    case 'hook':
      return { kind: 'hook', reason: cause.reason }
    /* v8 ignore next -- cancel accepts the closed AgentCancelCause union */
    default:
      return assertNever(cause)
  }
}

/** Drives one session through turn and step boundaries. */
export class ReactLoopAgent implements Agent {
  readonly durableAdmissionVersion = 1 as const
  readonly guardedClaimTurnVersion = 1 as const
  readonly guardedRequestBoundaryVersion = 1 as const
  readonly model: AgentModel
  readonly inbox: ReactLoopInbox
  private readonly admissions: AdmissionState
  private guardedClaimCoordinator: GuardedClaimCoordinator | undefined
  private guardedStartCallbacks: { before: NonNullable<GuardedClaimCoordinator['beforeTurnStart']>, cancel: NonNullable<GuardedClaimCoordinator['cancelTurnStart']> } | undefined
  private guardedCoordinatorReady: Promise<void> | undefined
  private guardedCoordinatorToken: object | undefined
  private guardedRequestGuard: GuardedClaimCoordinator['beforeModelRequest'] | undefined
  private guardedClaimFault = false
  private allowedGuardedClaims: ReadonlySet<string> | undefined
  private allowedGuardedDiscard: string | undefined
  private readonly reconciling = new Set<string>()
  private readonly removeAdmissionGuard: () => void
  private allowedAdmissionEvent: { type: string; messageId: string; admissionId: string } | undefined
  private allowedGuardedInput: UserMessage | undefined
  #guardedStartCallbackActive = false
  private preserveGuardedDuringDisposal = false
  private readonly stoppingInputs = new AsyncLocalStorage<StoppingInputs>()
  private phase: Phase
  private activityDone: Promise<void> = Promise.resolve()

  /** The agent-scoped registration boundary; the lifecycle owner unwinds it after the driver exits. */
  readonly scope: Scope
  readonly ctx: Context

  /** Fused dispatcher, built once in the constructor so hot-path dispatches never allocate. */
  private readonly dispatch: AgentEventDispatch

  /** Whether this loop instance has appended its initial/resume request anchor. */
  private requestHeaderLogged = false
  /** Surface generation at attachment or the preceding built request. */
  private requestSurfaceGeneration: number
  private readonly runtimeContext: RuntimeContextProjection
  /** Process-local revision of assistant frames for this attached Session. */
  private assistantStreamRevision = 0
  private assistantAttemptCounter = 0
  private readonly systemPrompt: SystemPromptProjection
  /** Identities fully frozen by this loop; weak references do not retain replaced history. */
  private readonly frozenMessages = new WeakSet<Message>()

  constructor(
    private loopCtx: Context,
    public readonly id: SessionId,
    public readonly options: AgentOptions,
    public readonly session: Session,
    projections: LoopProjections,
    lastTurn: number,
    private readonly lifetimeSignal: AbortSignal,
  ) {
    this.requestSurfaceGeneration = session.surface.contentGeneration
    this.dispatch = agentEvents(loopCtx, this)
    this.scope = createScope(loopCtx, this)
    this.ctx = this.scope.ctx
    this.model = new AgentModel(projections.modelSelection, session, options)
    this.admissions = AdmissionState.fromRecords(loopCtx.agents.guardedAdmissionRecordsOf(session))
    this.inbox = new ReactLoopInbox(projections.inbox, session, this.dispatch,
      id => (this.admissions.hasHeld(id) && this.allowedGuardedDiscard !== id)
        || (this.preserveGuardedDuringDisposal && this.admissions.hasSeen(id)),
      id => this.admissions.hasSeen(id))
    this.removeAdmissionGuard = session.registerAppendGuard((events) => { this.validateAdmissionBatch(events) })
    this.phase = { kind: 'idle', lastTurn }
    this.ctx.on('session/event', (subject, event) => {
      if (subject === this.session && event.type === 'agent/inbox/spliced') this.admissions.notifyChange()
    })
    this.runtimeContext = new RuntimeContextProjection(this.ctx, session)
    this.systemPrompt = new SystemPromptProjection(session)
  }

  get status(): AgentStatus {
    return this.phase.kind === 'idle' || this.phase.kind === 'maintenance' ? 'idle' : 'running'
  }

  /** Commit a phase and publish its externally visible status transition. */
  private setPhase(next: Phase): void {
    const previousStatus = this.status
    this.phase = next
    const status = this.status
    if (status !== previousStatus) {
      this.dispatch.emit('agent/status', { status })
    }
  }

  send(message: UserMessage, target: InboxTarget, wakeup: boolean): void {
    if (this.admissions.hasSeen(message.id)) throw new Error('message identity belongs to a guarded admission')
    this.sendUnchecked(message, target, wakeup)
  }

  private sendUnchecked(message: UserMessage, target: InboxTarget, wakeup: boolean, guarded = false): void {
    // Waking input cannot join an aborted activity, so it starts the next turn.
    // Captured before the insertion so a reentrant cancel from a splice observer cannot reclassify it.
    const wakingAfterAbort = wakeup && this.phase.kind !== 'idle' && this.phase.abort.signal.aborted
    const resolvedTarget = wakingAfterAbort ? 'next-turn' : target
    if (guarded && resolvedTarget !== target) throw new Error('guarded admission target changed')
    const stopping = this.stoppingInputs.getStore()
    const continuation = !guarded && resolvedTarget === 'next-step' && stopping?.open === true
      && this.phase.kind === 'running' && this.phase.turn === stopping.turn ? stopping : undefined
    continuation?.ids.add(message.id)
    try {
      if (guarded) this.inbox.insertGuarded(resolvedTarget, message)
      else this.inbox.splice(resolvedTarget, Infinity, 0, [message])
    } catch (error: unknown) {
      continuation?.ids.delete(message.id)
      throw error
    }
    if (wakeup) this.wakeDriver(wakingAfterAbort)
  }

  /** Keep the guard installed until the Session is detached from its store. */
  disposeAdmissionGuard(): void { this.removeAdmissionGuard() }

  private validateAdmissionBatch(events: readonly SessionEvent[]): void {
    if (this.#guardedStartCallbackActive) throw new Error('Session writes are not allowed in guarded start callbacks')
    const pending = {
      'next-step': [...this.inbox.nextStep],
      'next-turn': [...this.inbox.nextTurn],
    }
    for (const event of events) {
      if (event.type === 'agent/admission/hold' || event.type === 'agent/admission/release' || event.type === 'agent/admission/cancel' || event.type === 'agent/admission/reconciled') {
        const allowed = this.allowedAdmissionEvent
        if (events.length !== 1 || allowed === undefined || event.type !== allowed.type
          || event.data.messageId !== allowed.messageId || event.data.admissionId !== allowed.admissionId) {
          throw new Error('unauthorized guarded admission event')
        }
        continue
      }
      if (event.type !== 'agent/inbox/spliced') continue
      const splice = event.data
      const queue = pending[splice.target]
      // oxlint-disable-next-line typescript/no-unnecessary-condition -- untyped callers may violate the target type.
      if (queue === undefined || !Number.isSafeInteger(splice.start) || splice.start < 0 || splice.start > queue.length
        || !Number.isSafeInteger(splice.removedCount ?? 0) || (splice.removedCount ?? 0) < 0
        || splice.start + (splice.removedCount ?? 0) > queue.length || !Array.isArray(splice.inserted)) {
        throw new Error('invalid guarded inbox splice')
      }
      const removed = queue.slice(splice.start, splice.start + (splice.removedCount ?? 0))
      if (removed.some(message => (this.admissions.hasHeld(message.id) && this.allowedGuardedDiscard !== message.id)
        || (this.admissions.hasSeen(message.id) && !this.allowedGuardedClaims?.has(message.id) && this.allowedGuardedDiscard !== message.id)
        || (this.preserveGuardedDuringDisposal && this.admissions.hasSeen(message.id)))) {
        throw new Error('cannot remove an input with an unresolved admission')
      }
      if (splice.inserted.some(message => this.admissions.hasSeen(message.id)
        && (this.allowedGuardedInput === undefined || !isDeepStrictEqual(message, this.allowedGuardedInput)))) {
        throw new Error('message identity belongs to a guarded admission')
      }
      queue.splice(splice.start, splice.removedCount ?? 0, ...splice.inserted)
    }
  }

  private appendAdmission(type: 'agent/admission/hold' | 'agent/admission/release' | 'agent/admission/cancel' | 'agent/admission/reconciled', messageId: string, admissionId: string): SessionEvent {
    if (this.allowedAdmissionEvent !== undefined) throw new Error('nested guarded admission event')
    this.allowedAdmissionEvent = { type, messageId, admissionId }
    try {
      switch (type) {
        case 'agent/admission/hold': return this.session.append('agent/admission/hold', { messageId, admissionId })
        case 'agent/admission/release': return this.session.append('agent/admission/release', { messageId, admissionId })
        case 'agent/admission/cancel': return this.session.append('agent/admission/cancel', { messageId, admissionId })
        case 'agent/admission/reconciled': return this.session.append('agent/admission/reconciled', { messageId, admissionId })
      }
    } finally { this.allowedAdmissionEvent = undefined }
  }

  setGuardedClaimCoordinator(coordinator: GuardedClaimCoordinator, ready?: Promise<void>): () => void {
    if (typeof coordinator !== 'function' || this.guardedClaimCoordinator !== undefined || this.guardedClaimFault) {
      throw new Error('guarded claim coordinator is unavailable or already installed')
    }
    const before = coordinator.beforeTurnStart, cancel = coordinator.cancelTurnStart
    const requestGuard = coordinator.beforeModelRequest
    if (requestGuard !== undefined && typeof requestGuard !== 'function') throw new Error('invalid guarded request guard')
    if ((before === undefined) !== (cancel === undefined) || (before !== undefined && typeof before !== 'function') || (cancel !== undefined && typeof cancel !== 'function')) {
      throw new Error('guarded start callbacks must be installed together')
    }
    this.guardedClaimCoordinator = coordinator
    this.guardedRequestGuard = requestGuard
    this.guardedStartCallbacks = before !== undefined && cancel !== undefined ? { before, cancel } : undefined
    this.guardedCoordinatorReady = ready
    const token = {}
    this.guardedCoordinatorToken = token
    try {
      if ([...this.inbox.nextStep, ...this.inbox.nextTurn].some(message => this.admissions.hasSeen(message.id))) this.wakeDriver()
    } catch (error: unknown) {
      if (this.guardedCoordinatorToken === token) {
        this.guardedClaimCoordinator = undefined
        this.guardedRequestGuard = undefined
        this.guardedStartCallbacks = undefined
        this.guardedCoordinatorReady = undefined
        this.guardedCoordinatorToken = undefined
        this.admissions.notifyChange()
      }
      throw error
    }
    let installed = true
    return () => {
      if (!installed) return
      installed = false
      if (this.guardedCoordinatorToken === token) {
        this.guardedClaimCoordinator = undefined
        this.guardedRequestGuard = undefined
        this.guardedStartCallbacks = undefined
        this.guardedCoordinatorReady = undefined
        this.guardedCoordinatorToken = undefined
        this.admissions.notifyChange()
      }
    }
  }

  private failGuardedClaim(): void {
    this.guardedClaimFault = true
    this.admissions.notifyChange()
  }

  private async awaitGuardedCoordinator(signal: AbortSignal): Promise<void> {
    const coordinator = this.guardedClaimCoordinator
    const token = this.guardedCoordinatorToken
    if (coordinator === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
    const ready = this.guardedCoordinatorReady
    if (ready !== undefined) {
      let stop: (() => void) | undefined
      const aborted = new Promise<never>((_resolve, reject) => {
        // oxlint-disable-next-line typescript/prefer-promise-reject-errors -- preserve the caller's exact AbortSignal reason.
        stop = () => { reject(signal.reason) }
        signal.addEventListener('abort', stop, { once: true })
        if (signal.aborted) stop()
      })
      try { await Promise.race([ready, aborted]) } finally {
        if (stop !== undefined) signal.removeEventListener('abort', stop)
      }
    }
    signal.throwIfAborted()
    // oxlint-disable-next-line typescript/no-unnecessary-condition -- coordinator state can change while awaiting readiness.
    if (this.guardedCoordinatorToken !== token || this.guardedClaimFault) throw new Error('guarded claim coordinator changed')
  }

  async guardedSend(message: UserMessage, target: InboxTarget, admissionId: string): Promise<void> {
    if (this.guardedClaimCoordinator === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
    if (target === 'next-step' && this.phase.kind !== 'idle' && this.phase.abort.signal.aborted) {
      throw new Error('guarded admission target changed')
    }
    const input = snapshotJsonValue(message)
    if (input === undefined || typeof input.id !== 'string' || input.id === '' || typeof admissionId !== 'string' || admissionId === '') {
      throw new Error('invalid guarded admission input')
    }
    if (this.admissions.hasSeen(input.id) || [...this.inbox.nextStep, ...this.inbox.nextTurn].some(item => item.id === input.id)) {
      throw new Error('guarded message identity is already in use')
    }
    const persistence = this.loopCtx.get('sessionPersistence')
    if (persistence === undefined) throw new Error('guarded admission requires session persistence')
    const insertionReceipt = persistence.beginLiveEventReceipt(this.session.id, 'agent/inbox/spliced')
    let releaseReceipt: ReturnType<typeof persistence.beginLiveEventReceipt> | undefined
    let insertion: SessionEvent | undefined
    let ambiguous = false
    let stop: (() => void) | undefined
    try {
      stop = this.loopCtx.on('session/event', (session, event) => {
        if (session !== this.session || event.type !== 'agent/inbox/spliced') return
        const matching = event.data.inserted.filter(item => item.id === input.id)
        if (matching.length === 0) return
        if (matching.length !== 1 || insertion !== undefined || !isDeepStrictEqual(matching[0], input)) ambiguous = true
        else insertion = event
      })
      releaseReceipt = persistence.beginLiveEventReceipt(this.session.id, 'agent/admission/release')
      this.admissions.assertNew(input.id, admissionId)
      this.appendAdmission('agent/admission/hold', input.id, admissionId)
      this.admissions.hold(input.id, admissionId)
      this.allowedGuardedInput = input
      try { this.sendUnchecked(input, target, true, true) } finally { this.allowedGuardedInput = undefined }
      // oxlint-disable-next-line typescript/no-unnecessary-condition -- the event observer updates this during synchronous publication.
      if (ambiguous || insertion === undefined) throw new Error('guarded insertion outcome is unknown')
      await insertionReceipt.confirm(insertion)
      if (this.admissions.isCanceled(input.id)) throw new Error('guarded admission was canceled')
      const release = this.appendAdmission('agent/admission/release', input.id, admissionId)
      await releaseReceipt.confirm(release)
      if (this.admissions.isCanceled(input.id)) throw new Error('guarded admission was canceled')
      this.admissions.release(input.id, admissionId)
    } finally {
      try { stop?.() } finally {
        try { insertionReceipt.dispose() } finally { releaseReceipt?.dispose() }
      }
    }
  }

  async inspectGuardedAdmission(sessionId: string, message: UserMessage, target: InboxTarget, admissionId: string): Promise<{
    decision: 'unknown'
    observed?: { pending: true; admission: 'held' | 'released' | 'canceled' }
  }> {
    const unknown = { decision: 'unknown' } as const
    try {
      if (sessionId !== this.session.id || typeof admissionId !== 'string' || admissionId === '') return unknown
      const expected = snapshotJsonValue(message)
      if (expected === undefined || typeof expected.id !== 'string' || expected.id === '') return unknown
      // oxlint-disable-next-line typescript/no-unnecessary-condition -- this public boundary also checks untyped callers.
      if (target !== 'next-step' && target !== 'next-turn') return unknown
      const persistence = this.loopCtx.get('sessionPersistence')
      if (persistence === undefined) return unknown
      const handle = await persistence.open(this.session.id, 'read')
      try {
        const events = (await handle.read()).events
        const state = new AdmissionState(events)
        if (state.admissionId(expected.id) !== admissionId) return unknown
        let insertionCount = 0
        let admission: 'held' | 'released' | 'canceled' = 'held'
        let inbox: InboxState = inboxProjectionDefinition.init()
        for (const event of events) {
          if (event.type === 'user/message' && event.data.id === expected.id) return unknown
          if ((event.type === 'agent/admission/release' || event.type === 'agent/admission/cancel')
            && event.data.messageId === expected.id && event.data.admissionId === admissionId) {
            admission = event.type === 'agent/admission/release' ? 'released' : 'canceled'
          }
          if (event.type === 'agent/inbox/spliced') {
            for (const inserted of event.data.inserted) {
              if (inserted.id !== expected.id) continue
              if (++insertionCount !== 1 || event.data.target !== target || !isDeepStrictEqual(inserted, expected)) return unknown
            }
          }
          inbox = inboxProjectionDefinition.apply(inbox, event)
        }
        if (insertionCount !== 1 || !inbox[target].some(item => isDeepStrictEqual(item, expected))) return unknown
        const current = target === 'next-step' ? this.inbox.nextStep : this.inbox.nextTurn
        if (!current.some(item => isDeepStrictEqual(item, expected))) return unknown
        return { decision: 'unknown', observed: { pending: true, admission } }
      } finally { await handle.close() }
    } catch {
      // Missing, malformed, or inaccessible storage never authorizes a retry.
      return unknown
    }
  }

  async reconcileGuardedAdmission(sessionId: string, message: UserMessage, target: InboxTarget, admissionId: string): Promise<void> {
    if (this.guardedClaimCoordinator === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
    const expected = snapshotJsonValue(message)
    if (expected === undefined || sessionId !== this.session.id || !this.admissions.requiresRevalidation(expected.id)
      || this.admissions.admissionId(expected.id) !== admissionId || this.reconciling.has(expected.id)) {
      throw new Error('admission cannot be reconciled')
    }
    this.reconciling.add(expected.id)
    let appended = false
    try {
      const first = await this.inspectGuardedAdmission(sessionId, expected, target, admissionId)
      if (first.observed === undefined || first.observed.admission === 'canceled') throw new Error('admission outcome is unknown')
      const persistence = this.loopCtx.get('sessionPersistence')
      if (persistence === undefined || this.loopCtx.sessions.get(this.session.id) !== this.session) throw new Error('admission session is unavailable')
      const seq = this.session.seq
      await persistence.flushLiveSession(this.session.id)
      if (this.loopCtx.sessions.get(this.session.id) !== this.session || this.session.seq !== seq) throw new Error('admission session changed during flush')
      const checked = await this.inspectGuardedAdmission(sessionId, expected, target, admissionId)
      if (this.loopCtx.sessions.get(this.session.id) !== this.session || this.session.seq !== seq
        || checked.observed === undefined || checked.observed.admission === 'canceled' || this.admissions.isCanceled(expected.id)) {
        throw new Error('admission changed before reconciliation')
      }
      const eventType = checked.observed.admission === 'held' ? 'agent/admission/release' : 'agent/admission/reconciled'
      const receipt = persistence.beginLiveEventReceipt(this.session.id, eventType)
      try {
        if (this.admissions.isCanceled(expected.id)) throw new Error('admission was canceled')
        const event = this.appendAdmission(eventType, expected.id, admissionId)
        appended = true
        await receipt.confirm(event)
        const pending = target === 'next-step' ? this.inbox.nextStep : this.inbox.nextTurn
        if (this.admissions.isCanceled(expected.id) || !pending.some(item => isDeepStrictEqual(item, expected))) {
          throw new Error('admission changed during reconciliation')
        }
        if (eventType === 'agent/admission/release') this.admissions.release(expected.id, admissionId)
        this.admissions.revalidate(expected.id, admissionId)
      } finally { receipt.dispose() }
    } catch (error: unknown) {
      if (appended) this.failGuardedClaim()
      else this.admissions.requireRevalidation(expected.id)
      throw error
    } finally { this.reconciling.delete(expected.id) }
  }

  async cancelGuardedAdmission(sessionId: string, message: UserMessage, target: InboxTarget, admissionId: string): Promise<void> {
    const expected = snapshotJsonValue(message)
    if (expected === undefined || sessionId !== this.session.id || this.admissions.admissionId(expected.id) !== admissionId
      || this.reconciling.has(expected.id) || this.guardedClaimFault) throw new Error('guarded cancellation outcome is unknown')
    const pending = (): readonly UserMessage[] => target === 'next-step' ? this.inbox.nextStep : this.inbox.nextTurn
    // oxlint-disable-next-line typescript/no-unnecessary-condition -- this public boundary also checks untyped callers.
    if ((target !== 'next-step' && target !== 'next-turn') || !pending().some(item => isDeepStrictEqual(item, expected))) {
      throw new Error('guarded cancellation outcome is unknown')
    }
    const persistence = this.loopCtx.get('sessionPersistence')
    if (persistence === undefined) throw new Error('guarded cancellation requires persistence')
    const cancelReceipt = persistence.beginLiveEventReceipt(this.session.id, 'agent/admission/cancel')
    let spliceReceipt: ReturnType<typeof persistence.beginLiveEventReceipt> | undefined
    let stop: (() => void) | undefined
    let appended = false
    this.reconciling.add(expected.id)
    try {
      spliceReceipt = persistence.beginLiveEventReceipt(this.session.id, 'agent/inbox/spliced')
      let removed: SessionEvent | undefined
      let ambiguous = false
      stop = this.loopCtx.on('session/event', (session, event) => {
        if (session !== this.session || event.type !== 'agent/inbox/spliced' || event.data.outcome !== 'canceled') return
        if (removed !== undefined) ambiguous = true
        else removed = event
      })
      const tombstone = this.appendAdmission('agent/admission/cancel', expected.id, admissionId)
      appended = true
      this.admissions.cancel(expected.id, admissionId)
      await cancelReceipt.confirm(tombstone)
      if (!pending().some(item => isDeepStrictEqual(item, expected))) throw new Error('guarded cancellation outcome is unknown')
      this.allowedGuardedDiscard = expected.id
      try {
        if (!this.inbox.remove(expected.id)) throw new Error('guarded cancellation outcome is unknown')
      } finally { this.allowedGuardedDiscard = undefined }
      // oxlint-disable-next-line typescript/no-unnecessary-condition -- the event observer updates this during synchronous publication.
      if (ambiguous || removed === undefined) throw new Error('guarded cancellation removal is unknown')
      await spliceReceipt.confirm(removed)
      this.admissions.notifyChange()
    } catch (error: unknown) {
      if (appended) this.failGuardedClaim()
      else this.admissions.requireRevalidation(expected.id)
      throw error
    } finally {
      this.reconciling.delete(expected.id)
      try { stop?.() } finally { try { spliceReceipt?.dispose() } finally { cancelReceipt.dispose() } }
    }
  }

  followup(input: UserMessage): void {
    this.send(input, 'next-turn', true)
  }

  steer(input: UserMessage): void {
    this.send(input, 'next-step', true)
  }

  inject(input: UserMessage): void {
    this.send(input, 'next-step', false)
  }

  cancel(cause: AgentCancelCause, options: CancelOptions = {}): void {
    try {
      if (!options.keepInbox) {
        if (cause.kind !== 'disposed') {
          for (const message of [...this.inbox.nextStep, ...this.inbox.nextTurn]) {
            const admissionId = this.admissions.admissionId(message.id)
            if (admissionId === undefined || this.admissions.isCanceled(message.id)) continue
            try { this.appendAdmission('agent/admission/cancel', message.id, admissionId) } catch (error: unknown) {
              this.admissions.requireRevalidation(message.id)
              throw error
            }
            try { this.admissions.cancel(message.id, admissionId) } catch (error: unknown) {
              this.failGuardedClaim()
              throw error
            }
          }
        }
        this.preserveGuardedDuringDisposal = cause.kind === 'disposed'
        try { this.inbox.clear() } finally { this.preserveGuardedDuringDisposal = false }
      }
    } finally {
      // Queue persistence must not prevent cancellation of admitted work.
      if (this.phase.kind !== 'idle') {
        if (!options.keepInbox) this.phase.wakeRequested = false
        this.phase.abort.abort(cause)
      }
    }
  }

  runMaintenance<T>(job: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (this.phase.kind !== 'idle') throw new Error(`agent "${this.id}" already has active work`)
    const done = Promise.withResolvers<void>()
    const maintenance: Phase = {
      kind: 'maintenance',
      abort: new AbortController(),
      lastTurn: this.phase.lastTurn,
      wakeRequested: false,
    }
    this.setPhase(maintenance)
    this.activityDone = done.promise
    return (async () => {
      try {
        return await job(maintenance.abort.signal)
      } finally {
        this.setPhase({ kind: 'idle', lastTurn: maintenance.lastTurn })
        const cause = abortedCancelCause(maintenance.abort.signal)
        try {
          if (cause?.kind !== 'disposed' && maintenance.wakeRequested && this.inbox.hasPending) this.wakeDriver()
        } finally {
          done.resolve()
        }
      }
    })()
  }

  /**
   * Start one driver, or latch its wake behind maintenance or an aborted
   * activity. A wake sent while idle always opens its turn boundary, even
   * when its message was cleared; only a latched replay is suppressed when
   * the queue no longer holds the wake.
   * @param wakeAfterAbort - the {@link send} classification, captured before
   *   the inbox insertion so a reentrant cancel cannot reclassify it.
   */
  private wakeDriver(wakeAfterAbort = false): void {
    if (this.lifetimeSignal.aborted) return
    if (this.phase.kind !== 'idle') {
      // Maintenance and aborted drivers cannot deliver the wake: latch it for
      // replay at convergence. Live drivers claim queued work themselves;
      // disposal never latches, so teardown waits on no model turn.
      const reason = abortedCancelCause(this.phase.abort.signal)
      if (reason?.kind !== 'disposed' && (this.phase.kind === 'maintenance' || wakeAfterAbort)) {
        this.phase.wakeRequested = true
      }
      return
    }
    const driver = Promise.withResolvers<void>()
    const lastTurn = this.phase.lastTurn
    this.activityDone = driver.promise
    this.setPhase({
      kind: 'running',
      abort: new AbortController(),
      turn: lastTurn,
      step: 0,
      wakeRequested: false,
    })
    try {
      this.loopCtx.agents.withInitiator(this, () => this.kick()).then(driver.resolve, driver.reject)
    } catch (error: unknown) {
      // The initiator scope may close between the lifetime check and this
      // boundary. No driver started, so restore idle and release all waiters.
      this.setPhase({ kind: 'idle', lastTurn })
      driver.resolve()
      throw error
    }
  }

  async whenIdle(): Promise<void> {
    let activity: Promise<void>
    do {
      await (activity = this.activityDone)
    } while (activity !== this.activityDone)
  }

  /** Report one failure at its live boundary, then preserve it for driver containment. */
  private throwError(error: unknown): never {
    const turn = this.phase.kind === 'running' ? this.phase.turn : this.phase.lastTurn
    const step = this.phase.kind === 'running' ? this.phase.step : 0
    this.dispatch.emit('agent/error', { turn, step, error })
    throw error
  }

  private async kick(): Promise<void> {
    try {
      while (await this.turn()) {}
    } catch (_error) {
      // Reported failures and cancellation are contained at the driver boundary.
    } finally {
      /* v8 ignore next -- kick owns a running phase until this driver boundary */
      if (this.phase.kind === 'running') {
        const { turn, wakeRequested, abort } = this.phase
        const cause = abortedCancelCause(abort.signal)
        this.setPhase({ kind: 'idle', lastTurn: turn })
        if (cause?.kind !== 'disposed' && wakeRequested && this.inbox.hasPending) this.wakeDriver()
      }
    }
  }

  private proposedClaim(target: InboxTarget, turnKind: TurnKind, continuationIds: ReadonlySet<string>): ProposedInput[] {
    const proposed = [
      ...this.inbox.nextStep.map((message, index) => ({ message, target: 'next-step' as const, index })),
      ...(target === 'next-turn' ? this.inbox.nextTurn.slice(0, 1).map(message => ({ message, target: 'next-turn' as const, index: 0 })) : []),
    ]
    if (turnKind === 'guarded') return proposed.filter(item => item.target === 'next-step' && continuationIds.has(item.message.id))
    const firstGuarded = proposed.findIndex(item => this.admissions.hasSeen(item.message.id))
    if (firstGuarded < 0) return proposed
    if (turnKind === 'ordinary') return proposed.filter((item, index) => index < firstGuarded
      || (item.target === 'next-step' && continuationIds.has(item.message.id)))
    return proposed.slice(0, firstGuarded === 0 ? 1 : firstGuarded)
  }

  private async preStep(
    target: InboxTarget, position: { turn: number; step: number; turnStartSeq: number }, turnKind: TurnKind, continuationIds: Set<string>,
    initialGuarded?: readonly ProposedInput[],
    initialCoordinatorToken?: object,
  ): Promise<PreparedStep> {
    /* v8 ignore next -- private callers establish the running phase before proposing a step */
    if (this.phase.kind !== 'running') throw new Error(`agent "${this.id}": pre-step outside running phase`)
    const signal = this.phase.abort.signal
    let proposed: ReturnType<ReactLoopAgent['proposedClaim']>
    while (true) {
      proposed = this.proposedClaim(target, turnKind, continuationIds)
      if (turnKind === 'initial' && ((initialGuarded !== undefined && (!isDeepStrictEqual(proposed, initialGuarded) || initialCoordinatorToken !== this.guardedCoordinatorToken))
        || (initialGuarded === undefined && proposed.some(item => this.admissions.hasSeen(item.message.id))))) {
        for (const item of [...(initialGuarded ?? []), ...proposed]) this.admissions.requireRevalidation(item.message.id)
        this.phase.wakeRequested = true
        throw new Error('guarded turn candidate changed before claim')
      }
      const candidates = proposed.map(item => item.message.id)
      if (this.admissions.isBlocked(candidates)) {
        if (this.guardedClaimCoordinator === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
        await this.admissions.waitForChange(signal)
        continue
      }
      signal.throwIfAborted()
      if (candidates.some(id => this.admissions.hasSeen(id))) {
        await this.awaitGuardedCoordinator(signal)
        if (!isDeepStrictEqual(proposed, this.proposedClaim(target, turnKind, continuationIds))
          || (turnKind === 'initial' && initialGuarded !== undefined && initialCoordinatorToken !== this.guardedCoordinatorToken)) continue
      }
      break
    }
    const guarded: GuardedClaimInput[] = []
    for (const item of proposed) {
      const admissionId = this.admissions.admissionId(item.message.id)
      if (admissionId !== undefined) {
        const message = snapshotJsonValue(item.message)
        if (message === undefined) throw new Error('guarded input is not lossless JSON')
        guarded.push(deepFreeze({ admissionId, message, target: item.target }))
      }
    }
    let claimed: UserMessage[]
    const stepIndices = proposed.filter(item => item.target === 'next-step').map(item => item.index)
    const takeTurn = proposed.some(item => item.target === 'next-turn')
    const wholeBatch = stepIndices.length === this.inbox.nextStep.length
      && takeTurn === (target === 'next-turn' && this.inbox.nextTurn.length > 0)
    if (guarded.length === 0) {
      claimed = wholeBatch ? this.inbox.claim(target, position.turn) : this.inbox.claimSelected(stepIndices, takeTurn, position.turn)
    } else {
      const coordinator = this.guardedClaimCoordinator
      const token = this.guardedCoordinatorToken
      if (coordinator === undefined || token === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
      if (turnKind === 'initial' && initialGuarded !== undefined && token !== initialCoordinatorToken) throw new Error('guarded turn coordinator changed before claim')
      const original = snapshotJsonValue(proposed)
      if (original === undefined) throw new Error('guarded claim batch is not lossless JSON')
      const allowed = new Set(guarded.map(item => item.message.id))
      let called = false
      let closed = false
      let violated = false
      let result: UserMessage[] | undefined
      try {
        const decision: unknown = coordinator(this.session.id, deepFreeze(guarded), () => {
          if (closed || called) { violated = true; throw new Error('guarded claim called outside its single-use transaction') }
          called = true
          signal.throwIfAborted()
          if (this.guardedCoordinatorToken !== token || this.guardedClaimFault) throw new Error('guarded claim coordinator changed')
          const current = this.proposedClaim(target, turnKind, continuationIds)
          if (!isDeepStrictEqual(current, original)) throw new Error('guarded claim batch changed during decision')
          this.allowedGuardedClaims = allowed
          try { result = this.inbox.claimSelected(stepIndices, takeTurn, position.turn) } finally { this.allowedGuardedClaims = undefined }
        }, Object.freeze({ turn: position.turn, turnStartSeq: position.turnStartSeq }))
        closed = true
        if (decision instanceof Promise) void decision.catch(() => {})
        // oxlint-disable-next-line typescript/no-unnecessary-condition -- the coordinator calls the claim callback reentrantly.
        if (decision !== 'committed' || !called || violated || result === undefined) throw new Error('guarded claim decision was not committed')
        // oxlint-disable-next-line typescript/no-unnecessary-condition -- the coordinator can synchronously detach or fault.
        if (this.guardedCoordinatorToken !== token || this.guardedClaimFault) throw new Error('guarded claim coordinator changed')
        signal.throwIfAborted()
        claimed = result
      } catch (error: unknown) {
        closed = true
        this.failGuardedClaim()
        throw error
      }
    }
    for (const message of claimed) continuationIds.delete(message.id)
    const selection = this.model.current()
    if (selection === undefined) {
      throw new Error(`agent "${this.id}" has no provider/model: set AgentOptions.provider and AgentOptions.model or record a model selection`)
    }
    const toolSet = await this.loopCtx.tools.resolve({ agent: this, modelSelection: selection, signal })
    try {
      const assembly = await this.loopCtx.systemPrompt.assemble({ ...assembleContextFor(this, signal), modelSelection: selection, toolSet })
      signal.throwIfAborted()
      const decision = await this.dispatch.waterfall(
        'agent/pre-step', { messages: claimed, turn: position.turn, step: position.step, signal, modelSelection: selection },
        (): Promise<PreStepDecision> => {
          const sections = renderContextSections(assembly)
          const context = this.runtimeContext.project(sections)
          return Promise.resolve<PreStepDecision>({
            kind: 'enter',
            messages: context === undefined ? claimed : [...claimed, context],
          })
        },
      )
      signal.throwIfAborted()
      if (decision.kind === 'reject') {
        toolSet.dispose()
        return decision
      }
      return { ...decision, claimedGuarded: guarded.length > 0, assembly, selection, toolSet }
    } catch (error: unknown) {
      toolSet.dispose()
      throw error
    }
  }

  /** Whether the assembled tool schemas differ from the logged request header's. */
  private toolsChanged(tools: PromptAssembly['tools']): boolean {
    const baseline = this.session.requestHeader()
    if (baseline === undefined) return false
    return !headerEquals(baseline, canonicalHeader({ ...baseline, tools: [...tools] }))
  }

  /** Open one turn before claiming its first proposed step. */
  private async turn(): Promise<boolean> {
    if (this.phase.kind !== 'running') {
      this.throwError(new Error(`agent "${this.id}": turn without driver reservation`))
    }
    if (this.guardedClaimFault) throw new Error('guarded claim outcome is unknown')
    const phase = this.phase
    const { signal } = phase.abort
    signal.throwIfAborted()
    const continuations = new Set<string>()
    let initialGuarded: readonly ProposedInput[] | undefined
    let initialCoordinatorToken: object | undefined
    while (true) {
      const proposed = this.proposedClaim('next-turn', 'initial', continuations)
      const candidates = proposed.map(item => item.message.id)
      if (this.admissions.isBlocked(candidates)) {
        // oxlint-disable-next-line typescript/no-unnecessary-condition -- coordinator state can change between asynchronous waits.
        if (this.guardedClaimCoordinator === undefined || this.guardedClaimFault) throw new Error('guarded claim coordinator is unavailable')
        await this.admissions.waitForChange(signal)
        continue
      }
      signal.throwIfAborted()
      if (candidates.some(id => this.admissions.hasSeen(id))) {
        await this.awaitGuardedCoordinator(signal)
        if (!isDeepStrictEqual(proposed, this.proposedClaim('next-turn', 'initial', continuations))) continue
        const snapshot = snapshotJsonValue(proposed)
        if (snapshot === undefined || snapshot.length !== 1 || this.admissions.admissionId(snapshot[0]!.message.id) === undefined) {
          throw new Error('guarded turn requires one stable input')
        }
        initialGuarded = deepFreeze(snapshot)
        initialCoordinatorToken = this.guardedCoordinatorToken
        if (initialCoordinatorToken === undefined) throw new Error('guarded coordinator is unavailable')
      }
      break
    }
    const turn = phase.turn + 1
    let turnStartSeq: number
    let registeredStart: { coordinator: GuardedClaimCoordinator, cancel: NonNullable<GuardedClaimCoordinator['cancelTurnStart']>, input: Readonly<GuardedClaimInput>, turn: Readonly<{ turn: number, turnStartSeq: number }> } | undefined
    try {
      if (initialGuarded !== undefined) {
        const candidate = initialGuarded[0]!
        const admissionId = this.admissions.admissionId(candidate.message.id), coordinator = this.guardedClaimCoordinator
        if (admissionId === undefined || coordinator === undefined) throw new Error('guarded turn identity is unavailable')
        const callbacks = this.guardedStartCallbacks
        if (callbacks !== undefined) {
          const input = Object.freeze({ admissionId, message:candidate.message, target:candidate.target })
          const boundary = Object.freeze({ turn, turnStartSeq:this.session.seq })
          registeredStart = { coordinator, cancel:callbacks.cancel, input, turn:boundary }
          let result: unknown
          this.#guardedStartCallbackActive = true
          try { result = callbacks.before.call(coordinator, this.session, input, boundary) }
          finally { this.#guardedStartCallbackActive = false }
          if (result !== undefined) {
            this.failGuardedClaim()
            if (result instanceof Promise) void result.catch(() => {})
            throw new Error('guarded start registration must be synchronous')
          }
          if (this.guardedClaimCoordinator !== coordinator || this.guardedCoordinatorToken !== initialCoordinatorToken || signal.aborted || this.session.seq !== boundary.turnStartSeq
            || !isDeepStrictEqual(initialGuarded, this.proposedClaim('next-turn', 'initial', continuations))) throw new Error('guarded start changed before publication')
        }
      }
      turnStartSeq = this.session.append('turn/start', { turn }).seq
    } catch (error: unknown) {
      if (registeredStart !== undefined) {
        try {
          let result: unknown
          this.#guardedStartCallbackActive = true
          try { result = registeredStart.cancel.call(registeredStart.coordinator, this.session, registeredStart.input, registeredStart.turn) }
          finally { this.#guardedStartCallbackActive = false }
          if (result !== undefined) {
            if (result instanceof Promise) void result.catch(() => {})
            throw new Error('guarded start cleanup must be synchronous')
          }
        } catch (cleanupError: unknown) {
          this.failGuardedClaim()
          this.throwError(new AggregateError([error, cleanupError], 'guarded start cleanup failed', { cause:error }))
        }
      }
      if (initialGuarded !== undefined) {
        this.admissions.requireRevalidation(initialGuarded[0]!.message.id)
        phase.wakeRequested = true
      }
      this.throwError(error)
    }
    phase.turn = turn
    let turnEnds: TurnEndReason | null = null
    let target: InboxTarget = 'next-turn'
    let turnKind: TurnKind = 'initial'
    const stoppingInputs: StoppingInputs = { turn, ids: new Set(), open: false }
    try {
      while (true) {
        signal.throwIfAborted()
        const step = phase.step + 1
        const decision = await this.preStep(target, { turn, step, turnStartSeq }, turnKind, stoppingInputs.ids, initialGuarded, initialCoordinatorToken)
        if (decision.kind === 'reject') {
          turnEnds = { kind: 'blocked' }
          return false
        }
        if (turnKind === 'initial') turnKind = decision.claimedGuarded ? 'guarded' : 'ordinary'
        try {
          if (turnEnds && decision.messages.length === 0) break
          // A removed waking message or an enter decision rewritten to empty
          // still owns the initial turn boundary, but it spends no model call.
          if (phase.step === 0 && decision.messages.length === 0) {
            turnEnds = { kind: 'completed' }
            return false
          }
          signal.throwIfAborted()
          this.session.append('step/start', { turn, step })
          phase.step = step
          const toolRecovery = new ToolCallRecovery()
          const stopRecovery = this.ctx.on('session/event', (session, event) => {
            if (session === this.session) toolRecovery.observe(event)
          })
          try {
            // max-tokens is sticky: once any step hits the ceiling, later steps
            // that complete normally must not downgrade the turn outcome.
            const stepEnd = await this.step(decision, turnKind, initialCoordinatorToken)
            // max-tokens stays sticky: a later completed step must not
            // downgrade the turn outcome.
            if (turnEnds === null || turnEnds.kind !== 'max-tokens') turnEnds = stepEnd
          } catch (error: unknown) {
            try {
              for (const event of toolRecovery.results()) {
                this.session.append('tool/result', event.data, {
                  surfaceOp: 'append',
                  ...event.sourceEventSeqs === undefined ? {} : { sourceEventSeqs: event.sourceEventSeqs },
                })
              }
            } catch (recoveryError: unknown) {
              throw new AggregateError([error, recoveryError], 'Step failed and its pending tool results could not be recorded', { cause: error })
            }
            throw error
          } finally {
            stopRecovery()
            this.session.append('step/end', { turn, step })
          }
        } finally {
          // Admission closes once the step's tool batch settles or no step opens.
          decision.toolSet.dispose()
        }
        signal.throwIfAborted()
        if (turnEnds && this.proposedClaim('next-step', turnKind, stoppingInputs.ids).length === 0) {
          stoppingInputs.open = true
          try { await this.stoppingInputs.run(stoppingInputs, () => this.dispatch.serial('agent/turn-stopping', { turn, signal })) } finally { stoppingInputs.open = false }
          signal.throwIfAborted()
        }
        if (turnEnds && this.proposedClaim('next-step', turnKind, stoppingInputs.ids).length === 0) break
        target = 'next-step'
      }
    } catch (error: unknown) {
      // A cause is present exactly while the signal is aborted.
      const cause = abortedCancelCause(signal)
      if (cause !== undefined) {
        turnEnds = { kind: 'aborted', reason: cause }
        throw error
      }
      // Every failure is structured: an `LlmError` keeps its facts, a tool
      // activation failure keeps its code, and anything else flattens to
      // `errorChain` text under the `UNKNOWN` code.
      turnEnds = {
        kind: 'error',
        error: error instanceof LlmError
          ? error.failure
          : { message: errorChain(error), code: error instanceof ToolActivationError ? error.code : 'UNKNOWN' },
      }
      this.throwError(error)
    } finally {
      try {
        // oxlint-disable-next-line typescript/no-non-null-assertion -- every exit assigns a turn ending
        this.session.append('turn/end', { turn, reason: turnEnds! })
      } catch (error: unknown) {
        this.throwError(error)
      }
    }
    if (!this.inbox.hasPending) return false
    phase.abort = new AbortController()
    // A fresh controller makes a latch set on the old one stale: the live driver claims the queue itself.
    phase.wakeRequested = false
    phase.step = 0
    return true
  }

  private async step(decision: Extract<PreparedStep, { kind: 'enter' }>, turnKind: TurnKind, coordinatorToken: object | undefined): Promise<StepEndReason | null> {
    /* v8 ignore next -- private callers establish the running phase before executing a step */
    if (this.phase.kind !== 'running') throw new Error(`agent "${this.id}": step outside running phase`)
    const { turn, step, abort: { signal } } = this.phase
    signal.throwIfAborted()

    const { assembly } = decision
    const renderedPrompt = renderPrompt(assembly)
    let firstAttempt = true
    while (true) {
      const { config, preparedCall } = await this.prepareRequest(turn, step, decision.selection, signal)
      const currentContext = this.loopCtx.systemPrompt.refreshContext(assembly, {
        ...assembleContextFor(this, signal), modelSelection: decision.selection,
      })
      const sections = renderContextSections(currentContext)
      const context = this.runtimeContext.project(sections)
      signal.throwIfAborted()
      const startsRequestSeries = firstAttempt && decision.startsRequestSeries === true
      const commits = this.systemPrompt.project(renderedPrompt, {
        inHistory: preparedCall?.systemPromptUpdate === 'in-history',
        startsSeries: startsRequestSeries
          || this.requestSurfaceGeneration !== this.session.surface.contentGeneration
          || (preparedCall?.toolUpdate === undefined && this.toolsChanged(assembly.tools)),
      })
      for (const { message, intent } of commits) {
        this.session.append('system/message', { turn, step, message }, intent)
      }
      let contextAdmitted = false
      if (firstAttempt) {
        for (const message of decision.messages) {
          if (message.source.kind === 'runtime-context') {
            if (context !== undefined && !contextAdmitted) {
              this.session.append('user/message', context, { surfaceOp: 'append' })
              contextAdmitted = true
            }
          } else {
            this.session.append('user/message', message, { surfaceOp: 'append' })
          }
        }
      }
      if (context !== undefined && !contextAdmitted) {
        this.session.append('user/message', context, { surfaceOp: 'append' })
      }
      firstAttempt = false
      const request = this.buildRequest(config, preparedCall, assembly.tools, { turn, step }, startsRequestSeries, signal)
      if (turnKind === 'guarded') {
        const coordinator = this.guardedClaimCoordinator, guard = this.guardedRequestGuard
        if (coordinator === undefined || coordinatorToken === undefined || this.guardedCoordinatorToken !== coordinatorToken || this.guardedClaimFault) {
          throw new Error('guarded request authority is unavailable')
        }
        if (guard !== undefined) {
          const seq = this.session.seq
          let accepted: unknown
          this.#guardedStartCallbackActive = true
          try { accepted = guard.call(coordinator, this.session, request, Object.freeze({ turn, step })) }
          finally { this.#guardedStartCallbackActive = false }
          if (accepted !== true || this.guardedClaimCoordinator !== coordinator || this.guardedRequestGuard !== guard
            || this.guardedCoordinatorToken !== coordinatorToken || this.guardedClaimFault || this.session.seq !== seq || signal.aborted) {
            if (accepted instanceof Promise) void accepted.catch(() => {})
            throw new Error('guarded model request was not authorized')
          }
        }
      }
      const live = new AssistantStreamAttempt(
        this.session.id,
        ++this.assistantAttemptCounter,
        () => ++this.assistantStreamRevision,
        turn,
        step,
        (frame) => { this.dispatch.emit('agent/assistant-stream', { frame }) },
      )
      let started = false
      try {
        const stream = preparedCall?.stream(request) ?? this.loopCtx.llm.stream(request)
        signal.throwIfAborted()
        live.start()
        started = true
        for await (const chunk of stream) {
          signal.throwIfAborted()
          live.push(chunk)
        }
        signal.throwIfAborted()
      } catch (error: unknown) {
        if (!started) throw error
        try {
          if (signal.aborted) {
            const content = live.interruptedBlocks()
            if (content.length > 0) {
              live.settle('assistant/message', () => this.session.append('assistant/message', {
                turn,
                step,
                message: createAssistantMessage({
                  content,
                  source: {
                    provider: request.provider,
                    model: request.model,
                    ...live.replayState === undefined ? {} : { replayState: live.replayState },
                  },
                }),
                interrupted: true,
                ...live.usage === undefined ? {} : { usage: live.usage },
                stream: live.stream,
              }, { surfaceOp: 'append' }).seq)
            } else {
              live.settle(
                'assistant/attempt',
                () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
              )
            }
          } else {
            live.settle(
              'assistant/attempt',
              () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
            )
          }
        } catch (settlementError: unknown) {
          throw new AggregateError(
            [error, settlementError],
            'Assistant stream failed and its durable settlement was rejected',
            { cause: error },
          )
        }
        throw error
      }
      try {
        const finish = live.finish
        if (finish.kind === 'error' || finish.kind === 'aborted') {
          live.settle(
            'assistant/attempt',
            () => this.session.append('assistant/attempt', { turn, step, stream: live.stream }).seq,
          )
          const action = await this.dispatch.waterfall(
            'agent/request-error', {
              turn,
              step,
              provider: request.provider,
              failure: finish.failure,
              retryPolicy: preparedCall?.retryPolicy,
              signal,
            },
            () => Promise.resolve<RequestErrorAction>(undefined),
          )
          signal.throwIfAborted()
          if (action?.kind !== 'retry') {
            throw LlmError.fromFailure(finish.failure)
          }
          continue
        }

        const message = createAssistantMessage({
          content: live.blocks(),
          source: {
            provider: request.provider,
            model: request.model,
            ...live.replayState !== undefined ? { replayState: live.replayState } : {},
          },
        })
        live.settle(
          'assistant/message',
          () => this.session.append('assistant/message', {
            turn,
            step,
            message,
            ...live.usage === undefined ? {} : { usage: live.usage },
            stream: live.stream,
          }, { surfaceOp: 'append' }).seq,
        )
        if (finish.kind === 'max-tokens') return { kind: 'max-tokens' }

        const toolCalls = message.content.filter(block => block.type === 'tool-call')
        if (toolCalls.length === 0) return { kind: 'completed' }
        const maxParallelToolCalls = this.loopCtx.agentLoop.config.maxParallelToolCalls.get()
        try {
          const result = await this.loopCtx.tools.execute({
            calls: toolCalls, signal, agent: this, log: { turn, step },
            maxParallelToolCalls, toolSet: decision.toolSet,
          })
          for (const context of result.additionalContexts) {
            this.inbox.splice('next-step', this.inbox.nextStep.length, 0, [context])
          }
          return result.concluded ? { kind: 'completed' } : null
        } catch (error: unknown) {
          if (!(error instanceof ToolBatchExecutionError)) throw error
          for (const context of error.partial.additionalContexts) {
            this.inbox.splice('next-step', this.inbox.nextStep.length, 0, [context])
          }
          throw error.cause
        }
      } catch (error: unknown) {
        if (!live.ended) live.abandon()
        throw error
      }
    }
  }

  /** Resolve request config for the step's selection and bind its adapter before admitting model-visible input. */
  private async prepareRequest(
    turn: number,
    step: number,
    selection: Readonly<ModelSelection>,
    signal: AbortSignal,
  ): Promise<{ config: LlmCallConfig; preparedCall?: PreparedLlmCall }> {
    const { session } = this

    // A loop instance seeds output controls from its options until it logs its
    // own header; later steps re-resolve marked defaults.
    const maxTokens = this.options.maxTokens
    const persisted: Partial<LlmCallConfig> = this.requestHeaderLogged
      // oxlint-disable-next-line typescript/no-non-null-assertion -- the instance logged the header it now folds
      ? requestProposal(session.requestHeader()!)
      : maxTokens === undefined ? {} : { maxTokens }
    const { provider: _provider, model: _model, reasoningEffort: _effort, ...controls } = persisted
    const seedConfig = deepFreeze(structuredClone({ ...controls, ...selection }))
    const proposedConfig = await this.dispatch.waterfall(
      'agent/request', { turn, step, signal },
      () => Promise.resolve(seedConfig),
    )
    signal.throwIfAborted()
    if (proposedConfig.provider !== selection.provider || proposedConfig.model !== selection.model) {
      throw new Error(`agent "${this.id}": agent/request proposed "${proposedConfig.provider}/${proposedConfig.model}" after step preparation fixed "${selection.provider}/${selection.model}"; use agent.model.select() before step preparation`)
    }
    if (proposedConfig.reasoningEffort !== selection.reasoningEffort) {
      throw new Error(`agent "${this.id}": agent/request proposed reasoning effort "${proposedConfig.reasoningEffort ?? 'adapter default'}" after step preparation fixed "${selection.reasoningEffort ?? 'adapter default'}"; use agent.model.select() before step preparation`)
    }
    let config: LlmCallConfig
    let preparedCall: PreparedLlmCall | undefined
    try {
      preparedCall = await this.loopCtx.llm.prepareCall(proposedConfig, signal)
      config = preparedCall.config
    } catch (error: unknown) {
      // Middleware may serve an unregistered route; terminal dispatch still requires an adapter.
      if (!(error instanceof LlmError) || error.code !== 'NO_ADAPTER') throw error
      config = proposedConfig
    }
    signal.throwIfAborted()
    return { config, ...preparedCall === undefined ? {} : { preparedCall } }
  }

  /** Log the resolved envelope and derive a frozen request from the admitted surface. */
  private buildRequest(
    config: LlmCallConfig,
    preparedCall: PreparedLlmCall | undefined,
    tools: GenerateOptions['tools'] & object,
    position: { turn: number; step: number },
    startsRequestSeries: boolean,
    signal: AbortSignal,
  ): GenerateOptions {
    const { session } = this
    const surfaceGeneration = session.surface.contentGeneration
    const header = canonicalHeader({
      config,
      ...preparedCall === undefined ? {} : { adapterDefaults: preparedCall.adapterDefaults },
      ...tools.length > 0 ? { tools } : {},
    })
    const baseline = this.session.requestHeader()
    const startsSeries = startsRequestSeries
      || this.requestSurfaceGeneration !== surfaceGeneration
    let headerSeq: SessionSeq | undefined
    if (!this.requestHeaderLogged) {
      // Compaction during the first resumed pre-step must still mark a new series.
      headerSeq = this.session.append('request/header', {
        header,
        reason: baseline === undefined ? 'initial' : 'resume',
        ...startsSeries ? { startsSeries: true } : {},
      }).seq
      this.requestHeaderLogged = true
    } else if (baseline === undefined || !headerEquals(baseline, header)) {
      headerSeq = this.session.append('request/header', {
        header,
        reason: 'change',
        ...startsSeries ? { startsSeries: true } : {},
      }).seq
    } else if (startsSeries) {
      this.session.append('request/header', { header, reason: 'series' })
    }
    if (baseline !== undefined && headerSeq !== undefined) {
      const previousNames = new Set(baseline.tools?.map(tool => tool.name))
      const currentNames = new Set(tools.map(tool => tool.name))
      const additions = tools.filter(tool => !previousNames.has(tool.name))
        .map(tool => ({ type: 'tool-addition' as const, toolName: tool.name }))
      const removals = (baseline.tools ?? []).filter(tool => !currentNames.has(tool.name))
        .map(tool => ({ type: 'tool-removal' as const, toolName: tool.name }))
      if (additions.length > 0 || removals.length > 0) {
        session.append('developer/message', {
          ...position,
          message: createDeveloperMessage({ source: { kind: 'tool-registry' }, content: [...additions, ...removals] }),
          ...additions.length > 0 ? { headerSeq } : {},
        }, { surfaceOp: 'append' })
      }
    }
    this.requestSurfaceGeneration = surfaceGeneration

    const contextWindow = preparedCall?.context?.contextWindow
    const systemPromptUpdate = preparedCall?.systemPromptUpdate
    const requestContext: RequestContext = {
      provider: config.provider,
      model: config.model,
      ...contextWindow === undefined ? {} : { contextWindow },
      ...systemPromptUpdate === undefined ? {} : { systemPromptUpdate },
    }
    const previousContext = session.requestContext()
    if (previousContext?.provider !== requestContext.provider
      || previousContext.model !== requestContext.model
      || previousContext.contextWindow !== requestContext.contextWindow
      || previousContext.systemPromptUpdate !== requestContext.systemPromptUpdate) {
      session.append('request/context', requestContext)
    }
    signal.throwIfAborted()

    // canonicalHeader is shallow; append logs a detached snapshot, not these local values.
    deepFreeze(header)
    const boundaryMessages = session.deriveMessages()
    for (const message of boundaryMessages) {
      if (this.frozenMessages.has(message)) continue
      deepFreeze(message)
      this.frozenMessages.add(message)
    }
    Object.freeze(boundaryMessages)
    const request = markAgentLoopRequest(Object.freeze({
      ...header.config,
      messages: boundaryMessages,
      toolHistory: session.toolHistory(),
      ...header.tools !== undefined ? { tools: header.tools } : {},
      sessionId: this.session.id,
      signal,
    }))
    return request
  }
}
