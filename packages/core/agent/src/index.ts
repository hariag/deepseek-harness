/**
 * Agent service: live registry, factory delegation, and process-local
 * initiator scope. Concrete creation and driving belong to the loop.
 *
 * @module @deepseek-ai/dsh-agent
 */

import { Context, FiberState, getTraceable, Service, symbols } from '@deepseek-ai/cordis'
import type { Fiber } from '@deepseek-ai/cordis'
import { AsyncLocalStorage } from 'node:async_hooks'
import { isPromise } from 'node:util/types'
import { scopeTarget } from '@deepseek-ai/dsh-scope'
import type { Scoped } from '@deepseek-ai/dsh-scope'
import type { Session, SessionEvent, SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session'
import type {} from '@deepseek-ai/dsh-session-persistence'
import type { ProjectionHandle } from '@deepseek-ai/dsh-session-projection'
import { installTurnArchiveAdmission } from './archive-admission.ts'
import type { Agent, TurnBoundaryProjection } from './types.ts'
import type { AgentOptions, GuardedClaimCoordinator, SessionStartSource } from './runtime-types.ts'
import { turnBoundaryProjectionDefinition } from './projection.ts'
import { guardedAdmissionProjectionDefinition, guardedAdmissionRecords, validateGuardedAdmissionHistory } from './admission-projection.ts'
import type { GuardedAdmissionProjection, GuardedAdmissionRecord } from './admission-projection.ts'

export * from './runtime-types.ts'
export * from './types.ts'
export { AgentModel, modelSelectionProjectionDefinition, installModelSelection } from './model-selection.ts'
export type { ModelSelectionProjectionState } from './model-selection.ts'
export type * from './projection.ts'
export type { GuardedAdmissionProjection, GuardedAdmissionRecord } from './admission-projection.ts'
export * from './consumed-work.ts'
export { agentCarrier, agentEvents, assembleContextFor, emitAgentEvent } from './dispatch.ts'
export type { AgentEventDispatch, AgentSubjectEvent } from './dispatch.ts'

declare module '@deepseek-ai/cordis' {
  interface Context {
    agents: AgentRegistry
  }
}

/**
 * Synchronous finalizer returned by unpublished Agent setup when its
 * contributions need validation at the exact publication commit point.
 */
export interface AgentSetupCommit {
  /**
   * Validate and commit the prepared setup immediately before publication.
   * @throws when publication must roll the unpublished Agent back.
   */
  commit(): void
}

/**
 * Compose an unpublished Agent scope and optionally return its publication commit.
 * @param agentCtx - unpublished Agent scope.
 * @param agent - unpublished Agent being composed.
 * @returns an optional synchronous commit invoked after setup awaits settle and immediately before publication.
 */
export type AgentSetup = (
  agentCtx: Context,
  agent: Agent,
) => AgentSetupCommit | Promise<AgentSetupCommit | void> | void

/** Trusted host preparation of a guarded claim coordinator for an Agent. */
export type GuardedClaimProvider = (
  agent: Agent, signal: AbortSignal,
) => GuardedClaimCoordinator | undefined | Promise<GuardedClaimCoordinator | undefined>

interface GuardedProviderSlot {
  readonly provider: GuardedClaimProvider
  readonly abort: AbortController
  readonly ready: PromiseWithResolvers<void>
  readonly bindings: Map<Agent, () => void>
}

async function awaitGuardedProvider<T>(operation: PromiseLike<T> | T, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted()
  let stop: (() => void) | undefined
  const aborted = new Promise<never>((_resolve, reject) => {
    // oxlint-disable-next-line typescript/prefer-promise-reject-errors -- preserve the caller's exact AbortSignal reason.
    stop = () => { reject(signal.reason) }
    signal.addEventListener('abort', stop, { once: true })
    if (signal.aborted) stop()
  })
  try { return await Promise.race([operation, aborted]) } finally {
    if (stop !== undefined) signal.removeEventListener('abort', stop)
  }
}

/**
 * Options for programmatically creating an agent through the registry factory
 * ({@link AgentRegistry.create}). The caller supplies the single live
 * `sessionId` shared by the agent registry and session log (e.g. an
 * ACP-generated id), plus optional session metadata (the validated `cwd`, fork
 * lineage); the factory creates the session and agent under that identity.
 */
export interface CreateAgentOptions {
  /** The live agent/session identity. */
  readonly sessionId: SessionId
  /**
   * Session creation metadata: validated absolute `cwd`, `parentSession`
   * fork lineage, the `isSeeded` fork marker, the coarse `origin`
   * classification, and the `delegationDepth` recursion budget. Mirrors the
   * `cwd`/`parentSession`/`isSeeded`/`origin`/`delegationDepth` fields of
   * {@link CreateSessionOptions.meta} in dsh-session (the internal-only
   * `createdAt`, used when reconstructing a persisted session, is deliberately
   * excluded — a factory caller never sets it). This is durable session data,
   * so the session boundary validates and snapshots it before asynchronous
   * setup begins.
   */
  readonly meta?: {
    readonly cwd?: string
    readonly parentSession?: SessionId
    readonly isSeeded?: boolean
    readonly origin?: 'subagent'
    readonly delegationDepth?: number
    readonly agentPreset?: string
  }
  /** Exact fork-inherited prefix length when the session metadata sets `isSeeded`. */
  readonly inheritedEventCount?: SessionLogOffset
  /**
   * Initial replay/fork history, contiguous from seq 0 with lossless-JSON data.
   * A fork supplies an exact parent prefix, its inherited marker, and closers
   * for the open tail. Previously closed steps and turns remain unchanged.
   * The factory validates and snapshots the seed before publication.
   */
  readonly seed?: readonly SessionEvent[]
  /** Route fallback and output controls; see {@link AgentOptions} for live accessor semantics. */
  readonly agentOptions?: AgentOptions
  /** Optional creation-only cancellation signal; detached before the returned handle becomes visible. */
  readonly signal?: AbortSignal
  /**
   * Creation-time composition of the agent's scoped world. The factory awaits
   * setup after minting `agentCtx`, entering the SessionStore, and completing
   * `session/created`. The Session
   * accepts live events during setup; the Agent remains outside its registry
   * until setup completes. Agent creation is announced after setup. Setup may return an {@link AgentSetupCommit}; the factory invokes its
   * synchronous `commit()` after every setup await settles and immediately
   * before registry publication. This lets mutable provisioning revalidate at
   * the exact publication boundary. Everything registered through `agentCtx`
   * (scoped tools, prompt sections/variables, `restrict()`, listeners, awaited
   * child plugins) exists before `agent/created`,
   * and the first prompt assembly. A setup
   * throw/rejection, commit throw, or owner disposal rolls the scope back
   * without announcing Agent creation. Already committed Session events remain stored.
   *
   * **Setup composes, it never drives**: the callback is trusted same-process
   * code and receives the full scoped context, so this is a contract rather
   * than a runtime restriction. Drive the agent only after creation resolves.
   */
  readonly setup?: AgentSetup
}

/**
 * Options for resuming an agent on a persisted session
 * ({@link AgentRegistry.resume}).
 */
export interface ResumeAgentOptions {
  /** The persisted session id to load and use as the live agent/session identity. */
  readonly resumeSessionId: SessionId
  /** Route fallback and output controls; see {@link AgentOptions} for live accessor semantics. */
  readonly agentOptions?: AgentOptions
  /** Optional creation-only cancellation signal for persistence load/setup; detached before return. */
  readonly signal?: AbortSignal
  /**
   * Resume-time composition of the agent's fresh scoped world. Persistence is
   * loaded first; the factory then enters and announces the reconstructed Session and awaits
   * setup in the fresh `agentCtx` before registering the Agent. The callback has the
   * same trusted composition-only contract and optional synchronous
   * publication commit as {@link CreateAgentOptions.setup}: all registrations
   * exist before Agent creation is announced, and rejection, commit failure,
   * or owner disposal unwinds the transaction while preserving pending inbox input.
   */
  readonly setup?: AgentSetup
}

/**
 * An owned agent plus its disposer, returned by {@link AgentRegistry.create} /
 * {@link AgentRegistry.resume}. The disposer is a CAPABILITY: among consumers,
 * only the holder can tear this agent down. The registered factory provider is
 * also a structural owner because the scoped agent depends on that provider's
 * service API; provider unload stops and drains every live handle it made.
 * `dispose()` stops the loop, awaits its exit, unregisters the agent, removes
 * its session from the store, and finally unwinds its scoped world.
 *
 * `ctx.agents.get(id)` still returns a bare {@link Agent} — the handle is
 * exposed only to the consumer owner that created it; the structural provider
 * reaches the same teardown internally. Config-created agents (the loop's own
 * startup) are owned by the loop fiber and never need a handle.
 */
export interface AgentHandle {
  agent: Agent
  dispose(): Promise<void>
}

/**
 * The agent-creation factory the loop implementation provides to the registry
 * via {@link AgentRegistry.setFactory}. Kept on the `dsh-agent` interface so
 * consumers (e.g. the ACP bridge) program against `ctx.agents` without
 * depending on the concrete `dsh-agent-loop` package.
 */
export interface AgentFactory {
  /** Provider enforces durable pre-claim admission during resume before mutating the log. */
  readonly durableAdmissionVersion?: 1
  /** Factory participates in the registry's guarded provider publication barrier. */
  readonly guardedProviderVersion?: 1
  /**
   * Create a new agent on a caller-supplied session id. Async because creation
   * enters and announces the Session, awaits setup and its optional synchronous commit,
   * inserts the Agent, and awaits serial
   * `agent/created` listeners before releasing queued work. The sequence is
   * rollback-covered, but notifications delivered before a later listener
   * failure remain observable; every agent or session creation announcement
   * that began is paired by `agent/disposed` or `session/disposed` during
   * rollback. Once Agent initialization begins, failures reject after its
   * scope and acquired write handle finish cleanup. Cancellation during
   * persistence acquisition may reject before a late handle is available to close.
   * The owner disposes the resolved handle to stop/drain,
   * unregister, remove the session, and unwind the scope.
   * The registry passes a context carrying the `create()` caller's fiber and
   * scope as `ownerCtx`. The implementation attaches the unpublished
   * transaction and resulting lifecycle to that owner; it must not infer
   * ownership from the factory object's registration context.
   * @param ownerCtx - caller-bound context that owns the transaction and live handle.
   * @param options - agent/session identity, configuration, and setup.
   * @returns the owned handle after setup and both creation announcements complete.
   */
  createAgent(ownerCtx: Context, options: CreateAgentOptions): Promise<AgentHandle>
  /**
   * Resume an agent on a persisted session. Async because it opens the
   * persisted session for write, reads and repairs the log, publishes it, and
   * awaits the optional unpublished setup transaction; must be called after
   * `ctx.sessionPersistence` exists (consumers inject `sessionPersistence`).
   * Initialization cleanup and publication follow the same timing as
   * {@link createAgent}.
   * @param ownerCtx - caller-bound context that owns load, setup, and the live handle.
   * @param options - persisted identity, configuration, and setup.
   * @returns the owned handle after setup, both announcements, and loop start complete.
   */
  resume(ownerCtx: Context, options: ResumeAgentOptions): Promise<AgentHandle>
}

/** Thrown when create/resume is called before an agent factory is registered. */
const NO_FACTORY_MESSAGE = 'no agent factory registered (load an agent-loop plugin)'
const NO_INITIATOR_MESSAGE = 'no initiating agent is active'
const DISPOSED_INITIATOR_MESSAGE = 'agent initiator scope is disposed'

/** All mutable lifecycle state for one exact registry entry. */
interface AgentEntry {
  readonly id: SessionId
  readonly agent: Agent
  readonly carrier: Scoped<Agent>
  announced: boolean
  announcing: boolean
  detachRequested: boolean
}

/** One tracked boundary plus its inherited nesting chain. */
interface InitiatorRun {
  active: boolean
  readonly parent: InitiatorRun | undefined
}

/** Plain holder prevents Cordis from tracing the factory field before the caller context is known. */
interface FactorySlot {
  readonly target: AgentFactory
}

/**
 * Agent service (`ctx.agents`): tracks live agents and carries the initiating
 * Agent through one process-local asynchronous driver chain. Agent *creation*
 * is provided by whichever plugin implements the {@link AgentFactory}
 * (`@deepseek-ai/dsh-agent-loop`), registered via {@link setFactory}.
 *
 * Initiator methods provide same-process causal attribution only. Ambient
 * presence is neither liveness proof nor authorization; subjects and owners
 * remain explicit, as does identity at worker, process, persistence, and wire
 * boundaries. Returned Promise boundaries drain during teardown, except a
 * nested lineage that starts an owning-fiber unload is excluded from its own drain.
 */
export class AgentRegistry extends Service {
  static inject = ['sessionProjections']

  private turnBoundaryProjectionHandle!: ProjectionHandle<TurnBoundaryProjection>
  private guardedAdmissionProjectionHandle!: ProjectionHandle<GuardedAdmissionProjection>
  private store = new Map<SessionId, AgentEntry>()
  private readonly admissionGuardedSessions = new WeakSet<Session>()
  private guardedProvider: GuardedProviderSlot | undefined
  private factory: FactorySlot | undefined
  private readonly initiators = new AsyncLocalStorage<Agent | undefined>()
  private readonly initiatorRuns = new AsyncLocalStorage<InitiatorRun>()
  private initiatorState: 'active' | 'closing' | 'disposed' = 'active'
  private activeInitiatorRuns = 0
  private initiatorDrain: PromiseWithResolvers<void> | undefined
  private initiatorDisposal: Promise<void> | undefined

  constructor(ctx: Context) {
    super(ctx, 'agents')
    ctx.inject(['typert'], (typeCtx) => {
      typeCtx.typert.lookups.register('agent', {
        parameter: 'agent',
        wire: 'agentId',
        resource: 'session',
        hostTypeSymbol: '@deepseek-ai/dsh-agent#Agent',
        wireTypeSymbol: '@deepseek-ai/dsh-session/types#SessionId',
        resolve: sessionId => this.get(sessionId),
      })
      typeCtx.typert.contexts.registerHost('agent', {
        wire: 'agentId',
        wireTypeSymbol: '@deepseek-ai/dsh-session/types#SessionId',
        resource: 'session',
        resolve: sessionId => this.get(sessionId)?.ctx,
      })
    })
    ctx.on('internal/status', (fiber) => {
      if (fiber.state === FiberState.UNLOADING && this.hasLifecycleAncestor(fiber)) {
        this.closeInitiators()
      }
    })
    ctx.effect(function* (this: AgentRegistry) {
      yield () => this.disposeInitiators()
      yield () => { this.closeInitiators() }
    }.bind(this), 'agents.initiatorLifecycle()')
    // Archive admission: the Workspace registry asks what still runs for a
    // Session before hiding it; a running turn answers here, for every Agent.
    installTurnArchiveAdmission(ctx, sessionId => this.get(sessionId))
  }

  protected async [Service.init](): Promise<void> {
    this.turnBoundaryProjectionHandle = await this.ctx.sessionProjections.register(turnBoundaryProjectionDefinition)
    this.guardedAdmissionProjectionHandle = await this.ctx.sessionProjections.register(guardedAdmissionProjectionDefinition)
  }

  /**
   * Read the current durable guarded admissions without scanning Session history.
   * @param session - the Session whose projection is required.
   * @returns the current admission identities and durable states.
   */
  guardedAdmissionsOf(session: Session): GuardedAdmissionProjection {
    return this.guardedAdmissionProjectionHandle.stateOf(session)
  }

  /**
   * Enumerate current guarded identities without exposing mutable internal state.
   * @param session - the Session whose projection is required.
   * @returns a fresh array of immutable admission records.
   */
  guardedAdmissionRecordsOf(session: Session): readonly GuardedAdmissionRecord[] {
    return guardedAdmissionRecords(this.guardedAdmissionsOf(session))
  }

  /**
   * Validate admission transitions in an exclusively leased event snapshot before repair writes.
   * @param events - the complete leased Session event snapshot.
   * @throws if an admission identity or transition is invalid.
   */
  validateGuardedAdmissionHistory(events: readonly SessionEvent[]): void {
    validateGuardedAdmissionHistory(events)
  }

  /**
   * Read the turn and step positions recorded in a Session.
   * @param session - Session whose committed events are read.
   * @returns the current turn and step positions; do not mutate the returned value.
   */
  turnBoundaryOf(session: Session): TurnBoundaryProjection {
    return this.turnBoundaryProjectionHandle.stateOf(session)
  }

  /**
   * Read the Agent that initiated the inherited asynchronous driver chain.
   * Use this optional form for logging, tracing, metrics, or host attribution
   * that also supports agentless calls. When a parent creates a child, setup
   * reports the causal parent while the setup callback's Agent parameter
   * identifies the child.
   * @returns the inherited Agent, or `undefined` outside an initiator boundary
   *   and inside an explicit clearing boundary.
   * @throws when this service instance has been disposed.
   */
  currentInitiator(): Agent | undefined {
    this.assertInitiatorsReadable()
    return this.initiators.getStore()
  }

  /**
   * Read the initiating Agent and fail when no initiator boundary is active.
   * Use this for private helpers contractually below a driver, or for a
   * deployment-owned outbound request whose contract forbids agentless calls.
   * Generic or direct-call paths use optional lookup or explicit request fields.
   * @returns the inherited Agent.
   * @throws when no initiator is active or this service instance has been disposed.
   */
  requireInitiator(): Agent {
    const agent = this.currentInitiator()
    if (agent === undefined) throw new Error(NO_INITIATOR_MESSAGE)
    return agent
  }

  /**
   * Run an operation with one exact Agent as its process-local initiator. The
   * exact synchronous value or Promise returned by the operation is preserved.
   * Custom drivers and test harnesses wrap their complete returned foreground
   * lifetime.
   * A queue or wire receiver may establish this boundary only after validating
   * explicit identity and resolving the exact live Agent; this method does neither.
   * Detached work remains owned by the subsystem that starts it.
   * @param agent - initiating Agent to inherit; presence is neither liveness proof nor authorization.
   * @param operation - synchronous or asynchronous operation to invoke.
   * @returns the exact value returned by `operation`.
   * @throws when the initiator scope is closing/disposed, or when `operation` throws.
   */
  withInitiator<T>(agent: Agent, operation: () => T): T {
    return this.runWithInitiator(agent, operation)
  }

  /**
   * Run an operation inside a boundary that hides any inherited initiating
   * Agent. The exact synchronous value or Promise is preserved.
   * Use this while creating lazy shared timers, queue pumps, pool maintenance,
   * watchers, or exporters so they do not inherit the first Agent that happens
   * to initialize them. It clears only initiator attribution, not explicit
   * fields, and does not own or drain detached resources.
   * @param operation - synchronous or asynchronous operation to invoke without an initiator.
   * @returns the exact value returned by `operation`.
   * @throws when the initiator scope is closing/disposed, or when `operation` throws.
   */
  withoutInitiator<T>(operation: () => T): T {
    return this.runWithInitiator(undefined, operation)
  }

  /**
   * Register the agent-creation factory (the loop calls this on construction,
   * effect-scoped). A traced Cordis service is canonicalized to its concrete
   * target; each create/resume call is then traced through that caller's
   * context so ownership follows the caller without stacking proxy layers.
   * Throws if a factory is already registered. Returns the disposer; on
   * dispose the factory slot is cleared.
   * @param factory - the loop-owned factory {@link create}/{@link resume} delegate to.
   * @returns the disposer that clears the factory slot. The exact
   *   Cordis effect disposer (single-shot): composite (generator) effects may
   *   yield it directly — exact identity nests the teardown in order.
   */
  setFactory(factory: AgentFactory): () => void {
    const dispose = this.ctx.effect(() => {
      if (this.factory !== undefined) throw new Error('an agent factory is already registered')
      // Avoid stacking two Cordis shadow layers when a caller passes a Service
      // already read through a context. Calls are re-traced through their
      // actual owner context below.
      const target = (factory as AgentFactory & { [symbols.original]?: AgentFactory })[symbols.original] ?? factory
      this.factory = { target }
      return () => { this.factory = undefined }
    }, 'agents.setFactory()')
    // The exact cordis effect disposer (the agents.register() convention): a
    // caller's composite effect can yield it for in-order teardown; the
    // loop's constructor effect returns it directly, identity-nesting the
    // registration under that effect.
    // oxlint-disable-next-line typescript/no-misused-promises -- synchronous cleanup; direct return preserves disposer identity
    return dispose
  }

  /**
   * Install one trusted host provider and bind every currently idle Agent under
   * maintenance before enabling future publication. A busy or unsupported
   * Agent, failed preparation, duplicate provider, or owner disposal rejects
   * registration and rolls back all bindings. Guarded work on partially bound
   * Agents waits until the entire registration commits. New Agent publication
   * waits for this barrier; factories that do not declare its support are
   * rejected while a provider is installed.
   *
   * The provider is trusted host code. It may return `undefined` only after
   * checking its own journal for pending work targeting the Agent; the
   * registry additionally refuses that result for any guarded Session history.
   * Before registering, the host must own and fence the journal and resolve or
   * hold uncertain earlier claims. Each coordinator must revalidate authority
   * at the synchronous claim transaction. The owner must stop dispatch and
   * drain in-flight claims before disposing the returned registration.
   * @param provider - prepares the coordinator after the host has fenced its journal; honor its abort signal.
   * @returns the owner-scoped registration disposer.
   */
  async registerGuardedClaimProvider(provider: GuardedClaimProvider): Promise<() => void> {
    if (typeof provider !== 'function') throw new TypeError('guarded claim provider must be a function')
    const slot: GuardedProviderSlot = {
      provider, abort: new AbortController(), ready: Promise.withResolvers<void>(), bindings: new Map(),
    }
    void slot.ready.promise.catch(() => {})
    const dispose = this.ctx.effect(() => {
      if (this.guardedProvider !== undefined) throw new Error('a guarded claim provider is already registered')
      this.guardedProvider = slot
      return () => {
        slot.abort.abort(new Error('guarded claim provider disposed'))
        if (this.guardedProvider === slot) this.guardedProvider = undefined
        for (const release of slot.bindings.values()) release()
        slot.bindings.clear()
      }
    }, 'agents.guardedClaimProvider()')
    try {
      for (const agent of this.list()) {
        await agent.runMaintenance(async (signal) => {
          const prepare = await this.prepareGuardedClaimProviderForSlot(agent, signal, slot, false)
          prepare()
        })
      }
      if (this.guardedProvider !== slot || slot.abort.signal.aborted) throw new Error('guarded claim provider disposed during registration')
      slot.ready.resolve()
      // oxlint-disable-next-line typescript/no-misused-promises -- preserve the exact owner-scoped Cordis disposer.
      return dispose
    } catch (error: unknown) {
      slot.ready.reject(error)
      await dispose()
      throw error
    }
  }

  private async prepareGuardedClaimProviderForSlot(
    agent: Agent, signal: AbortSignal, slot: GuardedProviderSlot, waitReady: boolean,
  ): Promise<() => () => void> {
    const combined = AbortSignal.any([signal, slot.abort.signal])
    if (waitReady) await awaitGuardedProvider(slot.ready.promise, combined)
    const coordinator = await awaitGuardedProvider(Promise.resolve().then(() => slot.provider(agent, combined)), combined)
    combined.throwIfAborted()
    if (coordinator === undefined && this.guardedAdmissionsOf(agent.session).root !== null) {
      throw new Error(`agent "${agent.id}" has guarded history without a claim coordinator`)
    }
    return () => {
      combined.throwIfAborted()
      if (this.guardedProvider !== slot || slot.bindings.has(agent)) throw new Error('guarded claim provider changed during Agent publication')
      let release: () => void = () => {}
      if (coordinator !== undefined) {
        if (agent.setGuardedClaimCoordinator === undefined) throw new Error(`agent "${agent.id}" cannot install a guarded claim coordinator`)
        release = agent.setGuardedClaimCoordinator(coordinator, slot.ready.promise)
      }
      let installed = true
      const remove = (): void => {
        if (!installed) return
        installed = false
        slot.bindings.delete(agent)
        release()
      }
      slot.bindings.set(agent, remove)
      return remove
    }
  }

  /**
   * Prepare the registered host authority before a factory publishes an Agent.
   * The factory must invoke the returned synchronous commit immediately before
   * registry entry and retain its disposer through Agent teardown.
   * @param agent - the unpublished Agent to protect.
   * @param signal - cancellation of this publication attempt.
   * @returns a synchronous publication commit that yields the binding disposer.
   */
  async prepareGuardedClaimProvider(agent: Agent, signal: AbortSignal): Promise<() => () => void> {
    const slot = this.guardedProvider
    if (slot !== undefined) return this.prepareGuardedClaimProviderForSlot(agent, signal, slot, true)
    return () => {
      if (this.guardedProvider !== undefined) throw new Error('guarded claim provider changed during Agent publication')
      return () => {}
    }
  }

  /** Return the active creation factory. */
  private requireFactory(): FactorySlot {
    if (this.factory === undefined) throw new Error(NO_FACTORY_MESSAGE)
    if (this.guardedProvider !== undefined && this.factory.target.guardedProviderVersion !== 1) {
      throw new Error('agent factory does not support the guarded provider publication barrier')
    }
    return this.factory
  }

  /**
   * Create and publish a new agent through the registered factory.
   * Distinct from {@link register} (which records an already-constructed
   * agent): this constructs the agent and its session. Rejects if no factory is
   * registered or creation/setup fails. The resolved {@link AgentHandle} lets
   * the owner tear down exactly this agent.
   * @param options - shared identity, session seed/metadata, and agent options.
   * @returns the handle after setup, rollback-covered publication, and loop start complete.
   */
  async create(options: CreateAgentOptions): Promise<AgentHandle> {
    const ownerCtx = this.ctx
    // Re-trace a Service-backed factory through the accessing context
    // explicitly. This preserves AgentLoop's dependency origin while binding
    // its effects to ownerCtx; plain factories receive ownerCtx as an explicit
    // capability and need no Cordis tracker magic.
    const { target } = this.requireFactory()
    const receiver = getTraceable(ownerCtx, target)
    // oxlint-disable-next-line typescript/unbound-method -- Reflect.apply intentionally supplies the caller-traced receiver
    return Reflect.apply(target.createAgent, receiver, [ownerCtx, options])
  }

  /**
   * Load a persisted session and resume an agent on it through the registered
   * factory. Rejects if no factory is registered; the factory rejects if
   * session persistence is not configured or persistence/setup fails.
   * @param options - persisted identity, configuration, and setup.
   * @returns the handle after setup, rollback-covered publication, and loop start complete.
   */
  async resume(options: ResumeAgentOptions): Promise<AgentHandle> {
    const ownerCtx = this.ctx
    const { target } = this.requireFactory()
    const receiver = getTraceable(ownerCtx, target)
    // oxlint-disable-next-line typescript/unbound-method -- Reflect.apply explicitly supplies the traced receiver.
    const resume = (): Promise<AgentHandle> => Reflect.apply(target.resume, receiver, [ownerCtx, options])
    if (target.durableAdmissionVersion === 1) return resume()
    const persistence = ownerCtx.get('sessionPersistence')
    if (persistence === undefined) throw new Error('legacy Agent resume cannot validate required admission')
    if (typeof persistence.withWriteOpenValidation !== 'function') {
      throw new Error('legacy Agent resume requires a persistence backend with scoped write-open validation')
    }
    const guarded = await persistence.withWriteOpenValidation((id, _header, events) => {
      if (id !== options.resumeSessionId) throw new Error('legacy Agent attempted to open a different session for write')
      if (events.some(event => event.type === 'agent/admission/hold'
        || event.type === 'agent/admission/release' || event.type === 'agent/admission/cancel' || event.type === 'agent/admission/reconciled')) {
        throw new Error('legacy Agent cannot resume required admission events')
      }
    }, resume)
    if (!guarded.validated) {
      await guarded.value.dispose()
      throw new Error('legacy Agent resume did not validate a write handle')
    }
    return guarded.value
  }

  /**
   * Register a live agent with source `startup`. Rejects if the id is already registered or a
   * serial `agent/created` listener fails. Emits `agent/disposed`
   * when the calling fiber is disposed — both with the agent's scope carrier
   * (`scopeTarget(agent, agent)`): the subject is the agent in hand, so the
   * emits are scope-filtered regardless of which context invoked `register`
   * (calling through `agent.ctx` scopes EFFECTS; dispatch scoping always
   * requires passing the carrier). Await registration before using the agent.
   * @param agent - the already-constructed agent to record in the store.
   * @returns the awaitable Cordis effect disposer (single-shot; a repeat call
   *   returns undefined without awaiting an in-flight teardown). Exact
   *   identity is load-bearing: a composite (generator) effect that owns a
   *   teardown ORDER — the agent factory's lifecycle chain — must yield THIS
   *   function so Cordis nests the unregistration at that yield position;
   *   yielding a wrapper would leave it disposing as a concurrent sibling on
   *   owner unload, unregistering the agent (and emitting `agent/disposed`)
   *   while its final turn is still draining.
   */
  register(agent: Agent): ReturnType<Context['effect']> {
    return this.ctx.effect(async function* (this: AgentRegistry) {
      if (this.guardedProvider === undefined) {
        yield this.enter(agent)
      } else {
        const entered = await agent.runMaintenance(async (signal) => {
          const prepare = await this.prepareGuardedClaimProvider(agent, signal)
          const release = prepare()
          try { return { release, detach: this.enter(agent) } } catch (error: unknown) { release(); throw error }
        })
        yield () => { entered.detach(); entered.release() }
      }
      await this.announce(agent, 'startup')
    }.bind(this), 'agents.register()')
  }

  /**
   * Insert an already-constructed agent without announcing it. This is the
   * advanced ordered-lifecycle primitive used by the async agent factory: it
   * first completes setup while the agent is unpublished, then assigns the
   * returned detach closure into its pre-installed composite teardown before
   * calling {@link announce}. Ordinary callers use {@link register}.
   * @param agent - the prepared, unpublished agent.
   * @returns an idempotent closure that removes this exact entry and emits
   *   `agent/disposed` with listener failures contained. When called from a
   *   `agent/created` listener, removal and disposal wait until the serial
   *   creation dispatch settles.
   */
  enter(agent: Agent): () => void {
    const id = agent.id
    if (this.guardedProvider !== undefined && !this.guardedProvider.bindings.has(agent)) {
      throw new Error(`agent "${id}" has not passed the guarded provider publication barrier`)
    }
    if (id !== agent.session.id) {
      throw new Error(`agent id "${id}" does not match session id "${agent.session.id}"`)
    }
    const requiresAdmission = (type: string): boolean => type === 'agent/admission/hold'
      || type === 'agent/admission/release' || type === 'agent/admission/cancel' || type === 'agent/admission/reconciled'
    if (agent.durableAdmissionVersion !== 1 && this.guardedAdmissionsOf(agent.session).root !== null) {
      throw new Error(`agent "${id}" cannot resume required durable admission events`)
    }
    if (!this.admissionGuardedSessions.has(agent.session)) {
      const session = agent.session
      if (typeof session.registerAppendGuard !== 'function') {
        throw new Error(`agent "${id}" session cannot enforce required durable admission`)
      }
      session.registerAppendGuard((events) => {
        if (!events.some(event => requiresAdmission(event.type))) return
        const owner = this.store.get(id)?.agent
        if (owner?.session !== session || owner.durableAdmissionVersion !== 1) {
          throw new Error(`agent "${id}" does not support required durable admission`)
        }
      })
      this.admissionGuardedSessions.add(session)
    }
    const carrier = scopeTarget(agent, agent)
    // This is the authoritative collision boundary. Concurrent create/resume
    // operations may both prepare, but only one exact entry can publish.
    if (this.store.has(id)) throw new Error(`agent "${id}" is already registered`)
    const entry: AgentEntry = {
      id,
      agent,
      carrier,
      announced: false,
      announcing: false,
      detachRequested: false,
    }
    this.store.set(id, entry)
    let entered = true
    const detach = (): void => {
      if (!entered) return
      entered = false
      // Every callback reached by this creation dispatch must observe the same
      // live entry, and disposal must follow creation. A listener may own
      // the advanced detach capability, so make that ordering structural:
      // visibility and the paired disposal are deferred until announce()'s
      // serial dispatch has settled.
      if (entry.announcing) {
        entry.detachRequested = true
        return
      }
      this.detachEntered(entry)
    }
    return detach
  }

  /** Remove one exact entered agent and emit its paired disposal when announced. */
  private detachEntered(entry: AgentEntry): void {
    entry.detachRequested = false
    // A stale capability can never delete a later same-id lifecycle. The
    // captured entry identity is the final boundary.
    /* v8 ignore next -- enter() rejects replacement while this single-shot detach capability is live. */
    if (this.store.get(entry.id) !== entry) return
    this.store.delete(entry.id)
    // An insertion rolled back before announce was never externally created,
    // so emitting disposed would invent an impossible lifecycle edge. Marking
    // happens before the created emit: if a later created listener throws,
    // earlier listeners may already have observed it and must see disposal.
    if (!entry.announced) return
    this.emitDisposed(entry)
  }

  /** Emit the paired disposal edge through the entry's stable carrier. */
  private emitDisposed(entry: AgentEntry): void {
    const args: unknown[] = [entry.carrier, 'agent/disposed', { agent: entry.agent }]
    for (const callback of this.ctx.events.dispatch('emit', args)) {
      try {
        const returned: unknown = callback(...args)
        void Promise.resolve(returned).catch((error: unknown) => {
          this.ctx.logger.warn(`agent "${entry.id}": agent/disposed listener rejected: ${String(error)}`)
        })
      } catch (error: unknown) {
        this.ctx.logger.warn(`agent "${entry.id}": agent/disposed listener threw: ${String(error)}`)
      }
    }
  }

  /**
   * Announce an agent previously inserted with {@link enter}.
   * @param agent - the live inserted agent to announce.
   * @param source - fresh creation, resume, clear, or compaction source.
   * @param signal - optional factory initialization cancellation signal passed to listeners.
   * @returns completion of the serial creation listeners; a listener failure rejects.
   * @throws if `agent` is not the exact live registry entry for its id, or its
   *   creation announcement already began (including a reentrant call from a
   *   creation listener).
   */
  async announce(agent: Agent, source: SessionStartSource, signal?: AbortSignal): Promise<void> {
    const entry = this.store.get(agent.id)
    if (entry === undefined || entry.agent !== agent) {
      throw new Error(`agent "${agent.id}" is not live in this registry`)
    }
    if (entry.announced || entry.announcing) {
      throw new Error(`agent "${entry.id}" was already announced`)
    }
    // Mark before dispatch so a listener cannot recursively create a second
    // lifecycle edge; detach still pairs a partially delivered first edge.
    entry.announcing = true
    entry.announced = true
    try {
      await this.ctx.serial(entry.carrier, 'agent/created', {
        agent: entry.agent,
        source,
        ...signal === undefined ? {} : { signal },
      })
    } finally {
      entry.announcing = false
      if (entry.detachRequested) this.detachEntered(entry)
    }
  }

  /**
   * Look up a live agent.
   * @param id - the shared agent/session id to look up.
   * @returns the agent, or undefined when no live agent has that id.
   */
  get(id: SessionId): Agent | undefined {
    return this.store.get(id)?.agent
  }

  /**
   * All live agents, in registration order.
   * @returns a fresh array; mutating it does not affect the registry.
   */
  list(): Agent[] {
    return [...this.store.values()].map(entry => entry.agent)
  }

  /** Reject new initiator boundaries while inherited continuations drain. */
  private closeInitiators(): void {
    if (this.initiatorState === 'active') this.initiatorState = 'closing'
  }

  /** Wait for returned-Promise boundaries, then invalidate retained references. */
  private disposeInitiators(): Promise<void> {
    return (this.initiatorDisposal ??= (async () => {
      this.closeInitiators()
      this.releaseReentrantInitiatorRuns()
      if (this.activeInitiatorRuns !== 0) {
        this.initiatorDrain ??= Promise.withResolvers<void>()
        await this.initiatorDrain.promise
      }
      this.initiatorState = 'disposed'
      this.initiators.disable()
      this.initiatorRuns.disable()
    })())
  }

  /** Establish one tracked initiator or clearing boundary. */
  private runWithInitiator<T>(agent: Agent | undefined, operation: () => T): T {
    if (this.initiatorState !== 'active') throw new Error(DISPOSED_INITIATOR_MESSAGE)
    const run: InitiatorRun = {
      active: true,
      parent: this.initiatorRuns.getStore(),
    }
    this.activeInitiatorRuns += 1
    let result: T
    try {
      result = this.initiatorRuns.run(run, () => this.initiators.run(agent, operation))
    } catch (error: unknown) {
      this.releaseInitiatorRun(run)
      throw error
    }
    if (isPromise(result)) {
      try {
        void Promise.prototype.then.call(
          result,
          () => { this.releaseInitiatorRun(run) },
          () => { this.releaseInitiatorRun(run) },
        )
      } catch {
        // A branded Promise may expose a failing @@species. Observer setup did
        // not attach, so preserve the exact return without leaking the run.
        this.releaseInitiatorRun(run)
      }
    } else {
      this.releaseInitiatorRun(run)
    }
    return result
  }

  /** Whether one unloading fiber owns this service's lifecycle. */
  private hasLifecycleAncestor(candidate: Fiber): boolean {
    let fiber = this.ctx.fiber
    while (true) {
      if (fiber === candidate) return true
      const parent = fiber.parent.fiber
      if (parent === fiber) return false
      fiber = parent
    }
  }

  private assertInitiatorsReadable(): void {
    if (this.initiatorState === 'disposed') throw new Error(DISPOSED_INITIATOR_MESSAGE)
  }

  /** Exclude the boundary chain that initiated this teardown from its own drain. */
  private releaseReentrantInitiatorRuns(): void {
    let run = this.initiatorRuns.getStore()
    while (run !== undefined) {
      this.releaseInitiatorRun(run)
      run = run.parent
    }
  }

  private releaseInitiatorRun(run: InitiatorRun): void {
    if (!run.active) return
    run.active = false
    this.activeInitiatorRuns -= 1
    if (this.activeInitiatorRuns !== 0) return
    this.initiatorDrain?.resolve()
    this.initiatorDrain = undefined
  }
}

export default AgentRegistry
