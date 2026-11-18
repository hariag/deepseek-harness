/** Per-session slash command catalogs, invalidated by host registry and connection events. */
import type { CommandDescriptor } from '@deepseek-ai/dsh-commands/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import { resolveCommand } from '@deepseek-ai/dsh-commands/builtin'

export type { CommandDescriptor } from '@deepseek-ai/dsh-commands/types'

/**
 * cold = never pulled; pending = pull in flight with nothing servable;
 * ready = snapshot serving (a soft-invalidate repull keeps this status);
 * failed = last winning pull rejected, snapshot dropped.
 */
export type DirectoryStatus = 'cold' | 'pending' | 'ready' | 'failed'

/** Injected pull (the service binds command.list off the root connection). */
export type FetchCommands = (sessionId: SessionId) => Promise<readonly CommandDescriptor[]>

/** One session key's cache cell. */
class Entry {
  state: DirectoryStatus = 'cold'
  commands: readonly CommandDescriptor[] = []
  /** Registry revision at the start of the last completed request. */
  revision = -1
  flight: Promise<void> | undefined
  lastError: unknown
  waiters: Array<() => void> = []
}

/** The session-keyed directory cache. Plain class — the owning service wires events and RPC. */
export class CommandDirectory {
  private readonly entries = new Map<SessionId, Entry>()

  private revision = 0

  constructor(private readonly fetchCommands: FetchCommands) {}

  /**
   * Current cache status for one session.
   * @param sessionId - session key.
   * @returns the entry status (cold when never touched).
   */
  status(sessionId: SessionId): DirectoryStatus {
    return this.entries.get(sessionId)?.state ?? 'cold'
  }

  /**
   * Synchronous command lookup over one Session's ready catalog; exact names precede localized aliases.
   * @param sessionId - session key.
   * @param name - typed command spelling without the leading slash.
   * @returns the descriptor, or undefined when absent or the entry is not ready.
   */
  resolve(sessionId: SessionId, name: string): CommandDescriptor | undefined {
    const entry = this.entries.get(sessionId)
    if (entry === undefined || entry.state !== 'ready') return undefined
    return resolveCommand(name, entry.commands)
  }

  /** A registry change costs no RPCs. Only a reader of a stale catalog will reload it. */
  invalidateAll(): void {
    this.revision++
  }

  /** Drop this preset's catalog and wake pending readers to join its new composition. */
  resetSession(sessionId: SessionId): void {
    const entry = this.entries.get(sessionId)
    if (entry === undefined) return
    this.entries.delete(sessionId)
    notifyWaiters(entry)
  }

  /** Discard old connection data without reopening historical sessions. */
  resetConnected(): void {
    for (const key of this.entries.keys()) this.resetSession(key)
  }

  /**
   * Fire-and-forget prewarm of one session (the command source's scope-birth
   * warm hook lands here).
   * @param sessionId - session key.
   */
  warm(sessionId: SessionId): void {
    const entry = this.entry(sessionId)
    if (entry.state !== 'ready' || entry.revision !== this.revision) void this.refresh(sessionId)
  }

  /** Start or join the catalog fetch for this Session and connection/preset generation. */
  refresh(sessionId: SessionId): Promise<void> {
    const entry = this.entry(sessionId)
    if (entry.flight !== undefined) return entry.flight
    const flight = Promise.withResolvers<void>()
    entry.flight = flight.promise
    if (entry.state !== 'ready') entry.state = 'pending'
    void this.load(sessionId, entry, this.revision).finally(flight.resolve)
    return flight.promise
  }

  private async load(sessionId: SessionId, entry: Entry, revision: number): Promise<void> {
    try {
      entry.commands = await this.fetchCommands(sessionId)
      entry.state = 'ready'
      entry.lastError = undefined
    } catch (error) {
      entry.commands = []
      entry.state = 'failed'
      entry.lastError = error
    } finally {
      // A hard reset detached this cell from the map, so a late result
      // can never overwrite a replacement preset or connection.
      entry.revision = revision
      entry.flight = undefined
      notifyWaiters(entry)
    }
  }

  /**
   * An async slash operation needs a catalog at least as recent as the registry
   * at its start. Later registry events leave it stale for the next operation
   * without starving this one in a continuously changing host.
   */
  async ensureReady(sessionId: SessionId, signal: AbortSignal): Promise<readonly CommandDescriptor[]> {
    const revision = this.revision
    while (true) {
      if (signal.aborted) throw abortReason(signal)
      const entry = this.entry(sessionId)
      if (entry.state === 'ready' && entry.revision >= revision) return entry.commands
      const next = settled(entry, signal)
      void this.refresh(sessionId)
      await next
      if (this.entries.get(sessionId) === entry && entry.state === 'failed' && entry.revision >= revision) {
        throw new Error(`command directory warmup failed: ${entry.lastError instanceof Error ? entry.lastError.message : String(entry.lastError)}`)
      }
    }
  }

  private entry(sessionId: SessionId): Entry {
    let entry = this.entries.get(sessionId)
    if (entry === undefined) {
      entry = new Entry()
      this.entries.set(sessionId, entry)
    }
    return entry
  }
}

/** One settlement tick for one entry: resolves at the next winning publish, rejects on abort. */
function settled(entry: Entry, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(abortReason(signal))
  return new Promise((resolve, reject) => {
    const waiter = (): void => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }
    const onAbort = (): void => {
      entry.waiters = entry.waiters.filter(w => w !== waiter)
      reject(abortReason(signal))
    }
    signal.addEventListener('abort', onAbort, { once: true })
    entry.waiters.push(waiter)
  })
}

function notifyWaiters(entry: Entry): void {
  const woken = entry.waiters
  entry.waiters = []
  for (const wake of woken) wake()
}

/** Normalize an abort into an Error rejection. */
function abortReason(signal: AbortSignal): Error {
  return signal.reason instanceof Error ? signal.reason : new Error('command directory wait aborted')
}
