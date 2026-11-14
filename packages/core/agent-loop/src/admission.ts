/** Durable admission state for the Agent driver's pre-claim gate. */

import type { GuardedAdmissionRecord } from '@deepseek-ai/dsh-agent'

export interface AdmissionEvent {
  readonly type: string
  readonly data: unknown
}

interface AdmissionIdentity {
  readonly messageId: string
  readonly admissionId: string
}

function identity(value: unknown): AdmissionIdentity {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid admission event')
  const item = value as Record<string, unknown>
  if (typeof item.messageId !== 'string' || item.messageId === ''
    || typeof item.admissionId !== 'string' || item.admissionId === '') throw new Error('invalid admission identity')
  return { messageId: item.messageId, admissionId: item.admissionId }
}

/** Reconstructs required holds before the Agent can claim its inbox. */
export class AdmissionState {
  private readonly held = new Map<string, string>()
  private readonly seen = new Set<string>()
  private readonly seenAdmissions = new Set<string>()
  private readonly identities = new Map<string, string>()
  private readonly canceled = new Set<string>()
  private readonly needsRevalidation = new Set<string>()
  private readonly waiters = new Set<() => void>()

  constructor(events: readonly AdmissionEvent[]) {
    for (const event of events) {
      if (event.type === 'agent/admission/hold') {
        const { messageId, admissionId } = identity(event.data)
        this.hold(messageId, admissionId)
      } else if (event.type === 'agent/admission/release') {
        const { messageId, admissionId } = identity(event.data)
        this.release(messageId, admissionId)
      } else if (event.type === 'agent/admission/cancel') {
        const { messageId, admissionId } = identity(event.data)
        this.cancel(messageId, admissionId)
      } else if (event.type === 'agent/admission/reconciled') {
        const { messageId, admissionId } = identity(event.data)
        if (this.identities.get(messageId) !== admissionId || this.held.has(messageId) || this.canceled.has(messageId)) {
          throw new Error('admission reconciliation requires a released identity')
        }
      }
    }
    // Any guarded input still pending after recovery requires host reconciliation.
    for (const messageId of this.seen) this.needsRevalidation.add(messageId)
  }

  /** Restore current identities from the Session's incremental domain projection. */
  static fromRecords(records: readonly GuardedAdmissionRecord[]): AdmissionState {
    const state = new AdmissionState([])
    for (const record of records) {
      state.hold(record.messageId, record.admissionId)
      if (!record.held) state.release(record.messageId, record.admissionId)
      if (record.canceled) state.cancel(record.messageId, record.admissionId)
      state.needsRevalidation.add(record.messageId)
    }
    return state
  }

  hasHeld(messageId: string): boolean {
    return this.held.has(messageId) || this.canceled.has(messageId) || this.needsRevalidation.has(messageId)
  }
  hasSeen(messageId: string): boolean { return this.seen.has(messageId) }
  isCanceled(messageId: string): boolean { return this.canceled.has(messageId) }
  admissionId(messageId: string): string | undefined { return this.identities.get(messageId) }

  requiresRevalidation(messageId: string): boolean { return this.needsRevalidation.has(messageId) }

  /** Park a released input after a pre-claim turn identity changes. */
  requireRevalidation(messageId: string): void {
    if (!this.identities.has(messageId) || this.held.has(messageId) || this.canceled.has(messageId)) return
    this.needsRevalidation.add(messageId)
    this.notifyChange()
  }

  revalidate(messageId: string, admissionId: string): void {
    if (!this.needsRevalidation.has(messageId) || this.identities.get(messageId) !== admissionId
      || this.held.has(messageId) || this.canceled.has(messageId)) throw new Error('admission cannot be revalidated')
    this.needsRevalidation.delete(messageId)
    for (const notify of this.waiters) notify()
  }

  assertNew(messageId: string, admissionId: string): void {
    identity({ messageId, admissionId })
    if (this.seen.has(messageId) || this.seenAdmissions.has(admissionId)) throw new Error('duplicate admission identity')
  }

  hold(messageId: string, admissionId: string): void {
    this.assertNew(messageId, admissionId)
    this.seen.add(messageId)
    this.seenAdmissions.add(admissionId)
    this.identities.set(messageId, admissionId)
    this.held.set(messageId, admissionId)
  }

  release(messageId: string, admissionId: string): void {
    if (this.held.get(messageId) !== admissionId) throw new Error('admission release does not match an active hold')
    if (this.canceled.has(messageId)) throw new Error('canceled admission cannot be released')
    this.held.delete(messageId)
    for (const notify of this.waiters) notify()
  }

  cancel(messageId: string, admissionId: string): void {
    if (this.identities.get(messageId) !== admissionId) throw new Error('admission cancel does not match its identity')
    this.canceled.add(messageId)
    for (const notify of this.waiters) notify()
  }

  /** Wake blocked drivers after a durably removed admission. */
  notifyChange(): void { for (const notify of this.waiters) notify() }

  /** The caller must test again after every asynchronous wait and claim without another await. */
  isBlocked(messageIds: readonly string[]): boolean { return messageIds.some(id => this.hasHeld(id)) }

  /** Wait until a release or cancellation, without polling. */
  waitForChange(signal: AbortSignal): Promise<void> {
    signal.throwIfAborted()
    return new Promise<void>((resolve, reject) => {
      const cleanup = (): void => { this.waiters.delete(wake); signal.removeEventListener('abort', abort) }
      const wake = (): void => { cleanup(); resolve() }
      // oxlint-disable-next-line typescript/prefer-promise-reject-errors -- preserve the caller's exact AbortSignal reason.
      const abort = (): void => { cleanup(); reject(signal.reason) }
      this.waiters.add(wake)
      signal.addEventListener('abort', abort, { once: true })
      if (signal.aborted) abort()
    })
  }
}
