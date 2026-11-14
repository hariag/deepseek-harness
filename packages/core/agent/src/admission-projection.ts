/** Pure, durable indexed state for guarded Agent admissions. */
import { createHash } from 'node:crypto'
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import { z as zod } from 'zod'

/** Current durable state of one guarded input. */
export interface GuardedAdmissionRecord {
  /** Stable message identity. */
  readonly messageId: string
  /** Stable host admission identity. */
  readonly admissionId: string
  /** Whether the durable insertion is still held. */
  readonly held: boolean
  /** Whether the admission was durably canceled. */
  readonly canceled: boolean
}

type IndexEntry = { readonly key: string; readonly record: GuardedAdmissionRecord | null }
type IndexNode =
  | { readonly kind: 'leaf'; readonly hash: string; readonly entries: readonly IndexEntry[] }
  | { readonly kind: 'branch'; readonly prefix: string; readonly children: Readonly<Record<string, IndexNode>> }

/** JSON-compatible persistent index; both original identities are retained for exact duplicate checks. */
export interface GuardedAdmissionProjection {
  /** Immutable index root, or null if the Session has no guarded identities. */
  readonly root: IndexNode | null
}

const recordSchema = zod.object({
  messageId: zod.string().min(1), admissionId: zod.string().min(1), held: zod.boolean(), canceled: zod.boolean(),
}).readonly()
const nodeSchema: zod.ZodType<IndexNode> = zod.lazy(() => zod.discriminatedUnion('kind', [
  zod.object({
    kind: zod.literal('leaf'), hash: zod.string().regex(/^[0-9a-f]{64}$/),
    entries: zod.array(zod.object({ key: zod.string().min(3), record: recordSchema.nullable() }).readonly()).min(1).readonly(),
  }).readonly(),
  zod.object({ kind: zod.literal('branch'), prefix: zod.string().regex(/^[0-9a-f]{0,63}$/),
    children: zod.record(zod.string().regex(/^[0-9a-f]$/), nodeSchema).readonly() }).readonly(),
]))

// Hash the exact JS string code units: UTF-8 replacement would collapse distinct lone surrogates.
function hashKey(key: string): string { return createHash('sha256').update(key, 'utf16le').digest('hex') }

function digitAt(hash: string, index: number): string {
  const digit = hash.charAt(index)
  if (digit === '') throw new Error('invalid admission index path')
  return digit
}

function leaf(hash: string, entries: readonly IndexEntry[]): IndexNode {
  return Object.freeze({ kind: 'leaf', hash, entries: Object.freeze(entries) })
}

function branch(prefix: string, children: Record<string, IndexNode>): IndexNode {
  return Object.freeze({ kind: 'branch', prefix, children: Object.freeze(children) })
}

function findEntry(node: IndexNode | null, hash: string, key: string): IndexEntry | undefined {
  while (node !== null) {
    if (node.kind === 'leaf') return node.hash === hash ? node.entries.find(entry => entry.key === key) : undefined
    if (!hash.startsWith(node.prefix)) return undefined
    node = node.children[digitAt(hash, node.prefix.length)] ?? null
  }
  return undefined
}

/** Copy only the compressed path; full keys disambiguate even a SHA-256 collision. */
function putEntry(node: IndexNode | null, hash: string, entry: IndexEntry): IndexNode {
  if (node === null) return leaf(hash, [Object.freeze(entry)])
  const prefix = node.kind === 'leaf' ? node.hash : node.prefix
  let shared = 0
  while (shared < prefix.length && prefix[shared] === hash[shared]) shared += 1
  if (shared < prefix.length) {
    return branch(hash.slice(0, shared), {
      [digitAt(prefix, shared)]: node, [digitAt(hash, shared)]: leaf(hash, [Object.freeze(entry)]),
    })
  }
  if (node.kind === 'leaf') {
    const index = node.entries.findIndex(item => item.key === entry.key)
    Object.freeze(entry)
    return leaf(hash, index < 0 ? [...node.entries, entry] : node.entries.toSpliced(index, 1, entry))
  }
  const digit = digitAt(hash, node.prefix.length)
  return branch(node.prefix, { ...node.children, [digit]: putEntry(node.children[digit] ?? null, hash, entry) })
}

/**
 * Enumerate the current records once when an Agent attaches or resumes.
 * @param projection - the live or restored admission projection.
 * @returns a fresh array of immutable admission records.
 */
export function guardedAdmissionRecords(projection: GuardedAdmissionProjection): GuardedAdmissionRecord[] {
  const result: GuardedAdmissionRecord[] = []
  const pending: IndexNode[] = projection.root === null ? [] : [projection.root]
  while (pending.length !== 0) {
    const node = pending.pop()
    if (node === undefined) throw new Error('invalid admission traversal')
    if (node.kind === 'branch') pending.push(...Object.values(node.children))
    else for (const entry of node.entries) if (entry.record !== null) result.push(entry.record)
  }
  return result
}

const schema = zod.object({ root: nodeSchema.nullable() }).readonly().superRefine((state, context) => {
  const seen = new Map<string, IndexEntry>()
  const visit = (node: IndexNode, parentPrefix: string): void => {
    if (node.kind === 'branch') {
      if (!node.prefix.startsWith(parentPrefix) || Object.keys(node.children).length < 2) {
        context.addIssue({ code: 'custom', message: 'invalid admission index branch' }); return
      }
      for (const [digit, child] of Object.entries(node.children)) visit(child, node.prefix + digit)
      return
    }
    if (!node.hash.startsWith(parentPrefix)) context.addIssue({ code: 'custom', message: 'invalid admission index path' })
    for (const entry of node.entries) {
      if (hashKey(entry.key) !== node.hash || seen.has(entry.key)) context.addIssue({ code: 'custom', message: 'invalid admission index key' })
      seen.set(entry.key, entry)
    }
  }
  if (state.root !== null) visit(state.root, '')
  const aliases = new Set<string>()
  for (const { key, record } of seen.values()) {
    if (record === null) continue
    const alias = `a:${record.admissionId}`
    if (key !== `m:${record.messageId}` || seen.get(alias)?.record !== null || aliases.has(alias)) {
      context.addIssue({ code: 'custom', message: 'invalid admission record' })
    }
    aliases.add(alias)
  }
  for (const { key, record } of seen.values()) {
    if (record === null && !aliases.has(key)) context.addIssue({ code: 'custom', message: 'orphaned admission alias' })
  }
  const records = guardedAdmissionRecords(state)
  if (seen.size !== records.length * 2) context.addIssue({ code: 'custom', message: 'orphaned admission alias' })
}).transform((state) => {
  const pending: IndexNode[] = state.root === null ? [] : [state.root]
  while (pending.length !== 0) {
    const node = pending.pop()
    if (node === undefined) throw new Error('invalid admission traversal')
    if (node.kind === 'branch') {
      pending.push(...Object.values(node.children))
      Object.freeze(node.children)
    } else {
      for (const entry of node.entries) {
        if (entry.record !== null) Object.freeze(entry.record)
        Object.freeze(entry)
      }
      Object.freeze(node.entries)
    }
    Object.freeze(node)
  }
  return Object.freeze(state)
})

/** Incrementally reconstruct the durable identity index without synchronous history scans. */
export const guardedAdmissionProjectionDefinition = {
  key: 'guardedAdmission', stateVersion: 3, stateSchema: schema, replayOnHydrate: true,
  init: (): GuardedAdmissionProjection => Object.freeze({ root: null }),
  apply: (state, event) => {
    if (event.type !== 'agent/admission/hold' && event.type !== 'agent/admission/release'
      && event.type !== 'agent/admission/cancel' && event.type !== 'agent/admission/reconciled') return state
    const { messageId, admissionId } = event.data
    if (typeof messageId !== 'string' || messageId === '' || typeof admissionId !== 'string' || admissionId === '') {
      throw new Error('invalid admission identity')
    }
    const messageKey = `m:${messageId}`, admissionKey = `a:${admissionId}`
    const messageHash = hashKey(messageKey), admissionHash = hashKey(admissionKey)
    const record = findEntry(state.root, messageHash, messageKey)?.record
    if (event.type === 'agent/admission/hold') {
      if (record !== undefined || findEntry(state.root, admissionHash, admissionKey) !== undefined) throw new Error('duplicate admission identity')
      const next: GuardedAdmissionRecord = Object.freeze({ messageId, admissionId, held: true, canceled: false })
      const root = putEntry(state.root, messageHash, { key: messageKey, record: next })
      return Object.freeze({ root: putEntry(root, admissionHash, { key: admissionKey, record: null }) })
    }
    if (record === undefined || record === null || record.admissionId !== admissionId) throw new Error('admission event does not match its identity')
    if (event.type === 'agent/admission/reconciled') {
      if (record.held || record.canceled) throw new Error('admission reconciliation requires a released identity')
      return state
    }
    if (event.type === 'agent/admission/release' && (!record.held || record.canceled)) throw new Error('admission release does not match an active hold')
    const next = Object.freeze(event.type === 'agent/admission/release' ? { ...record, held: false } : { ...record, canceled: true })
    return Object.freeze({ root: putEntry(state.root, messageHash, { key: messageKey, record: next }) })
  },
} satisfies ProjectionDefinition<'guardedAdmission', GuardedAdmissionProjection>

/**
 * Validate admission transitions in an exclusively leased event snapshot before repair writes.
 * @param events - the complete leased Session event snapshot.
 * @throws if an admission identity or transition is invalid.
 */
export function validateGuardedAdmissionHistory(events: readonly SessionEvent[]): void {
  let state = guardedAdmissionProjectionDefinition.init()
  for (const event of events) state = guardedAdmissionProjectionDefinition.apply(state, event)
}
