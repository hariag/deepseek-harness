import { describe, expect, it } from 'vitest'
import { AdmissionState } from '../src/admission.ts'
import { guardedAdmissionProjectionDefinition, guardedAdmissionRecords } from '../../agent/src/admission-projection.ts'
import { Session, SessionId } from '@deepseek-ai/dsh-session'

const hold = (messageId: string, admissionId: string) => ({ type: 'agent/admission/hold', data: { messageId, admissionId } })
const release = (messageId: string, admissionId: string) => ({ type: 'agent/admission/release', data: { messageId, admissionId } })

describe('durable admission gate state', () => {
  it('rebuilds identities from the incremental projection and rejects malformed transitions', () => {
    const session = Session.create(SessionId('admission-projection'))
    const fold = guardedAdmissionProjectionDefinition
    let state = fold.init()
    const held = session.append('agent/admission/hold', { messageId: 'one', admissionId: 'first' })
    const released = session.append('agent/admission/release', { messageId: 'one', admissionId: 'first' })
    const another = session.append('agent/admission/hold', { messageId: 'two', admissionId: 'second' })
    const canceled = session.append('agent/admission/cancel', { messageId: 'two', admissionId: 'second' })
    for (const event of [held, released, another, canceled]) state = fold.apply(state, event)
    expect(guardedAdmissionRecords(state)).toEqual(expect.arrayContaining([
      { messageId: 'one', admissionId: 'first', held: false, canceled: false },
      { messageId: 'two', admissionId: 'second', held: true, canceled: true },
    ]))
    expect(guardedAdmissionRecords(state)).toHaveLength(2)
    const restored = AdmissionState.fromRecords(guardedAdmissionRecords(state))
    expect(restored.requiresRevalidation('one')).toBe(true)
    expect(restored.isCanceled('two')).toBe(true)
    expect(restored.hasHeld('two')).toBe(true)
    expect(() => fold.apply(state, held)).toThrow('duplicate admission identity')
    expect(() => fold.apply(state, released)).toThrow('active hold')
    const checkpoint = fold.stateSchema.parse(JSON.parse(JSON.stringify(state)))
    expect(guardedAdmissionRecords(checkpoint)).toEqual(expect.arrayContaining(guardedAdmissionRecords(state)))
    expect(Object.isFrozen(checkpoint)).toBe(true)
    expect(Object.isFrozen(checkpoint.root)).toBe(true)
    expect(guardedAdmissionRecords(checkpoint).every(record => Object.isFrozen(record))).toBe(true)
    expect(Reflect.set(guardedAdmissionRecords(checkpoint)[0]!, 'held', true)).toBe(false)
    expect(guardedAdmissionRecords(checkpoint).find(record => record.messageId === 'one')?.held).toBe(false)
    expect(() => fold.apply(checkpoint, held)).toThrow('duplicate admission identity')
    const invalid = JSON.parse(JSON.stringify(state)) as { root: { kind: string; prefix?: string; hash?: string } }
    if (invalid.root.kind === 'branch') invalid.root.prefix = 'f'.repeat(64)
    else invalid.root.hash = 'not-a-hash'
    expect(() => fold.stateSchema.parse(invalid)).toThrow()
  })

  it('indexes distinct legacy lone-surrogate identities without replacement collisions', () => {
    const session = Session.create(SessionId('surrogate-admission-projection'))
    const fold = guardedAdmissionProjectionDefinition
    let state = fold.init()
    for (let index = 0; index < 2048; index += 1) {
      const messageId = String.fromCharCode(0xd800 + (index >> 10), 0xd800 + (index & 1023))
      state = fold.apply(state, session.append('agent/admission/hold', { messageId, admissionId: `action-${index}` }))
    }
    const leafSizes = (node: NonNullable<typeof state.root>): number[] => node.kind === 'leaf'
      ? [node.entries.length]
      : Object.values(node.children).flatMap(leafSizes)
    expect(state.root).not.toBeNull()
    expect(Math.max(...leafSizes(state.root!))).toBe(1)
    const checkpoint = fold.stateSchema.parse(JSON.parse(JSON.stringify(state)))
    expect(guardedAdmissionRecords(checkpoint)).toHaveLength(2048)
    const duplicate = String.fromCharCode(0xd800, 0xd800)
    expect(() => fold.apply(checkpoint, session.append('agent/admission/hold', { messageId: duplicate, admissionId: 'new' })))
      .toThrow('duplicate admission identity')
  })
  it('restores unresolved holds, rejects duplicate identities and retains history after release', () => {
    const state = new AdmissionState([])
    state.hold('message-1', 'action-1')
    expect(state.isBlocked(['other', 'message-1'])).toBe(true)
    expect(state.isBlocked(['other'])).toBe(false)
    expect(() => { state.release('message-1', 'wrong') }).toThrow('does not match')
    state.release('message-1', 'action-1')
    expect(state.hasHeld('message-1')).toBe(false)
    expect(() => { state.hold('message-1', 'action-2') }).toThrow('duplicate')
    expect(() => { state.hold('message-2', 'action-1') }).toThrow('duplicate')
    const restored = new AdmissionState([hold('message-1', 'action-1'), release('message-1', 'action-1')])
    expect(restored.hasHeld('message-1')).toBe(true)
    expect(restored.hasSeen('message-1')).toBe(true)
  })

  it('fails closed on malformed, duplicate or unmatched required records', () => {
    expect(() => new AdmissionState([hold('', 'action')])).toThrow('invalid')
    expect(() => new AdmissionState([release('message', 'action')])).toThrow('does not match')
    expect(() => new AdmissionState([hold('message', 'action'), hold('other', 'action')])).toThrow('duplicate')
  })

  it('wakes on release but a new hold must be checked again before claim', async () => {
    const state = new AdmissionState([hold('message-1', 'action-1')])
    let woke = false
    const wait = state.waitForChange(new AbortController().signal).then(() => { woke = true })
    await Promise.resolve()
    expect(woke).toBe(false)
    state.release('message-1', 'action-1')
    state.hold('message-2', 'action-2')
    await wait
    expect(state.isBlocked(['message-2'])).toBe(true)
  })

  it('leaves the hold intact when an Agent wait is canceled', async () => {
    const state = new AdmissionState([hold('message', 'action')])
    const controller = new AbortController()
    const wait = state.waitForChange(controller.signal)
    controller.abort(new Error('test abort'))
    await expect(wait).rejects.toThrow('test abort')
    expect(state.hasHeld('message')).toBe(true)
  })

  it('never releases a canceled input and restores it blocked', () => {
    const state = new AdmissionState([hold('message', 'action')])
    state.cancel('message', 'action')
    expect(() => { state.release('message', 'action') }).toThrow('canceled')
    expect(state.isBlocked(['message'])).toBe(true)
    const restored = new AdmissionState([hold('message', 'action'), { type: 'agent/admission/cancel', data: { messageId: 'message', admissionId: 'action' } }])
    expect(restored.isBlocked(['message'])).toBe(true)
  })
})
