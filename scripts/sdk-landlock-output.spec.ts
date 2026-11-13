import { describe, expect, it } from 'vitest'
import { expectedPlatformOutput } from './sdk-landlock-output.js'

describe('SDK platform output goldens', () => {
  const plain = JSON.stringify({ text: 'producer ready\n[status: running, user steered]' })
  const partial = JSON.stringify({ text: 'producer ready\n[stderr]\nlandlock-run: partial enforcement (older Landlock ABI)\n[status: running, user steered]' })
  it('accepts the ordinary result without changing the golden', () => {
    expect(expectedPlatformOutput(plain, plain, 'background-wait-steer')).toBe(plain)
  })
  it('retains and compares the exact partial-enforcement notice', () => {
    expect(expectedPlatformOutput(plain, partial, 'background-wait-steer')).toBe(partial)
  })
  it('matches the exact bash warning without accepting extra stderr', () => {
    const bash = JSON.stringify({ text: 'dsh-sdk-proof-7391\n' })
    const warning = JSON.stringify({ text: 'dsh-sdk-proof-7391\n[stderr]\nlandlock-run: partial enforcement (older Landlock ABI)\n' })
    expect(expectedPlatformOutput(bash, warning, 'bash-tool')).toBe(warning)
    expect(expectedPlatformOutput(bash, warning.replace('older Landlock ABI', 'different ABI'), 'bash-tool')).toBe(bash)
  })
  it('does not accept a forged, altered, or extra warning', () => {
    for (const altered of [partial.replace('older Landlock ABI', 'different ABI'), partial.replace('partial enforcement', 'full enforcement'), partial.replace('[status:', 'extra output\\n[status:')]) {
      expect(expectedPlatformOutput(plain, altered, 'background-wait-steer')).toBe(plain)
      expect(altered).not.toBe(plain)
    }
  })
  it('fails when the observed notice has no expected counterpart', () => {
    expect(() => expectedPlatformOutput('{}', partial, 'background-wait-steer')).toThrow('no matching golden')
  })
  it('does not adapt unrelated scenarios', () => {
    expect(expectedPlatformOutput(plain, partial, 'unrelated')).toBe(plain)
  })
})
