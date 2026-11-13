/** Platform-specific output is retained and compared explicitly; no runtime output is changed. */
const NOTICE = '[stderr]\nlandlock-run: partial enforcement (older Landlock ABI)\n'
const variants: Record<string, [string, string]> = {
  'background-wait-steer': ['producer ready\n[status: running, user steered]', `producer ready\n${NOTICE}[status: running, user steered]`],
  'bash-tool': ['dsh-sdk-proof-7391\n', `dsh-sdk-proof-7391\n${NOTICE}`],
}

/** Match a golden to an observed exact platform variant, without weakening other output checks. */
export function expectedPlatformOutput(expected: string, observed: string, scenario: string): string {
  const variant = variants[scenario]
  if (variant === undefined) return expected
  const [plain, partial] = variant.map(value => JSON.stringify(value)) as [string, string]
  if (!observed.includes(partial)) return expected
  if (expected.includes(partial)) return expected
  if (!expected.includes(plain)) throw new Error(`${scenario}: partial Landlock output has no matching golden`)
  return expected.replaceAll(plain, partial)
}
