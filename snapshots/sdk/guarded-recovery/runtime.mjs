/** Trusted test host only: this is not the DS-Bot journal or an external-effect fence. */
import { open, readFile, mkdir } from 'node:fs/promises'
import { dirname } from 'node:path'

export const name = 'guarded-sdk-test-host'
export const inject = ['agents', 'sessionPersistence']

export async function apply(ctx) {
  const path = process.env.DSH_GUARDED_SNAPSHOT_JOURNAL
  if (!path) throw new Error('guarded SDK fixture requires a private journal')
  let dispose = await ctx.agents.registerGuardedClaimProvider(() => (_session, _inputs, claim) => {
    claim()
    return 'committed'
  })
  ctx.on('agent/created', async ({ agent, source }) => {
    if (agent.session.header.origin === 'subagent') return
    if (source === 'resume') {
      const record = JSON.parse(await readFile(path, 'utf8'))
      const observation = await agent.inspectGuardedAdmission(agent.id, record.message, record.target, record.admissionId)
      if (observation.observed?.pending !== true) throw new Error('guarded SDK fixture could not verify the pending input')
      await agent.reconcileGuardedAdmission(agent.id, record.message, record.target, record.admissionId)
      return
    }
    const record = {
      message: { id: 'guarded-sdk-message', role: 'user', content: [{ type: 'text', text: 'Complete the guarded SDK fixture.' }],
        source: { kind: `plugin:${name}` } },
      admissionId: 'guarded-sdk-admission', target: 'next-turn',
    }
    await mkdir(dirname(path), { recursive: true })
    const journal = await open(path, 'wx', 0o600)
    try { await journal.writeFile(JSON.stringify(record)); await journal.sync() } finally { await journal.close() }
    await agent.guardedSend(record.message, record.target, record.admissionId)
    await dispose()
    dispose = () => {}
  })
}
