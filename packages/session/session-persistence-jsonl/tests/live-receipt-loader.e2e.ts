import { Context } from '@deepseek-ai/cordis'
import Loader from '@deepseek-ai/cordis-plugin-loader'
import Include from '@deepseek-ai/cordis-plugin-include'
import * as SessionPlugin from '@deepseek-ai/dsh-session'
import { SessionId, type SessionEvent } from '@deepseek-ai/dsh-session'
import type { SessionLiveEventReceipt } from '@deepseek-ai/dsh-session-persistence'
import * as JsonlPlugin from '@deepseek-ai/dsh-session-persistence-jsonl'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { expect, it } from 'vitest'

it('confirms an event through the live writer in a Loader composition', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-live-receipt-loader-'))
  const ctx = new Context()
  const done = Promise.withResolvers<{ event: SessionEvent; receipt: SessionLiveEventReceipt }>()
  const consumer = {
    name: 'live-receipt-consumer',
    inject: ['sessionPersistence', 'sessions'],
    async apply(consumerCtx: Context) {
      const session = consumerCtx.sessions.create(SessionId('loader-receipt'))
      await consumerCtx.sessionPersistence.create(session.header)
      const receipt = consumerCtx.sessionPersistence.beginLiveEventReceipt(session.id, 'turn/start')
      const event = session.append('turn/start', { turn: 1 })
      await receipt.confirm(event)
      done.resolve({ event, receipt })
    },
  }
  try {
    const config = join(root, 'cordis.yml')
    await writeFile(config, [
      '- id: sessions', "  name: '@deepseek-ai/dsh-session'",
      '- id: persistence', "  name: '@deepseek-ai/dsh-session-persistence-jsonl'",
      '  config:', `    root: ${JSON.stringify(root)}`, '    compression: none',
      '- id: consumer', '  name: live-receipt-consumer', '',
    ].join('\n'))
    ctx.baseUrl = pathToFileURL(root).href + '/'
    await ctx.plugin(Loader)
    ctx.loader.builtins.include = Include
    ctx.loader.internal = {
      version: 'v2', loadCache: new Map(),
      async import(specifier: string) {
        switch (specifier) {
          case '@deepseek-ai/dsh-session': return SessionPlugin
          case '@deepseek-ai/dsh-session-persistence-jsonl': return JsonlPlugin
          case 'live-receipt-consumer': return consumer
          default: throw new Error(`Unexpected plugin: ${specifier}`)
        }
      },
      register(): never { throw new Error('Unexpected module hook registration') },
      getOrCreateModuleJob(): never { throw new Error('Unexpected module job creation') },
      resolveSync(): never { throw new Error('Unexpected synchronous module resolution') },
      load(): never { throw new Error('Unexpected module load') },
    }
    await ctx.loader.create({ name: 'cordis:include', config: { path: pathToFileURL(config).href } })
    await ctx.loader.await()
    const { event, receipt } = await done.promise
    const handle = await ctx.sessionPersistence.open(SessionId('loader-receipt'), 'read')
    try { expect((await handle.read(event.seq, 1)).events).toEqual([event]) } finally { await handle.close() }
    const provider = [...ctx.loader.entries()].find(entry => entry.options.id === 'persistence')
    if (provider?.fiber === undefined) throw new Error('missing persistence fiber')
    await provider.fiber.dispose()
    await expect(receipt.confirm(event)).rejects.toThrow('closed')
    receipt.dispose()
    expect(ctx.get('sessionPersistence')).toBeUndefined()
  } finally {
    try { await ctx.fiber.dispose() } finally { await rm(root, { recursive: true, force: true }) }
  }
})
