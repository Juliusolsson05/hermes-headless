// Stage 0 recorder (decomposition §7): drive a REAL `hermes serve` through each
// scenario and write every frame, both directions, to research/corpus/.
//
// Usage: npm run record -- [S1 S3 …]   (no arguments = every scenario)
//
// The backend is the live harness (testing/live-hermes): Hermes v0.21.7 from
// the pinned reference source, the auth gate ON (password provider), and the
// scripted model (testing/scripted-model). S2 (Nous OAuth) is not recordable
// here; it needs a real Nous account and a registered dashboard.
//
// WHY one harness boot per scenario (not one shared backend): scenarios change
// server config (approval timeout, reap grace, token TTL) and some restart or
// starve the backend. A fresh home also makes `session.list` and row ids start
// from zero, so recordings are comparable run to run.

import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { startScriptedModel } from '../src/testing/scripted-model/server.js'
import { removeLiveHome, startLiveHermes, type LiveHermes, type LiveHermesOptions } from '../src/testing/live-hermes/harness.js'
import { RawPeer, eventType, mintTicket, passwordNativeLogin, probe, refreshTokens, type Tokens } from './driver.js'
import { WireTap } from './tap.js'

const corpusRoot = fileURLToPath(new URL('./corpus/', import.meta.url))
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Ctx = { hermes: LiveHermes; tap: WireTap; tokens: Tokens }

async function connect(ctx: Ctx): Promise<RawPeer> {
  const ticket = await mintTicket(ctx.tap, ctx.hermes.baseUrl, ctx.tokens.access_token)
  const peer = new RawPeer(ctx.tap)
  const url = new URL('/api/ws', ctx.hermes.baseUrl)
  url.protocol = 'ws:'
  url.searchParams.set('ticket', ticket)
  await peer.open(url.toString())
  await peer.waitEvent('gateway.ready', 15_000)
  await peer.call('client.capabilities', { server_requests: true })
  return peer
}

async function createSession(peer: RawPeer): Promise<{ sid: string; stored: string; result: Record<string, unknown> }> {
  const result = await peer.call<Record<string, unknown>>('session.create', { source: 'agent-code', cols: 96 })
  return { sid: String(result.session_id), stored: String(result.stored_session_id), result }
}

async function prompt(peer: RawPeer, sid: string, text: string): Promise<void> {
  await peer.call('prompt.submit', { session_id: sid, text })
}

/** Wait for the turn's message.complete for this runtime session id. */
function turnComplete(peer: RawPeer, sid: string, timeoutMs = 120_000): Promise<Record<string, unknown>> {
  return peer.waitFor((frame) => eventType(frame) === 'message.complete' && (frame.params as { session_id?: string }).session_id === sid, timeoutMs)
}

async function resumeInNewConnection(ctx: Ctx, stored: string, extra: Record<string, unknown> = {}): Promise<{ peer: RawPeer; result: Record<string, unknown> }> {
  const peer = await connect(ctx)
  const result = await peer.call<Record<string, unknown>>('session.resume', { session_id: stored, cols: 96, ...extra })
  return { peer, result }
}

type Scenario = { id: string; title: string; options?: Partial<LiveHermesOptions>; run(ctx: Ctx): Promise<void> }

const scenarios: Scenario[] = [
  {
    id: 'S1',
    title: 'native sign-in, ticketed socket, and the auth failure paths',
    async run(ctx) {
      const peer = await connect(ctx)
      // A reused ticket must be refused (single use, ws_tickets.py).
      ctx.tap.note('negative: reuse a consumed ticket')
      const ticket = await mintTicket(ctx.tap, ctx.hermes.baseUrl, ctx.tokens.access_token)
      const first = new RawPeer(ctx.tap)
      const url = new URL('/api/ws', ctx.hermes.baseUrl)
      url.protocol = 'ws:'
      url.searchParams.set('ticket', ticket)
      await first.open(url.toString())
      await first.waitEvent('gateway.ready', 15_000)
      const second = new RawPeer(ctx.tap)
      await second.open(url.toString()).catch((error: Error) => ctx.tap.note('reused ticket open failed', { message: error.message }))
      await Promise.race([second.closed, sleep(3000)])
      first.close()
      ctx.tap.note('negative: wrong password')
      await passwordNativeLogin(ctx.tap, ctx.hermes.baseUrl, ctx.hermes.username, 'wrong-password-x').catch((error: Error) =>
        ctx.tap.note('wrong password rejected', { message: error.message }))
      ctx.tap.note('negative: bearer that is not a token')
      await mintTicket(ctx.tap, ctx.hermes.baseUrl, 'not-a-real-access-token').catch((error: Error) =>
        ctx.tap.note('bad bearer rejected', { message: error.message }))
      ctx.tap.note('refresh rotates, and the old refresh token is then refused')
      const refreshed = await refreshTokens(ctx.tap, ctx.hermes.baseUrl, ctx.tokens)
      if (refreshed.status === 200) {
        await refreshTokens(ctx.tap, ctx.hermes.baseUrl, ctx.tokens)
      }
      peer.close()
    },
  },
  {
    id: 'S3',
    title: 'new conversation, first prompt, plain reply, then resume elsewhere',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] Say hello.')
      await turnComplete(peer, sid)
      await prompt(peer, sid, '[[hh:plain]] And again.')
      await turnComplete(peer, sid)
      await sleep(1500)
      await peer.call('session.list', { limit: 20 })
      const other = await resumeInNewConnection(ctx, stored)
      await other.peer.call('session.history', { session_id: String(other.result.session_id) })
      other.peer.close()
      peer.close()
    },
  },
  {
    id: 'S3b',
    title: 'create but never prompt: is the stored id real before the first prompt?',
    async run(ctx) {
      const peer = await connect(ctx)
      const { stored } = await createSession(peer)
      await sleep(1000)
      await peer.call('session.list', { limit: 20 })
      peer.close()
      await sleep(500)
      const other = await connect(ctx)
      await other.call('session.resume', { session_id: stored, cols: 96 }).catch((error: Error) =>
        ctx.tap.note('resume of unprompted session failed', { message: error.message }))
      other.close()
    },
  },
  {
    id: 'S4',
    title: 'a turn with three tool calls (one failing, one slow)',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:tools]] Run the tools.')
      await turnComplete(peer, sid)
      await sleep(1500)
      const other = await resumeInNewConnection(ctx, stored)
      other.peer.close()
      peer.close()
    },
  },
  {
    id: 'S5',
    title: 'approval: once, session, deny, and one left to time out',
    options: { approvalTimeoutSeconds: 8 },
    async run(ctx) {
      for (const choice of ['once', 'session', 'deny', 'timeout']) {
        ctx.tap.note(`approval answer: ${choice}`)
        const peer = await connect(ctx)
        const { sid } = await createSession(peer)
        peer.onServerRequest = (frame) => {
          if (frame.method !== 'approval') return undefined
          return choice === 'timeout' ? 'defer' : { choice }
        }
        await prompt(peer, sid, '[[hh:approval]] Delete the probe directory.')
        await turnComplete(peer, sid)
        await sleep(1000)
        peer.close()
      }
    },
  },
  {
    id: 'S6',
    title: 'clarify (choice, multi-select, free text), sudo, secret',
    async run(ctx) {
      const peer = await connect(ctx)
      peer.onServerRequest = (frame) => {
        const params = frame.params as Record<string, unknown>
        if (frame.method === 'clarify') {
          const questions = params.questions as Array<{ qid: string }>
          return { answers: { [questions[0].qid]: 'blue', [questions[1].qid]: 'a, c', [questions[2].qid]: 'nothing else' } }
        }
        // Skip both: an empty value is Hermes' documented "skip", so no real
        // sudo runs and no secret is written into the test home's .env.
        if (frame.method === 'sudo') return { value: '' }
        if (frame.method === 'secret') return { value: '' }
        return undefined
      }
      for (const scenario of ['clarify', 'sudo', 'secret']) {
        const { sid } = await createSession(peer)
        await prompt(peer, sid, `[[hh:${scenario}]] Go.`)
        await turnComplete(peer, sid)
      }
      ctx.tap.note('secret answered with a value (masked in the recording)')
      peer.onServerRequest = (frame) => (frame.method === 'secret' ? { value: 'hh-secret-value-123' } : undefined)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:secret]] Go.')
      await turnComplete(peer, sid)
      peer.close()
    },
  },
  {
    id: 'S7',
    title: 'interrupt mid-stream, and interrupt with an open approval',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:slow]] Stream slowly.')
      await peer.waitFor((frame) => eventType(frame) === 'message.delta', 60_000)
      await sleep(2500)
      await peer.call('session.interrupt', { session_id: sid })
      await turnComplete(peer, sid)
      ctx.tap.note('interrupt while an approval is open')
      peer.onServerRequest = (frame) => (frame.method === 'approval' ? 'defer' : undefined)
      const second = await createSession(peer)
      await prompt(peer, second.sid, '[[hh:approval]] Delete it.')
      await peer.waitFor((frame) => frame.method === 'approval', 60_000)
      await peer.call('session.interrupt', { session_id: second.sid })
      await turnComplete(peer, second.sid)
      await sleep(1000)
      peer.close()
    },
  },
  {
    id: 'S8',
    title: 'drop the socket mid-turn for <20 s, reconnect, replay',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:slow]] Stream slowly.')
      await peer.waitFor((frame) => eventType(frame) === 'message.delta', 60_000)
      await sleep(2000)
      const lastSeq = Math.max(...peer.frames.map((frame) => Number((frame.params as { seq?: number })?.seq ?? 0)))
      peer.terminate()
      await peer.closed
      ctx.tap.note('dropped; reconnecting after 5 s', { lastSeq })
      await sleep(5000)
      const again = await connect(ctx)
      // Desktop's order: replay first (session.events.since), then activate.
      await again.call('session.events.since', { session_id: sid, last_seen: lastSeq })
      await again.call('session.activate', { session_id: sid, cols: 96 })
      await turnComplete(again, sid)
      await sleep(1000)
      await again.call('session.resume', { session_id: stored, cols: 96 }).catch((error: Error) =>
        ctx.tap.note('resume after replay failed', { message: error.message }))
      again.close()
    },
  },
  {
    id: 'S9',
    title: 'drop the socket mid-turn for longer than the reap grace, then resume',
    options: { orphanReapGraceSeconds: 4 },
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:slow]] Stream slowly.')
      await peer.waitFor((frame) => eventType(frame) === 'message.delta', 60_000)
      await sleep(2000)
      peer.terminate()
      await peer.closed
      ctx.tap.note('dropped; waiting past the 4 s grace')
      await sleep(12_000)
      const again = await connect(ctx)
      await again.call('session.events.since', { session_id: sid, last_seen: 0 }).catch((error: Error) =>
        ctx.tap.note('events.since on a reaped runtime id failed', { message: error.message }))
      await again.call('session.activate', { session_id: sid, cols: 96 }).catch((error: Error) =>
        ctx.tap.note('activate on a reaped runtime id failed', { message: error.message }))
      await again.call('session.resume', { session_id: stored, cols: 96 })
      await sleep(30_000)
      await again.call('session.resume', { session_id: stored, cols: 96 })
      again.close()
    },
  },
  {
    id: 'S10',
    title: 'drop with an open approval, reconnect, answer it',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      peer.onServerRequest = (frame) => (frame.method === 'approval' ? 'defer' : undefined)
      await prompt(peer, sid, '[[hh:approval]] Delete it.')
      await peer.waitFor((frame) => frame.method === 'approval', 60_000)
      const lastSeq = Math.max(...peer.frames.map((frame) => Number((frame.params as { seq?: number })?.seq ?? 0)))
      peer.terminate()
      await peer.closed
      await sleep(3000)
      const again = await connect(ctx)
      again.onServerRequest = (frame) => (frame.method === 'approval' ? { choice: 'once' } : undefined)
      const replay = await again.call<Record<string, unknown>>('session.events.since', { session_id: sid, last_seen: lastSeq })
      const open = (replay.open_requests as Array<{ id: string; method: string }> | undefined) ?? []
      ctx.tap.note('open_requests after reconnect', { count: open.length })
      await again.call('session.activate', { session_id: sid, cols: 96 })
      for (const request of open) {
        // The replayed request has no live frame on this socket; answer it via
        // request.answer, the RPC Hermes provides for exactly this case.
        if (request.method === 'approval') await again.call('request.answer', { id: request.id, result: { choice: 'once' } })
      }
      await turnComplete(again, sid)
      again.close()
    },
  },
  {
    id: 'S11',
    title: 'backend restart: epoch change, runtime ids gone, stored ids survive',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] Hi.')
      await turnComplete(peer, sid)
      await sleep(1000)
      peer.terminate()
      ctx.hermes = await ctx.hermes.restart()
      ctx.tap.note('backend restarted', { port: ctx.hermes.port })
      const again = await connect(ctx)
      await again.call('session.events.since', { session_id: sid, last_seen: 3 }).catch((error: Error) =>
        ctx.tap.note('events.since after restart failed', { message: error.message }))
      await again.call('session.resume', { session_id: stored, cols: 96 })
      again.close()
    },
  },
  {
    id: 'S12',
    title: 'resume a long conversation (60 turns) incl. a compression',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      for (let turn = 0; turn < 60; turn += 1) {
        await prompt(peer, sid, `[[hh:plain]] Turn ${turn}.`)
        await turnComplete(peer, sid)
      }
      await peer.call('session.compress', { session_id: sid }).catch((error: Error) => ctx.tap.note('compress failed', { message: error.message }))
      await sleep(3000)
      peer.close()
      const other = await resumeInNewConnection(ctx, stored)
      other.peer.close()
    },
  },
  {
    id: 'S13',
    title: 'slash completion, catalog, exec, a skill command, /stop',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await peer.call('commands.catalog', { session_id: sid })
      await peer.call('complete.slash', { text: '/', session_id: sid })
      await peer.call('complete.slash', { text: '/mo', session_id: sid })
      for (const command of ['help', 'model', 'usage', 'hh-secret', 'stop']) {
        await peer.call('slash.exec', { session_id: sid, command }).catch((error: Error) =>
          ctx.tap.note(`slash.exec ${command} failed`, { message: error.message }))
        await peer.call('command.dispatch', { session_id: sid, name: command }).catch((error: Error) =>
          ctx.tap.note(`command.dispatch ${command} failed`, { message: error.message }))
      }
      peer.close()
    },
  },
  {
    id: 'S14',
    title: 'access token expiry while the socket is open; refresh; redial',
    // The basic provider clamps the TTL to >= 60 s (plugins/dashboard_auth/basic: max(60, ttl)).
    options: { sessionTtlSeconds: 60 },
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] Before expiry.')
      await turnComplete(peer, sid)
      ctx.tap.note('waiting past the 60 s token lifetime with the socket open')
      await sleep(70_000)
      await prompt(peer, sid, '[[hh:plain]] After expiry, same socket.')
      await turnComplete(peer, sid)
      await mintTicket(ctx.tap, ctx.hermes.baseUrl, ctx.tokens.access_token).catch((error: Error) =>
        ctx.tap.note('ticket with expired token failed', { message: error.message }))
      const refreshed = await refreshTokens(ctx.tap, ctx.hermes.baseUrl, ctx.tokens)
      if (refreshed.status === 200) ctx.tokens = (await refreshed.json()) as Tokens
      const again = await connect(ctx)
      again.close()
      peer.close()
    },
  },
  {
    id: 'S15',
    title: 'model provider error',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:error]] Fail.')
      await turnComplete(peer, sid, 180_000)
      peer.close()
    },
  },
  {
    id: 'S16',
    title: 'two clients on one session; busy-session prompt',
    async run(ctx) {
      const a = await connect(ctx)
      const { sid, stored } = await createSession(a)
      await prompt(a, sid, '[[hh:plain]] Warm up.')
      await turnComplete(a, sid)
      const b = await connect(ctx)
      await b.call('session.resume', { session_id: stored, cols: 96 })
      await prompt(a, sid, '[[hh:slow]] Long one.')
      await a.waitFor((frame) => eventType(frame) === 'message.delta', 60_000)
      ctx.tap.note('second prompt while busy (queue / steer / redirect?)')
      await a.call('prompt.submit', { session_id: sid, text: '[[hh:plain]] Busy prompt.' })
      await turnComplete(a, sid)
      await sleep(15_000)
      a.close()
      b.close()
    },
  },
]

const wanted = process.argv.slice(2)
const selected = wanted.length ? scenarios.filter((scenario) => wanted.includes(scenario.id)) : scenarios
const runId = new Date().toISOString().replace(/[:.]/g, '-')
for (const scenario of selected) {
  const model = await startScriptedModel()
  const hermes = await startLiveHermes({ modelUrl: model.url, ...scenario.options })
  const dir = join(corpusRoot, scenario.id)
  mkdirSync(dir, { recursive: true })
  const tap = new WireTap(join(dir, `${runId}.jsonl`))
  tap.note(scenario.title, { scenario: scenario.id, hermesSource: 'v0.21.7 71d34bad', options: scenario.options ?? {} })
  const started = Date.now()
  // ctx.hermes is replaced by S11's restart; stop whichever process is current.
  const ctx: Ctx = { hermes, tap, tokens: undefined as unknown as Tokens }
  try {
    await probe(tap, hermes.baseUrl)
    ctx.tokens = await passwordNativeLogin(tap, hermes.baseUrl, hermes.username, hermes.password)
    await scenario.run(ctx)
    tap.note('scenario finished')
    console.log(`${scenario.id} ok (${Math.round((Date.now() - started) / 1000)} s)`)
  } catch (error) {
    tap.note('scenario failed', { message: error instanceof Error ? error.message : String(error) })
    console.log(`${scenario.id} FAILED: ${error instanceof Error ? error.message : String(error)}`)
  } finally {
    await ctx.hermes.stop()
    await model.close()
    removeLiveHome(hermes.home)
  }
}
