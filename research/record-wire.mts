// Stage 0 recorder (decomposition §7): drive a REAL `hermes serve` through each
// scenario and write every frame, both directions, to research/corpus/.
//
// Usage:
//   HERMES_LIVE_RUNTIME=<runtime> node --import tsx research/record-wire.mts \
//     [--release v0.22.0] [S1 S3 …]      (no scenario ids = every scenario)
//
// WHY `node --import tsx` and not the tsx CLI: with a long TMPDIR the tsx CLI
// fails to bind its IPC pipe (`listen EINVAL … tsx-501/….pipe`, report r8).
//
// The backend is the live harness (testing/live-hermes): the Hermes release
// under HERMES_LIVE_RUNTIME (default: the pinned v0.21.7 reference source in
// .cache/hermes-runtime), the auth gate ON (password provider), and the
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

import { startScriptedModel, type ScriptedModelServer } from '../src/testing/scripted-model/server.js'
import { removeLiveHome, startLiveHermes, type LiveHermes, type LiveHermesOptions } from '../src/testing/live-hermes/harness.js'
import { startLaneHermes, type LaneHermes } from '../src/testing/live-hermes/lane.js'
import { RawPeer, eventType, mintTicket, passwordNativeLogin, probe, refreshTokens, restCall, type Tokens } from './driver.js'
import { parseReleaseArgs, readProvenance, recordingFileName } from './provenance.js'
import { WireTap } from './tap.js'

const corpusRoot = fileURLToPath(new URL('./corpus/', import.meta.url))
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Ctx = {
  hermes: LiveHermes
  tap: WireTap
  tokens: Tokens
  /** The scripted model, so a scenario can read what Hermes sent it (S23, S21). */
  model: ScriptedModelServer
  /** Extra processes a scenario started (S24's gateway); stopped in `finally`. */
  cleanup?: () => Promise<void>
  /** The gateway + stand-in setup, for scenarios booted with `boot: 'lane'`. */
  lane?: LaneHermes
}

async function connect(ctx: Ctx, label?: string): Promise<RawPeer> {
  const ticket = await mintTicket(ctx.tap, ctx.hermes.baseUrl, ctx.tokens.access_token)
  const peer = new RawPeer(ctx.tap, label)
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

type Scenario = {
  id: string
  title: string
  options?: Partial<LiveHermesOptions>
  /**
   * Excluded from a bare `record` run. S25 needs a v0.20.4 runtime, where every
   * other scenario fails at the handshake by design, so it only runs when named.
   */
  onlyWhenNamed?: boolean
  /** `lane`: boot `hermes gateway run` + serve (src/testing/live-hermes/lane.ts) instead of serve alone. */
  boot?: 'serve' | 'lane'
  run(ctx: Ctx): Promise<void>
}

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
      const a = await connect(ctx, 'A')
      const { sid, stored } = await createSession(a)
      await prompt(a, sid, '[[hh:plain]] Warm up.')
      await turnComplete(a, sid)
      const b = await connect(ctx, 'B')
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
  {
    id: 'S17',
    title: 'commands.catalog + complete.slash with a quick command, a plugin-free skill and the bundled skills',
    options: {
      extraConfigYaml: [
        // Two user quick commands, one per type the catalog distinguishes
        // (tui_gateway/methods_tools.py _catalog_quick_commands /
        // _dispatch_quick): `exec` runs a shell command, `alias` rewrites.
        'quick_commands:',
        '  hhq:',
        '    type: exec',
        '    command: echo hh-quick-ok',
        '    description: Scripted quick command',
        '  hhalias:',
        '    type: alias',
        '    target: /help',
      ],
    },
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      // The skill is the harness's plugin-free `hh-secret` (HERMES_HOME/skills).
      await peer.call('commands.catalog', { session_id: sid })
      for (const text of ['/', '/hh', '/hhq', '/re', '/bt', '/compr', '/restart', '/hh-secret']) {
        await peer.call('complete.slash', { text, session_id: sid }).catch((error: Error) => ctx.tap.note(`complete.slash ${text} failed`, { message: error.message }))
      }
      // How the quick commands and a terminal-only command actually run.
      for (const command of ['hhq', 'hhalias', 'restart']) {
        await peer.call('slash.exec', { session_id: sid, command }).catch((error: Error) => ctx.tap.note(`slash.exec ${command} failed`, { message: error.message }))
        await peer.call('command.dispatch', { session_id: sid, name: command }).catch((error: Error) => ctx.tap.note(`command.dispatch ${command} failed`, { message: error.message }))
      }
      peer.close()
    },
  },
  {
    id: 'S18',
    title: '/bg via prompt.background → background.complete (idle session, then mid-turn)',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] Warm up.')
      await turnComplete(peer, sid)
      const first = peer.waitFor((frame) => eventType(frame) === 'background.complete', 60_000)
      await peer.call('prompt.background', { session_id: sid, text: '[[hh:say-bg-idle]] Background task on an idle session.' })
      await first
      ctx.tap.note('background while a turn is running')
      await prompt(peer, sid, '[[hh:slow-main]] A long main turn.')
      await peer.waitFor((frame) => eventType(frame) === 'message.delta', 60_000)
      const second = peer.waitFor((frame) => eventType(frame) === 'background.complete', 60_000)
      await peer.call('prompt.background', { session_id: sid, text: '[[hh:say-bg-busy]] Background task during a turn.' })
      await second
      await turnComplete(peer, sid)
      await sleep(1000)
      await peer.call('session.history', { session_id: sid })
      peer.close()
    },
  },
  {
    id: 'S19',
    title: '/btw via prompt.btw → btw.complete (with history; nothing written to the transcript)',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] Remember the probe colour is blue.')
      await turnComplete(peer, sid)
      const done = peer.waitFor((frame) => eventType(frame) === 'btw.complete', 60_000)
      await peer.call('prompt.btw', { session_id: sid, text: '[[hh:say-btw]] What colour did I mention?' })
      await done
      await sleep(1000)
      await peer.call('session.history', { session_id: sid })
      peer.close()
    },
  },
  {
    id: 'S20',
    title: '/compress via session.compress: too small (noop), then a real compression, then resume elsewhere',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid, stored } = await createSession(peer)
      await prompt(peer, sid, '[[hh:plain]] One short turn.')
      await turnComplete(peer, sid)
      ctx.tap.note('compress with fewer than MIN_MESSAGES=4 messages (expected noop)')
      await peer.call('session.compress', { session_id: sid }).catch((error: Error) => ctx.tap.note('small compress failed', { message: error.message }))
      // Many turns, not a few bulky ones. The first attempt (5 turns of ~5 KB
      // each) came back `refused_would_grow`: with a 256k context the tail
      // budget (threshold × 0.2 ≈ 25k tokens, context_compressor.py
      // tail_token_budget) keeps nearly everything, the anchors keep the
      // last user/assistant rows, and the summary wrapper then outweighs the
      // tiny middle. S12's 60 short turns compressed fine (removed 108), so
      // this uses enough short turns to give the compressor a real middle.
      for (let turn = 1; turn <= 30; turn += 1) {
        await prompt(peer, sid, `[[hh:plain]] Turn ${turn} with a little padding so the middle has weight. ${'pad '.repeat(40)}`)
        await turnComplete(peer, sid)
      }
      await sleep(1000)
      await peer.call('session.compress', { session_id: sid }).catch((error: Error) => ctx.tap.note('compress failed', { message: error.message }))
      await sleep(2000)
      await peer.call('session.history', { session_id: sid })
      peer.close()
      const other = await resumeInNewConnection(ctx, stored)
      await other.peer.call('session.history', { session_id: String(other.result.session_id) })
      other.peer.close()
    },
  },
  {
    id: 'S21',
    title: 'notification.show/clear (credit headers), memory write, review.summary (/refine), subagent.start/complete (delegate_task)',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      ctx.tap.note('notifications: depleted credit headers, then restored')
      await prompt(peer, sid, '[[hh:credits-out]] Turn with depleted credit headers.')
      await turnComplete(peer, sid)
      await sleep(500)
      await prompt(peer, sid, '[[hh:credits-ok]] Turn with restored credit headers.')
      await turnComplete(peer, sid)
      await sleep(500)
      ctx.tap.note('memory write in an ordinary turn')
      await prompt(peer, sid, '[[hh:memwrite]] Remember something.')
      await turnComplete(peer, sid)
      ctx.tap.note('review.summary via /refine (a background review fork that writes memory)')
      const review = peer.waitFor((frame) => eventType(frame) === 'review.summary', 90_000)
      await peer.call('slash.exec', { session_id: sid, command: 'refine [[hh:review]]' }).catch((error: Error) => ctx.tap.note('slash.exec refine failed', { message: error.message }))
      await review.catch((error: Error) => ctx.tap.note('no review.summary', { message: error.message }))
      ctx.tap.note('delegate_task: subagent.start / subagent.complete')
      const subDone = peer.waitFor((frame) => eventType(frame) === 'subagent.complete', 120_000)
      await prompt(peer, sid, '[[hh:delegate]] Delegate a task.')
      await turnComplete(peer, sid)
      await subDone.catch((error: Error) => ctx.tap.note('no subagent.complete', { message: error.message }))
      // Background delegation re-enters the parent with the child's result
      // as a new turn; give it time to land so the recording shows it.
      await sleep(8000)
      await peer.call('session.history', { session_id: sid })
      peer.close()
    },
  },
  {
    id: 'S22',
    title: 'two clients: client B answers an approval; what client A sees (the r4 F4 shape)',
    async run(ctx) {
      const a = await connect(ctx, 'A')
      const { sid, stored } = await createSession(a)
      await prompt(a, sid, '[[hh:plain]] Warm up.')
      await turnComplete(a, sid)
      const b = await connect(ctx, 'B')
      const resumed = await b.call<Record<string, unknown>>('session.resume', { session_id: stored, cols: 96 })
      ctx.tap.note('B attached', { bSid: resumed.session_id, aSid: sid })
      // A never answers; B answers whatever approval reaches it.
      a.onServerRequest = (frame) => (frame.method === 'approval' ? 'defer' : undefined)
      let bAnswered = false
      b.onServerRequest = (frame) => {
        if (frame.method !== 'approval') return undefined
        bAnswered = true
        ctx.tap.note('B received the approval request and answers it')
        return { choice: 'once' }
      }
      const aDone = turnComplete(a, sid)
      await prompt(a, sid, '[[hh:approval]] Delete the probe directory.')
      const approval = await a.waitFor((frame) => frame.method === 'approval', 60_000)
      await sleep(2000)
      if (!bAnswered) {
        // The live request went only to A's socket; B answers through the
        // RPC Hermes provides for answering from another client.
        ctx.tap.note('approval did not reach B; B answers via request.answer')
        await b.call('request.answer', { id: approval.id, result: { choice: 'once' } }).catch((error: Error) =>
          ctx.tap.note('request.answer from B failed', { message: error.message }))
      }
      await aDone
      await sleep(3000)
      a.close()
      b.close()
    },
  },
  {
    id: 'S23',
    title: 'idle prompt.submit with queued:true vs without, same text, two fresh sessions (plan H2)',
    async run(ctx) {
      const peer = await connect(ctx)
      // A warm-up session first. Without it the FIRST variant carries
      // Hermes' one-time "[System note: This is the user's very first message
      // ever…]" in its model input (seen in the first S23 run) and its session
      // title is the un-suffixed one — differences caused by session order,
      // not by `queued`, which would make the comparison meaningless.
      const warm = await createSession(peer)
      const warmDone = turnComplete(peer, warm.sid)
      await prompt(peer, warm.sid, '[[hh:plain]] Warm-up session.')
      await warmDone
      await sleep(3000)
      ctx.model.requests.length = 0
      for (const queued of [true, false]) {
        ctx.tap.note('variant', { queued })
        const { sid } = await createSession(peer)
        const done = turnComplete(peer, sid)
        await peer.call('prompt.submit', { session_id: sid, text: '[[hh:plain]] Identical idle prompt.', ...(queued ? { queued: true } : {}) })
        await done
        await sleep(1500)
        await peer.call('session.history', { session_id: sid })
        ctx.tap.note('variant finished', { queued })
      }
      // What Hermes sent the model for each variant (the other half of
      // "equivalent"): same marker, same message roles.
      ctx.tap.note('model requests', ctx.model.requests
        .filter((request) => request.path.endsWith('/chat/completions'))
        .map((request) => {
          const body = (request.body ?? {}) as { messages?: Array<{ role: string; content?: unknown }>; stream?: boolean }
          return { stream: body.stream === true, roles: (body.messages ?? []).map((message) => message.role), lastUser: JSON.stringify((body.messages ?? []).filter((message) => message.role === 'user').at(-1)?.content ?? null).slice(0, 200) }
        }))
      peer.close()
    },
  },
  {
    id: 'S24',
    title: 'POST /api/gateway/restart against a local gateway process; action status; /api/status until gateway_running returns',
    boot: 'lane',
    async run(ctx) {
      const lane = ctx.lane!
      const base = ctx.hermes.baseUrl
      const token = ctx.tokens.access_token
      await restCall(ctx.tap, base, '/api/status', token)
      const originalPid = lane.gatewayPid()
      ctx.tap.note('gateway before restart', { pidKnownToRecorder: originalPid !== undefined })
      const restart = await restCall(ctx.tap, base, '/api/gateway/restart', token, { method: 'POST' })
      const restartPid = (restart.body as { pid?: number } | undefined)?.pid
      // The replacement gateway runs INSIDE the dashboard's detached action
      // child (hermes_cli/gateway.py _cmd_restart manual path on macOS), i.e.
      // outside every process handle this recorder owns. Kill it in cleanup
      // so no `hermes gateway` outlives the run (the next restart's orphan
      // reaper scans every process of this user for gateway argv).
      ctx.cleanup = async () => {
        if (typeof restartPid === 'number') {
          try {
            process.kill(restartPid, 'SIGTERM')
            await sleep(4000)
            process.kill(restartPid, 'SIGKILL')
          } catch {
            // already gone
          }
        }
      }
      let sawDown = false
      let back = false
      const end = Date.now() + 120_000
      while (Date.now() < end) {
        await sleep(1000)
        const status = await restCall(ctx.tap, base, '/api/status', token)
        const running = (status.body as { gateway_running?: boolean } | undefined)?.gateway_running
        if (running === false) sawDown = true
        if (running === true && (sawDown || Date.now() > end - 100_000)) {
          back = true
          break
        }
      }
      ctx.tap.note('gateway_running after restart', { sawDown, back })
      await restCall(ctx.tap, base, '/api/actions/gateway-restart/status?lines=40', token)
      // The decisive check that a gateway is really back: a phone message is
      // answered by the replacement (it reads the same home's .env).
      ctx.tap.note('phone message after restart')
      await lane.dm('[[hh:say-phone-after-restart]] Is the gateway back?')
      await lane.waitPhoneText('ANSWER phone-after-restart', undefined, 90_000)
        .then(() => ctx.tap.note('phone answered after restart'))
        .catch((error: Error) => ctx.tap.note('phone NOT answered after restart', { message: error.message }))
      ctx.tap.note('second restart inside the 10 s coalescing cooldown')
      await restCall(ctx.tap, base, '/api/gateway/restart', token, { method: 'POST' })
      await restCall(ctx.tap, base, '/api/actions/gateway-restart/status?lines=40', token)
    },
  },
  {
    id: 'S25',
    title: 'v0.20.4 handshake: ticket → upgrade → gateway.ready → client.capabilities answers -32601',
    onlyWhenNamed: true,
    // See LiveHermesOptions.bindHost: the only way to engage v0.20.4's auth
    // gate (as on the VPS) while staying loopback-only.
    options: { bindHost: 'localhost.' },
    async run(ctx) {
      const ticket = await mintTicket(ctx.tap, ctx.hermes.baseUrl, ctx.tokens.access_token)
      const peer = new RawPeer(ctx.tap)
      const url = new URL('/api/ws', ctx.hermes.baseUrl)
      url.protocol = 'ws:'
      url.searchParams.set('ticket', ticket)
      await peer.open(url.toString())
      await peer.waitFor((frame) => eventType(frame) === 'gateway.ready', 15_000, true)
      await peer.call('client.capabilities', { server_requests: true }).catch((error: Error) =>
        ctx.tap.note('client.capabilities failed', { message: error.message }))
      // What the old server does with the next calls the client would make
      // if it ignored the handshake error (r1: v0.20.4 also lacks
      // session.events.since and request.answer).
      await peer.call('session.events.since', { session_id: 'none', last_seen: 0 }).catch((error: Error) =>
        ctx.tap.note('session.events.since failed', { message: error.message }))
      await sleep(500)
      peer.close()
      await Promise.race([peer.closed, sleep(2000)])
    },
  },
  {
    id: 'S26',
    title: 'connection.request (form): manage_connections installs a catalog MCP that needs a credential; the card is skipped',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:connect-form]] Connect n8n.')
      const request = await peer.waitFor((frame) => eventType(frame) === 'connection.request', 60_000)
      const payload = (request.params as { payload: { op_id: string; targets: Array<{ name: string }> } }).payload
      ctx.tap.note('connection.request targets', { names: payload.targets.map((target) => target.name) })
      // Skip, as the card's Esc does: the turn must resume with the tool's answer.
      await peer.call('connection.respond', { op_id: payload.op_id, owner: { session_id: sid, type: 'session' }, result: { targets: payload.targets.map((target) => ({ name: target.name, status: 'skipped' })) } })
      await turnComplete(peer, sid)
      peer.close()
    },
  },
  {
    id: 'S27',
    title: 'connection.request (oauth): a catalog MCP whose install is a browser authorization; the card is skipped',
    async run(ctx) {
      const peer = await connect(ctx)
      const { sid } = await createSession(peer)
      await prompt(peer, sid, '[[hh:connect-oauth]] Connect Airtable.')
      const request = await peer.waitFor((frame) => eventType(frame) === 'connection.request', 60_000)
      const payload = (request.params as { payload: { op_id: string; targets: Array<{ name: string }> } }).payload
      ctx.tap.note('connection.request targets', { names: payload.targets.map((target) => target.name) })
      // Hold the card a moment so any watcher transitions (connection.update) are recorded.
      await sleep(3000)
      await peer.call('connection.respond', { op_id: payload.op_id, owner: { session_id: sid, type: 'session' }, result: { targets: payload.targets.map((target) => ({ name: target.name, status: 'skipped' })) } })
      await turnComplete(peer, sid)
      peer.close()
    },
  },
]

// `--release <tag>` names the Hermes release under HERMES_LIVE_RUNTIME (default
// v0.21.7, the release the original corpus and every existing test use). The
// provenance note's commit comes from the runtime itself (research/
// provenance.ts), and files are named `<release>-<run id>.jsonl` so releases
// sit side by side in one scenario directory.
const { release, rest: wanted } = parseReleaseArgs(process.argv.slice(2))
const provenance = readProvenance(release)
const selected = wanted.length ? scenarios.filter((scenario) => wanted.includes(scenario.id)) : scenarios.filter((scenario) => !scenario.onlyWhenNamed)
if (wanted.length && selected.length !== wanted.length) {
  const known = new Set(scenarios.map((scenario) => scenario.id))
  throw new Error(`unknown scenario(s): ${wanted.filter((id) => !known.has(id)).join(', ')}`)
}
const runId = new Date().toISOString().replace(/[:.]/g, '-')
for (const scenario of selected) {
  const model = await startScriptedModel()
  // S24 needs a gateway process next to serve; the lane harness is exactly
  // that (a real `hermes gateway run` + serve on one home, Telegram pointed
  // at upstream's stand-in), so it is reused rather than duplicated.
  const lane = scenario.boot === 'lane' ? await startLaneHermes({ modelUrl: model.url }) : undefined
  const hermes = lane ? lane.serve : await startLiveHermes({ modelUrl: model.url, ...scenario.options })
  const dir = join(corpusRoot, scenario.id)
  mkdirSync(dir, { recursive: true })
  const tap = new WireTap(join(dir, recordingFileName(release, runId)))
  tap.note(scenario.title, {
    scenario: scenario.id,
    release: provenance.release,
    sourceCommit: provenance.sourceCommit,
    // Kept for readers of the original v0.21.7 files, which only had this.
    hermesSource: `${provenance.release} ${provenance.sourceCommit.slice(0, 8)}`,
    recordedAt: new Date().toISOString(),
    options: scenario.options ?? {},
  })
  const started = Date.now()
  // ctx.hermes is replaced by S11's restart; stop whichever process is current.
  const ctx: Ctx = { hermes, tap, tokens: undefined as unknown as Tokens, model, lane }
  try {
    await probe(tap, hermes.baseUrl)
    ctx.tokens = await passwordNativeLogin(tap, hermes.baseUrl, hermes.username, hermes.password)
    await scenario.run(ctx)
    tap.note('scenario finished')
    console.log(`${scenario.id} ok (${Math.round((Date.now() - started) / 1000)} s)`)
  } catch (error) {
    tap.note('scenario failed', { message: error instanceof Error ? error.message : String(error) })
    console.log(`${scenario.id} FAILED: ${error instanceof Error ? error.message : String(error)}`)
    process.exitCode = 1
  } finally {
    await ctx.cleanup?.()
    if (lane) await lane.stop()
    else await ctx.hermes.stop()
    await model.close()
    removeLiveHome(hermes.home)
  }
}
