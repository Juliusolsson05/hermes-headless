// LIVE integration tier: a real `hermes serve` (v0.21.7, auth gate on) + the
// scripted model, driven through the public HermesHeadless API exactly as the
// app will drive it: native PKCE sign-in, a ticket per dial, one conversation.
//
// Run with `npm run live:setup` once, then `npm run test:live`. Skipped (not
// failed) when the runtime is absent, because CI has no Python/Hermes; the
// recorded-corpus tiers are what CI runs.

import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { HermesHeadless, type HermesRequest, type HermesSessionState } from './HermesHeadless.js'
import type { CommittedRow, SemanticEvent } from './reconcile/ConversationReconciler.js'
import { buildWsUrl, wsTicketProtocols, HERMES_GATEWAY_SUBPROTOCOL, createPkce, exchangeCode, mintWsTicket, endpoint, type HermesTokens } from './auth/index.js'
import { backendCompatibility } from './version.js'
import { liveRuntimeAvailable, removeLiveHome, startLiveHermes, type LiveHermes } from './testing/live-hermes/harness.js'
import { startScriptedModel, type ScriptedModelServer } from './testing/scripted-model/server.js'

const live = liveRuntimeAvailable() ? describe : describe.skip

/** Headless password sign-in through the real native PKCE routes. */
async function signIn(hermes: LiveHermes): Promise<HermesTokens> {
  const pkce = createPkce()
  const authorize = new URL(endpoint(hermes.baseUrl, '/auth/native/authorize'))
  authorize.searchParams.set('code_challenge', pkce.challenge)
  authorize.searchParams.set('code_challenge_method', 'S256')
  authorize.searchParams.set('redirect_uri', 'http://127.0.0.1:53682/callback')
  authorize.searchParams.set('state', pkce.state)
  const first = await fetch(authorize, { redirect: 'manual' })
  const cookie = first.headers.getSetCookie().map((line) => line.split(';')[0]).join('; ')
  const login = await fetch(endpoint(hermes.baseUrl, '/auth/password-login'), {
    method: 'POST',
    headers: { 'content-type': 'application/json', cookie },
    body: JSON.stringify({ provider: 'basic', username: hermes.username, password: hermes.password }),
  })
  const next = new URL(((await login.json()) as { next: string }).next)
  return exchangeCode({ baseUrl: hermes.baseUrl, code: next.searchParams.get('code')!, codeVerifier: pkce.verifier })
}

type Harness = {
  headless: HermesHeadless
  semantic: SemanticEvent[]
  committed: CommittedRow[]
  requests: HermesRequest[]
  sessions: HermesSessionState[]
  sockets: WebSocket[]
}

function attach(hermes: () => LiveHermes, tokens: () => HermesTokens, target: ConstructorParameters<typeof HermesHeadless>[0]['target']): Harness {
  const sockets: WebSocket[] = []
  const headless = new HermesHeadless({
    target,
    dial: async () => {
      const { ticket } = await mintWsTicket({ baseUrl: hermes().baseUrl, accessToken: tokens().accessToken })
      // The ticket rides the subprotocol, never the URL (auth/index.ts buildWsUrl).
      const socket = new WebSocket(buildWsUrl(hermes().baseUrl), wsTicketProtocols(ticket))
      sockets.push(socket)
      await new Promise<void>((resolve, reject) => {
        socket.addEventListener('open', () => resolve(), { once: true })
        socket.addEventListener('error', () => reject(new Error('socket failed to open')), { once: true })
      })
      // A REAL server proves it took the ticket from the header: it answers
      // with the gateway subprotocol alone.
      expect(socket.protocol).toBe(HERMES_GATEWAY_SUBPROTOCOL)
      return socket
    },
  })
  const harness: Harness = { headless, semantic: [], committed: [], requests: [], sessions: [], sockets }
  headless.semantic.on('event', (event: SemanticEvent) => harness.semantic.push(event))
  headless.committed.on('entry', (row: CommittedRow) => harness.committed.push(row))
  headless.on('request', (request: HermesRequest) => harness.requests.push(request))
  headless.on('session', (state: HermesSessionState) => harness.sessions.push(state))
  return harness
}

async function until<T>(read: () => T | undefined | null | false, timeoutMs = 60_000, what = 'condition'): Promise<T> {
  const started = Date.now()
  for (;;) {
    const value = read()
    if (value) return value
    if (Date.now() - started > timeoutMs) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

const completedTurns = (harness: Harness) => harness.semantic.filter((event) => event.type === 'turn_completed').length
const lastText = (harness: Harness) => harness.semantic.filter((event) => event.type === 'text_delta').map((event) => String(event.textSoFar)).at(-1) ?? ''

live('HermesHeadless against a real hermes serve', () => {
  let model: ScriptedModelServer
  let hermes: LiveHermes
  let tokens: HermesTokens

  beforeAll(async () => {
    model = await startScriptedModel()
    hermes = await startLiveHermes({ modelUrl: model.url, approvalTimeoutSeconds: 60 })
    tokens = await signIn(hermes)
  }, 180_000)

  afterAll(async () => {
    await hermes?.stop()
    await model?.close()
    if (hermes) removeLiveHome(hermes.home)
  })

  it('creates a conversation on a tested backend, streams a reply and commits its rows', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    const info = harness.sessions.at(-1)!.info
    expect(backendCompatibility(info?.desktop_contract).kind).toBe('tested')
    await harness.headless.prompt('[[hh:plain]] Say hello.')
    await until(() => completedTurns(harness) === 1, 60_000, 'turn')
    expect(lastText(harness)).toBe('Hello from the scripted model.')
    await until(() => harness.committed.some((row) => row.row.role === 'assistant'), 20_000, 'committed assistant row')
    expect(harness.committed.map((row) => row.row.role)).toEqual(['user', 'assistant'])
    await harness.headless.stop()
  }, 120_000)

  it('runs tools and answers an approval, a clarify, a sudo and a secret request', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    harness.headless.on('request', (request: HermesRequest) => {
      if (request.method === 'approval') void harness.headless.answerRequest(request.id, { choice: 'once' })
      if (request.method === 'clarify') {
        const questions = request.params.questions as Array<{ qid: string }>
        void harness.headless.answerRequest(request.id, { answers: Object.fromEntries(questions.map((question) => [question.qid, 'red'])) })
      }
      if (request.method === 'sudo' || request.method === 'secret') void harness.headless.declineRequest(request)
    })
    await harness.headless.start()
    for (const [index, scenario] of ['tools', 'approval', 'clarify', 'sudo', 'secret'].entries()) {
      await harness.headless.prompt(`[[hh:${scenario}]] Go.`)
      await until(() => completedTurns(harness) === index + 1, 90_000, `${scenario} turn`)
    }
    expect(harness.requests.map((request) => request.method)).toEqual(['approval', 'clarify', 'sudo', 'secret'])
    const results = harness.semantic.filter((event) => event.type === 'tool_result')
    expect(results.length).toBeGreaterThanOrEqual(7)
    await harness.headless.stop()
  }, 300_000)

  it('survives a dropped socket mid-turn: reconnect replays the gap exactly once', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    await harness.headless.prompt('[[hh:slow]] Stream slowly.')
    await until(() => lastText(harness).includes('chunk-03'), 60_000, 'some chunks')
    harness.sockets.at(-1)!.close(4000, 'test-drop')
    await new Promise((resolve) => setTimeout(resolve, 3000))
    await harness.headless.reconnect()
    await until(() => completedTurns(harness) === 1, 60_000, 'turn after reconnect')
    const expected = Array.from({ length: 40 }, (_, index) => `chunk-${String(index + 1).padStart(2, '0')}`).join(' ')
    expect(lastText(harness).trim()).toBe(expected)
    await harness.headless.stop()
  }, 120_000)

  it('holds a prompt sent while a turn runs instead of letting Hermes redirect (kill) the turn', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    await harness.headless.prompt('[[hh:slow]] Long one.')
    await until(() => lastText(harness).includes('chunk-02'), 60_000, 'slow turn streaming')
    const outcome = await harness.headless.prompt('[[hh:plain]] Busy prompt.')
    expect(outcome.status).toBe('queued')
    await until(() => completedTurns(harness) === 2, 90_000, 'both turns')
    const stops = harness.semantic.filter((event) => event.type === 'turn_stopped').map((event) => event.stopReason)
    expect(stops).toEqual(['end_turn', 'end_turn'])
    await harness.headless.stop()
  }, 150_000)

  it('/new starts a fresh conversation in the same headless; the old one resumes elsewhere', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    await harness.headless.prompt('[[hh:plain]] First conversation.')
    await until(() => completedTurns(harness) === 1, 60_000, 'first turn')
    const firstStored = harness.headless.storedSessionId
    expect(await harness.headless.runSlash('/new')).toEqual({ kind: 'new-conversation' })
    expect(harness.headless.storedSessionId).not.toBe(firstStored)
    await harness.headless.stop()

    const resumed = attach(() => hermes, () => tokens, { mode: 'resume', storedSessionId: firstStored })
    await resumed.headless.start()
    expect(resumed.committed.map((row) => row.row.text)).toContain('Hello from the scripted model.')
    await resumed.headless.stop()
  }, 120_000)

  it('routes slash commands like Hermes Desktop: built-ins print, skills become prompts', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    // Hermes builds the agent lazily on the first prompt (session.create:
    // "agent builds after the reply"); before that `/model` truthfully prints
    // "Current model: (unknown)". Recorded here as the first assertion, then
    // the realistic case: a command in a chat that has started.
    expect(await harness.headless.runSlash('/model')).toMatchObject({ kind: 'output', text: expect.stringContaining('(unknown)') })
    await harness.headless.prompt('[[hh:plain]] Hi.')
    await until(() => completedTurns(harness) === 1, 60_000, 'first turn')
    const model = await harness.headless.runSlash('/model')
    expect(model).toMatchObject({ kind: 'output' })
    expect((model as { text: string }).text).toContain('scripted-1')
    const completions = await harness.headless.completeSlash('/mo')
    expect(completions.items?.some((item) => item.text === 'model')).toBe(true)
    const skill = await harness.headless.runSlash('/hh-secret')
    expect(skill).toMatchObject({ kind: 'prompt', display: expect.stringContaining('hh-secret') })
    await harness.headless.stop()
  }, 120_000)

  it('resumes by stored id after a backend restart (new epoch)', async () => {
    const harness = attach(() => hermes, () => tokens, { mode: 'create', source: 'agent-code' })
    await harness.headless.start()
    await harness.headless.prompt('[[hh:plain]] Before restart.')
    await until(() => completedTurns(harness) === 1, 60_000, 'turn')
    await until(() => harness.committed.some((row) => row.row.role === 'assistant'), 20_000, 'rows')
    hermes = await hermes.restart()
    // The basic provider's tokens are signed with the fixed secret, so they
    // survive the restart exactly as on a VPS with BASIC_AUTH_SECRET set.
    await harness.headless.reconnect()
    expect(harness.committed.filter((row) => row.row.role === 'assistant').length).toBeGreaterThanOrEqual(1)
    await harness.headless.prompt('[[hh:plain]] After restart.')
    await until(() => completedTurns(harness) === 2, 60_000, 'turn after restart')
    await harness.headless.stop()
  }, 180_000)
})
