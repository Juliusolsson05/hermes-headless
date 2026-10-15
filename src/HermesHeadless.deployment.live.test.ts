// DEPLOYMENT smoke (agent-code#1502): everything Agent Code does when it
// connects, against a REAL already-deployed Hermes (the owner's VPS), in
// order, so a failure names the first hop that broke.
//
//   npx tsx research/deployment-sign-in.mts <url> <token file>     # once
//   HERMES_DEPLOYMENT_TEST=1 HERMES_DEPLOYMENT_URL=<url> \
//     HERMES_DEPLOYMENT_TOKEN_FILE=<token file> npm run test:live
//
// WHY opt-in by an explicit flag rather than "skip when the URL is unset": a
// URL left in someone's shell must not make `npm run test:live` dial a
// production server by surprise. Without the flag the whole suite is skipped.
//
// READ-ONLY by construction: no prompt, no session.create/resume, no restart,
// no REST write. The only server-side effect is a ws-ticket, which expires on
// its own. (Unverified, see the PR: the handshake announces
// `client.capabilities` like every client must, and refuses any server
// request at once; this socket never binds a session, so none is expected.)
//
// OUTPUT IS PASTEABLE, on success AND failure (review of hermes-headless#4):
// every hop runs through `hop()`, which replaces whatever the library threw
// with a fixed sentence plus allowlisted fields (an HTTP status, an RPC code,
// an error class). Raw causes, response bodies, close reasons and parser
// messages never reach the reporter: a JSON.parse error quotes the token
// file, an assertion on `socket.protocol` would quote the ticket.
//
// It does not refresh an expired token on purpose: refresh rotates the
// refresh token, and silently rewriting the owner's token file from a test is
// a side effect nobody asked for. An expired file fails with the command that
// mints a new one.

import { readFileSync } from 'node:fs'

import { afterAll, describe, it } from 'vitest'

import { buildWsUrl, endpoint, HERMES_GATEWAY_SUBPROTOCOL, mintWsTicket, wsTicketProtocols, type HermesTokens } from './auth/index.js'
import { summarizeLanes } from './lane/lanes.js'
import type { SessionListRow } from './rest/HermesRest.js'
import { RpcConnection, type WebSocketLike } from './transport/RpcConnection.js'
import { TESTED_HERMES_RELEASES } from './version.js'

const enabled = process.env.HERMES_DEPLOYMENT_TEST === '1'
const deployment = enabled ? describe : describe.skip

/** Every hop's own deadline, kept under the per-test timeout so the hop's sentence wins the race. */
const HOP_MS = 15_000
const TEST_TIMEOUT_MS = 30_000

/** A failure that is safe to print: no cause is attached, on purpose. */
class HopFailure extends Error {}

function safeDetail(error: unknown): string {
  if (error instanceof HopFailure) return error.message
  const record = (error && typeof error === 'object' ? error : {}) as Record<string, unknown>
  const parts = [error instanceof Error ? error.constructor.name : typeof error]
  for (const key of ['status', 'code', 'kind'] as const) {
    const value = record[key]
    if (typeof value === 'number' || (typeof value === 'string' && /^[\w.-]{1,40}$/.test(value))) parts.push(`${key} ${value}`)
  }
  return parts.join(', ')
}

let failedAt: string | null = null

/**
 * Run one hop. After the first failure every later hop is skipped and says
 * which hop it depended on: step 3 dialing production with an `undefined`
 * ticket used to report a second, unrelated-looking fault (review).
 */
async function hop<T>(name: string, skip: (note?: string) => void, run: () => Promise<T>): Promise<T> {
  if (failedAt) {
    skip(`not run: ${failedAt} failed first`)
    throw new HopFailure('unreachable')
  }
  try {
    return await run()
  } catch (error) {
    failedAt = name
    throw new HopFailure(`${name} failed: ${safeDetail(error)}`)
  }
}

function withDeadline<T>(promise: Promise<T>, what: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new HopFailure(`${what} did not complete within ${HOP_MS / 1000} s`)), HOP_MS) }),
  ])
}

function config(): { baseUrl: string; tokens: HermesTokens } {
  const raw = process.env.HERMES_DEPLOYMENT_URL
  const tokenFile = process.env.HERMES_DEPLOYMENT_TOKEN_FILE
  if (!raw || !tokenFile) {
    throw new HopFailure('HERMES_DEPLOYMENT_TEST=1 needs HERMES_DEPLOYMENT_URL and HERMES_DEPLOYMENT_TOKEN_FILE (see the header of this file).')
  }
  // A scheme-less or malformed URL must fail HERE, by name, not as an
  // "Invalid URL" that reads like the server being unreachable.
  let baseUrl: string
  try {
    const url = new URL(raw)
    if (url.protocol !== 'https:' && url.protocol !== 'http:') throw new Error('scheme')
    if (url.username || url.password) throw new Error('credentials')
    baseUrl = `${url.origin}${url.pathname.replace(/\/+$/, '')}`
  } catch {
    throw new HopFailure('HERMES_DEPLOYMENT_URL must be an http(s) URL without credentials, e.g. https://hermes.example.com')
  }
  let tokens: HermesTokens
  try {
    tokens = JSON.parse(readFileSync(tokenFile, 'utf8')) as HermesTokens
  } catch {
    // Never the parser's message: it quotes the file, i.e. token bytes.
    throw new HopFailure(`${tokenFile} is missing or not valid JSON; write it with research/deployment-sign-in.mts.`)
  }
  if (typeof tokens.accessToken !== 'string' || !tokens.accessToken) {
    throw new HopFailure(`${tokenFile} has no accessToken; write it with research/deployment-sign-in.mts.`)
  }
  if (typeof tokens.expiresAt === 'number' && tokens.expiresAt <= Date.now()) {
    throw new HopFailure(`The token in ${tokenFile} expired at ${new Date(tokens.expiresAt).toISOString()}. Mint a new one: npx tsx research/deployment-sign-in.mts ${baseUrl} ${tokenFile}`)
  }
  return { baseUrl, tokens }
}

/** One line per finding, printed once at the end so the report reads as a whole. */
const report: string[] = []

deployment('a deployed Hermes accepts what Agent Code does on connect (read-only)', { timeout: TEST_TIMEOUT_MS }, () => {
  let baseUrl = ''
  let tokens: HermesTokens
  let ticket = ''
  // Suite scope, both: whichever hop fails, teardown closes what was opened
  // (review: a failed subprotocol check left an authenticated socket open).
  let socket: WebSocket | null = null
  let connection: RpcConnection | null = null
  let tornDown = false

  afterAll(() => {
    tornDown = true
    connection?.close(1000, 'deployment-smoke-done')
    socket?.close(1000, 'deployment-smoke-done')
    console.info(`\nHermes deployment smoke\n${report.map((line) => `  ${line}`).join('\n')}${failedAt ? `\n  stopped at: ${failedAt}` : ''}\n`)
  })

  it('0. configuration', async ({ skip }) => {
    ;({ baseUrl, tokens } = await hop('configuration', skip, async () => config()))
  })

  it('1. /api/status answers without auth', async ({ skip }) => {
    await hop('/api/status', skip, async () => {
      const response = await fetch(endpoint(baseUrl, '/api/status'), { signal: AbortSignal.timeout(HOP_MS) })
      if (response.status !== 200) throw new HopFailure(`/api/status answered HTTP ${response.status}`)
      const status = (await response.json().catch(() => { throw new HopFailure('/api/status did not answer JSON (a proxy page?)') })) as { version?: unknown; auth_required?: unknown; gateway_running?: unknown }
      const version = String(status.version ?? '')
      // REPORTED, not asserted. Every recorded v0.21.7/v0.22.0 server in the
      // corpus says `version: "unknown"` here (a source install has no package
      // version), so a version gate failed real, compatible servers. What
      // decides compatibility is the wire: step 3 fails with
      // HermesIncompatibleBackendError on a server too old for
      // `client.capabilities` (recorded S25, v0.20.4).
      const known = TESTED_HERMES_RELEASES.map((release) => release.replace(/^v/, '')).includes(version.replace(/^v/, ''))
      report.push(`status: version ${/^[\w.+-]{1,40}$/.test(version) ? version : '(unreadable)'}${known ? ' (tested)' : ' (not a tested release number; the wire decides)'}, auth_required ${String(status.auth_required === true)}, gateway_running ${String(status.gateway_running === true)}`)
    })
  })

  it('2. the bearer token mints a WebSocket ticket', async ({ skip }) => {
    await hop('ws-ticket', skip, async () => {
      const minted = await withDeadline(mintWsTicket({ baseUrl, accessToken: tokens.accessToken }), 'POST /api/auth/ws-ticket')
      if (!minted.ticket) throw new HopFailure('the ws-ticket answer carried no ticket')
      ticket = minted.ticket
      report.push(`ticket: minted (ttl ${Number(minted.ttlSeconds) || '?'}s)`)
    })
  })

  it('3. the WSS route takes the ticket in the subprotocol and completes the handshake', async ({ skip }) => {
    await hop('wss handshake', skip, async () => {
      const opened = new WebSocket(buildWsUrl(baseUrl), wsTicketProtocols(ticket))
      socket = opened
      await withDeadline(new Promise<void>((resolve, reject) => {
        opened.addEventListener('open', () => resolve(), { once: true })
        opened.addEventListener('error', () => reject(new HopFailure('the WebSocket did not open (proxy route, TLS, or the ticket was refused)')), { once: true })
      }), 'the WSS upgrade')
      // A boolean, never `expect(socket.protocol).toBe(…)`: a misbehaving
      // proxy may select the TICKET protocol, and the assertion's diff would
      // print the ticket (review).
      if (opened.protocol !== HERMES_GATEWAY_SUBPROTOCOL) {
        throw new HopFailure('the server (or a proxy in front of it) did not answer with the gateway subprotocol')
      }
      // Nothing here answers server requests: every one is refused at once
      // rather than left hanging on the server.
      const rpc = await RpcConnection.open(opened as unknown as WebSocketLike, {
        canAnswer: () => false,
        watermarks: new Map(),
        readyTimeoutMs: HOP_MS,
        requestTimeoutMs: HOP_MS,
      })
      // A handshake that finished after teardown (a test timeout) must not
      // leave a live connection behind.
      if (tornDown) {
        rpc.close(1000, 'deployment-smoke-done')
        return
      }
      connection = rpc
      report.push(`wss: open, subprotocol echoed, gateway.ready + client.capabilities ok (epoch ${rpc.epoch ? 'present' : 'absent'})`)
    })
  })

  it('4. session.list answers over the socket', async ({ skip }) => {
    await hop('ws session.list', skip, async () => {
      const result = await connection!.call<{ sessions?: unknown[] }>('session.list', { limit: 20 }, HOP_MS)
      if (!Array.isArray(result?.sessions)) throw new HopFailure('session.list answered without a sessions array')
      report.push(`ws session.list: ${result.sessions.length} row(s)`)
    })
  })

  it('5. REST /api/sessions accepts the same bearer token through the deployment\'s proxy', async ({ skip }) => {
    await hop('rest /api/sessions', skip, async () => {
      // Raw fetch, not HermesRest: the library turns any 200 without a
      // `sessions` array into an empty list, so a proxy's HTML login page
      // passed this step as "0 rows" (review). Here the deployment's own
      // answer is checked.
      const response = await fetch(endpoint(baseUrl, '/api/sessions?source=telegram&order=recent&limit=100'), {
        headers: { authorization: `Bearer ${tokens.accessToken}`, accept: 'application/json' },
        signal: AbortSignal.timeout(HOP_MS),
      })
      if (response.status !== 200) throw new HopFailure(`/api/sessions answered HTTP ${response.status} (does the proxy forward Authorization?)`)
      if (!/application\/json/i.test(response.headers.get('content-type') ?? '')) {
        throw new HopFailure('/api/sessions did not answer JSON (a proxy login page or SPA fallback?)')
      }
      const body = (await response.json().catch(() => null)) as { sessions?: unknown } | null
      if (!body || !Array.isArray(body.sessions)) throw new HopFailure('/api/sessions answered JSON without a sessions array')
      const lanes = summarizeLanes(body.sessions as SessionListRow[])
      report.push(`rest /api/sessions?source=telegram: ${body.sessions.length} row(s), ${lanes.length} messaging thread(s)`)
    })
  })
})
