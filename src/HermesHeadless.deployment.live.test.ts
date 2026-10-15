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
// no REST write. It must be safe to point at a server with live Telegram
// users. The only server-side effect is a ws-ticket, which expires on its own.
//
// What it prints is counts and versions only. No titles, chat ids, message
// text or tokens: the output is meant to be pasted into an issue.
//
// It does not refresh an expired token on purpose: refresh rotates the
// refresh token, and silently rewriting the owner's token file from a test is
// a side effect nobody asked for. An expired file fails with the command that
// mints a new one.

import { readFileSync } from 'node:fs'

import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { buildWsUrl, endpoint, HERMES_GATEWAY_SUBPROTOCOL, mintWsTicket, wsTicketProtocols, type HermesTokens } from './auth/index.js'
import { summarizeLanes } from './lane/lanes.js'
import { HermesRest } from './rest/HermesRest.js'
import { RpcConnection, type WebSocketLike } from './transport/RpcConnection.js'
import { TESTED_HERMES_RELEASES } from './version.js'

const enabled = process.env.HERMES_DEPLOYMENT_TEST === '1'
const deployment = enabled ? describe : describe.skip

function config(): { baseUrl: string; tokens: HermesTokens } {
  const baseUrl = process.env.HERMES_DEPLOYMENT_URL
  const tokenFile = process.env.HERMES_DEPLOYMENT_TOKEN_FILE
  if (!baseUrl || !tokenFile) {
    throw new Error('HERMES_DEPLOYMENT_TEST=1 needs HERMES_DEPLOYMENT_URL and HERMES_DEPLOYMENT_TOKEN_FILE (see the header of this file).')
  }
  const tokens = JSON.parse(readFileSync(tokenFile, 'utf8')) as HermesTokens
  if (typeof tokens.accessToken !== 'string' || !tokens.accessToken) {
    throw new Error(`${tokenFile} has no accessToken; write it with research/deployment-sign-in.mts.`)
  }
  if (typeof tokens.expiresAt === 'number' && tokens.expiresAt <= Date.now()) {
    throw new Error(`The token in ${tokenFile} expired at ${new Date(tokens.expiresAt).toISOString()}. Mint a new one: npx tsx research/deployment-sign-in.mts ${baseUrl} ${tokenFile}`)
  }
  return { baseUrl, tokens }
}

/** One line per finding, collected and printed once so the report reads as a whole. */
const report: string[] = []

deployment('a deployed Hermes accepts what Agent Code does on connect (read-only)', () => {
  let baseUrl: string
  let tokens: HermesTokens
  let ticket: string
  let connection: RpcConnection | null = null

  beforeAll(() => {
    ;({ baseUrl, tokens } = config())
  })

  afterAll(() => {
    connection?.close(1000, 'deployment-smoke-done')
    console.info(`\nHermes deployment smoke\n${report.map((line) => `  ${line}`).join('\n')}\n`)
  })

  it('1. /api/status answers without auth', async () => {
    const response = await fetch(endpoint(baseUrl, '/api/status'), { signal: AbortSignal.timeout(15_000) })
    expect(response.status).toBe(200)
    const status = (await response.json()) as { version?: unknown; auth_required?: unknown; gateway_running?: unknown }
    const version = String(status.version ?? '')
    // REPORTED, not asserted. Every recorded v0.21.7/v0.22.0 server in the
    // corpus says `version: "unknown"` here (a source install has no package
    // version), so a version gate failed real, compatible servers. What
    // decides compatibility is the wire: step 3 fails with
    // HermesIncompatibleBackendError on a server too old for
    // `client.capabilities` (recorded S25, v0.20.4).
    const known = TESTED_HERMES_RELEASES.map((release) => release.replace(/^v/, '')).includes(version.replace(/^v/, ''))
    report.push(`status: version ${version}${known ? ' (tested)' : ' (not a tested release number; the wire decides)'}, auth_required ${String(status.auth_required)}, gateway_running ${String(status.gateway_running)}`)
  })

  it('2. the bearer token mints a WebSocket ticket', async () => {
    const minted = await mintWsTicket({ baseUrl, accessToken: tokens.accessToken })
    expect(minted.ticket).toBeTruthy()
    ticket = minted.ticket
    report.push(`ticket: minted (ttl ${minted.ttlSeconds}s)`)
  })

  it('3. the WSS route takes the ticket in the subprotocol and completes the handshake', async () => {
    const socket = new WebSocket(buildWsUrl(baseUrl), wsTicketProtocols(ticket))
    await new Promise<void>((resolve, reject) => {
      socket.addEventListener('open', () => resolve(), { once: true })
      socket.addEventListener('error', () => reject(new Error('the WebSocket did not open (proxy route, TLS, or the ticket was refused)')), { once: true })
    })
    // A proxy that strips Sec-WebSocket-Protocol breaks ticket auth in a way
    // that looks like "socket closed": say it plainly.
    expect(socket.protocol, 'the server (or a proxy in front of it) did not echo the gateway subprotocol').toBe(HERMES_GATEWAY_SUBPROTOCOL)
    // Nothing here answers server requests: read-only, so every one is
    // refused at once rather than left hanging on the server.
    connection = await RpcConnection.open(socket as unknown as WebSocketLike, { canAnswer: () => false, watermarks: new Map() })
    report.push(`wss: open, subprotocol echoed, gateway.ready + client.capabilities ok (epoch ${connection.epoch ? 'present' : 'absent'})`)
  })

  it('4. session.list answers over the socket', async () => {
    expect(connection).not.toBeNull()
    const result = await connection!.call<{ sessions?: unknown[] }>('session.list', { limit: 20 })
    expect(Array.isArray(result.sessions)).toBe(true)
    report.push(`ws session.list: ${result.sessions!.length} row(s)`)
  })

  it('5. REST /api/sessions accepts the same bearer token through the deployment\'s proxy', async () => {
    const rest = new HermesRest({ baseUrl, accessToken: async () => tokens.accessToken })
    const listing = await rest.listSessions({ source: 'telegram', order: 'recent', limit: 100 })
    const lanes = summarizeLanes(listing.sessions)
    report.push(`rest /api/sessions?source=telegram: ${listing.sessions.length} row(s), ${lanes.length} messaging thread(s)`)
    expect(Array.isArray(listing.sessions)).toBe(true)
  })
})
