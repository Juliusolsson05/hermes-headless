// Auth client against RECORDED HTTP exchanges (research/corpus/S1, S14).
// The fake fetch answers with the recorded response for the same method and
// path, so these tests break if Hermes' auth wire changes and the corpus is
// re-recorded, not if our assumptions change.

import { describe, expect, it } from 'vitest'

import {
  HermesAuthError,
  buildAuthorizeUrl,
  buildWsUrl,
  createPkce,
  endpoint,
  exchangeCode,
  mintWsTicket,
  normalizeBaseUrl,
  probe,
  readAuthorizationCode,
  refreshTokens,
} from './index.js'
import { latestRecording, type CorpusEntry } from '../testing/corpus.js'

type Exchange = { method: string; path: string; status: number; body: unknown }

function exchanges(entries: CorpusEntry[]): Exchange[] {
  const out: Exchange[] = []
  for (let index = 0; index < entries.length - 1; index += 1) {
    const request = entries[index]
    const response = entries[index + 1]
    if (request.kind !== 'http' || request.dir !== 'out' || response.kind !== 'http' || response.dir !== 'in') continue
    const url = new URL(String(request.frame.url))
    out.push({
      method: String(request.frame.method ?? 'GET'),
      path: url.pathname,
      status: Number(response.frame.status),
      body: response.frame.body,
    })
  }
  return out
}

/** A fetch that replays recorded exchanges in order per (method, path). */
function replayFetch(recorded: Exchange[]) {
  const queues = new Map<string, Exchange[]>()
  for (const exchange of recorded) {
    const key = `${exchange.method} ${exchange.path}`
    queues.set(key, [...(queues.get(key) ?? []), exchange])
  }
  const calls: string[] = []
  const inits: Array<{ key: string; init: RequestInit | undefined }> = []
  const impl = async (input: string, init?: RequestInit) => {
    const key = `${init?.method ?? 'GET'} ${new URL(input).pathname}`
    calls.push(key)
    inits.push({ key, init })
    const next = queues.get(key)?.shift()
    if (!next) throw new Error(`no recorded exchange left for ${key}`)
    return new Response(typeof next.body === 'string' ? next.body : JSON.stringify(next.body), { status: next.status })
  }
  return { impl, calls, inits }
}

const bodyOf = (init: RequestInit | undefined) => JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>

const s1 = exchanges(latestRecording('S1'))
const base = 'http://hermes.example:9119'

describe('probe (recorded S1)', () => {
  it('reports a gated backend with the password provider and the native PKCE flow', async () => {
    const { impl } = replayFetch(s1)
    const result = await probe(base, impl)
    expect(result.authRequired).toBe(true)
    expect(result.authFlows).toContain('native_pkce')
    expect(result.providers).toEqual([{ name: 'basic', displayName: 'Username & Password', supportsPassword: true }])
  })
})

describe('token exchange and tickets (recorded S1)', () => {
  it('turns the recorded token response into a bearer pair with a millisecond expiry', async () => {
    const recorded = s1.filter((exchange) => exchange.path === '/auth/native/token')
    const { impl, inits } = replayFetch(recorded)
    const tokens = await exchangeCode({ baseUrl: base, code: 'c', codeVerifier: 'v' }, impl)
    // The verifier is what binds the code to THIS sign-in (PKCE); a request
    // without it would be refused by Hermes, and must never be sent empty.
    expect(bodyOf(inits[0].init)).toEqual({ code: 'c', code_verifier: 'v' })
    // Credentialed requests never follow redirects: a 30x to another origin
    // must not receive the body (the sign-in code) or the bearer.
    expect(inits[0].init?.redirect).toBe('manual')
    expect(tokens.provider).toBe('basic')
    expect(tokens.userId).toBe('agentcode')
    // The wire sends epoch seconds; a seconds value left as-is would make
    // every token look expired in 1970 and force a refresh on every dial.
    expect(tokens.expiresAt).toBeGreaterThan(1_700_000_000_000)
    expect(tokens.accessToken.length).toBeGreaterThan(20)
  })

  it('classifies the recorded 401 for a refused bearer as access-expired (refresh, then retry)', async () => {
    // Hermes answers a bad or expired access token with its structured
    // `session_expired` 401 so native clients refresh first. Treating it as
    // "sign in again" would send users to the browser every ~15 minutes.
    const rejected = s1.filter((exchange) => exchange.path === '/api/auth/ws-ticket' && exchange.status === 401)
    expect(rejected.length).toBeGreaterThan(0)
    expect(rejected[0].body).toMatchObject({ error: 'session_expired', reason: 'invalid_or_expired_session' })
    const { impl } = replayFetch(rejected)
    await expect(mintWsTicket({ baseUrl: base, accessToken: 'bogus' }, impl)).rejects.toMatchObject({ kind: 'access-expired', status: 401 })
  })

  it('returns the recorded ticket with its 30 s lifetime', async () => {
    const minted = s1.filter((exchange) => exchange.path === '/api/auth/ws-ticket' && exchange.status === 200).slice(0, 1)
    const { impl, inits } = replayFetch(minted)
    const ticket = await mintWsTicket({ baseUrl: base, accessToken: 'a' }, impl)
    expect((inits[0].init?.headers as Record<string, string>).authorization).toBe('Bearer a')
    expect(inits[0].init?.redirect).toBe('manual')
    expect(ticket.ttlSeconds).toBe(30)
    expect(ticket.ticket.length).toBeGreaterThan(10)
  })

  it('rotates tokens on the recorded refresh', async () => {
    const recorded = s1.filter((exchange) => exchange.path === '/auth/native/refresh').slice(0, 1)
    const { impl, inits } = replayFetch(recorded)
    const tokens = await refreshTokens({ baseUrl: base, refreshToken: 'r', provider: 'basic' }, impl)
    expect(tokens.provider).toBe('basic')
    // `provider` routes the refresh to the provider that minted the pair
    // (routes.py auth_native_refresh); omitting it makes Hermes try them all.
    expect(bodyOf(inits[0].init)).toEqual({ refresh_token: 'r', provider: 'basic' })
    expect(inits[0].init?.redirect).toBe('manual')
  })

  it('treats a refresh 401 as session-expired, never as retryable', async () => {
    // Justified literal: the password provider's 30-day refresh tokens never
    // expired inside a recording. The body is routes.py auth_native_refresh's
    // documented 401 verbatim.
    const impl = async () => new Response(JSON.stringify({ error: 'session_expired', detail: 'Refresh token expired or invalid; start a new sign-in.' }), { status: 401 })
    await expect(refreshTokens({ baseUrl: base, refreshToken: 'r', provider: 'basic' }, impl)).rejects.toMatchObject({ kind: 'session-expired' })
  })

  it('reports a refused connection as transport with the socket error code', async () => {
    const impl = async () => {
      throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } })
    }
    const error = await probe(base, impl).catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(HermesAuthError)
    expect(error).toMatchObject({ kind: 'transport', causeCode: 'ECONNREFUSED' })
  })
})

describe('access token expiry (recorded S14, 60 s lifetime)', () => {
  it('an expired token is refused with access-expired, and one refresh restores ticket minting', async () => {
    const s14 = exchanges(latestRecording('S14'))
    // The recorded tail: ticket 401 (expired) → refresh 200 → ticket 200.
    const tail = s14.slice(s14.findIndex((exchange) => exchange.path === '/api/auth/ws-ticket' && exchange.status === 401))
    expect(tail.map((exchange) => `${exchange.path} ${exchange.status}`)).toEqual([
      '/api/auth/ws-ticket 401',
      '/auth/native/refresh 200',
      '/api/auth/ws-ticket 200',
    ])
    const { impl } = replayFetch(tail)
    await expect(mintWsTicket({ baseUrl: base, accessToken: 'expired' }, impl)).rejects.toMatchObject({ kind: 'access-expired' })
    const refreshed = await refreshTokens({ baseUrl: base, refreshToken: 'r', provider: 'basic' }, impl)
    await expect(mintWsTicket({ baseUrl: base, accessToken: refreshed.accessToken }, impl)).resolves.toMatchObject({ ttlSeconds: 30 })
  })
})

describe('URLs', () => {
  it('normalizes what users paste, keeping a reverse-proxy prefix', () => {
    expect(normalizeBaseUrl('100.64.0.1:9119')).toBe('http://100.64.0.1:9119')
    expect(normalizeBaseUrl(' https://vps.example/hermes/ ')).toBe('https://vps.example/hermes')
    expect(() => normalizeBaseUrl('ftp://x')).toThrow(/http/)
    expect(endpoint('https://vps.example/hermes', '/api/status')).toBe('https://vps.example/hermes/api/status')
  })

  it('builds wss for https and keeps the prefix in the socket URL', () => {
    expect(buildWsUrl('https://vps.example/hermes', 'a b')).toBe('wss://vps.example/hermes/api/ws?ticket=a%20b')
    expect(buildWsUrl('http://127.0.0.1:9119', 't')).toBe('ws://127.0.0.1:9119/api/ws?ticket=t')
  })

  it('derives the challenge as S256 of the verifier (what Hermes checks at /auth/native/token)', async () => {
    const { createHash } = await import('node:crypto')
    const pkce = createPkce()
    const expected = createHash('sha256').update(pkce.verifier).digest('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
    expect(pkce.challenge).toBe(expected)
    expect(pkce.challenge).not.toBe(pkce.verifier)
  })

  it('builds the S256 authorize URL and checks state on the way back', () => {
    const pkce = createPkce()
    const url = new URL(buildAuthorizeUrl({ baseUrl: base, codeChallenge: pkce.challenge, redirectUri: 'http://127.0.0.1:5000/cb', state: pkce.state }))
    expect(url.pathname).toBe('/auth/native/authorize')
    expect(url.searchParams.get('code_challenge_method')).toBe('S256')
    expect(readAuthorizationCode(`http://127.0.0.1:5000/cb?code=abc&state=${pkce.state}`, pkce.state)).toBe('abc')
    expect(() => readAuthorizationCode('http://127.0.0.1:5000/cb?code=abc&state=forged', pkce.state)).toThrow(/state/)
  })
})
