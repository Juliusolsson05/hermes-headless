// Hermes remote sign-in, as pure HTTP. No storage, no browser, no timers.
//
// WHY the package stops at HTTP (decomposition §10, owner rule 2026-09-12):
// the consumer owns every credential and every side effect. Storing tokens
// (safeStorage), opening the system browser, running the loopback listener
// and single-flighting refreshes are the app's job, because only the app knows
// which connection a token belongs to and when a refresh is allowed. This
// module only knows the wire, so it can be tested against recorded exchanges
// with an injected fetch.
//
// The flow is Hermes' RFC 8252 native-app login (hermes_cli/dashboard_auth/
// routes.py, native_flow.py), recorded in research/corpus/S1:
//   GET  /auth/native/authorize?code_challenge&S256&redirect_uri&state  (system browser)
//   →    password form or OAuth upstream → 302 to http://127.0.0.1:<port>/…?code&state
//   POST /auth/native/token   {code, code_verifier}        → bearer pair
//   POST /auth/native/refresh {refresh_token, provider}    → rotated pair
//   POST /api/auth/ws-ticket  (Bearer)                     → {ticket, ttl_seconds: 30}
//   WS   /api/ws?ticket=…                                   (single use)

import { createHash, randomBytes } from 'node:crypto'

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>

export type HermesAuthErrorKind =
  /** Nothing usable came back: DNS, refused, timeout, TLS. The backend may be fine. */
  | 'transport'
  /** 401/403 for a credential that should be valid: sign in again. */
  | 'auth-rejected'
  /**
   * The ACCESS token was refused on an API route (recorded S1: a bad bearer on
   * /api/auth/ws-ticket answers 401 `{"error":"session_expired","reason":
   * "invalid_or_expired_session"}`, middleware.py's structured 401 that exists
   * "so the desktop refreshes/re-logs"). Refresh once and retry; only a failed
   * refresh means signing in again.
   */
  | 'access-expired'
  /** The refresh token itself is dead (`{"error":"session_expired"}`): sign in again. */
  | 'session-expired'
  /** 503: an auth provider (IdP) behind Hermes is unreachable. Retry later. */
  | 'unavailable'
  /** A 2xx with a body that is not the documented shape, or an unexpected status. */
  | 'protocol'

export class HermesAuthError extends Error {
  constructor(
    readonly kind: HermesAuthErrorKind,
    message: string,
    readonly status?: number,
    /** Node's network error code (ECONNREFUSED, ENOTFOUND, …) for transport failures. */
    readonly causeCode?: string,
  ) {
    super(message)
    this.name = 'HermesAuthError'
  }
}

export type HermesTokens = {
  accessToken: string
  refreshToken: string
  /** Epoch MILLISECONDS (the wire sends seconds; converted once, here). */
  expiresAt: number
  /** Which auth provider minted the pair; refresh must name it back. */
  provider: string
  userId: string
}

export type HermesAuthProvider = { name: string; displayName: string; supportsPassword: boolean }

export type HermesProbe = {
  authRequired: boolean
  authFlows: string[]
  providers: HermesAuthProvider[]
  /** `unknown` when Hermes runs from a source tree without package metadata. */
  version: string
  releaseDate: string | null
}

/**
 * Normalize a user-typed backend URL. Mirrors Hermes Desktop's
 * `normalizeRemoteBaseUrl` (apps/desktop/electron/connection-config.ts) so a
 * URL that works there works here: scheme-less `host:port` gets `http://`
 * (users paste Tailscale IPs that way), query/hash are dropped, a path prefix
 * (reverse proxy at /hermes) is kept, trailing slashes are stripped.
 */
export function normalizeBaseUrl(raw: string): string {
  let value = raw.trim()
  if (!value) throw new Error('The Hermes URL is empty.')
  if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) value = `http://${value}`
  let parsed: URL
  try {
    parsed = new URL(value)
  } catch (error) {
    throw new Error(`The Hermes URL is not valid: ${error instanceof Error ? error.message : String(error)}`)
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`The Hermes URL must start with http:// or https://, got ${parsed.protocol}`)
  }
  // `https://user:pass@host` can never work (fetch refuses URLs carrying
  // credentials) and the consumer would store and display the password in
  // plain text. Refuse it, without echoing what was typed (round-1 review c1 #3).
  if (parsed.username || parsed.password) {
    throw new Error('Remove the user name and password from the Hermes URL. Sign in with the button instead; a reverse proxy that needs its own password is not supported.')
  }
  parsed.hash = ''
  parsed.search = ''
  parsed.pathname = parsed.pathname.replace(/\/+$/, '')
  return parsed.toString().replace(/\/+$/, '')
}

/** `<base>/<path>` that keeps a reverse-proxy prefix (`new URL('/x', base)` would drop it). */
export function endpoint(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/+$/, '')}${path.startsWith('/') ? path : `/${path}`}`
}

function b64url(buffer: Buffer): string {
  return buffer.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

export type PkcePair = { verifier: string; challenge: string; state: string }

export function createPkce(): PkcePair {
  const verifier = b64url(randomBytes(32))
  return {
    verifier,
    challenge: b64url(createHash('sha256').update(verifier).digest()),
    state: b64url(randomBytes(16)),
  }
}

/**
 * The URL to open in the system browser. `redirectUri` MUST be a canonical
 * loopback IP literal (`http://127.0.0.1:<port>/…`); Hermes rejects
 * `localhost` (RFC 8252 §8.3, routes.py `_validate_loopback_redirect_uri`).
 * `provider` is optional: with one interactive provider Hermes picks it, with
 * several it renders its own chooser page.
 */
export function buildAuthorizeUrl(args: {
  baseUrl: string
  codeChallenge: string
  redirectUri: string
  state: string
  provider?: string
}): string {
  const url = new URL(endpoint(args.baseUrl, '/auth/native/authorize'))
  url.searchParams.set('code_challenge', args.codeChallenge)
  url.searchParams.set('code_challenge_method', 'S256')
  url.searchParams.set('redirect_uri', args.redirectUri)
  url.searchParams.set('state', args.state)
  if (args.provider) url.searchParams.set('provider', args.provider)
  return url.toString()
}

/** The `code` from the loopback redirect, after checking `state` (CSRF guard). */
export function readAuthorizationCode(callbackUrl: string, expectedState: string): string {
  const url = new URL(callbackUrl, 'http://127.0.0.1')
  const error = url.searchParams.get('error')
  if (error) throw new HermesAuthError('auth-rejected', `Sign-in was cancelled or refused: ${error}`)
  if (url.searchParams.get('state') !== expectedState) {
    throw new HermesAuthError('protocol', 'Sign-in state did not match; the redirect was not for this sign-in.')
  }
  const code = url.searchParams.get('code')
  if (!code) throw new HermesAuthError('protocol', 'The sign-in redirect carried no code.')
  return code
}

async function send(fetchImpl: FetchLike, url: string, init: RequestInit): Promise<Response> {
  try {
    return await fetchImpl(url, { ...init, redirect: 'manual' })
  } catch (error) {
    // undici's fetch throws `TypeError: fetch failed` with the socket error as
    // `cause`; the code is what lets the UI say "refused" vs "no such host".
    const cause = (error as { cause?: { code?: string } })?.cause
    throw new HermesAuthError(
      'transport',
      `Could not reach Hermes at ${new URL(url).origin}: ${cause?.code ?? (error instanceof Error ? error.message : String(error))}`,
      undefined,
      cause?.code,
    )
  }
}

function statusError(response: Response, what: string, body: unknown): HermesAuthError {
  const detail = typeof body === 'object' && body && 'detail' in body ? String((body as { detail: unknown }).detail) : ''
  if (response.status === 401 || response.status === 403) {
    if (typeof body === 'object' && body && (body as { error?: unknown }).error === 'session_expired') {
      return new HermesAuthError('access-expired', `${what}: the Hermes access token expired or was refused.`, response.status)
    }
    return new HermesAuthError('auth-rejected', `${what}: Hermes refused the credentials${detail ? ` (${detail})` : ''}.`, response.status)
  }
  if (response.status === 503) {
    return new HermesAuthError('unavailable', `${what}: the Hermes sign-in provider is unavailable${detail ? ` (${detail})` : ''}.`, response.status)
  }
  return new HermesAuthError('protocol', `${what}: unexpected HTTP ${response.status}${detail ? ` (${detail})` : ''}.`, response.status)
}

async function json(response: Response): Promise<unknown> {
  const text = await response.text()
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function tokensFrom(body: unknown, what: string): HermesTokens {
  const record = body as Record<string, unknown> | null
  if (!record || typeof record.access_token !== 'string' || typeof record.refresh_token !== 'string') {
    throw new HermesAuthError('protocol', `${what}: the response carried no bearer tokens.`)
  }
  const expiresAt = typeof record.expires_at === 'number' ? record.expires_at * 1000 : Date.now() + 10 * 60_000
  return {
    accessToken: record.access_token,
    refreshToken: record.refresh_token,
    expiresAt,
    provider: typeof record.provider === 'string' ? record.provider : '',
    userId: typeof record.user_id === 'string' ? record.user_id : '',
  }
}

/** Public probes: what the backend is and how it wants to be signed into. */
export async function probe(baseUrl: string, fetchImpl: FetchLike = fetch): Promise<HermesProbe> {
  const status = await send(fetchImpl, endpoint(baseUrl, '/api/status'), {})
  if (!status.ok) throw statusError(status, 'Status check', await json(status))
  const statusBody = (await json(status)) as Record<string, unknown>
  const providersResponse = await send(fetchImpl, endpoint(baseUrl, '/api/auth/providers'), {})
  const providersBody = providersResponse.ok ? ((await json(providersResponse)) as { providers?: unknown[] }) : { providers: [] }
  const health = await send(fetchImpl, endpoint(baseUrl, '/api/health'), {})
  const healthBody = health.ok ? ((await json(health)) as Record<string, unknown>) : {}
  return {
    authRequired: statusBody.auth_required === true,
    authFlows: Array.isArray(statusBody.auth_flows) ? statusBody.auth_flows.map(String) : [],
    providers: (providersBody.providers ?? []).flatMap((entry) => {
      const provider = entry as Record<string, unknown>
      if (typeof provider.name !== 'string') return []
      return [{
        name: provider.name,
        displayName: typeof provider.display_name === 'string' ? provider.display_name : provider.name,
        supportsPassword: provider.supports_password === true,
      }]
    }),
    version: typeof healthBody.version === 'string' ? healthBody.version : String(statusBody.version ?? 'unknown'),
    releaseDate: typeof statusBody.release_date === 'string' ? statusBody.release_date : null,
  }
}

export async function exchangeCode(
  args: { baseUrl: string; code: string; codeVerifier: string },
  fetchImpl: FetchLike = fetch,
): Promise<HermesTokens> {
  const response = await send(fetchImpl, endpoint(args.baseUrl, '/auth/native/token'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ code: args.code, code_verifier: args.codeVerifier }),
  })
  const body = await json(response)
  // The token endpoint answers EVERY failure with a generic 400 (no verifier
  // oracle, routes.py auth_native_token): from here a 400 means "start over".
  if (response.status === 400) throw new HermesAuthError('auth-rejected', 'The sign-in code was invalid or expired; sign in again.', 400)
  if (!response.ok) throw statusError(response, 'Sign-in', body)
  return tokensFrom(body, 'Sign-in')
}

export async function refreshTokens(
  args: { baseUrl: string; refreshToken: string; provider: string },
  fetchImpl: FetchLike = fetch,
): Promise<HermesTokens> {
  const response = await send(fetchImpl, endpoint(args.baseUrl, '/auth/native/refresh'), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ refresh_token: args.refreshToken, provider: args.provider }),
  })
  const body = await json(response)
  if (!response.ok) {
    // A refresh 401 is always "session expired" (routes.py auth_native_refresh),
    // whatever the body says: the caller must not retry the same token.
    if (response.status === 401) throw new HermesAuthError('session-expired', 'The Hermes session expired; sign in again.', 401)
    throw statusError(response, 'Token refresh', body)
  }
  return tokensFrom(body, 'Token refresh')
}

export async function mintWsTicket(
  args: { baseUrl: string; accessToken: string },
  fetchImpl: FetchLike = fetch,
): Promise<{ ticket: string; ttlSeconds: number }> {
  const response = await send(fetchImpl, endpoint(args.baseUrl, '/api/auth/ws-ticket'), {
    method: 'POST',
    headers: { authorization: `Bearer ${args.accessToken}` },
  })
  const body = (await json(response)) as Record<string, unknown> | null
  if (!response.ok) throw statusError(response, 'Connecting', body)
  if (!body || typeof body.ticket !== 'string') throw new HermesAuthError('protocol', 'Connecting: Hermes returned no WebSocket ticket.')
  return { ticket: body.ticket, ttlSeconds: typeof body.ttl_seconds === 'number' ? body.ttl_seconds : 30 }
}

/** The subprotocol Hermes answers a ticketed upgrade with (web_server_chat.py). */
export const HERMES_GATEWAY_SUBPROTOCOL = 'hermes-gateway-v1'

/**
 * `ws(s)://host[/prefix]/api/ws`, WITHOUT the ticket.
 *
 * WHY the ticket left the URL (r5, agent-code#1497 Stage B): a query-string
 * ticket lands in every reverse proxy's access log (Traefik on the owner's
 * VPS logs request URIs), and upstream's own docs warn about exactly that
 * (bot-screen.md). It is single-use with a 30 s life, so the exposure was
 * small, but Hermes has accepted the ticket as a WebSocket subprotocol since
 * v0.21.7 (web_server_chat.py:208; v0.22.0 :202-218), which no proxy logs.
 * Pass {@link wsTicketProtocols} as the socket's protocols.
 */
export function buildWsUrl(baseUrl: string): string {
  const parsed = new URL(baseUrl)
  const scheme = parsed.protocol === 'https:' ? 'wss' : 'ws'
  const prefix = parsed.pathname.replace(/\/+$/, '')
  return `${scheme}://${parsed.host}${prefix}/api/ws`
}

/**
 * `Sec-WebSocket-Protocol: hermes-gateway-v1, hermes-gateway-ticket.<t>`.
 * Hermes answers with ONLY `hermes-gateway-v1` (it never echoes the ticket),
 * so a consumer can check `socket.protocol` to prove the server took the
 * ticket from the header.
 */
export function wsTicketProtocols(ticket: string): string[] {
  return [HERMES_GATEWAY_SUBPROTOCOL, `hermes-gateway-ticket.${ticket}`]
}

/**
 * One authenticated JSON request to a dashboard REST route, with the same
 * error vocabulary as the sign-in calls (so the consumer's broker can treat
 * a REST 401 exactly like a refused ticket mint: refresh once, then sign
 * out). Every gated route accepts the native-app bearer
 * (dashboard_auth/middleware.py:169-180, "RFC 8252 native-app bearer path").
 */
export async function authorizedJson<T>(
  args: { baseUrl: string; accessToken: string; path: string; method?: 'GET' | 'POST'; body?: unknown; what: string },
  fetchImpl: FetchLike = fetch,
): Promise<T> {
  const response = await send(fetchImpl, endpoint(args.baseUrl, args.path), {
    method: args.method ?? 'GET',
    headers: {
      authorization: `Bearer ${args.accessToken}`,
      ...(args.body !== undefined ? { 'content-type': 'application/json' } : {}),
    },
    ...(args.body !== undefined ? { body: JSON.stringify(args.body) } : {}),
  })
  const body = await json(response)
  if (!response.ok) throw statusError(response, args.what, body)
  return body as T
}
