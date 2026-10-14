// Naive traffic driver for Stage 0 recordings.
//
// WHY this duplicates what src/auth and src/transport will do: the recordings
// must be produced WITHOUT the code they will later judge (see tap.ts). This
// driver is deliberately minimal — fetch with manual cookies, one WebSocket,
// request/response matching by id — and does no reconciliation, replay or
// retry. Anything cleverer belongs in src/ where it is tested against these
// recordings, not here where it would shape them.

import { createHash, randomBytes } from 'node:crypto'

import type { WireTap } from './tap.js'

export type Tokens = {
  access_token: string
  refresh_token: string
  expires_at: number
  provider: string
  user_id: string
}

function b64url(buffer: Buffer): string {
  return buffer.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

async function tapFetch(tap: WireTap, url: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(url, { ...init, redirect: 'manual' })
  const text = await response.clone().text()
  let body: unknown = text
  try {
    body = JSON.parse(text)
  } catch {
    // HTML login pages and empty redirects are recorded as text (truncated:
    // the page markup is not wire contract, only its status/headers are).
    body = text.length > 400 ? `${text.slice(0, 400)}…(${text.length} chars)` : text
  }
  const headers: Record<string, string> = {}
  response.headers.forEach((value, key) => {
    headers[key] = value
  })
  tap.write('out', 'http', { method: init.method ?? 'GET', url, headers: init.headers ?? {}, body: init.body ? safeJson(String(init.body)) : undefined })
  tap.write('in', 'http', { url, status: response.status, headers, body })
  return response
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

/** The RFC 8252 PKCE flow, driven headlessly through the password provider. */
export async function passwordNativeLogin(
  tap: WireTap,
  baseUrl: string,
  username: string,
  password: string,
): Promise<Tokens> {
  const verifier = b64url(randomBytes(32))
  const challenge = b64url(createHash('sha256').update(verifier).digest())
  const state = b64url(randomBytes(16))
  // The redirect is never served: a headless driver reads the code out of the
  // `next` field the password form would have navigated to. The port is fixed
  // only so recordings are stable; Hermes validates shape, not reachability.
  const redirectUri = 'http://127.0.0.1:53682/callback'
  const authorize = new URL('/auth/native/authorize', baseUrl)
  authorize.searchParams.set('code_challenge', challenge)
  authorize.searchParams.set('code_challenge_method', 'S256')
  authorize.searchParams.set('redirect_uri', redirectUri)
  authorize.searchParams.set('state', state)
  const authorizeResponse = await tapFetch(tap, authorize.toString())
  const cookie = (authorizeResponse.headers.getSetCookie?.() ?? [])
    .map((line) => line.split(';')[0])
    .join('; ')
  const login = await tapFetch(tap, new URL('/auth/password-login', baseUrl).toString(), {
    method: 'POST',
    headers: { 'content-type': 'application/json', cookie },
    body: JSON.stringify({ provider: 'basic', username, password }),
  })
  const loginBody = (await login.json()) as { ok?: boolean; next?: string }
  if (!loginBody.next) throw new Error(`password login returned no next: ${JSON.stringify(loginBody)}`)
  const next = new URL(loginBody.next)
  if (next.searchParams.get('state') !== state) throw new Error('state mismatch')
  const code = next.searchParams.get('code')
  if (!code) throw new Error('no code in loopback redirect')
  const token = await tapFetch(tap, new URL('/auth/native/token', baseUrl).toString(), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ code, code_verifier: verifier }),
  })
  if (token.status !== 200) throw new Error(`token exchange failed: ${token.status}`)
  return (await token.json()) as Tokens
}

export async function refreshTokens(tap: WireTap, baseUrl: string, tokens: Tokens): Promise<Response> {
  return tapFetch(tap, new URL('/auth/native/refresh', baseUrl).toString(), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ refresh_token: tokens.refresh_token, provider: tokens.provider }),
  })
}

export async function mintTicket(tap: WireTap, baseUrl: string, accessToken: string): Promise<string> {
  const response = await tapFetch(tap, new URL('/api/auth/ws-ticket', baseUrl).toString(), {
    method: 'POST',
    headers: { authorization: `Bearer ${accessToken}` },
  })
  if (response.status !== 200) throw new Error(`ws-ticket failed: ${response.status}`)
  return ((await response.json()) as { ticket: string }).ticket
}

export async function probe(tap: WireTap, baseUrl: string): Promise<void> {
  for (const path of ['/api/status', '/api/auth/providers', '/api/health']) {
    await tapFetch(tap, new URL(path, baseUrl).toString())
  }
}

type Pending = { resolve: (value: unknown) => void; reject: (error: Error) => void }

/**
 * One JSON-RPC peer over a WebSocket. Every frame in either direction is
 * tapped before anything else touches it.
 */
export class RawPeer {
  private socket!: WebSocket
  private nextId = 1
  private readonly pending = new Map<string, Pending>()
  readonly frames: Array<Record<string, unknown>> = []
  private readonly waiters: Array<{ match: (frame: Record<string, unknown>) => boolean; resolve: (frame: Record<string, unknown>) => void }> = []
  /** Server→client request handler; the default declines with -32601. */
  onServerRequest: (frame: Record<string, unknown>) => unknown | undefined = () => undefined
  closed: Promise<{ code: number; reason: string }> = Promise.resolve({ code: 0, reason: '' })

  constructor(private readonly tap: WireTap) {}

  async open(url: string): Promise<void> {
    this.socket = new WebSocket(url)
    this.closed = new Promise((resolve) => {
      this.socket.addEventListener('close', (event) => {
        this.tap.write('in', 'ws-close', { code: event.code, reason: event.reason })
        resolve({ code: event.code, reason: event.reason })
      })
    })
    this.socket.addEventListener('message', (event) => {
      const text = typeof event.data === 'string' ? event.data : String(event.data)
      const frame = JSON.parse(text) as Record<string, unknown>
      this.tap.write('in', 'ws', frame)
      this.frames.push(frame)
      this.dispatch(frame)
    })
    await new Promise<void>((resolve, reject) => {
      this.socket.addEventListener('open', () => {
        this.tap.write(null, 'ws-open', { url })
        resolve()
      }, { once: true })
      this.socket.addEventListener('error', () => reject(new Error('ws error before open')), { once: true })
    })
  }

  private dispatch(frame: Record<string, unknown>): void {
    const id = frame.id
    if (typeof id === 'string' && typeof frame.method === 'string' && frame.method !== 'event') {
      const answer = this.onServerRequest(frame)
      if (answer === undefined) {
        this.send({ jsonrpc: '2.0', id, error: { code: -32601, message: 'not supported by recorder' } })
      } else if (answer !== 'defer') {
        this.send({ jsonrpc: '2.0', id, result: answer })
      }
    } else if (id !== undefined && ('result' in frame || 'error' in frame)) {
      const waiter = this.pending.get(String(id))
      if (waiter) {
        this.pending.delete(String(id))
        if ('error' in frame) waiter.reject(Object.assign(new Error(JSON.stringify(frame.error)), { rpcError: frame.error }))
        else waiter.resolve(frame.result)
      }
    }
    for (let index = this.waiters.length - 1; index >= 0; index -= 1) {
      if (this.waiters[index].match(frame)) {
        const [waiter] = this.waiters.splice(index, 1)
        waiter.resolve(frame)
      }
    }
  }

  send(frame: Record<string, unknown>): void {
    this.tap.write('out', 'ws', frame)
    this.socket.send(JSON.stringify(frame))
  }

  call<T = unknown>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    const id = `r${this.nextId++}`
    const promise = new Promise<unknown>((resolve, reject) => this.pending.set(id, { resolve, reject }))
    this.send({ jsonrpc: '2.0', id, method, params })
    return promise as Promise<T>
  }

  /** Resolve on the first (future or already-seen) frame matching `match`. */
  waitFor(match: (frame: Record<string, unknown>) => boolean, timeoutMs = 60_000, includeSeen = false): Promise<Record<string, unknown>> {
    if (includeSeen) {
      const seen = this.frames.find(match)
      if (seen) return Promise.resolve(seen)
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`waitFor timed out after ${timeoutMs} ms`)), timeoutMs)
      this.waiters.push({ match, resolve: (frame) => {
        clearTimeout(timer)
        resolve(frame)
      } })
    })
  }

  waitEvent(type: string, timeoutMs = 60_000): Promise<Record<string, unknown>> {
    return this.waitFor((frame) => frame.method === 'event' && (frame.params as { type?: string })?.type === type, timeoutMs)
  }

  close(code = 1000): void {
    this.socket.close(code)
  }

  /** Abrupt drop: what a laptop lid or Wi-Fi loss looks like to the server. */
  terminate(): void {
    // A WHATWG WebSocket has no terminate(); closing with 4000 from the client
    // still lets the server see a disconnect without a clean session.close.
    this.socket.close(4000, 'recorder-drop')
  }
}

export function eventType(frame: Record<string, unknown>): string | undefined {
  return frame.method === 'event' ? (frame.params as { type?: string })?.type : undefined
}
