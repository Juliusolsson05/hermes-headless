// One JSON-RPC connection to `hermes serve` over ONE WebSocket generation.
//
// Hard part #1 of the decomposition (§8 Stage 3, isolation table §9). It owns:
//   - request/response matching with per-call timeouts,
//   - the `gateway.ready` → `client.capabilities` handshake,
//   - the heartbeat Hermes asks for (`gateway.ready.heartbeat`),
//   - declining server→client requests no consumer can answer,
//   - EXACTLY-ONCE event dispatch across reconnects (seq watermarks + replay hold).
//
// It deliberately does NOT own reconnecting. A reconnect needs a fresh
// single-use ticket, which needs the app's credentials (decomposition §10), so
// the owner (HermesHeadless, driven by the app) dials a new socket and opens a
// new RpcConnection over it, passing the SAME watermark store. Keeping the
// loop out of here is also what keeps this class testable against recordings
// with a fake socket and no clock games.
//
// Behaviour is calibrated on recordings, not on Hermes' source alone:
//   S1  gateway.ready is the first frame; capabilities answers with the list.
//   S8  a <20 s drop: `session.events.since` returns exactly seq 10..17 and the
//       live stream resumes at 18 on the new socket.
//   S10 `open_requests` re-delivers an unanswered approval after a drop.
//   S11 a backend restart: `events.since` answers `[]` with a NEW epoch.

import { EventEmitter } from 'node:events'

import { classifyFrame, envelopeFromReplay } from '../contract/frames.js'
import type { EventEnvelope, GatewayReadyPayload, OpenRequestEntry, RpcErrorBody, SessionEventsSinceResult } from '../contract/wire.js'

/** The subset of the WHATWG WebSocket this class needs; Node 22+ and Electron provide it. */
export type WebSocketLike = {
  readonly readyState: number
  send(data: string): void
  close(code?: number, reason?: string): void
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void
  addEventListener(type: 'close', listener: (event: { code: number; reason: string }) => void): void
  addEventListener(type: 'error', listener: (event: unknown) => void): void
}

export class RpcError extends Error {
  constructor(readonly code: number, message: string, readonly data?: unknown, readonly method?: string) {
    super(message)
    this.name = 'RpcError'
  }
}

/** The call was written but no answer arrived in time. Its effect is UNKNOWN. */
export class RpcTimeoutError extends Error {
  constructor(readonly method: string, readonly timeoutMs: number) {
    super(`${method} timed out after ${timeoutMs} ms`)
    this.name = 'RpcTimeoutError'
  }
}

/** The socket closed while the call was outstanding. Its effect is UNKNOWN. */
export class ConnectionClosedError extends Error {
  constructor(readonly method: string | null, readonly code: number, readonly reason: string) {
    super(`Hermes connection closed (${code}${reason ? ` ${reason}` : ''})${method ? ` during ${method}` : ''}`)
    this.name = 'ConnectionClosedError'
  }
}

export type ServerRequest = {
  id: string
  method: string
  params: Record<string, unknown>
  /** True when re-delivered from `open_requests` after a reconnect (S10). */
  replayed: boolean
}

export type RpcConnectionOptions = {
  /** Which server→client request methods have a consumer. Everything else gets -32601 at once (H11). */
  canAnswer: (method: string) => boolean
  /**
   * Per-session highest dispatched `seq`. SHARED across connections by the
   * owner: it is what lets the next connection replay exactly the gap.
   */
  watermarks: Map<string, number>
  requestTimeoutMs?: number
  readyTimeoutMs?: number
  heartbeatIntervalMs?: number
  heartbeatDeadlineMs?: number
  /** Injected clock for tests. */
  setTimer?: (fn: () => void, ms: number) => unknown
  clearTimer?: (handle: unknown) => void
}

type Pending = {
  method: string
  resolve: (value: unknown) => void
  reject: (error: Error) => void
  timer: unknown
}

export type ReplayOutcome =
  /** Missing events were dispatched in order; live delivery continues. */
  | { kind: 'replayed'; dispatched: number; openRequests: OpenRequestEntry[] }
  /** The ring no longer holds the gap (512 events / 4 MiB per session): re-snapshot. */
  | { kind: 'truncated'; openRequests: OpenRequestEntry[] }
  /** A different backend process: runtime ids and seqs are void, re-snapshot by stored id. */
  | { kind: 'epoch-changed'; epoch: string }

// Hermes' own client defaults (apps/shared/src/json-rpc-gateway.ts,
// json-rpc-channel.ts): 120 s per request, 15 s heartbeat, 45 s deadline.
const DEFAULT_REQUEST_TIMEOUT_MS = 120_000
const DEFAULT_READY_TIMEOUT_MS = 15_000
const DEFAULT_HEARTBEAT_INTERVAL_MS = 15_000
const DEFAULT_HEARTBEAT_DEADLINE_MS = 45_000
const OPEN = 1

export class RpcConnection extends EventEmitter {
  private nextId = 1
  private readonly pending = new Map<string, Pending>()
  /** Sessions whose replay fetch is in flight; their live seq'd events wait here. */
  private readonly held = new Map<string, EventEnvelope[]>()
  /** Requests already surfaced on THIS connection, so a replayed copy is not delivered twice. */
  private readonly surfacedRequests = new Set<string>()
  /** Requests surfaced from `open_requests` (no live frame on this socket); see answer(). */
  private readonly replayedRequests = new Set<string>()
  private closed: { code: number; reason: string } | null = null
  private lastInbound = Date.now()
  private heartbeat: unknown = null
  private readyPayload: GatewayReadyPayload | null = null
  private readonly setTimer: (fn: () => void, ms: number) => unknown
  private readonly clearTimer: (handle: unknown) => void

  private constructor(private readonly socket: WebSocketLike, private readonly options: RpcConnectionOptions) {
    super()
    this.setTimer = options.setTimer ?? ((fn, ms) => setTimeout(fn, ms))
    this.clearTimer = options.clearTimer ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>))
    socket.addEventListener('message', (event) => this.onMessage(event.data))
    socket.addEventListener('close', (event) => this.onClose(event.code, event.reason))
    socket.addEventListener('error', () => {
      // The close event that follows carries the code; nothing to do here
      // except make sure an error never surfaces as an unhandled emitter error.
    })
  }

  /**
   * Wrap an OPEN socket, wait for `gateway.ready`, announce that this client
   * answers server requests, and start the heartbeat when Hermes asks for one.
   *
   * WHY capabilities is sent on every connection, before anything else:
   * without it Hermes treats this client as an app build older than
   * server→client requests and fails every approval for it at once
   * (tui_gateway/AGENTS.md, server_requests.py:149-154): the agent would
   * proceed as if the user had not answered.
   */
  static async open(socket: WebSocketLike, options: RpcConnectionOptions): Promise<RpcConnection> {
    const connection = new RpcConnection(socket, options)
    const ready = await connection.waitForReady(options.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS)
    connection.readyPayload = ready
    await connection.call('client.capabilities', { server_requests: true })
    if (ready.heartbeat === true) connection.startHeartbeat()
    return connection
  }

  /** The backend process identity; a change means every runtime id is void. */
  get epoch(): string | null {
    return typeof this.readyPayload?.replay_epoch === 'string' ? this.readyPayload.replay_epoch : null
  }

  get isOpen(): boolean {
    return this.closed === null && this.socket.readyState === OPEN
  }

  private waitForReady(timeoutMs: number): Promise<GatewayReadyPayload> {
    return new Promise((resolve, reject) => {
      const timer = this.setTimer(() => {
        cleanup()
        reject(new Error(`Hermes did not send gateway.ready within ${timeoutMs} ms`))
        this.close(4000, 'no-ready')
      }, timeoutMs)
      const onReady = (payload: GatewayReadyPayload) => {
        cleanup()
        resolve(payload)
      }
      const onClose = (info: { code: number; reason: string }) => {
        cleanup()
        reject(new ConnectionClosedError(null, info.code, info.reason))
      }
      const cleanup = () => {
        this.clearTimer(timer)
        this.off('ready', onReady)
        this.off('close', onClose)
      }
      this.on('ready', onReady)
      this.on('close', onClose)
    })
  }

  call<T = unknown>(method: string, params: Record<string, unknown> = {}, timeoutMs?: number): Promise<T> {
    if (this.closed) return Promise.reject(new ConnectionClosedError(method, this.closed.code, this.closed.reason))
    const id = `hh${this.nextId++}`
    const limit = timeoutMs ?? this.options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS
    return new Promise<T>((resolve, reject) => {
      const timer = this.setTimer(() => {
        this.pending.delete(id)
        reject(new RpcTimeoutError(method, limit))
      }, limit)
      this.pending.set(id, { method, resolve: resolve as (value: unknown) => void, reject, timer })
      try {
        this.socket.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }))
      } catch (error) {
        this.clearTimer(timer)
        this.pending.delete(id)
        reject(new ConnectionClosedError(method, 1006, error instanceof Error ? error.message : String(error)))
      }
    })
  }

  /**
   * Answer a server→client request, by the path that reaches Hermes.
   *
   * WHY two paths: a request that arrived as a live frame on THIS socket is
   * answered with a JSON-RPC response carrying its id. A request re-delivered
   * from `open_requests` after a reconnect never had a frame on this socket;
   * Hermes provides `request.answer {id, result}` for exactly that client
   * (contracts/prompt_voice.py), and S10 records it settling the approval
   * (`{"status":"ok"}`, then the tool completes).
   */
  async answer(id: string, result: unknown): Promise<void> {
    if (this.replayedRequests.has(id)) {
      await this.call('request.answer', { id, result })
      return
    }
    this.write({ jsonrpc: '2.0', id, result })
  }

  /** Decline a server→client request (default: method not found, as Hermes' own client does). */
  decline(id: string, code = -32601, message = 'not supported by this client'): void {
    this.write({ jsonrpc: '2.0', id, error: { code, message } })
  }

  /**
   * Fetch the events this client missed for `sessionId` and dispatch exactly
   * the gap, in order, with no duplicates against frames arriving live.
   *
   * WHY the hold: the replay response and live frames race on the same
   * socket. Without parking live seq'd frames for this session until the
   * replay lands, a live frame either dispatches twice (once live, once in the
   * replay) or advances the watermark past the gap so the replay's older
   * events are dropped as duplicates (Hermes Desktop hit both,
   * json-rpc-gateway.ts replayHold comment).
   */
  async replay(sessionId: string, expectedEpoch: string | null): Promise<ReplayOutcome> {
    const lastSeen = this.options.watermarks.get(sessionId) ?? 0
    this.held.set(sessionId, [])
    let result: SessionEventsSinceResult
    try {
      result = await this.call<SessionEventsSinceResult>('session.events.since', { session_id: sessionId, last_seen: lastSeen }, 10_000)
    } catch (error) {
      this.releaseHold(sessionId)
      throw error
    }
    const openRequests = Array.isArray(result.open_requests) ? result.open_requests : []
    // Recorded in S11: after a backend restart the reply is `[]` with a new
    // epoch. Treating that as "nothing missed" would silently lose the turn.
    if (expectedEpoch && result.epoch && result.epoch !== expectedEpoch) {
      this.options.watermarks.delete(sessionId)
      this.held.delete(sessionId)
      return { kind: 'epoch-changed', epoch: result.epoch }
    }
    if (result.truncated) {
      // The gap is gone from the ring. Move the watermark to the head so live
      // delivery resumes, and tell the owner to re-snapshot (session.resume).
      this.options.watermarks.set(sessionId, Math.max(lastSeen, result.latest_seq))
      this.releaseHold(sessionId)
      return { kind: 'truncated', openRequests }
    }
    let dispatched = 0
    const events = result.events
      .map(envelopeFromReplay)
      .filter((event): event is EventEnvelope => event !== null)
      .sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0))
    for (const event of events) {
      if (this.dispatchSequenced(event, true)) dispatched += 1
    }
    this.releaseHold(sessionId)
    for (const request of openRequests) this.surfaceRequest(request.id, request.method, request.params ?? {}, true)
    return { kind: 'replayed', dispatched, openRequests }
  }

  /**
   * After a snapshot (resume) the client holds the conversation up to "now"
   * but no seq for it. Ask the ring for its head so the NEXT drop replays only
   * what follows the snapshot. Without this a drop before the first live
   * event replays the whole ring from seq 0 and re-paints every earlier turn
   * on top of the rows the snapshot already committed.
   */
  async primeWatermark(sessionId: string): Promise<void> {
    const result = await this.call<SessionEventsSinceResult>('session.events.since', {
      session_id: sessionId,
      // Past any real seq: the ring returns nothing but reports its head.
      last_seen: Number.MAX_SAFE_INTEGER,
    }, 10_000)
    const head = typeof result.latest_seq === 'number' ? result.latest_seq : 0
    this.options.watermarks.set(sessionId, Math.max(this.options.watermarks.get(sessionId) ?? 0, head))
  }

  /** Re-deliver `open_requests` from a create/resume/activate snapshot. */
  surfaceOpenRequests(openRequests: OpenRequestEntry[] | null | undefined): void {
    for (const request of openRequests ?? []) this.surfaceRequest(request.id, request.method, request.params ?? {}, true)
  }

  close(code = 1000, reason = ''): void {
    if (this.closed) return
    try {
      this.socket.close(code, reason)
    } catch {
      // Already closing; the close event (or our synthetic one below) settles state.
    }
    this.onClose(code, reason)
  }

  private releaseHold(sessionId: string): void {
    const parked = this.held.get(sessionId) ?? []
    this.held.delete(sessionId)
    for (const event of parked) this.dispatchSequenced(event, false)
  }

  /** Dispatch a seq'd event once. Returns false when it was a duplicate. */
  private dispatchSequenced(event: EventEnvelope, replayed: boolean): boolean {
    const sid = event.session_id
    if (!sid || typeof event.seq !== 'number') {
      this.emit('event', event, { replayed })
      return true
    }
    const watermark = this.options.watermarks.get(sid) ?? 0
    if (event.seq <= watermark) return false
    this.options.watermarks.set(sid, event.seq)
    this.emit('event', event, { replayed })
    return true
  }

  private surfaceRequest(id: string, method: string, params: Record<string, unknown>, replayed: boolean): void {
    if (this.surfacedRequests.has(id)) return
    this.surfacedRequests.add(id)
    if (replayed) this.replayedRequests.add(id)
    if (!this.options.canAnswer(method)) {
      // A replayed request has no live frame on THIS socket to answer, so a
      // JSON-RPC error response would go nowhere. Hermes' `request.answer`
      // with an error-free empty result is not a decline either; leave it to
      // time out server-side, which is what an unsupported bridge does today.
      if (!replayed) this.decline(id)
      return
    }
    const request: ServerRequest = { id, method, params, replayed }
    this.emit('request', request)
  }

  private onMessage(data: unknown): void {
    this.lastInbound = Date.now()
    const text = typeof data === 'string' ? data : data instanceof ArrayBuffer ? new TextDecoder().decode(data) : String(data)
    let classified
    try {
      classified = classifyFrame(JSON.parse(text))
    } catch (error) {
      this.emit('unknown', { reason: `unparseable frame: ${error instanceof Error ? error.message : String(error)}` })
      return
    }
    switch (classified.kind) {
      case 'response': {
        const pending = this.pending.get(classified.id)
        if (!pending) return
        this.pending.delete(classified.id)
        this.clearTimer(pending.timer)
        if (classified.error) {
          pending.reject(new RpcError(classified.error.code, classified.error.message, classified.error.data, pending.method))
        } else {
          pending.resolve(classified.result)
        }
        return
      }
      case 'request':
        this.surfaceRequest(classified.id, classified.method, classified.params, false)
        return
      case 'event': {
        const event = classified.event
        if (event.type === 'gateway.ready') {
          this.emit('ready', (event.payload ?? {}) as GatewayReadyPayload)
          return
        }
        const sid = event.session_id
        if (sid && typeof event.seq === 'number' && this.held.has(sid)) {
          this.held.get(sid)!.push(event)
          return
        }
        this.dispatchSequenced(event, false)
        return
      }
      case 'invalid':
        this.emit('unknown', { reason: classified.reason })
    }
  }

  private onClose(code: number, reason: string): void {
    if (this.closed) return
    this.closed = { code, reason }
    if (this.heartbeat !== null) this.clearTimer(this.heartbeat)
    for (const [id, pending] of this.pending) {
      this.clearTimer(pending.timer)
      pending.reject(new ConnectionClosedError(pending.method, code, reason))
      this.pending.delete(id)
    }
    this.held.clear()
    this.emit('close', { code, reason })
  }

  private write(frame: Record<string, unknown>): void {
    if (this.closed) return
    try {
      this.socket.send(JSON.stringify(frame))
    } catch {
      // The close handler owns the consequences.
    }
  }

  private startHeartbeat(): void {
    const interval = this.options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS
    const deadline = this.options.heartbeatDeadlineMs ?? DEFAULT_HEARTBEAT_DEADLINE_MS
    const tick = () => {
      if (this.closed) return
      // Any inbound frame counts as liveness (Hermes' own client, #115251):
      // a turn streaming deltas is alive even if a ping reply is queued
      // behind them.
      if (Date.now() - this.lastInbound > deadline) {
        this.close(4000, 'heartbeat-timeout')
        return
      }
      this.call('gateway.ping', {}, deadline).catch(() => {
        // The deadline check above, not this promise, decides liveness.
      })
      this.heartbeat = this.setTimer(tick, interval)
    }
    this.heartbeat = this.setTimer(tick, interval)
  }
}

export type { RpcErrorBody }
