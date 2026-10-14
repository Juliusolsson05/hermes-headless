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
import { HermesIncompatibleBackendError, INCOMPATIBLE_RPC_CODES } from '../errors.js'
import type { EventEnvelope, GatewayReadyPayload, RpcErrorBody, SessionEventsSinceResult } from '../contract/wire.js'

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
  /** Seq'd events parked while a conversation is being attached (see acquire). */
  private acquiring: EventEnvelope[] | null = null
  /**
   * Live server→client request frames that arrived during an acquire. Held
   * like events, because a resume snapshot closes every card as superseded:
   * surfacing one before the snapshot opened a card the snapshot then closed.
   * The ring read is AFTER all of them, so its `open_requests` decides which
   * are still open (round-1 review c2 #1).
   */
  private acquiringRequests: Array<{ id: string; method: string; params: Record<string, unknown> }> = []
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
    try {
      const ready = await connection.waitForReady(options.readyTimeoutMs ?? DEFAULT_READY_TIMEOUT_MS)
      connection.readyPayload = ready
      try {
        await connection.call('client.capabilities', { server_requests: true })
      } catch (error) {
        // A server older than contract 7 has no `client.capabilities`
        // (recorded S25, v0.20.4): typed, so the consumer can say "update
        // Hermes" instead of "failed to start" (agent-code#1495).
        if (error instanceof RpcError && INCOMPATIBLE_RPC_CODES.has(error.code)) {
          throw new HermesIncompatibleBackendError('client.capabilities', error.code)
        }
        throw error
      }
      if (ready.heartbeat === true) connection.startHeartbeat()
      return connection
    } catch (error) {
      // WHY close here: a half-opened socket nobody holds a reference to
      // would otherwise stay up until the server's idle reap, and keep
      // receiving broadcasts (r1 H5). The caller only ever sees the error.
      connection.close(4000, 'handshake-failed')
      throw error
    }
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
   * Attach a conversation to THIS socket and hand the caller one consistent
   * view of it: the attach result, the ring, and every event exactly once.
   *
   * WHY (package review A findings 1-3, each probed on recordings): Hermes
   * starts routing a session's events to a socket the moment `session.resume`
   * or `session.activate` rebinds it, BEFORE the response arrives, and the
   * response's snapshot is taken at the rebind. So without a hold:
   *   - a delta arriving before the resume response was overwritten by the
   *     snapshot (the answer lost a chunk);
   *   - replaying BEFORE activate left the replay→activate interval unread
   *     (a chunk and a freshly opened approval lost);
   *   - the resume snapshot's `open_requests` is older than a cancel that
   *     arrives while the client is still settling, and resurrected the card.
   *
   * Order, each step relying on the one before:
   *   1. park every seq'd event from now on (the runtime id of a resume is
   *      unknown until it answers, so ALL sessions are parked);
   *   2. `attach()`;
   *   3. read the ring for the attached runtime id from `lastSeen`;
   *   4. `plan(result, ring)` applies the snapshot and says from which seq the
   *      RING should be dispatched (null: none, the snapshot covers it);
   *   5. dispatch those ring events in seq order (the watermark follows the
   *      events dispatched, never the ring's head; see below);
   *   6. surface the held live requests, then the ring's other open requests;
   *   7. release the parked events in arrival order, dropping only those at
   *      or below the watermark.
   */
  async acquire<T extends { session_id: string }>(
    attach: () => Promise<T>,
    plan: (result: T, ring: SessionEventsSinceResult) => { fromSeq: number | null },
    lastSeen = 0,
  ): Promise<{ result: T; ring: SessionEventsSinceResult }> {
    if (this.acquiring) throw new Error('A conversation is already being attached on this connection.')
    this.acquiring = []
    let result: T
    let ring: SessionEventsSinceResult
    try {
      result = await attach()
      ring = await this.call<SessionEventsSinceResult>('session.events.since', {
        session_id: result.session_id,
        last_seen: lastSeen,
      }, 10_000)
    } catch (error) {
      // No ring to judge them by. On a connection that is gone (stop(),
      // sign-out) nothing is published: the consumer has already let go, and
      // a request surfaced now would reopen a card no one can answer
      // (round-2 review c1 #4, failure A). Otherwise the held live requests
      // go first and the parked events after them, so a cancel received
      // while attaching still closes its card (failure B).
      const held = this.acquiringRequests
      this.acquiringRequests = []
      if (this.closed) {
        this.acquiring = null
      } else {
        for (const request of held) this.surfaceRequest(request.id, request.method, request.params, false)
        this.releaseAcquired()
      }
      throw error
    }
    const sid = result.session_id
    const parked = this.acquiring ?? []
    const { fromSeq } = plan(result, ring)
    const ringEvents = ring.events
      .map(envelopeFromReplay)
      .filter((event): event is EventEnvelope => event !== null && event.session_id === sid && typeof event.seq === 'number')
      .sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0))
    if (fromSeq !== null) {
      // Replay the ring from `fromSeq`; each dispatch moves the watermark,
      // and the parked live copies of those events then drop as duplicates.
      this.options.watermarks.set(sid, fromSeq - 1)
      for (const event of ringEvents) this.dispatchSequenced(event, true)
    } else {
      // The snapshot covers every event BEFORE the rebind. Hermes routes the
      // session's events to this socket from the rebind on, so a ring event
      // that also arrived live (parked) is post-rebind and must still be
      // dispatched; only the ones that did not are covered.
      const liveSeqs = new Set(parked.filter((event) => event.session_id === sid).map((event) => event.seq))
      const covered = ringEvents.map((event) => event.seq as number).filter((seq) => !liveSeqs.has(seq))
      if (covered.length > 0) this.options.watermarks.set(sid, Math.max(this.options.watermarks.get(sid) ?? 0, ...covered))
    }
    // WHY no jump to `latest_seq` (round-2 review c1 #3): Hermes reads the
    // ring's events and its head under separate locks
    // (methods_session.py session.events.since → event_replay), so an event
    // stamped between the two reads is counted in the head but missing from
    // the list. Jumping to the head then dropped its live copy as a
    // duplicate: a message.complete lost, a turn that never settled.
    //
    // Requests (step 6). A live frame held during the attach is SHOWN even
    // when the ring omits it: the ring's open-request list is built before
    // its reply is written, so a request registered in between reaches us
    // first and is missing from the list (round-2 review c1 #2). One that was
    // cancelled while we attached is closed by its parked request.cancel,
    // released just below in arrival order. The ring still re-delivers what
    // is open without a live frame here (a reconnect), and stays the
    // authority over a snapshot that closed cards as superseded (round-1
    // review c2 #1); the reconciler's dedupe keeps a still-open card single.
    const heldLive = new Map(this.acquiringRequests.map((request) => [request.id, request]))
    this.acquiringRequests = []
    for (const request of heldLive.values()) {
      this.surfacedRequests.delete(request.id)
      this.replayedRequests.delete(request.id)
      this.surfaceRequest(request.id, request.method, request.params, false)
    }
    for (const request of ring.open_requests ?? []) {
      if (heldLive.has(request.id)) continue
      // A request whose live frame reached THIS socket before the attach
      // stays live, so its answer still goes back as a response frame.
      const live = this.surfacedRequests.has(request.id) && !this.replayedRequests.has(request.id)
      this.surfacedRequests.delete(request.id)
      this.replayedRequests.delete(request.id)
      this.surfaceRequest(request.id, request.method, request.params ?? {}, !live)
    }
    this.releaseAcquired()
    return { result, ring }
  }

  private releaseAcquired(): void {
    const parked = this.acquiring ?? []
    this.acquiring = null
    for (const event of parked) this.dispatchSequenced(event, false)
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
        if (this.acquiring) {
          this.acquiringRequests.push({ id: classified.id, method: classified.method, params: classified.params })
          return
        }
        this.surfaceRequest(classified.id, classified.method, classified.params, false)
        return
      case 'event': {
        const event = classified.event
        if (event.type === 'gateway.ready') {
          this.emit('ready', (event.payload ?? {}) as GatewayReadyPayload)
          return
        }
        const sid = event.session_id
        if (sid && typeof event.seq === 'number' && this.acquiring) {
          this.acquiring.push(event)
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
    this.acquiring = null
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
