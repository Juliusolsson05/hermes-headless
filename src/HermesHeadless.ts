// HermesHeadless: one Hermes conversation over a remote `hermes serve`.
//
// The boundary the app consumes (decomposition §8 Stage 5). It composes the
// transport (RpcConnection) and the reconciler (ConversationReconciler) and
// does the I/O between them. Like every sibling package it NEVER spawns or
// owns anything: the consumer hands it `dial()`, which mints a fresh ticket
// and opens a socket, and the consumer decides WHEN to reconnect (it owns the
// credentials and the backoff policy). This class only knows HOW to resume a
// conversation on a new socket without losing or doubling anything.
//
// Reconnect ladder (each rung recorded):
//   1. replay the gap on the runtime id (`session.events.since`, S8) and
//      re-attach it (`session.activate`); open approvals come back (S10);
//   2. if the backend restarted (new epoch, S11), the gap is truncated, or the
//      runtime id was reaped (idle past the grace, S9) → `session.resume` by
//      the STORED id, which is a fresh snapshot (reset + rows + inflight).

import { EventEmitter } from 'node:events'

import { CommittedChannel, ScreenChannel, SemanticChannel } from './channels/index.js'
import { isAnswerableRequest, type AnswerableRequestMethod } from './contract/catalog.js'
import type {
  CompleteSlashResult,
  OpenRequestEntry,
  PromptSubmitResult,
  SessionCreateResult,
  SessionHistoryResult,
  SessionLiveInfo,
  SessionResumeResult,
  SlashExecResult,
} from './contract/wire.js'
import { ConversationReconciler } from './reconcile/ConversationReconciler.js'
import { ConnectionClosedError, RpcConnection, RpcError, RpcTimeoutError, type ServerRequest, type WebSocketLike } from './transport/RpcConnection.js'

export type HermesDial = () => Promise<WebSocketLike>

export type HermesTarget =
  | { mode: 'create'; source: string; title?: string }
  | { mode: 'resume'; storedSessionId: string }

export type HermesConnectionState =
  | { kind: 'connecting' }
  | { kind: 'ready'; epoch: string | null }
  | { kind: 'disconnected'; code: number; reason: string }
  | { kind: 'stopped' }

export type HermesSessionState = {
  storedSessionId: string
  runtimeSessionId: string
  title: string | null
  info: SessionLiveInfo | null
}

export type HermesRequest = {
  id: string
  method: AnswerableRequestMethod
  params: Record<string, unknown>
  replayed: boolean
}

export type SlashOutcome =
  | { kind: 'output'; text: string; warning?: string | null }
  | { kind: 'prompt'; result: PromptSubmitResult; display: string }
  | { kind: 'prefill'; text: string }
  | { kind: 'new-conversation' }
  | { kind: 'stopped' }

export type PromptOutcome = {
  /** `queued`: Hermes is holding it until the running turn ends. */
  status: PromptSubmitResult['status']
  userRowId: number | null
}

/**
 * Why a prompt failed, in the terms the app's delivery contract needs:
 * `refused` = Hermes answered with an error, so nothing ran;
 * `unknown` = the request may have reached Hermes (socket closed or timed out
 * after sending), so resending could run the turn twice.
 */
export class HermesPromptError extends Error {
  constructor(readonly kind: 'not-ready' | 'refused' | 'unknown', message: string, readonly code?: number) {
    super(message)
    this.name = 'HermesPromptError'
  }
}

export type HermesHeadlessOptions = {
  dial: HermesDial
  target: HermesTarget
  requestTimeoutMs?: number
  /** Overrides for tests. */
  heartbeatIntervalMs?: number
}

/** Hermes' "session not found" RPC code (methods_session.py resume, 4007). */
const SESSION_NOT_FOUND = 4007

export class HermesHeadless extends EventEmitter {
  readonly semantic = new SemanticChannel()
  readonly committed = new CommittedChannel()
  readonly screen = new ScreenChannel()

  private connection: RpcConnection | null = null
  private readonly watermarks = new Map<string, number>()
  private epoch: string | null = null
  private stopped = false
  private readonly reconciler: ConversationReconciler
  private historyFetch: Promise<void> | null = null

  constructor(private readonly options: HermesHeadlessOptions) {
    super()
    this.reconciler = new ConversationReconciler({
      semantic: (event) => this.semantic.publish(event),
      committed: (row) => this.committed.publishRow(row),
      historyReset: (generation, reason) => this.committed.publishBoundary({ type: 'reset', generation, reason }),
      historyCaughtUp: (generation) => this.committed.publishBoundary({ type: 'caught-up', generation }),
      activity: (state) => this.screen.publishActivity(state),
      session: (state) => this.emit('session', state satisfies HermesSessionState),
      requestOpened: (request) => {
        if (isAnswerableRequest(request.method)) {
          this.emit('request', { id: request.id, method: request.method, params: request.params, replayed: request.replayed } satisfies HermesRequest)
        }
      },
      requestClosed: (id, reason) => this.emit('request-closed', { id, reason }),
      turnSettled: () => this.scheduleHistoryFetch(),
      unknown: (shape) => this.emit('unknown', shape),
    })
  }

  get storedSessionId(): string {
    return this.reconciler.currentStoredSessionId
  }

  get isConnected(): boolean {
    return this.connection?.isOpen === true
  }

  get turnInFlight(): boolean {
    return this.reconciler.turnInFlight
  }

  /**
   * Connect and bind the conversation. Resolves once the snapshot is applied
   * (history committed, any in-flight turn and open request restored).
   *
   * A resume of a stored id the backend does not know (deleted, or never
   * prompted and since reaped: S3b showed an unprompted session has no row)
   * falls back to a NEW conversation and says so with `session-replaced`,
   * instead of leaving the pane dead.
   */
  async start(): Promise<void> {
    const connection = await this.connect()
    const target = this.options.target
    if (target.mode === 'resume') {
      try {
        const result = await connection.call<SessionResumeResult>('session.resume', { session_id: target.storedSessionId, cols: 96 })
        this.reconciler.applySnapshot(result, 'snapshot')
        await connection.primeWatermark(result.session_id)
        connection.surfaceOpenRequests(result.open_requests)
        return
      } catch (error) {
        if (!(error instanceof RpcError) || error.code !== SESSION_NOT_FOUND) throw error
        this.emit('session-replaced', { previousStoredSessionId: target.storedSessionId, reason: 'not-found' })
      }
    }
    const created = await connection.call<SessionCreateResult>('session.create', {
      source: target.mode === 'create' ? target.source : 'agent-code',
      cols: 96,
      ...(target.mode === 'create' && target.title ? { title: target.title } : {}),
    })
    this.reconciler.applySnapshot(created, 'snapshot')
  }

  /**
   * Resume the same conversation on a NEW socket after a drop. The caller
   * decides when (backoff, network change, wake); this decides how.
   */
  async reconnect(): Promise<void> {
    const runtime = this.reconciler.currentRuntimeSessionId
    const stored = this.reconciler.currentStoredSessionId
    const previousEpoch = this.epoch
    const connection = await this.connect()
    if (runtime && previousEpoch && connection.epoch === previousEpoch) {
      const outcome = await connection.replay(runtime, previousEpoch)
      if (outcome.kind === 'replayed') {
        try {
          // Re-attach the runtime session to this socket so live events and
          // new server requests flow here (S8: live resumed at seq 18).
          await connection.call('session.activate', { session_id: runtime, cols: 96 })
          return
        } catch (error) {
          // Reaped while we were away (idle past the grace, S9). Fall through.
          if (!(error instanceof RpcError)) throw error
        }
      }
    }
    if (!stored) throw new Error('Cannot reconnect: this conversation has no stored id yet.')
    const result = await connection.call<SessionResumeResult>('session.resume', { session_id: stored, cols: 96 })
    this.watermarks.clear()
    this.reconciler.applySnapshot(result, 'snapshot')
    await connection.primeWatermark(result.session_id)
    connection.surfaceOpenRequests(result.open_requests)
  }

  /**
   * Send a user message.
   *
   * WHY `queued: true` while a turn runs: a plain prompt.submit to a busy
   * session REDIRECTS (recorded S16: `{"status":"redirected"}` and the
   * running turn's answer never arrived). That is Telegram's "a new message
   * interrupts" behaviour; in a composer it would silently kill work. `queued`
   * is Hermes' flag for "hold it until the turn ends" (prompt_voice.py:
   * "the busy path must hold it, never redirect/steer").
   */
  async prompt(text: string): Promise<PromptOutcome> {
    const connection = this.connection
    const runtime = this.reconciler.currentRuntimeSessionId
    if (!connection?.isOpen || !runtime) throw new HermesPromptError('not-ready', 'Hermes is not connected.')
    try {
      const result = await connection.call<PromptSubmitResult>('prompt.submit', {
        session_id: runtime,
        text,
        ...(this.reconciler.turnInFlight ? { queued: true } : {}),
      })
      return { status: result.status ?? null, userRowId: typeof result.user_row_id === 'number' ? result.user_row_id : null }
    } catch (error) {
      if (error instanceof RpcError) throw new HermesPromptError('refused', error.message, error.code)
      if (error instanceof ConnectionClosedError || error instanceof RpcTimeoutError) {
        throw new HermesPromptError('unknown', `The message may or may not have reached Hermes: ${error.message}`)
      }
      throw error
    }
  }

  async interrupt(): Promise<void> {
    const runtime = this.reconciler.currentRuntimeSessionId
    if (!this.connection?.isOpen || !runtime) return
    await this.connection.call('session.interrupt', { session_id: runtime })
  }

  /**
   * `/new`: close this conversation's live session and start a fresh one in
   * the same pane (Telegram semantics, decomposition Decision 5). The old
   * conversation stays resumable in Hermes; `session.close` does not delete.
   */
  async newConversation(): Promise<void> {
    const connection = this.requireConnection()
    const runtime = this.reconciler.currentRuntimeSessionId
    if (runtime) {
      await connection.call('session.close', { session_id: runtime }).catch(() => {
        // Closing is best-effort: an already-reaped runtime is the same end state.
      })
      this.watermarks.delete(runtime)
    }
    const created = await connection.call<SessionCreateResult>('session.create', {
      source: this.options.target.mode === 'create' ? this.options.target.source : 'agent-code',
      cols: 96,
    })
    this.reconciler.beginNewConversation(created)
  }

  /** Answer an open approval / clarify / sudo / secret request. */
  async answerRequest(id: string, result: unknown): Promise<void> {
    const connection = this.requireConnection()
    await connection.answer(id, result)
    this.reconciler.closeRequest(id, 'answered')
  }

  /** Decline with each method's documented "no" (deny / cancel-all / skip). */
  async declineRequest(request: Pick<HermesRequest, 'id' | 'method'>): Promise<void> {
    const decline: Record<AnswerableRequestMethod, unknown> = {
      approval: { choice: 'deny' },
      // A clarify response without `answers` is cancel-all (ClarifyResult doc).
      clarify: {},
      sudo: { value: '' },
      secret: { value: '' },
    }
    await this.answerRequest(request.id, decline[request.method])
  }

  async completeSlash(text: string): Promise<CompleteSlashResult> {
    return this.requireConnection().call<CompleteSlashResult>('complete.slash', {
      text,
      session_id: this.reconciler.currentRuntimeSessionId,
    })
  }

  /**
   * Run `/command args` the way Hermes Desktop does (recorded S13):
   *  - `/new` and `/stop` are client-side (`/stop` must interrupt the turn
   *    first; Hermes' own `/stop` only stops background processes);
   *  - everything else → `slash.exec`; a skill or quick command answers 4018
   *    "use command.dispatch", whose directive is a prompt to submit.
   */
  async runSlash(input: string): Promise<SlashOutcome> {
    const body = input.trim().replace(/^\//, '')
    const [name = '', ...rest] = body.split(/\s+/)
    const arg = rest.join(' ')
    if (name === 'new' || name === 'reset') {
      await this.newConversation()
      return { kind: 'new-conversation' }
    }
    const connection = this.requireConnection()
    const runtime = this.reconciler.currentRuntimeSessionId
    if (name === 'stop') {
      await this.interrupt()
      await connection.call('slash.exec', { session_id: runtime, command: body }).catch(() => undefined)
      return { kind: 'stopped' }
    }
    let result: SlashExecResult
    try {
      result = await connection.call<SlashExecResult>('slash.exec', { session_id: runtime, command: body })
    } catch (error) {
      if (!(error instanceof RpcError) || error.code !== 4018) throw error
      result = await connection.call<SlashExecResult>('command.dispatch', { session_id: runtime, name, ...(arg ? { arg } : {}) })
    }
    if ((result.type === 'skill' || result.type === 'send') && result.message) {
      const outcome = await this.prompt(result.message)
      return { kind: 'prompt', result: { status: outcome.status, user_row_id: outcome.userRowId }, display: result.display ?? `/${name}` }
    }
    if (result.type === 'prefill' && result.message) return { kind: 'prefill', text: result.message }
    return { kind: 'output', text: result.output ?? result.notice ?? result.display ?? '', warning: result.warning ?? null }
  }

  /**
   * Detach. Deliberately NOT `session.close`: closing a pane must not end
   * the conversation (it stays resumable, and Hermes' reaper owns cleanup).
   */
  async stop(): Promise<void> {
    this.stopped = true
    this.connection?.close(1000, 'client-stop')
    this.connection = null
    this.emit('connection', { kind: 'stopped' } satisfies HermesConnectionState)
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private requireConnection(): RpcConnection {
    if (!this.connection?.isOpen) throw new HermesPromptError('not-ready', 'Hermes is not connected.')
    return this.connection
  }

  private async connect(): Promise<RpcConnection> {
    if (this.stopped) throw new Error('HermesHeadless was stopped')
    this.emit('connection', { kind: 'connecting' } satisfies HermesConnectionState)
    this.connection?.close(1000, 'replaced')
    const socket = await this.options.dial()
    const connection = await RpcConnection.open(socket, {
      canAnswer: isAnswerableRequest,
      watermarks: this.watermarks,
      requestTimeoutMs: this.options.requestTimeoutMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
    })
    this.connection = connection
    connection.on('event', (event) => this.reconciler.applyEvent(event))
    connection.on('request', (request: ServerRequest) => this.reconciler.openRequest(request as OpenRequestEntry & { replayed: boolean }))
    connection.on('unknown', (shape) => this.emit('unknown', { eventType: 'frame', reason: String((shape as { reason?: string }).reason) }))
    connection.on('close', (info: { code: number; reason: string }) => {
      if (this.connection !== connection) return
      if (!this.stopped) this.emit('connection', { kind: 'disconnected', code: info.code, reason: info.reason } satisfies HermesConnectionState)
    })
    this.epoch = connection.epoch
    this.emit('connection', { kind: 'ready', epoch: connection.epoch } satisfies HermesConnectionState)
    return connection
  }

  /**
   * After a turn settles, its rows are committed in Hermes' store; fetch them
   * so the durable channel owns what the live turn painted. One fetch at a
   * time: a second settle while one is in flight re-runs it once afterwards.
   */
  private scheduleHistoryFetch(): void {
    if (this.historyFetch) {
      void this.historyFetch.then(() => this.scheduleHistoryFetch())
      return
    }
    const runtime = this.reconciler.currentRuntimeSessionId
    const connection = this.connection
    if (!runtime || !connection?.isOpen) return
    this.historyFetch = connection
      .call<SessionHistoryResult>('session.history', { session_id: runtime })
      .then((result) => this.reconciler.applyHistory(result.messages ?? []))
      .catch((error: unknown) => this.committed.publishError(error instanceof Error ? error : new Error(String(error))))
      .finally(() => {
        this.historyFetch = null
      })
  }
}
