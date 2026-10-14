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
  /** Bumped by every connect() and by stop(): a dial that resolves late belongs to a dead attempt. */
  private connectGeneration = 0
  /**
   * Prompts Hermes accepted (or we are sending) whose turn has not settled.
   * WHY not only `reconciler.turnInFlight` (package review A #4): Hermes
   * answers prompt.submit `streaming` ~1.3 s before `message.start` arrives
   * (recorded S8; 6.5 s in S16). A second prompt in that window without
   * `queued: true` REDIRECTS, killing the first. This counts work from the
   * moment we send it until its turn settles.
   */
  private pendingTurns = 0

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
      turnSettled: () => {
        this.pendingTurns = Math.max(0, this.pendingTurns - 1)
        this.scheduleHistoryFetch()
      },
      reclaimed: () => {
        void this.reattach().catch((error: unknown) => this.emit('connection', {
          kind: 'disconnected',
          code: 4000,
          reason: `reclaimed and could not re-attach: ${error instanceof Error ? error.message : String(error)}`,
        } satisfies HermesConnectionState))
      },
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
    return this.reconciler.turnInFlight || this.pendingTurns > 0
  }

  /**
   * Connect and bind the conversation. Resolves once the snapshot is applied
   * (history committed, any in-flight turn and open request restored).
   *
   * A resume of a stored id the backend does not know (4007: deleted from
   * another client; Hermes materialises minted-but-unprompted ids itself,
   * methods_session.py _resume_materialize_minted, so that is not a cause)
   * falls back to a NEW conversation and says so with `session-replaced`,
   * instead of leaving the pane dead. reattach() does the same on reconnect.
   */
  async start(): Promise<void> {
    const connection = await this.connect()
    const target = this.options.target
    let replaced = false
    if (target.mode === 'resume') {
      try {
        await this.attachByResume(connection, target.storedSessionId)
        return
      } catch (error) {
        if (!(error instanceof RpcError) || error.code !== SESSION_NOT_FOUND) throw error
        this.emit('session-replaced', { previousStoredSessionId: target.storedSessionId, reason: 'not-found' })
        replaced = true
      }
    }
    await connection.acquire(
      () => connection.call<SessionCreateResult>('session.create', {
        source: target.mode === 'create' ? target.source : 'agent-code',
        cols: 96,
        ...(target.mode === 'create' && target.title ? { title: target.title } : {}),
      }),
      (created) => {
        // A replacement is a DIFFERENT conversation, reset as one (the reason
        // a consumer uses to drop the old conversation's live turns), exactly
        // as reattach()'s fallback does (round-2 review x2 suspicion).
        if (replaced) this.reconciler.beginNewConversation(created)
        else this.reconciler.applySnapshot(created, 'snapshot')
        this.pendingTurns = 0
        return { fromSeq: null }
      },
    )
  }

  /**
   * Resume the same conversation on a NEW socket after a drop. The caller
   * decides when (backoff, network change, wake); this decides how:
   *   1. same backend process (epoch) and runtime id still live → re-attach
   *      with `session.activate` and dispatch the missed seq gap from the ring
   *      (S8), under the acquisition hold so nothing slips between them;
   *   2. otherwise (restart S11, reaped S9, gap truncated) → `session.resume`
   *      by the stored id: a fresh snapshot.
   */
  async reconnect(): Promise<void> {
    const runtime = this.reconciler.currentRuntimeSessionId
    const previousEpoch = this.epoch
    const connection = await this.connect()
    if (runtime && previousEpoch && connection.epoch === previousEpoch) {
      const lastSeen = this.watermarks.get(runtime) ?? 0
      let truncated = false
      try {
        await connection.acquire(
          () => connection.call<SessionResumeResult>('session.activate', { session_id: runtime, cols: 96 }),
          (_activated, ring) => {
            truncated = ring.truncated
            // A truncated gap cannot be replayed exactly: dispatch nothing
            // and re-snapshot below instead.
            return { fromSeq: ring.truncated ? null : lastSeen + 1 }
          },
          lastSeen,
        )
        if (!truncated) {
          // A turn may have completed while we were away; its rows commit
          // through history, not the replayed events alone.
          this.scheduleHistoryFetch()
          return
        }
      } catch (error) {
        // Reaped while we were away (idle past the grace, S9): resume below.
        if (!(error instanceof RpcError)) throw error
      }
    }
    await this.reattach(connection)
  }

  /**
   * Re-attach by stored id on the current (or a fresh) connection.
   *
   * A conversation deleted on the server while we were away (Hermes Desktop,
   * the CLI) answers 4007, exactly as at start(). It gets the same fallback:
   * a NEW conversation announced with `session-replaced`. Without it the
   * error escaped to the consumer's reconnect loop, which redialled forever
   * against a server that was perfectly reachable (round-1 review c1 #1).
   */
  private async reattach(connection?: RpcConnection): Promise<void> {
    const stored = this.reconciler.currentStoredSessionId
    if (!stored) throw new Error('Cannot re-attach: this conversation has no stored id yet.')
    const live = connection ?? (this.connection?.isOpen ? this.connection : await this.connect())
    try {
      await this.attachByResume(live, stored)
    } catch (error) {
      if (!(error instanceof RpcError) || error.code !== SESSION_NOT_FOUND) throw error
      this.emit('session-replaced', { previousStoredSessionId: stored, reason: 'not-found' })
      await live.acquire(
        () => live.call<SessionCreateResult>('session.create', { source: this.createSource(), cols: 96 }),
        (created) => {
          // A different conversation: history resets as for `/new`.
          this.reconciler.beginNewConversation(created)
          this.watermarks.clear()
          this.pendingTurns = 0
          return { fromSeq: null }
        },
      )
    }
  }

  private createSource(): string {
    return this.options.target.mode === 'create' ? this.options.target.source : 'agent-code'
  }

  /**
   * `session.resume` under the acquisition hold. When the conversation is
   * mid-turn and the ring still holds that turn's `message.start`, the turn
   * is rebuilt EXACTLY from the ring (every delta, tool and request in
   * order) and the snapshot's partial `inflight` text is not seeded; only
   * when the ring lost the start does the snapshot's text stand in.
   */
  private async attachByResume(connection: RpcConnection, storedSessionId: string): Promise<void> {
    await connection.acquire(
      () => connection.call<SessionResumeResult>('session.resume', { session_id: storedSessionId, cols: 96 }),
      (resumed, ring) => {
        const turnStart = openTurnStart(ring.events, resumed.session_id)
        this.watermarks.clear()
        this.reconciler.applySnapshot(resumed, 'snapshot', { seedInflight: turnStart === null })
        this.pendingTurns = resumed.running ? 1 : 0
        // A long turn (> the 512-event ring) whose start was evicted, but which
        // completed between the rebind and the ring read: the snapshot seeded
        // it as running and nothing would ever settle it, so dispatch the
        // ring from its `message.complete` (round-1 review c2 #3). Deltas in
        // that window are not replayed; the history fetch commits the answer.
        //
        // ONLY for a truncated ring whose snapshot holds a partial answer: a
        // prompt Hermes accepted but has not started yet also snapshots as
        // running with no start in the ring, and there the last completion is
        // the PREVIOUS turn's; settling on it replayed an old answer and let
        // the next prompt redirect the running one (round-2 review c1 #1).
        const evictedStart = ring.truncated === true && typeof resumed.inflight?.assistant === 'string' && resumed.inflight.assistant.length > 0
        const fromSeq = turnStart ?? (resumed.running && evictedStart ? lastTurnEnd(ring.events, resumed.session_id) : null)
        return { fromSeq }
      },
    )
    this.scheduleHistoryFetch()
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
    const busy = this.turnInFlight
    // Counted BEFORE the call resolves: a second prompt sent while this one
    // is still in flight must already see it.
    this.pendingTurns += 1
    try {
      const result = await connection.call<PromptSubmitResult>('prompt.submit', {
        session_id: runtime,
        text,
        ...(busy ? { queued: true } : {}),
      })
      // `steered` folds the text into the running turn: no turn of its own
      // will settle for it.
      if (result.status === 'steered') this.pendingTurns = Math.max(0, this.pendingTurns - 1)
      return { status: result.status ?? null, userRowId: typeof result.user_row_id === 'number' ? result.user_row_id : null }
    } catch (error) {
      // Refused: no turn will come. Unknown (socket died after sending): a
      // turn MAY run; keep counting it so the next prompt queues rather than
      // risking a redirect, and the reconnect snapshot resets the count.
      if (error instanceof RpcError) this.pendingTurns = Math.max(0, this.pendingTurns - 1)
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
    await connection.acquire(
      () => connection.call<SessionCreateResult>('session.create', {
        source: this.createSource(),
        cols: 96,
      }),
      (created) => {
        this.reconciler.beginNewConversation(created)
        this.pendingTurns = 0
        return { fromSeq: null }
      },
    )
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
    // Invalidates a dial still in flight (review A #6): when it resolves,
    // connect() sees a newer generation and closes that socket.
    this.connectGeneration += 1
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
    const generation = ++this.connectGeneration
    this.emit('connection', { kind: 'connecting' } satisfies HermesConnectionState)
    // Detach BEFORE closing: replacing our own socket is not a disconnect.
    // Its close listener saw `this.connection === connection` and reported
    // one, the consumer scheduled another reconnect, and that one replaced
    // the healthy socket again, forever (round-1 review c2 #2: 153 dials in 3 s).
    const previous = this.connection
    this.connection = null
    previous?.close(1000, 'replaced')
    const socket = await this.options.dial()
    if (generation !== this.connectGeneration) {
      socket.close(1000, 'superseded')
      throw new Error('HermesHeadless connect was cancelled')
    }
    const connection = await RpcConnection.open(socket, {
      canAnswer: isAnswerableRequest,
      watermarks: this.watermarks,
      requestTimeoutMs: this.options.requestTimeoutMs,
      heartbeatIntervalMs: this.options.heartbeatIntervalMs,
    })
    if (generation !== this.connectGeneration) {
      connection.close(1000, 'superseded')
      throw new Error('HermesHeadless connect was cancelled')
    }
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
      // Only for the conversation it was asked about: a reply that lands
      // after `/new` must not commit the old rows into the new one.
      .then((result) => {
        if (this.reconciler.currentRuntimeSessionId === runtime) this.reconciler.applyHistory(result.messages ?? [])
      })
      .catch((error: unknown) => this.committed.publishError(error instanceof Error ? error : new Error(String(error))))
      .finally(() => {
        this.historyFetch = null
      })
  }
}

/**
 * The seq of the newest `message.start` in a ring that has no
 * `message.complete` after it: the turn running right now. Null when no turn
 * is open in the ring (idle, or its start was evicted).
 */
/** Seq of the ring's last `message.complete` with no `message.start` after it. */
function lastTurnEnd(events: Array<Record<string, unknown>>, sessionId: string): number | null {
  let end: number | null = null
  const ordered = events
    .filter((event) => event.session_id === sessionId && typeof event.seq === 'number')
    .sort((a, b) => (a.seq as number) - (b.seq as number))
  for (const event of ordered) {
    if (event.type === 'message.complete') end = event.seq as number
    if (event.type === 'message.start') end = null
  }
  return end
}

function openTurnStart(events: Array<Record<string, unknown>>, sessionId: string): number | null {
  let start: number | null = null
  const ordered = events
    .filter((event) => event.session_id === sessionId && typeof event.seq === 'number')
    .sort((a, b) => (a.seq as number) - (b.seq as number))
  for (const event of ordered) {
    if (event.type === 'message.start') start = event.seq as number
    if (event.type === 'message.complete') start = null
  }
  return start
}
