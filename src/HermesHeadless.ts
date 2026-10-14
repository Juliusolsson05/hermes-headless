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
  CommandsCatalogResult,
  CompleteSlashResult,
  TranscriptMessage,
  OpenRequestEntry,
  PromptSubmitResult,
  SessionCreateResult,
  SessionHistoryResult,
  SessionLiveInfo,
  SessionResumeResult,
  SlashExecResult,
  TaskIdResult,
} from './contract/wire.js'
import { HermesLaneNotFoundError, HermesStartError, type HermesStartStage } from './errors.js'
import { defaultTelegramLane, pickLaneRow, summarizeLanes, type LaneSummary } from './lane/lanes.js'
import { isTailBusy, laneTail, STALE_BUSY_TAIL_MS, type LaneTail } from './lane/laneBusy.js'
import type { HermesRest } from './rest/HermesRest.js'
import { parseSlash, routeSlash } from './slash/routes.js'
import type { HermesNotice, HermesSideAnswer } from './reconcile/ConversationReconciler.js'
import { ConversationReconciler } from './reconcile/ConversationReconciler.js'
import { ConnectionClosedError, RpcConnection, RpcError, RpcTimeoutError, type ServerRequest, type WebSocketLike } from './transport/RpcConnection.js'

export type HermesDial = () => Promise<WebSocketLike>

export type HermesTarget =
  | { mode: 'create'; source: string; title?: string }
  | { mode: 'resume'; storedSessionId: string }
  /**
   * Follow a messaging lane (the owner's Telegram DM thread) instead of a
   * fixed stored conversation: the stored id is re-resolved from the
   * gateway's routing key on open, on every `sessions.changed`, on the
   * backstop poll and before every prompt (lane/lanes.ts says why).
   * Requires `rest`.
   */
  | { mode: 'lane'; sessionKey: string; profile?: string }

/** The lane's live state, for the consumer's "replying on Telegram" line and held prompts. */
export type HermesLaneState = {
  sessionKey: string
  storedSessionId: string | null
  /** A turn from ANOTHER surface (the phone) is running on the lane. */
  busy: boolean
  /** Prompts the pane is holding until the lane is free, oldest first. */
  held: Array<{ id: string; text: string; heldAt: number }>
}

/** Why the lane's stored conversation changed under the pane. */
export type LaneMove = {
  from: string
  to: string
  /** `compression` is seamless; anything else is a different conversation (Telegram /new, /resume). */
  reason: 'compression' | 'new-conversation'
}

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
  /** `/bg` or `/btw` started; its answer arrives later as a `side-answer` event. */
  | { kind: 'side-task'; taskId: string; side: 'background' | 'btw' }

export type PromptOutcome = {
  /**
   * `queued`: Hermes is holding it until the running turn ends.
   * `held`: Telegram-lane mode is holding it HERE, unsent, until the phone's
   * turn finishes (lane/laneBusy.ts); `holdId` names it for release/cancel.
   */
  status: PromptSubmitResult['status'] | 'held'
  userRowId: number | null
  holdId?: string
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
  /** The dashboard REST client. Required for `target.mode === 'lane'`. */
  rest?: HermesRest
  /**
   * Lane backstop poll. `sessions.changed` is the primary signal (≤ 2.5 s);
   * the poll covers a missed broadcast. 30 s is what Hermes Desktop uses when
   * the server advertises change events (use-background-sync.ts:656-658).
   */
  laneBackstopMs?: number
  /** Debounce for bursts of `sessions.changed` (Hermes floors them at 2 s, but several clients may echo). */
  laneDebounceMs?: number
  /** Injected clock for the stale-busy rule in tests. */
  now?: () => number
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

  // ── Telegram-lane mode (plan Stage D); all inert for create/resume ─────────
  /** User rows WE submitted: our own turn at the tail is not "the phone is busy". */
  private readonly ownUserRowIds = new Set<number>()
  /** The last `session.history` result, for the busy rule. */
  private lastHistory: TranscriptMessage[] | null = null
  private laneBusy = false
  /** The tail the busy rule last saw and when it first saw it (local clock), for STALE_BUSY_TAIL_MS. */
  private laneTailSeen: { key: string; since: number } | null = null
  private readonly held: Array<{ id: string; text: string; heldAt: number }> = []
  private holdCounter = 0
  private draining = false
  private laneRefresh: Promise<void> | null = null
  private laneRefreshAgain = false
  private laneDebounce: ReturnType<typeof setTimeout> | null = null
  private laneBackstop: ReturnType<typeof setInterval> | null = null
  private lanePollingPaused = false
  private readonly now: () => number

  constructor(private readonly options: HermesHeadlessOptions) {
    super()
    this.now = options.now ?? Date.now
    if (options.target.mode === 'lane' && !options.rest) {
      throw new Error('HermesHeadless: a lane target needs the REST client (options.rest).')
    }
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
        // In lane mode our turn ending can free held prompts only after the
        // tail is re-read (a phone message may have queued behind it).
        void this.scheduleHistoryFetch().then(() => this.evaluateLane())
      },
      reclaimed: () => {
        void this.reattach().catch((error: unknown) => this.emit('connection', {
          kind: 'disconnected',
          code: 4000,
          reason: `reclaimed and could not re-attach: ${error instanceof Error ? error.message : String(error)}`,
        } satisfies HermesConnectionState))
      },
      unknown: (shape) => this.emit('unknown', shape),
      notice: (notice: HermesNotice) => this.emit('notice', notice),
      noticeCleared: (key: string) => this.emit('notice-cleared', { key }),
      sideAnswer: (answer: HermesSideAnswer) => this.emit('side-answer', answer),
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
    // WHY the stage is tracked here and not in connect(): connect() also
    // serves reconnect(), whose caller (the app's reconnect loop) branches on
    // the RAW dial error types (signed out, connection removed). Only the
    // first start needs "where did it fail" for the user-facing sentence
    // (agent-code#1495), so only start() wraps.
    let stage: HermesStartStage = 'dial'
    try {
      const connection = await this.connect(() => {
        stage = 'handshake'
      })
      stage = 'attach'
      await this.bindTarget(connection)
    } catch (error) {
      throw new HermesStartError(stage, error)
    }
  }

  /** start()'s attach step: resume (with the 4007 fallback) or create. */
  private async bindTarget(connection: RpcConnection): Promise<void> {
    const target = this.options.target
    if (target.mode === 'lane') {
      const tip = await this.resolveLaneTip()
      if (!tip) throw new HermesLaneNotFoundError(target.sessionKey)
      await this.attachByResume(connection, tip)
      this.startLaneBackstop()
      // The resume already fetched history; evaluate busy from it so a pane
      // opened DURING a phone turn shows it immediately.
      await this.scheduleHistoryFetch()
      this.evaluateLane()
      return
    }
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
    const hadTurn = this.turnInFlight
    const connection = await this.connect()
    if (hadTurn && previousEpoch && connection.epoch !== previousEpoch) {
      // A NEW backend process: the turn that was running died with the old
      // one (its reply is not in the store; Hermes retires undrained queue
      // rows on resume, #125577). The resume below silently resets the
      // bubble, so say it, or the pane looks like the answer just vanished
      // (r4 F7, plan Stage A).
      this.emit('turn-lost', { reason: 'backend-restarted' })
    }
    if (this.options.target.mode === 'lane') {
      // Whatever happened while we were away (Telegram /new, compression),
      // the lane must be re-resolved once we are attached again.
      try {
        await this.reconnectAttach(connection, runtime, previousEpoch)
      } finally {
        void this.refreshLane()
      }
      return
    }
    await this.reconnectAttach(connection, runtime, previousEpoch)
  }

  private async reconnectAttach(connection: RpcConnection, runtime: string, previousEpoch: string | null): Promise<void> {
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
  private async attachByResume(connection: RpcConnection, storedSessionId: string, reason: 'snapshot' | 'new-conversation' = 'snapshot'): Promise<void> {
    await connection.acquire(
      () => connection.call<SessionResumeResult>('session.resume', { session_id: storedSessionId, cols: 96 }),
      (resumed, ring) => {
        const turnStart = openTurnStart(ring.events, resumed.session_id)
        this.watermarks.clear()
        // A lane that moved to a DIFFERENT conversation (Telegram /new) resets
        // as one, so the consumer draws its divider and drops the old turn.
        if (reason === 'new-conversation') this.reconciler.beginNewConversation(resumed)
        else this.reconciler.applySnapshot(resumed, 'snapshot', { seedInflight: turnStart === null })
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
  async prompt(text: string, options: { force?: boolean } = {}): Promise<PromptOutcome> {
    if (this.options.target.mode === 'lane') {
      if (!this.connection?.isOpen || !this.reconciler.currentRuntimeSessionId) throw new HermesPromptError('not-ready', 'Hermes is not connected.')
      // BEFORE every submit (t1 F2e): a pane still holding the id from before
      // a Telegram /new would REOPEN that ended row and fork the thread.
      // refreshLane also refetches history, so the busy decision below is
      // made on the tail as of now, not as of the last signal.
      await this.refreshLane()
      if (!options.force && (this.laneBusy || this.held.length > 0)) {
        // Behind a running phone turn, or behind earlier held prompts (order
        // is the user's order). See lane/laneBusy.ts for why holding is not
        // optional.
        const hold = { id: `hold-${++this.holdCounter}`, text, heldAt: this.now() }
        this.held.push(hold)
        this.emitLane()
        return { status: 'held', userRowId: null, holdId: hold.id }
      }
    }
    return this.submit(text)
  }

  private async submit(text: string): Promise<PromptOutcome> {
    const connection = this.connection
    const runtime = this.reconciler.currentRuntimeSessionId
    if (!connection?.isOpen || !runtime) throw new HermesPromptError('not-ready', 'Hermes is not connected.')
    // Counted BEFORE the call resolves: a second prompt sent while this one
    // is still in flight must already see it.
    this.pendingTurns += 1
    try {
      // ALWAYS `queued: true` (r4 F3, plan H1/H2). Hermes reads it only on its
      // busy path (methods_prompt.py:808-826), so an idle submit is unchanged
      // (recorded S23). It matters when the session is busy for a reason this
      // client cannot see: another client's prompt in the ~1.3 s before its
      // `message.start`, a server-started turn. Sent without it, the default
      // `display.busy_input_mode=interrupt` REDIRECTS that other turn.
      const result = await connection.call<PromptSubmitResult>('prompt.submit', {
        session_id: runtime,
        text,
        queued: true,
      })
      if (typeof result.user_row_id === 'number') this.ownUserRowIds.add(result.user_row_id)
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
    if (this.options.target.mode === 'lane') {
      // serve has no RPC that resets the GATEWAY's routing for the lane; a
      // session.create here would quietly leave the Telegram thread.
      throw new HermesPromptError('refused', 'A Telegram-thread pane cannot start a new conversation; type /new in Telegram.')
    }
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

  /** Every slash command this session can run, with Hermes' availability metadata (plan Stage E). */
  async commandCatalog(): Promise<CommandsCatalogResult> {
    return this.requireConnection().call<CommandsCatalogResult>('commands.catalog', {
      session_id: this.reconciler.currentRuntimeSessionId,
    })
  }

  async completeSlash(text: string): Promise<CompleteSlashResult> {
    return this.requireConnection().call<CompleteSlashResult>('complete.slash', {
      text,
      session_id: this.reconciler.currentRuntimeSessionId,
    })
  }

  /**
   * Run `/command args` the way Hermes Desktop does (slash/routes.ts says why
   * each family goes where it goes; recorded S13, S18–S20):
   *  - `/new` and `/stop` are client-side (`/stop` must interrupt the turn
   *    first; Hermes' own `/stop` only stops background processes, so it is
   *    still forwarded afterwards);
   *  - `/bg`, `/btw` → side-agent RPCs; the answer arrives as `side-answer`;
   *  - `/compress`, `/title`, `/status`, `/save` → their session RPCs;
   *  - everything else → `slash.exec`; a skill or quick command answers 4018
   *    "use command.dispatch", whose directive is a prompt to submit.
   */
  async runSlash(input: string): Promise<SlashOutcome> {
    const { name, arg, body } = parseSlash(input)
    const route = routeSlash(name)
    if (route.kind === 'new-conversation') {
      await this.newConversation()
      return { kind: 'new-conversation' }
    }
    const connection = this.requireConnection()
    const runtime = this.reconciler.currentRuntimeSessionId
    switch (route.kind) {
      case 'stop':
        await this.interrupt()
        await connection.call('slash.exec', { session_id: runtime, command: body }).catch(() => undefined)
        return { kind: 'stopped' }
      case 'side': {
        if (!arg) return { kind: 'output', text: `Usage: /${name} <what to ${route.side === 'btw' ? 'ask' : 'do'}>` }
        const started = await connection.call<TaskIdResult>(route.side === 'btw' ? 'prompt.btw' : 'prompt.background', { session_id: runtime, text: arg })
        return { kind: 'side-task', taskId: String(started.task_id ?? ''), side: route.side }
      }
      case 'compress': {
        const result = await connection.call<Record<string, unknown>>('session.compress', { session_id: runtime, ...(arg ? { focus_topic: arg } : {}) })
        // The compressed transcript has new row ids; the history fetch
        // turns that into one rewrite reset instead of duplicates (S12).
        void this.scheduleHistoryFetch()
        return { kind: 'output', text: compressSummary(result) }
      }
      case 'title': {
        const result = await connection.call<{ title?: string; pending?: boolean }>('session.title', { session_id: runtime, ...(arg ? { title: arg } : {}) })
        const title = String(result.title ?? '')
        return { kind: 'output', text: arg ? `Renamed to "${title}"${result.pending ? ' (applied after the first message)' : ''}.` : `Title: ${title || '(untitled)'}` }
      }
      case 'status': {
        const result = await connection.call<{ output?: string }>('session.status', { session_id: runtime })
        return { kind: 'output', text: String(result.output ?? '') }
      }
      case 'save': {
        const result = await connection.call<{ file?: string | null }>('session.save', { session_id: runtime })
        return { kind: 'output', text: result.file ? `Saved the transcript on the Hermes server: ${result.file}` : 'Saved the transcript on the Hermes server.' }
      }
      case 'exec':
        break
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
      return { kind: 'prompt', result: { status: outcome.status === 'held' ? 'queued' : outcome.status, user_row_id: outcome.userRowId }, display: result.display ?? `/${name}` }
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
    if (this.laneDebounce) clearTimeout(this.laneDebounce)
    if (this.laneBackstop) clearInterval(this.laneBackstop)
    this.laneDebounce = null
    this.laneBackstop = null
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

  /** `onDialed` fires once the socket is open and the JSON-RPC handshake starts (start()'s stage tracking). */
  private async connect(onDialed?: () => void): Promise<RpcConnection> {
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
    onDialed?.()
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
    connection.on('event', (event) => {
      if (event.type === 'sessions.changed') this.onSessionsChanged()
      this.reconciler.applyEvent(event)
    })
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
  private scheduleHistoryFetch(): Promise<void> {
    if (this.historyFetch) {
      return this.historyFetch.then(() => this.scheduleHistoryFetch())
    }
    const runtime = this.reconciler.currentRuntimeSessionId
    const connection = this.connection
    if (!runtime || !connection?.isOpen) return Promise.resolve()
    const fetch = connection
      .call<SessionHistoryResult>('session.history', { session_id: runtime })
      // Only for the conversation it was asked about: a reply that lands
      // after `/new` must not commit the old rows into the new one.
      .then((result) => {
        if (this.reconciler.currentRuntimeSessionId !== runtime) return
        const messages = result.messages ?? []
        this.lastHistory = messages
        this.reconciler.applyHistory(messages)
      })
      .catch((error: unknown) => this.committed.publishError(error instanceof Error ? error : new Error(String(error))))
      .finally(() => {
        this.historyFetch = null
      })
    this.historyFetch = fetch
    return fetch
  }

  // ── Telegram lane ──────────────────────────────────────────────────────────

  /** The lane's stored conversation right now, following compression; null when the lane has none. */
  private async resolveLaneTip(): Promise<string | null> {
    const target = this.options.target
    if (target.mode !== 'lane' || !this.options.rest) return null
    // The platform is the key's third field (`agent:<profile>:<platform>:…`,
    // gateway/session.py build_session_key); scoping the listing to it keeps
    // the page small enough that the lane is always on it.
    const platform = target.sessionKey.split(':')[2] || undefined
    const listing = await this.options.rest.listSessions({ source: platform, order: 'recent', limit: 100, profile: target.profile })
    const head = pickLaneRow(listing.sessions, target.sessionKey)
    if (!head) return null
    const tip = await this.options.rest.latestDescendant(head.id, target.profile)
    return tip.session_id || head.id
  }

  /**
   * Re-resolve the lane and refresh the transcript. Single flight; a call
   * that arrives while one runs makes it run once more afterwards, so the
   * LAST signal is always reflected.
   */
  private refreshLane(): Promise<void> {
    if (this.options.target.mode !== 'lane' || this.stopped) return Promise.resolve()
    if (this.laneRefresh) {
      this.laneRefreshAgain = true
      return this.laneRefresh
    }
    const run = (async () => {
      do {
        this.laneRefreshAgain = false
        await this.refreshLaneOnce()
      } while (this.laneRefreshAgain && !this.stopped)
    })()
      .catch((error: unknown) => {
        this.emit('lane-error', error instanceof Error ? error : new Error(String(error)))
      })
      .finally(() => {
        this.laneRefresh = null
      })
    this.laneRefresh = run
    return run
  }

  private async refreshLaneOnce(): Promise<void> {
    const connection = this.connection
    if (!connection?.isOpen) return
    const tip = await this.resolveLaneTip()
    const current = this.reconciler.currentStoredSessionId
    if (tip && current && tip !== current) {
      const reason = await this.laneMoveReason(current, tip)
      this.emit('lane-moved', { from: current, to: tip, reason } satisfies LaneMove)
      await this.attachByResume(connection, tip, reason === 'new-conversation' ? 'new-conversation' : 'snapshot')
      this.ownUserRowIds.clear()
    }
    await this.scheduleHistoryFetch()
    this.evaluateLane()
  }

  /** Compression keeps the conversation (seamless); anything else is a different one. */
  private async laneMoveReason(from: string, to: string): Promise<LaneMove['reason']> {
    try {
      const descendant = await this.options.rest!.latestDescendant(from, this.options.target.mode === 'lane' ? this.options.target.profile : undefined)
      return descendant.session_id === to ? 'compression' : 'new-conversation'
    } catch {
      return 'new-conversation'
    }
  }

  private evaluateLane(): void {
    if (this.options.target.mode !== 'lane') return
    const tail: LaneTail | null = laneTail(this.lastHistory ?? [])
    const now = this.now()
    if (tail?.key !== this.laneTailSeen?.key) this.laneTailSeen = tail ? { key: tail.key, since: now } : null
    const stale = this.laneTailSeen !== null && now - this.laneTailSeen.since > STALE_BUSY_TAIL_MS
    // Our OWN turn is never "the phone is busy": Hermes' turn lease orders a
    // phone message behind it, and our later prompts go with `queued: true`.
    const busy = !this.turnInFlight && !stale && isTailBusy(tail, this.ownUserRowIds)
    const changed = busy !== this.laneBusy
    this.laneBusy = busy
    if (changed) this.emitLane()
    if (!busy) void this.drainHeld()
  }

  private async drainHeld(): Promise<void> {
    if (this.draining) return
    this.draining = true
    try {
      while (this.held.length > 0 && !this.laneBusy && !this.stopped) {
        const hold = this.held.shift()!
        this.emitLane()
        try {
          const outcome = await this.submit(hold.text)
          this.emit('hold-released', { holdId: hold.id, outcome })
        } catch (error) {
          this.emit('hold-failed', { holdId: hold.id, text: hold.text, error: error instanceof Error ? error : new Error(String(error)) })
        }
      }
    } finally {
      this.draining = false
    }
  }

  /** "Send now": submit a held prompt despite the busy lane (the user accepts the T4 framing risk). */
  async releaseHeld(holdId: string): Promise<PromptOutcome | null> {
    const index = this.held.findIndex((hold) => hold.id === holdId)
    if (index < 0) return null
    const [hold] = this.held.splice(index, 1)
    this.emitLane()
    const outcome = await this.submit(hold!.text)
    this.emit('hold-released', { holdId, outcome })
    return outcome
  }

  /** Drop a held prompt; returns its text so the consumer can put it back in the composer. */
  cancelHeld(holdId: string): string | null {
    const index = this.held.findIndex((hold) => hold.id === holdId)
    if (index < 0) return null
    const [hold] = this.held.splice(index, 1)
    this.emitLane()
    return hold!.text
  }

  get laneState(): HermesLaneState | null {
    const target = this.options.target
    if (target.mode !== 'lane') return null
    return {
      sessionKey: target.sessionKey,
      storedSessionId: this.reconciler.currentStoredSessionId || null,
      busy: this.laneBusy,
      held: this.held.map((hold) => ({ ...hold })),
    }
  }

  /** Pause the backstop poll while nobody can see the pane; `sessions.changed` still refreshes. */
  setLanePollingPaused(paused: boolean): void {
    this.lanePollingPaused = paused
    if (!paused) void this.refreshLane()
  }

  private emitLane(): void {
    const state = this.laneState
    if (state) this.emit('lane', state)
  }

  private onSessionsChanged(): void {
    if (this.options.target.mode !== 'lane') return
    // The signal is GLOBAL and payload-free (change_watcher.py): any session
    // on the server changed. Debounced, then one refresh decides whether it
    // was ours.
    if (this.laneDebounce) clearTimeout(this.laneDebounce)
    this.laneDebounce = setTimeout(() => {
      this.laneDebounce = null
      void this.refreshLane()
    }, this.options.laneDebounceMs ?? 300)
  }

  private startLaneBackstop(): void {
    if (this.laneBackstop || this.options.target.mode !== 'lane') return
    this.laneBackstop = setInterval(() => {
      if (!this.lanePollingPaused) void this.refreshLane()
    }, this.options.laneBackstopMs ?? 30_000)
    // Never keep a Node process alive just to poll.
    ;(this.laneBackstop as { unref?: () => void }).unref?.()
  }

  /** Lanes a pane could follow on this server (Settings / "Change Telegram Thread…"). */
  static async listLanes(rest: HermesRest, profile?: string): Promise<LaneSummary[]> {
    const listing = await rest.listSessions({ order: 'recent', limit: 100, profile })
    return summarizeLanes(listing.sessions)
  }

  /** The lane a new "Telegram thread" pane opens (plan Decision 2). */
  static async defaultLane(rest: HermesRest, profile?: string): Promise<LaneSummary | null> {
    return defaultTelegramLane(await HermesHeadless.listLanes(rest, profile))
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

/** One line for `/compress`'s result (SessionCompressResult; open shape under turn isolation). */
function compressSummary(result: Record<string, unknown>): string {
  if (result.status === 'aborted' || result.compressed === false) {
    return typeof result.message === 'string' && result.message ? result.message : 'Hermes did not compress this conversation.'
  }
  const before = typeof result.before_messages === 'number' ? result.before_messages : null
  const after = typeof result.after_messages === 'number' ? result.after_messages : null
  if (before !== null && after !== null) return `Compressed the conversation: ${before} → ${after} messages.`
  return typeof result.message === 'string' && result.message ? result.message : 'Compressed the conversation.'
}
