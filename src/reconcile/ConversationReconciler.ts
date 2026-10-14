// Snapshot + live events + replay + committed rows → ONE conversation stream.
//
// Hard part #2 of the decomposition (§8 Stage 4, isolation §9). Four sources
// describe the same conversation at different times with different
// guarantees, and this class is the only place they meet:
//
//   source                         guarantee                      owner here
//   create/resume snapshot         durable rows up to "now"       committed (reset + rows)
//   live `event` frames            this turn, as it streams       semantic turn/blocks
//   replayed events (transport)    the gap after a drop           same path as live (seq'd)
//   `session.history` after a turn the rows the turn committed    committed (new row ids only)
//
// It is PURE: no sockets, no timers. HermesHeadless feeds it and does the I/O
// (fetching history after a turn). That keeps every rule below testable by
// folding recorded frames, and the golden test (reconcile.system.test.ts)
// compares a live fold against a resume fold of the same conversation.
//
// OUTPUT VOCABULARY: the semantic events are emitted directly in Agent Code's
// fold vocabulary (session-runtime/semantic/foldEvent.ts): numeric
// `blockIndex`, `textSoFar`, `thinkingSoFar`, `toolUseId`, `inputJson` +
// `parsed`. WHY in the package and not an app adapter (unlike
// opencode-headless + semanticMapping.ts): this package has one consumer that
// already defines the vocabulary, and #394 phase 6 asked for a unified event
// vocabulary instead of a translator per provider.

import type {
  ErrorSurface,
  EventEnvelope,
  HermesUsage,
  MessageCompletePayload,
  MessageInterimPayload,
  OpenRequestEntry,
  PersistedTurn,
  RequestCancelPayload,
  SessionLiveInfo,
  SessionResumeResult,
  SessionTitlePayload,
  StatusUpdatePayload,
  StreamDeltaPayload,
  ToolCompletePayload,
  ToolStartPayload,
  TranscriptMessage,
} from '../contract/wire.js'
import { isHandledEvent, isIgnoredEvent } from '../contract/catalog.js'

/** The `source` stamped on every semantic event (the app's fold policy trusts it). */
export const HERMES_SEMANTIC_SOURCE = 'hermes-ws'

export type SemanticEvent = { type: string; source: string; turnId?: string; [key: string]: unknown }

export type CommittedRow = {
  /** Durable key: `row:<row_id>`, `tool:<tool_call_id>`, or `idx:<n>` for a row with neither. */
  key: string
  storedSessionId: string
  row: TranscriptMessage
}

export type ReconcilerOutput = {
  semantic(event: SemanticEvent): void
  /** A durable row, emitted once per key per history generation. */
  committed(row: CommittedRow): void
  /**
   * Everything committed so far is superseded: a new conversation (`/new`), a
   * resume, or a server-side rewrite (compression gave old rows new ids, S12).
   * The rows of the new generation follow, then `historyCaughtUp`.
   */
  historyReset(generation: number, reason: 'snapshot' | 'new-conversation' | 'rewrite'): void
  historyCaughtUp(generation: number): void
  activity(state: { active: boolean; status: string | null }): void
  session(state: { storedSessionId: string; runtimeSessionId: string; title: string | null; info: SessionLiveInfo | null }): void
  requestOpened(request: OpenRequestEntry & { replayed: boolean }): void
  requestClosed(id: string, reason: string): void
  /** A turn finished; HermesHeadless fetches history so its rows commit. */
  turnSettled(turn: { persisted: PersistedTurn | null; status: string }): void
  /**
   * The backend took THIS conversation's live session away (reaped, evicted,
   * another client). The owner re-attaches it by stored id.
   */
  reclaimed(reason: string): void
  unknown(shape: { eventType: string; reason: string }): void
  /**
   * Something the user should read that is not part of the conversation:
   * Hermes' out-of-band notifications, the background memory/skill review,
   * a risky tool output, a connector the turn waits on. Before
   * agent-code#1497 every one of these was dropped silently (r4 F9).
   */
  notice(notice: HermesNotice): void
  /** A keyed `notification.show` was withdrawn. */
  noticeCleared(key: string): void
  /**
   * A connection operation (Hermes' manage_connections) opened, moved, or
   * settled. Every frame carries the full snapshot, so the consumer replaces
   * its copy; `settled: true` is the last one for that `opId`.
   *
   * Optional only because consumers that fold recordings without a client
   * (the app's projection tests, reconcile tests) have no card to draw;
   * HermesHeadless always wires it.
   */
  connection?(operation: HermesConnectionOperation): void
  /**
   * The answer to `/bg` or `/btw`. NOT a turn: `/btw` is answered "from a
   * snapshot … history, alternation and prompt cache stay untouched"
   * (methods_prompt.py), and `/bg` runs in its own session. The consumer
   * shows it beside the conversation, never as an assistant reply.
   */
  sideAnswer(answer: HermesSideAnswer): void
}

/**
 * Tool results observed LIVE, by tool call id. Hermes' history projection
 * drops tool output (recorded S4), so this is the only place a committed tool
 * row gets its output back. A consumer that rebuilds the client for the same
 * conversation (Reload Agent) passes one cache that outlives it; otherwise a
 * reload turned every chip into "output is kept on your server" (owner's
 * debug bundle 2026-10-14T16-26-49: 187 of 206 chips).
 */
export type ToolResultCache = {
  get(toolCallId: string): { content: string; isError: boolean } | undefined
  set(toolCallId: string, result: { content: string; isError: boolean }): void
}

/**
 * One connection operation, as Hermes' own card draws it
 * (tools/connectors/operation.py Target.snapshot / request_payload; recorded
 * S26 form-less catalog install, S27 OAuth catalog install, v0.22.0).
 *
 * WHY a structured operation and not a notice: the turn WAITS on it (up to
 * `timeout_seconds`, 300 s in v0.22.0) and only a client answer, the
 * deadline, or an interrupt ends the wait. A sentence saying "go to the
 * dashboard" left the owner's turn stuck with no way to act from the pane
 * (2026-10-14: "what is Hermes on about, I thought I was authenticated?").
 */
export type HermesConnectionOperation = {
  opId: string
  seq: number
  /** Epoch ms; the turn stops waiting then. */
  deadlineAt: number | null
  settled: boolean
  settledBy: string | null
  /** The tool call this operation belongs to, when Hermes says (request frame only). */
  toolCallId: string | null
  targets: HermesConnectionTarget[]
}

export type HermesConnectionTarget = {
  name: string
  /** `connector` (a hosted account), `mcp`, `plugin`, `skill`. */
  kind: string
  /** `connect`, `reconnect`, `authorize`, `install`, `enable`. */
  action: string
  /** `pending`, `initiated`, `connected`, `skipped`, `failed`, `expired`, `not_connected`. */
  state: string
  resolved: boolean
  detail: string | null
  /** Setup text written for the user (the card's body). */
  instructions: string | null
  /** The browser sign-in link a hosted connector minted; absent for catalog installs. */
  connectUrl: string | null
  /** Credentials an install still needs; values go back with an approval. */
  requiredEnv: Array<{ name: string; prompt: string | null; required: boolean; secret: boolean; default: string }>
  alias: string | null
}

export type HermesNotice = {
  /** Hermes' replace/clear key, or null for a one-off. */
  key: string | null
  level: 'info' | 'warn' | 'error' | 'success'
  /**
   * Where it came from. `ttl` notices are transient by Hermes' own
   * definition (the consumer may show them as status text); every other
   * kind stays readable.
   */
  kind: 'sticky' | 'ttl' | 'agent' | 'review' | 'tool-risk' | 'connection'
  text: string
}

export type HermesSideAnswer = {
  taskId: string
  kind: 'background' | 'btw'
  /** `/btw` echoes its question; `/bg` does not. */
  question: string | null
  text: string
}

type Block = {
  index: number
  kind: 'text' | 'thinking' | 'tool_use'
  text: string
  toolUseId?: string
  /** Kept so an open turn can be re-announced after a history reset (replayOpenTurn). */
  sealed?: boolean
  tool?: { name: string; args: Record<string, unknown>; context: string | null; result?: { content: string; isError: boolean } }
}

type LiveTurn = {
  turnId: string
  nextBlockIndex: number
  /** The block text deltas append to, if it is still open. */
  openText: Block | null
  openThinking: Block | null
  /** All streamed answer text, segments joined, for turn_delta and the reasoning dedupe. */
  answerText: string
  sawReasoningDelta: boolean
  toolBlocks: Map<string, Block>
  /** Every block of the turn, in order (replayOpenTurn). */
  blocks: Block[]
}

/** Hermes separates a post-tool answer segment with a leading blank line (S6, S8). */
function normalizeAnswer(text: string): string {
  return text.replace(/\s+/g, ' ').trim()
}

/**
 * The durable identity of a projected row. Recorded (S4 resume): user and
 * assistant rows carry `row_id`, but TOOL rows are projected WITHOUT one and
 * without their result, carrying only `tool_call_id`. A position index would
 * shift whenever an earlier row is rewritten, so tool rows key on their call
 * id, which is also what the live tool chip is keyed by.
 */
function rowKey(row: TranscriptMessage, index: number): string {
  if (typeof row.row_id === 'number') return `row:${row.row_id}`
  if (typeof row.tool_call_id === 'string' && row.tool_call_id) return `tool:${row.tool_call_id}`
  return `idx:${index}`
}

function textOf(value: unknown): string {
  if (typeof value === 'string') return value
  if (value === null || value === undefined) return ''
  return JSON.stringify(value)
}

/** A tool result counts as an error when Hermes said so in any of its documented ways. */
function toolResultIsError(payload: ToolCompletePayload): boolean {
  const result = payload.result as Record<string, unknown> | undefined
  if (!result || typeof result !== 'object') return false
  if (result.success === false) return true
  if (typeof result.error === 'string' && result.error) return true
  if (typeof result.exit_code === 'number' && result.exit_code !== 0) return true
  return false
}

function noticeLevel(level: unknown): HermesNotice['level'] {
  return level === 'warn' || level === 'error' || level === 'success' ? level : 'info'
}

export class ConversationReconciler {
  private storedSessionId = ''
  /** Results seen live by tool call id; see withLiveToolResult. Cleared per conversation. */
  private readonly liveToolResults: ToolResultCache
  private runtimeSessionId = ''
  private title: string | null = null
  private info: SessionLiveInfo | null = null
  private generation = 0
  private readonly emittedKeys = new Set<string>()
  /** Row ids in the order the server last listed them; used to detect rewrites. */
  private knownRowIds: number[] = []
  private turn: LiveTurn | null = null
  private readonly openRequests = new Set<string>()
  private readonly unknownTypes = new Set<string>()

  /** True when the consumer owns the cache (it outlives this reconciler; never cleared here). */
  private readonly sharedResults: boolean

  constructor(private readonly out: ReconcilerOutput, toolResults?: ToolResultCache) {
    this.liveToolResults = toolResults ?? new Map()
    this.sharedResults = toolResults !== undefined
  }

  get currentStoredSessionId(): string {
    return this.storedSessionId
  }

  get currentRuntimeSessionId(): string {
    return this.runtimeSessionId
  }

  get turnInFlight(): boolean {
    return this.turn !== null
  }

  /**
   * A create or resume result: the authoritative state at "now". Replaces the
   * committed history (reset + rows + caught-up), seeds an in-flight turn from
   * `inflight` (open requests come from the ring, see below).
   *
   * WHY a reset even on the first snapshot: the consumer may already show rows
   * from a previous runtime of the same pane (an app reload restores entries
   * from disk). The snapshot is fresher than any of them.
   */
  applySnapshot(
    result: SessionResumeResult & { stored_session_id?: string | null },
    reason: 'snapshot' | 'new-conversation',
    options: { seedInflight?: boolean } = {},
  ): void {
    this.runtimeSessionId = result.session_id
    this.storedSessionId = String(result.stored_session_id ?? (result as { resumed?: string }).resumed ?? this.storedSessionId)
    this.info = result.info ?? this.info
    this.title = typeof result.info?.title === 'string' && result.info.title ? result.info.title : this.title
    this.publishSession()
    this.resetHistory(reason, result.messages ?? [])
    this.closeAllRequests('superseded')
    this.turn = null
    const inflight = result.inflight
    // `seedInflight: false` when the owner will rebuild the running turn
    // EXACTLY from the replay ring (RpcConnection.acquire found its
    // message.start): seeding the snapshot's text as well would paint it twice.
    if (options.seedInflight !== false && inflight && (inflight.streaming || result.running)) {
      // Resume DURING a turn: rebuild the live bubble from the server's
      // in-flight snapshot; live deltas continue from here.
      this.startTurn(`${this.runtimeSessionId}:resume-${this.generation}`)
      if (inflight.assistant) this.appendAnswer(inflight.assistant)
      this.out.activity({ active: true, status: inflight.status ?? null })
    } else {
      this.out.activity({ active: false, status: null })
    }
    // Open requests are NOT taken from the snapshot: it was captured at the
    // rebind, before anything the client hears while settling. The owner
    // surfaces the ring's open requests (read after the snapshot) through the
    // transport instead; taking these too resurrected a card whose cancel had
    // already arrived (package review A #3).
  }

  /**
   * Rows from `session.history` (after a turn settles). Emits only rows not
   * seen in this generation. If rows the consumer already holds are GONE from
   * the list, the server rewrote history (manual /compress gives old content
   * new row ids, recorded in S12); the whole list then becomes a new
   * generation instead of appending 100 duplicates.
   */
  applyHistory(messages: TranscriptMessage[]): void {
    const ids = messages.map((row) => row.row_id).filter((id): id is number => typeof id === 'number')
    const listed = new Set(ids)
    // A rewrite is a known id that VANISHED from the range the listing covers.
    // WHY the range (owner's debug bundle 2026-10-14T16-26-49): when the
    // resume snapshot and `session.history` cover different windows of a
    // long conversation, ids outside the listing's span are simply not in
    // it; treating them as gone fired a false 'rewrite' reset right after a
    // reload, which wiped the pane and (through the renderer's reset gate)
    // silenced the turn that was running.
    const low = ids.length > 0 ? Math.min(...ids) : Number.POSITIVE_INFINITY
    const high = ids.length > 0 ? Math.max(...ids) : Number.NEGATIVE_INFINITY
    const vanishedInside = this.knownRowIds.some((id) => id >= low && id <= high && !listed.has(id))
    // Compression REPLACES the transcript with rows whose ids are all above
    // every id the client knows (recorded: T7b in-place compaction
    // [1..51] → [52..63]; S20 /compress [1..62] → [65..76]). No known id is
    // inside that span, so it needs its own rule: a listing that shares NO
    // row with what we hold is a different transcript, not a narrower window
    // (a narrower window of a growing conversation always holds our newest
    // rows). Review finding, hermes-headless#2 round 1.
    const disjoint = ids.length > 0 && this.knownRowIds.length > 0 && !this.knownRowIds.some((id) => listed.has(id))
    const rewritten = vanishedInside || disjoint
    if (rewritten) {
      this.resetHistory('rewrite', messages)
      this.replayOpenTurn()
      return
    }
    messages.forEach((row, index) => this.commitRow(row, index))
    this.knownRowIds = ids
    this.out.historyCaughtUp(this.generation)
  }

  /** One event from the transport (live or replayed; the transport already deduped by seq). */
  applyEvent(event: EventEnvelope): void {
    if (event.session_id && this.runtimeSessionId && event.session_id !== this.runtimeSessionId) {
      // Another session on the same connection (a second pane shares the
      // backend). Not ours; the transport routes by session_id upstream, this
      // is a second fence.
      return
    }
    const payload = (event.payload ?? {}) as Record<string, unknown>
    switch (event.type) {
      case 'session.info':
        this.info = payload as SessionLiveInfo
        if (typeof this.info.stored_session_id === 'string' && this.info.stored_session_id) {
          this.storedSessionId = this.info.stored_session_id
        }
        this.publishSession()
        return
      case 'session.title': {
        const title = payload as SessionTitlePayload
        this.title = title.title
        this.publishSession()
        return
      }
      case 'message.start':
        this.startTurn(`${this.runtimeSessionId}:${event.seq ?? Date.now()}`)
        this.out.activity({ active: true, status: null })
        return
      case 'message.delta':
        this.ensureTurn(event)
        this.appendAnswer((payload as StreamDeltaPayload).text ?? '')
        return
      case 'message.interim':
        this.ensureTurn(event)
        this.applyInterim(payload as MessageInterimPayload)
        return
      case 'reasoning.delta':
        this.ensureTurn(event)
        this.turn!.sawReasoningDelta = true
        this.appendThinking((payload as StreamDeltaPayload).text ?? '')
        return
      case 'reasoning.available':
        this.ensureTurn(event)
        this.applyReasoningAvailable((payload as StreamDeltaPayload).text ?? '')
        return
      case 'thinking.delta': {
        // Spinner rewrites ("(◔_◔) musing...", S3) and provider-wait notes.
        // Hermes Desktop keeps them out of the transcript (message-stream.ts
        // 243-253); here they are activity text only.
        const text = String((payload as StreamDeltaPayload).text ?? '').trim()
        if (this.turn) this.out.activity({ active: true, status: text || null })
        return
      }
      case 'tool.generating':
        this.out.activity({ active: true, status: `Preparing ${String((payload as { name?: string }).name ?? 'a tool')}` })
        return
      case 'tool.start':
        this.ensureTurn(event)
        this.applyToolStart(payload as ToolStartPayload)
        return
      case 'tool.complete':
        this.ensureTurn(event)
        this.applyToolComplete(payload as ToolCompletePayload)
        return
      case 'status.update': {
        const status = payload as StatusUpdatePayload
        // `ready` is Hermes saying "idle" after compaction (S12); every other
        // kind is a transient line worth showing while something runs.
        if (status.kind === 'status' && status.text === 'ready') {
          this.out.activity({ active: this.turn !== null, status: null })
        } else {
          this.out.activity({ active: true, status: status.text })
        }
        return
      }
      case 'session.usage':
        if (this.turn) this.emit({ type: 'usage_updated', turnId: this.turn.turnId, usage: (payload as { usage?: HermesUsage }).usage ?? {} })
        return
      case 'message.complete':
        this.ensureTurn(event)
        this.completeTurn(payload as MessageCompletePayload)
        return
      case 'error':
        this.emit({ type: 'api_error', message: String((payload as { message?: string }).message ?? 'Hermes error'), layer: 'session' })
        return
      case 'notice':
        this.out.activity({ active: this.turn !== null, status: String((payload as { message?: string }).message ?? '') || null })
        return
      case 'notification.show': {
        const shown = payload as { text?: string; level?: string; kind?: string; key?: string | null }
        const text = String(shown.text ?? '').trim()
        if (!text) return
        this.out.notice({
          key: typeof shown.key === 'string' && shown.key ? shown.key : null,
          level: noticeLevel(shown.level),
          kind: shown.kind === 'ttl' || shown.kind === 'agent' ? shown.kind : 'sticky',
          text,
        })
        return
      }
      case 'notification.clear': {
        const key = (payload as { key?: string }).key
        if (typeof key === 'string' && key) this.out.noticeCleared(key)
        return
      }
      case 'review.summary': {
        const text = String((payload as { text?: string }).text ?? '').trim()
        if (text) this.out.notice({ key: null, level: 'info', kind: 'review', text })
        return
      }
      case 'subagent.start': {
        // Activity only (no roster): the parent turn's tool chip already
        // shows the delegate_task call; this says what the child is doing.
        const goal = String((payload as { goal?: string }).goal ?? '').trim()
        this.out.activity({ active: true, status: goal ? `Delegating: ${goal}` : 'Delegating a task' })
        return
      }
      case 'subagent.complete': {
        const child = payload as { goal?: string; status?: string }
        const goal = String(child.goal ?? '').trim()
        const status = typeof child.status === 'string' && child.status ? child.status : 'finished'
        this.out.activity({ active: this.turn !== null, status: goal ? `Delegated task ${status}: ${goal}` : `Delegated task ${status}` })
        return
      }
      case 'tool.output_risk': {
        const risk = payload as { name?: string; risk?: string; findings?: unknown; redacted?: boolean }
        const findings = Array.isArray(risk.findings) ? risk.findings.filter((f): f is string => typeof f === 'string') : []
        const what = findings.length > 0 ? `: ${findings.join('; ')}` : ''
        this.out.notice({
          key: null,
          level: 'warn',
          kind: 'tool-risk',
          text: `Hermes flagged the output of ${String(risk.name ?? 'a tool')} as ${String(risk.risk ?? 'risky')}${what}${risk.redacted ? ' (redacted)' : ''}.`,
        })
        return
      }
      case 'connection.request':
      case 'connection.update': {
        const operation = connectionOperation(payload)
        if (operation) this.out.connection?.(operation)
        return
      }
      case 'background.complete':
      case 'btw.complete': {
        const done = payload as { task_id?: string; text?: string; question?: string | null }
        this.out.sideAnswer({
          taskId: String(done.task_id ?? ''),
          kind: event.type === 'btw.complete' ? 'btw' : 'background',
          question: typeof done.question === 'string' && done.question ? done.question : null,
          text: String(done.text ?? ''),
        })
        return
      }
      case 'sessions.changed':
        // Consumed by HermesHeadless (Telegram-lane refetch); nothing for the
        // conversation fold itself.
        return
      case 'request.cancel': {
        const cancel = payload as RequestCancelPayload
        this.closeRequest(cancel.id, cancel.reason)
        return
      }
      case 'approval.cancelled':
        for (const id of ((payload as { request_ids?: string[] }).request_ids ?? [])) this.closeRequest(id, String((payload as { reason?: string }).reason ?? 'cancelled'))
        return
      case 'session.reclaimed': {
        // A BROADCAST (session_lifecycle.py _announce_session_reclaimed): it
        // reaches every client with no outer session id, naming the reclaimed
        // session only in its payload. Acting on another session's reclaim
        // wiped this conversation's open approvals (package review A #5).
        const reclaimed = payload as { session_id?: string; stored_session_id?: string; reason?: string }
        const ours = reclaimed.session_id === this.runtimeSessionId ||
          (Boolean(reclaimed.stored_session_id) && reclaimed.stored_session_id === this.storedSessionId)
        if (!ours) return
        this.closeAllRequests('reclaimed')
        this.out.activity({ active: false, status: 'Reconnecting to the conversation' })
        this.out.reclaimed(String(reclaimed.reason ?? 'reclaimed'))
        return
      }
      default:
        if (isHandledEvent(event.type) || isIgnoredEvent(event.type)) return
        if (!this.unknownTypes.has(event.type)) {
          this.unknownTypes.add(event.type)
          this.out.unknown({ eventType: event.type, reason: 'event type not in the catalog' })
        }
    }
  }

  /** A server→client request the consumer may answer (live or replayed). */
  openRequest(request: OpenRequestEntry & { replayed: boolean }): void {
    if (this.openRequests.has(request.id)) return
    this.openRequests.add(request.id)
    this.out.requestOpened(request)
  }

  /** The consumer answered; the card goes away without waiting for the server. */
  closeRequest(id: string, reason: string): void {
    if (!this.openRequests.delete(id)) return
    this.out.requestClosed(id, reason)
  }

  /** `/new`: the pane keeps its identity, the conversation starts over. */
  beginNewConversation(result: SessionResumeResult & { stored_session_id?: string | null }): void {
    this.knownRowIds = []
    // A consumer-owned cache is keyed by tool call id, which is unique
    // across conversations; clearing it would drop results another pane of
    // the previous conversation (or a later reload of it) still needs.
    if (!this.sharedResults) (this.liveToolResults as Map<string, unknown>).clear()
    this.applySnapshot(result, 'new-conversation')
  }

  // ── internals ──────────────────────────────────────────────────────────────

  private publishSession(): void {
    if (!this.runtimeSessionId) return
    this.out.session({ storedSessionId: this.storedSessionId, runtimeSessionId: this.runtimeSessionId, title: this.title, info: this.info })
  }

  private resetHistory(reason: 'snapshot' | 'new-conversation' | 'rewrite', messages: TranscriptMessage[]): void {
    this.generation += 1
    this.emittedKeys.clear()
    this.out.historyReset(this.generation, reason)
    messages.forEach((row, index) => this.commitRow(row, index))
    this.knownRowIds = messages.map((row) => row.row_id).filter((id): id is number => typeof id === 'number')
    this.out.historyCaughtUp(this.generation)
  }

  private commitRow(row: TranscriptMessage, index: number): void {
    const key = rowKey(row, index)
    if (this.emittedKeys.has(key)) return
    this.emittedKeys.add(key)
    this.out.committed({ key, storedSessionId: this.storedSessionId, row: this.withLiveToolResult(row) })
  }

  /**
   * Hermes' history projection drops tool output (recorded S4: tool rows carry
   * only role/name/context/tool_call_id/timestamp/args), yet the committed row
   * OWNS its tool unit downstream (the ledger claims by call id). Committed
   * bare, every chip lost its output the moment a turn's history landed, and
   * a failed call turned into a success (round-2 review x1 #1). This is the
   * one place the live result and the committed row meet, so the result
   * observed live is attached here: `content` as Hermes would have stored it,
   * and `tool_error` (an Agent Code key, not Hermes') for its failure flag.
   * After a reload nothing was observed live and the row stays bare.
   */
  private withLiveToolResult(row: TranscriptMessage): TranscriptMessage {
    if (row.role !== 'tool' || typeof row.tool_call_id !== 'string') return row
    if (row.content !== undefined && row.content !== null && row.content !== '') return row
    const live = this.liveToolResults.get(this.toolResultKey(row.tool_call_id))
    if (!live) return row
    return { ...row, content: live.content, ...(live.isError ? { tool_error: true } : {}) } as TranscriptMessage
  }

  private emit(event: Omit<SemanticEvent, 'source'>): void {
    this.out.semantic({ ...event, source: HERMES_SEMANTIC_SOURCE } as SemanticEvent)
  }

  private startTurn(turnId: string): void {
    if (this.turn) {
      // A new turn while one is open means we never saw its message.complete
      // (a drop whose replay was truncated). Close it as stopped rather than
      // let two live turns fight for the same bubble.
      this.emit({ type: 'turn_stopped', turnId: this.turn.turnId, stopReason: 'superseded', interruption: 'transport-gap' })
      this.emit({ type: 'turn_completed', turnId: this.turn.turnId })
    }
    this.turn = {
      turnId,
      nextBlockIndex: 0,
      openText: null,
      openThinking: null,
      answerText: '',
      sawReasoningDelta: false,
      toolBlocks: new Map(),
      blocks: [],
    }
    this.emit({ type: 'turn_started', turnId })
  }

  /** A turn event arrived without `message.start` (resume mid-turn, replay edge): open one. */
  private ensureTurn(event: EventEnvelope): void {
    if (!this.turn) this.startTurn(`${this.runtimeSessionId}:${event.seq ?? Date.now()}-implicit`)
  }

  private openBlock(kind: Block['kind'], extra: Record<string, unknown> = {}): Block {
    const turn = this.turn!
    const block: Block = { index: turn.nextBlockIndex++, kind, text: '' }
    turn.blocks.push(block)
    this.emit({ type: 'block_started', turnId: turn.turnId, blockIndex: block.index, kind, ...extra })
    return block
  }

  private sealText(): void {
    const turn = this.turn
    if (!turn?.openText) return
    this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: turn.openText.index, kind: 'text', text: turn.openText.text })
    turn.openText.sealed = true
    turn.openText = null
  }

  private sealThinking(): void {
    const turn = this.turn
    if (!turn?.openThinking) return
    this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: turn.openThinking.index, kind: 'thinking' })
    turn.openThinking.sealed = true
    turn.openThinking = null
  }

  private appendAnswer(delta: string): void {
    const turn = this.turn!
    if (!delta) return
    this.sealThinking()
    if (!turn.openText) {
      turn.openText = this.openBlock('text')
      // A new segment after a tool call starts with "\n\n" (recorded S6/S8);
      // the bubble should not open with blank lines.
      delta = delta.replace(/^\s+/, '')
      if (!delta) return
    }
    turn.openText.text += delta
    turn.answerText += (turn.answerText && turn.openText.text === delta ? '\n\n' : '') + delta
    this.emit({ type: 'text_delta', turnId: turn.turnId, blockIndex: turn.openText.index, textDelta: delta, textSoFar: turn.openText.text })
    this.emit({ type: 'turn_delta', turnId: turn.turnId, fullText: turn.answerText })
  }

  private appendThinking(delta: string): void {
    const turn = this.turn!
    if (!delta) return
    if (!turn.openThinking) {
      this.sealText()
      turn.openThinking = this.openBlock('thinking')
    }
    turn.openThinking.text += delta
    this.emit({ type: 'thinking_delta', turnId: turn.turnId, blockIndex: turn.openThinking.index, thinkingDelta: delta, thinkingSoFar: turn.openThinking.text })
  }

  private applyInterim(payload: MessageInterimPayload): void {
    // Interim commentary is sealed as its own segment. When it was already
    // streamed as deltas, sealing the open text block is all that is left;
    // otherwise its text arrives only here.
    if (!payload.already_streamed && payload.text) this.appendAnswer(payload.text)
    this.sealText()
  }

  /**
   * `reasoning.available` carries a COMPLETED reasoning block for providers
   * that do not stream reasoning. In every recording it carried the final
   * ANSWER text verbatim instead (S3/S4/S5: same string as message.complete),
   * which Hermes Desktop would show as a duplicate inside its reasoning
   * disclosure. Rule: only real reasoning becomes a thinking block, i.e. text
   * that is not the answer and was not already streamed as reasoning deltas.
   */
  private applyReasoningAvailable(text: string): void {
    const turn = this.turn!
    if (!text.trim() || turn.sawReasoningDelta) return
    const normalized = normalizeAnswer(text)
    const answer = normalizeAnswer(turn.answerText)
    const lastSegment = normalizeAnswer(turn.openText?.text ?? '')
    if (normalized === answer || normalized === lastSegment || (answer && answer.endsWith(normalized))) return
    const block = this.openBlock('thinking')
    block.text = text
    block.sealed = true
    this.emit({ type: 'thinking_delta', turnId: turn.turnId, blockIndex: block.index, thinkingDelta: text, thinkingSoFar: text })
    this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: block.index, kind: 'thinking' })
  }

  private applyToolStart(payload: ToolStartPayload): void {
    const turn = this.turn!
    if (turn.toolBlocks.has(payload.tool_id)) return
    this.sealText()
    this.sealThinking()
    const block = this.openBlock('tool_use', { toolName: payload.name, toolUseId: payload.tool_id })
    block.toolUseId = payload.tool_id
    turn.toolBlocks.set(payload.tool_id, block)
    const args = payload.args && typeof payload.args === 'object' ? payload.args : {}
    block.tool = { name: payload.name, args, context: payload.context ?? null }
    this.emit({
      type: 'tool_input_finalized',
      turnId: turn.turnId,
      blockIndex: block.index,
      toolName: payload.name,
      toolUseId: payload.tool_id,
      inputJson: JSON.stringify(args),
      parsed: args,
      // Hermes' one-line description of the call ("echo hh-tools-ok",
      // "sleep 3 + 1 command"): the chip's label.
      context: payload.context ?? null,
    })
    this.out.activity({ active: true, status: `Running ${payload.name}` })
  }

  private applyToolComplete(payload: ToolCompletePayload): void {
    const turn = this.turn!
    if (!turn.toolBlocks.has(payload.tool_id)) {
      // A completion whose start we never saw (resume mid-tool): open the
      // block so the result has somewhere to land.
      this.applyToolStart({ tool_id: payload.tool_id, name: payload.name, args: payload.args ?? {} })
    }
    const content = typeof payload.result_text === 'string' && payload.result_text
      ? payload.result_text
      : typeof payload.summary === 'string' && payload.summary
        ? payload.summary
        : textOf(payload.result)
    const isError = toolResultIsError(payload)
    this.liveToolResults.set(this.toolResultKey(payload.tool_id), { content, isError })
    const toolBlock = turn.toolBlocks.get(payload.tool_id)
    if (toolBlock?.tool) toolBlock.tool.result = { content, isError }
    this.emit({
      type: 'tool_result',
      turnId: turn.turnId,
      toolUseId: payload.tool_id,
      content,
      isError,
      durationSeconds: payload.duration_s ?? null,
      inlineDiff: payload.inline_diff ?? null,
    })
    this.out.activity({ active: true, status: null })
  }

  private completeTurn(payload: MessageCompletePayload): void {
    const turn = this.turn!
    const status = payload.status ?? 'complete'
    const finalText = typeof payload.text === 'string' ? payload.text : ''
    // Non-streaming providers and provider ERRORS deliver their text only
    // here (S15: the explanation of a 400 never streamed). Show it.
    if (finalText && !turn.answerText) this.appendAnswer(finalText)
    this.sealText()
    this.sealThinking()
    if (status === 'error') {
      const surface = (payload.error_surface ?? null) as ErrorSurface | null
      this.emit({
        type: 'api_error',
        turnId: turn.turnId,
        message: payload.error ?? (finalText || 'The turn failed'),
        layer: surface?.layer ?? null,
        code: surface?.code ?? null,
        retryable: surface?.retryable ?? null,
        recoverable: payload.recoverable ?? null,
      })
    }
    if (payload.usage) this.emit({ type: 'usage_updated', turnId: turn.turnId, usage: payload.usage })
    this.emit({
      type: 'turn_stopped',
      turnId: turn.turnId,
      stopReason: status === 'complete' ? 'end_turn' : status === 'interrupted' ? 'interrupted' : 'error',
    })
    this.emit({ type: 'turn_completed', turnId: turn.turnId })
    this.turn = null
    // Every answerable request blocks the agent thread, so once the turn has
    // ended nothing can still be waiting on one. Hermes emits NOTHING to the
    // other attached clients when one of them answers (server_requests.py
    // resolve_response, r4 F4), so a card answered in Hermes Desktop or
    // Telegram stayed up here and silently swallowed the next click
    // (recorded S22). Closing at turn end is the earliest point that is
    // provably safe; closing on the next stream frame was considered and
    // rejected because a spinner frame can arrive while a clarify is open.
    this.closeAllRequests('settled')
    this.out.activity({ active: false, status: null })
    this.out.turnSettled({ persisted: payload.persisted_turn ?? null, status })
  }

  /**
   * Tool call ids are NOT unique across conversations: recorded S5 (v0.22.0)
   * has `call_approval_0_0` in two conversations, one ran and one was denied.
   * A consumer-shared cache keyed by call id alone showed one conversation's
   * output in the other (review finding, both reviewers). Keyed by the
   * STORED id, which survives a client rebuild (the cache's reason to exist);
   * a rotating compression changes it, which only costs re-fetching outputs.
   */
  private toolResultKey(toolCallId: string): string {
    return `${this.storedSessionId}:${toolCallId}`
  }

  /**
   * Re-announce the running turn after a history reset.
   *
   * WHY: the consumer wipes its live turn on every reset and then ignores
   * every semantic event until a NEW `turn_started` (Agent Code's
   * historyAwaitingTurnStart gate, useIpcSubscriptions.ts). A reset in the
   * MIDDLE of a turn therefore silenced it: in the owner's bundle the running
   * terminal tool never appeared and the pane said "Hermes is thinking" for
   * the rest of the turn. Replaying what this turn has produced so far, under
   * the same turn id, reopens the consumer's fold exactly where it was.
   */
  private replayOpenTurn(): void {
    const turn = this.turn
    if (!turn) return
    this.emit({ type: 'turn_started', turnId: turn.turnId })
    for (const block of turn.blocks) {
      if (block.kind === 'tool_use' && block.tool) {
        this.emit({ type: 'block_started', turnId: turn.turnId, blockIndex: block.index, kind: 'tool_use', toolName: block.tool.name, toolUseId: block.toolUseId })
        this.emit({
          type: 'tool_input_finalized',
          turnId: turn.turnId,
          blockIndex: block.index,
          toolName: block.tool.name,
          toolUseId: block.toolUseId,
          inputJson: JSON.stringify(block.tool.args),
          parsed: block.tool.args,
          context: block.tool.context,
        })
        if (block.tool.result) {
          this.emit({ type: 'tool_result', turnId: turn.turnId, toolUseId: block.toolUseId, content: block.tool.result.content, isError: block.tool.result.isError, durationSeconds: null, inlineDiff: null })
        }
        continue
      }
      this.emit({ type: 'block_started', turnId: turn.turnId, blockIndex: block.index, kind: block.kind })
      if (block.kind === 'text' && block.text) {
        this.emit({ type: 'text_delta', turnId: turn.turnId, blockIndex: block.index, textDelta: block.text, textSoFar: block.text })
      }
      if (block.kind === 'thinking' && block.text) {
        this.emit({ type: 'thinking_delta', turnId: turn.turnId, blockIndex: block.index, thinkingDelta: block.text, thinkingSoFar: block.text })
      }
      if (block.sealed) {
        this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: block.index, kind: block.kind, ...(block.kind === 'text' ? { text: block.text } : {}) })
      }
    }
    if (turn.answerText) this.emit({ type: 'turn_delta', turnId: turn.turnId, fullText: turn.answerText })
  }

  private closeAllRequests(reason: string): void {
    for (const id of [...this.openRequests]) this.closeRequest(id, reason)
  }
}

/** A `connection.request` / `connection.update` payload as a HermesConnectionOperation; null when malformed. */
export function connectionOperation(payload: unknown): HermesConnectionOperation | null {
  if (!payload || typeof payload !== 'object') return null
  const raw = payload as Record<string, unknown>
  if (typeof raw.op_id !== 'string' || !Array.isArray(raw.targets)) return null
  const str = (value: unknown) => (typeof value === 'string' && value.length > 0 ? value : null)
  const targets: HermesConnectionTarget[] = []
  for (const entry of raw.targets) {
    if (!entry || typeof entry !== 'object') continue
    const target = entry as Record<string, unknown>
    if (typeof target.name !== 'string') continue
    targets.push({
      name: target.name,
      kind: String(target.kind ?? ''),
      action: String(target.action ?? ''),
      state: String(target.state ?? 'pending'),
      resolved: target.resolved === true,
      detail: str(target.detail),
      instructions: str(target.instructions),
      connectUrl: str(target.connect_url),
      requiredEnv: Array.isArray(target.required_env)
        ? target.required_env.flatMap((field) => {
          if (!field || typeof field !== 'object' || typeof (field as { name?: unknown }).name !== 'string') return []
          const spec = field as Record<string, unknown>
          return [{ name: String(spec.name), prompt: str(spec.prompt), required: spec.required !== false, secret: spec.secret === true, default: typeof spec.default === 'string' ? spec.default : '' }]
        })
        : [],
      alias: str(target.alias),
    })
  }
  const deadline = typeof raw.deadline_at === 'number' ? Math.round(raw.deadline_at * 1000) : null
  return {
    opId: raw.op_id,
    seq: typeof raw.seq === 'number' ? raw.seq : 0,
    deadlineAt: deadline,
    // The request frame has no settled fields; an update says `settled` and
    // `settled_at` (recorded S26/S27).
    settled: raw.settled === true || (typeof raw.settled_at === 'number'),
    settledBy: str(raw.settled_by),
    toolCallId: str(raw.tool_call_id),
    targets,
  }
}
