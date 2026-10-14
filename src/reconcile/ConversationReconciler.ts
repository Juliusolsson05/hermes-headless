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
}

type Block = { index: number; kind: 'text' | 'thinking' | 'tool_use'; text: string; toolUseId?: string }

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

export class ConversationReconciler {
  private storedSessionId = ''
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

  constructor(private readonly out: ReconcilerOutput) {}

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
    const rewritten = this.knownRowIds.some((id) => !listed.has(id))
    if (rewritten) {
      this.resetHistory('rewrite', messages)
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
    this.out.committed({ key, storedSessionId: this.storedSessionId, row })
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
    this.emit({ type: 'block_started', turnId: turn.turnId, blockIndex: block.index, kind, ...extra })
    return block
  }

  private sealText(): void {
    const turn = this.turn
    if (!turn?.openText) return
    this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: turn.openText.index, kind: 'text', text: turn.openText.text })
    turn.openText = null
  }

  private sealThinking(): void {
    const turn = this.turn
    if (!turn?.openThinking) return
    this.emit({ type: 'block_completed', turnId: turn.turnId, blockIndex: turn.openThinking.index, kind: 'thinking' })
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
    this.emit({
      type: 'tool_result',
      turnId: turn.turnId,
      toolUseId: payload.tool_id,
      content,
      isError: toolResultIsError(payload),
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
    this.out.activity({ active: false, status: null })
    this.out.turnSettled({ persisted: payload.persisted_turn ?? null, status })
  }

  private closeAllRequests(reason: string): void {
    for (const id of [...this.openRequests]) this.closeRequest(id, reason)
  }
}
