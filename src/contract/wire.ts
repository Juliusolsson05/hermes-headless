// The slice of Hermes' `tui_gateway` wire contract this package speaks.
//
// SOURCE OF TRUTH: Hermes declares its wire in Python (tui_gateway/contracts/
// *.py, Pydantic) and generates TypeScript from it into
// apps/shared/src/gateway-contract.generated.ts. Every type below is copied
// from that generated file at Hermes v0.21.7 (commit 71d34bad) and trimmed to
// the fields this package reads or writes. The comment on each names its
// upstream type so a version bump is a mechanical diff.
//
// WHY a hand-copied subset and not the whole 6.2k-line generated file
// (decomposition graveyard G7): Hermes moves its contract daily (+520/−189
// lines between v0.21.7 and main one day later), almost all of it in surfaces
// a chat client never touches. Copying everything would turn each unrelated
// upstream move into a diff we must review. Copying only what we consume, and
// declaring the rest ignored in catalog.ts, keeps a bump honest: a change that
// matters shows up here; one that does not, does not.
//
// WHY every object keeps `[key: string]: unknown` where upstream has it: the
// payloads are producer-owned. New optional fields appear without notice, and
// the parser must pass them through rather than reject a frame that is fine.

/** One JSON-RPC 2.0 frame as it arrives on the socket, before classification. */
export type RawFrame = {
  jsonrpc?: string
  id?: string | number | null
  method?: string
  params?: unknown
  result?: unknown
  error?: RpcErrorBody
}

export type RpcErrorBody = { code: number; message: string; data?: unknown }

/** `{method:"event", params:{type, session_id, payload?, seq?}}` (tui_gateway/server.py:700-703). */
export type EventEnvelope = {
  type: string
  /** The RUNTIME session id (8 hex). Absent on connection-wide events. */
  session_id?: string
  payload?: unknown
  /** Per-session monotonic sequence (event_replay._stamp_event). Absent on connection-wide events. */
  seq?: number
}

// ── gateway.ready / client.capabilities ──────────────────────────────────────

/** Upstream `GatewayReadyPayload`. */
export type GatewayReadyPayload = {
  change_events?: boolean
  replay_epoch?: string
  heartbeat?: boolean | null
  [key: string]: unknown
}

// ── sessions ─────────────────────────────────────────────────────────────────

/** Upstream `Usage` (trimmed to what a chat header shows). */
export type HermesUsage = {
  model?: string
  input?: number
  output?: number
  reasoning?: number
  total?: number
  calls?: number
  context_used?: number | null
  context_max?: number | null
  context_percent?: number | null
  cost_usd?: number | null
  [key: string]: unknown
}

/** Upstream `SessionLiveInfo`. */
export type SessionLiveInfo = {
  model?: string
  provider?: string
  approval_mode?: string
  cwd?: string
  running?: boolean
  title?: string
  stored_session_id?: string
  /** Upstream `DESKTOP_BACKEND_CONTRACT` (8 at v0.21.7). See version.ts. */
  desktop_contract?: number | string | null
  version?: string
  release_date?: string
  usage?: HermesUsage | null
  profile_name?: string | null
  credential_warning?: string | null
  lazy?: boolean | null
  [key: string]: unknown
}

/** Upstream `TranscriptMessage`: one durable row as the gateway projects it. */
export type TranscriptMessage = {
  role: string
  text?: string | null
  content?: unknown | null
  tool_call_id?: string | null
  timestamp?: number | null
  /** The durable SQLite row id. The ONLY identity a committed row has. */
  row_id?: number | null
  display_kind?: string | null
  display_metadata?: unknown | null
  name?: string | null
  context?: string | null
  args?: Record<string, unknown> | null
  reasoning?: string | null
  [key: string]: unknown
}

/** Upstream `InflightTurn`: the running (or retained failed) turn a reconnecting client rebuilds from. */
export type InflightTurn = {
  assistant?: string
  streaming?: boolean
  user?: string
  error?: string | null
  status?: string | null
  recoverable?: boolean | null
  error_surface?: Record<string, unknown> | null
}

/** Upstream `OpenRequestEntry`: an unanswered server→client request. */
export type OpenRequestEntry = { id: string; method: string; params: Record<string, unknown> }

/** Upstream `SessionCreateResult`. */
export type SessionCreateResult = {
  session_id: string
  stored_session_id: string
  message_count: number
  messages: TranscriptMessage[]
  info: SessionLiveInfo
}

/** Upstream `SessionResumeResult` (the `LiveSessionSnapshot` union). */
export type SessionResumeResult = {
  session_id: string
  message_count: number
  messages: TranscriptMessage[]
  info: SessionLiveInfo
  stored_session_id?: string | null
  messages_omitted?: boolean | null
  hydrating?: boolean | null
  running?: boolean | null
  status?: string | null
  inflight?: InflightTurn | null
  queued?: { user: string } | null
  open_requests?: OpenRequestEntry[] | null
  [key: string]: unknown
}

/** Upstream `SessionEventsSinceResult`. `events` are envelope objects (EventEnvelope). */
export type SessionEventsSinceResult = {
  events: Array<Record<string, unknown>>
  latest_seq: number
  truncated: boolean
  count: number
  epoch: string
  open_requests: OpenRequestEntry[]
}

/** Upstream `SessionHistoryResult` (`methods_session.py` session.history). */
export type SessionHistoryResult = { count: number; messages: TranscriptMessage[] }

/** Upstream `PromptSubmitResult`. */
export type PromptSubmitResult = {
  status?: 'streaming' | 'queued' | 'steered' | 'redirected' | null
  user_row_id?: number | null
  [key: string]: unknown
}

/** Upstream `SessionInterruptResult`. */
export type SessionInterruptResult = { status: 'interrupted' | 'not_interrupted'; interrupted?: boolean | null }

// ── slash commands ───────────────────────────────────────────────────────────

/** Upstream `CompletionItem` (methods_complete.py). */
export type CompletionItem = { text: string; display?: string; meta?: string; [key: string]: unknown }

/** Upstream `CompleteSlashResult`. */
export type CompleteSlashResult = { items?: CompletionItem[]; replace_from?: number | null }

/** Upstream `DispatchType` (apps/shared/src/slash.ts parseCommandDispatch). */
export type DispatchType = 'exec' | 'alias' | 'plugin' | 'send' | 'skill' | 'prefill'

/** Upstream `SlashExecResult` ∪ `CommandDispatchResult`. */
export type SlashExecResult = {
  output?: string | null
  warning?: string | null
  type?: DispatchType | null
  target?: string | null
  message?: string | null
  notice?: string | null
  display?: string | null
  name?: string | null
  status?: string | null
}

// ── event payloads ───────────────────────────────────────────────────────────

/** Upstream `StreamDeltaPayload` (message.delta, reasoning.delta, reasoning.available, thinking.delta). */
export type StreamDeltaPayload = { text: string; rendered?: string | null; verbose?: boolean | null }

/** Upstream `MessageInterimPayload`. */
export type MessageInterimPayload = { text: string; already_streamed: boolean }

/** Upstream `ErrorSurface`: which layer failed, advisory. */
export type ErrorSurface = {
  layer: string
  code: string
  retryable: boolean
  provider?: string | null
  model?: string | null
  resets_at?: number | null
  [key: string]: unknown
}

/** Upstream `PersistedTurn`: committed row ids for this turn. */
export type PersistedTurn = {
  row_ids: number[]
  complete: boolean
  user_row_id?: number | null
  final_assistant_row_id?: number | null
}

/** Upstream `MessageCompletePayload`. */
export type MessageCompletePayload = {
  text?: string | unknown
  usage?: HermesUsage | null
  status?: 'complete' | 'error' | 'interrupted' | null
  reasoning?: string | null
  warning?: string | null
  failure_reason?: string | null
  error?: string | null
  recoverable?: boolean | null
  error_surface?: ErrorSurface | null
  partial?: boolean | null
  persisted_turn?: PersistedTurn | null
  [key: string]: unknown
}

/** Upstream `StatusUpdatePayload`. */
export type StatusUpdatePayload = { kind: string; text: string }

/** Upstream `SessionUsagePayload`. */
export type SessionUsagePayload = { usage: HermesUsage }

/** Upstream `SessionTitlePayload`. `session_id` here is the STORED id. */
export type SessionTitlePayload = { session_id: string; title: string }

/** Upstream `ToolStartPayload`. */
export type ToolStartPayload = {
  tool_id: string
  name: string
  context?: string | null
  args?: Record<string, unknown> | null
  args_text?: string | null
  preview?: string | null
  [key: string]: unknown
}

/** Upstream `ToolCompletePayload`. */
export type ToolCompletePayload = {
  tool_id: string
  name: string
  args?: Record<string, unknown> | null
  duration_s?: number | null
  result?: unknown
  summary?: string | null
  result_text?: string | null
  inline_diff?: string | null
  [key: string]: unknown
}

/** Upstream `ToolGeneratingPayload`. */
export type ToolGeneratingPayload = { name: string }

/** Upstream `ErrorPayload` / `NoticePayload`. */
export type MessagePayload = { message: string }

/** Upstream `RequestCancelPayload`. */
export type RequestCancelPayload = { id: string; method: string; reason: string }

/** Upstream `ApprovalCancelledPayload`. */
export type ApprovalCancelledPayload = {
  session_id: string
  stored_session_id: string
  reason: string
  cancelled_count: number
  request_ids: string[]
}

/** Upstream `SessionReclaimedPayload`. */
export type SessionReclaimedPayload = { session_id: string; stored_session_id: string; reason: string }

// ── server→client requests ───────────────────────────────────────────────────

/** Upstream `ApprovalChoice`. */
export type ApprovalChoice = 'once' | 'session' | 'always' | 'deny'

/** Upstream `ApprovalRequestParams` (command already redacted server-side). */
export type ApprovalRequestParams = {
  session_id: string
  request_id: string
  command?: string
  description?: string
  choices?: ApprovalChoice[]
  allow_permanent?: boolean | null
  allow_session?: boolean | null
  tool_name?: string | null
  [key: string]: unknown
}

/** Upstream `ClarifyQuestion` / `ClarifyRequestParams`. */
export type ClarifyQuestion = { qid: string; question: string; choices?: string[] | null; multi_select?: boolean }
export type ClarifyRequestParams = {
  session_id: string
  questions: ClarifyQuestion[]
  answers?: Record<string, string | null> | null
}

/** Upstream `SudoRequestParams`. */
export type SudoRequestParams = { session_id: string; command?: string }

/** Upstream `SecretRequestParams`. */
export type SecretRequestParams = { session_id: string; env_var: string; prompt: string; metadata?: Record<string, unknown> | null }

/** Upstream `ApprovalResult` / `ClarifyResult` / `ValueResult`. `value: ''` means skipped. */
export type ApprovalResult = { choice: ApprovalChoice; all?: boolean | null }
export type ClarifyResult = { answers?: Record<string, string | null> | null }
export type ValueResult = { value: string }

/**
 * Upstream `CommandsCatalogResult` (tui_gateway/contracts/tools_commands.py,
 * v0.21.7 and v0.22.0): every slash command the session can run, with
 * Hermes' own availability metadata. `pairs` are `[name, description]`.
 * Built-ins marked `gateway_only` (e.g. /restart) are deliberately absent.
 */
export type CommandCatalogMeta = {
  argument_mode?: 'options' | 'text' | 'mixed' | null
  /**
   * Desktop availability: null = offered; `hidden` = runs but is not listed;
   * any other string is a reason it is unavailable outside a terminal
   * (`terminal`, `messaging`, `settings`, `advanced`, `composer-voice`).
   */
  desktop?: string | null
  desktop_subcommands?: string[] | null
}

export type CommandsCatalogResult = {
  pairs?: Array<[string, string] | string[]>
  sub?: Record<string, string[]>
  canon?: Record<string, string>
  commands?: Record<string, CommandCatalogMeta>
  categories?: Array<{ name: string; pairs?: Array<[string, string] | string[]> }>
  skills?: Record<string, { usage?: number; origin?: string }>
  skill_count?: number
  warning?: string
}

/** Upstream `TaskIdResult` (prompt.background / prompt.btw). */
export type TaskIdResult = { task_id: string }
