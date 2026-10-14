// hermes-headless: drive a remote Hermes Agent (`hermes serve`) programmatically.
// The consumer owns every socket dial, credential and reconnect decision; see
// README.md for the shape and research/corpus for the recorded wire it is
// tested against.

export { HermesHeadless, HermesPromptError } from './HermesHeadless.js'
export type {
  HermesConnectionState,
  HermesDial,
  HermesHeadlessOptions,
  HermesLaneState,
  HermesRequest,
  LaneMove,
  HermesSessionState,
  HermesTarget,
  PromptOutcome,
  SlashOutcome,
} from './HermesHeadless.js'
export { HermesIncompatibleBackendError, HermesLaneNotFoundError, HermesStartError, INCOMPATIBLE_RPC_CODES } from './errors.js'
export type { HermesStartStage } from './errors.js'
export { RpcConnection, RpcError, RpcTimeoutError, ConnectionClosedError } from './transport/RpcConnection.js'
export type { ServerRequest, WebSocketLike } from './transport/RpcConnection.js'
export { ConversationReconciler, HERMES_SEMANTIC_SOURCE } from './reconcile/ConversationReconciler.js'
export type { CommittedRow, SemanticEvent } from './reconcile/ConversationReconciler.js'
export { CommittedChannel, ScreenChannel, SemanticChannel } from './channels/index.js'
export type { HistoryBoundary, ScreenActivityEvent } from './channels/index.js'
export * from './auth/index.js'
export { backendCompatibility, MIN_DESKTOP_CONTRACT, TESTED_DESKTOP_CONTRACT, TESTED_HERMES_RELEASE } from './version.js'
export type { BackendCompatibility } from './version.js'
export { ANSWERABLE_REQUESTS, isAnswerableRequest } from './contract/catalog.js'
export type { AnswerableRequestMethod } from './contract/catalog.js'
export type * from './contract/wire.js'
export { HermesRest } from './rest/HermesRest.js'
export type { AccessTokenSource, GatewayRestartResult, HermesRestOptions, LatestDescendant, SessionListResult, SessionListRow } from './rest/HermesRest.js'
export { defaultTelegramLane, pickLaneRow, summarizeLanes } from './lane/lanes.js'
export type { LaneSummary } from './lane/lanes.js'
export { isTailBusy, laneTail, STALE_BUSY_TAIL_MS } from './lane/laneBusy.js'
export type { LaneTail } from './lane/laneBusy.js'
export { parseSlash, routeSlash } from './slash/routes.js'
export type { ParsedSlash, SlashRoute } from './slash/routes.js'
export type { HermesNotice, HermesSideAnswer } from './reconcile/ConversationReconciler.js'
