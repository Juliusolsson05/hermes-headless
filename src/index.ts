// hermes-headless: drive a remote Hermes Agent (`hermes serve`) programmatically.
// The consumer owns every socket dial, credential and reconnect decision; see
// README.md for the shape and research/corpus for the recorded wire it is
// tested against.

export { HermesHeadless, HermesPromptError } from './HermesHeadless.js'
export type {
  HermesConnectionState,
  HermesDial,
  HermesHeadlessOptions,
  HermesRequest,
  HermesSessionState,
  HermesTarget,
  PromptOutcome,
  SlashOutcome,
} from './HermesHeadless.js'
export { RpcConnection, RpcError, RpcTimeoutError, ConnectionClosedError } from './transport/RpcConnection.js'
export type { ReplayOutcome, ServerRequest, WebSocketLike } from './transport/RpcConnection.js'
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
