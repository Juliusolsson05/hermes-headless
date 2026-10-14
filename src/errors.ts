// Typed failures a consumer must tell apart to say WHY a Hermes pane could
// not start (agent-code#1495, plan Stage A).
//
// WHY typed errors and not message parsing: the consumer (Agent Code) launders
// every start failure to a fixed, curated sentence before it crosses IPC,
// because an upstream error string can carry server detail. To pick the RIGHT
// sentence it needs a stable discriminator that survives a refactor of any
// message text. Before these existed, a v0.20.4 server's `-32601` on the
// handshake and a dead network looked identical to the user: "Session failed
// to start" (2026-10-14, the owner's first connection).

/** Where in startup a failure happened. */
export type HermesStartStage =
  /** The consumer's dial(): minting the ticket, opening the socket, the HTTP upgrade. */
  | 'dial'
  /** The socket opened but `gateway.ready` / `client.capabilities` did not complete. */
  | 'handshake'
  /** Connected; binding the conversation (`session.resume` / `session.create`) failed. */
  | 'attach'

/**
 * A start() failure, with the stage it happened in and the original error as
 * `cause`. The consumer classifies `cause` (its own dial errors, RpcError
 * codes, …) and uses `stage` when the cause alone is ambiguous: a closed
 * socket during the handshake means something different from one during
 * attach.
 */
export class HermesStartError extends Error {
  constructor(readonly stage: HermesStartStage, readonly cause: unknown) {
    super(`Hermes ${stage} failed: ${cause instanceof Error ? cause.message : String(cause)}`)
    this.name = 'HermesStartError'
  }
}

/**
 * The server does not implement a method this client cannot work without.
 *
 * Recorded (S25, v0.20.4, desktop contract 6): `client.capabilities` answers
 * JSON-RPC -32601 "method not found". Every Hermes before server→client
 * requests (contract 7) does the same. The fix is always "update Hermes", so
 * it gets its own type instead of riding a generic RpcError.
 */
export class HermesIncompatibleBackendError extends Error {
  constructor(readonly method: string, readonly code: number) {
    super(`This Hermes server does not support ${method} (JSON-RPC ${code}); it is older than this client supports.`)
    this.name = 'HermesIncompatibleBackendError'
  }
}

/** JSON-RPC "method not found" / "invalid params": the shapes an older server answers a newer call with. */
export const INCOMPATIBLE_RPC_CODES: ReadonlySet<number> = new Set([-32601, -32602])

/**
 * A lane-mode pane found no live conversation for its routing key: the owner
 * has no Telegram DM with the bot yet (or it was deleted), or the REST
 * listing is not available to this sign-in. Distinct from "unreachable":
 * the server answered.
 */
export class HermesLaneNotFoundError extends Error {
  constructor(readonly sessionKey: string) {
    super(`No live Hermes conversation found for ${sessionKey}.`)
    this.name = 'HermesLaneNotFoundError'
  }
}
