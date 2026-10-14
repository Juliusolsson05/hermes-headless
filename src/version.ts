// Backend compatibility, from what Hermes actually reports.
//
// Hermes has no handshake version. The one wire-level signal is
// `info.desktop_contract` (tui_gateway/server.py DESKTOP_BACKEND_CONTRACT) on
// every create/resume result: 8 at v0.21.7. Contract 7 introduced
// server→client requests; a backend below 7 still sends `<kind>.request`
// notifications instead, which this package does not speak, so approvals
// would silently never appear. `/api/health` `version` is "unknown" for a
// source-tree install (recorded), so it cannot be the gate.

export const TESTED_DESKTOP_CONTRACT = 8
export const MIN_DESKTOP_CONTRACT = 7
/**
 * Releases the corpus AND the live tier have been run against. Not the gate
 * (desktop_contract is: v0.21.7 and v0.22.0 both say 8, so a contract number
 * alone never meant "tested", r7 F3); it is what a release bump must update
 * after `test:live` passes against the new tag (live-matrix.yml).
 */
export const TESTED_HERMES_RELEASES = ['v0.21.7', 'v0.22.0'] as const
export const TESTED_HERMES_RELEASE = TESTED_HERMES_RELEASES[TESTED_HERMES_RELEASES.length - 1]!

export type BackendCompatibility =
  | { kind: 'tested'; contract: number }
  /** Newer or older but still speaking server→client requests: works, warn. */
  | { kind: 'skew'; contract: number }
  /** Pre-v7 wire or no contract at all: approvals would never reach the user. */
  | { kind: 'unsupported'; contract: number | null }

export function backendCompatibility(desktopContract: unknown): BackendCompatibility {
  // `Number(null)` and `Number('')` are 0: a backend that reported NOTHING
  // must read as "no contract", not as contract 0.
  const contract = typeof desktopContract === 'number'
    ? desktopContract
    : typeof desktopContract === 'string' && desktopContract.trim() !== '' ? Number(desktopContract) : Number.NaN
  if (!Number.isFinite(contract)) return { kind: 'unsupported', contract: null }
  if (contract < MIN_DESKTOP_CONTRACT) return { kind: 'unsupported', contract }
  if (contract === TESTED_DESKTOP_CONTRACT) return { kind: 'tested', contract }
  return { kind: 'skew', contract }
}
