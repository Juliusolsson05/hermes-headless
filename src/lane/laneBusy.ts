// "Is a Telegram turn running on this lane right now?" (plan §6.3, H3).
//
// WHY the pane must know (recorded t2 run5 / T4): `prompt.submit` writes the
// pane's user row IMMEDIATELY, outside Hermes' cross-process turn lease
// (session_workdir.py _write_submit_user_row). When a phone turn is running,
// that row lands inside the phone turn; the phone turn's lease waiter then
// reloads with `repair_alternation=True` and the model receives the pane's
// prompt TWICE (merged into the phone message, then again as the new turn).
// Model execution itself is serialized; the conversation framing is not.
// Holding the pane prompt until the phone turn has written its final reply
// avoids the observed order (T5).
//
// There is no busy signal for a GATEWAY turn on the WS: `session.active_list`
// is serve-local and `last_activity_at` is deliberately left out of the
// change signature. So busy is inferred from the durable transcript tail,
// fetched fresh before every decision.
//
// Residual race (documented, not closable client-side): a phone message that
// commits between our history fetch and our `prompt.submit` (one round trip).
// The real fix is upstream: `reject_active_turn_lease=True` on the submit
// write (t1 F2c), listed in the plan §12.

import type { TranscriptMessage } from '../contract/wire.js'

export type LaneTail = {
  /** Stable identity of the tail, so "unchanged for N minutes" can be measured on the local clock. */
  key: string
  role: string
  rowId: number | null
}

/** The last row a user would see; Hermes' `hidden` rows are model context only. */
export function laneTail(messages: readonly TranscriptMessage[]): LaneTail | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const row = messages[index]
    if (!row || row.display_kind === 'hidden') continue
    const rowId = typeof row.row_id === 'number' ? row.row_id : null
    const key = rowId !== null ? `row:${rowId}` : typeof row.tool_call_id === 'string' && row.tool_call_id ? `tool:${row.tool_call_id}` : `idx:${index}`
    return { key, role: String(row.role ?? ''), rowId }
  }
  return null
}

/**
 * Busy when the conversation's tail shows a turn that has started but not
 * answered: a user row we did not write (the phone's), or a tool row (a turn
 * mid-tool; its final assistant row has not landed). An assistant row ends a
 * turn. Our OWN user row at the tail is our turn, not the phone's: it needs
 * no hold (and Hermes' `queued: true` covers our own ordering).
 */
export function isTailBusy(tail: LaneTail | null, ownUserRowIds: ReadonlySet<number>): boolean {
  if (!tail) return false
  if (tail.role === 'tool') return true
  if (tail.role === 'user') return tail.rowId === null || !ownUserRowIds.has(tail.rowId)
  return false
}

/**
 * How long a busy tail may stay unchanged before it is presumed dead.
 *
 * WHY 5 minutes: Hermes' durable turn lease has a 300 s TTL refreshed every
 * 60 s (turn_facade_lease.py:26,52), so a turn that crashed without writing
 * its reply releases the lease within 5 minutes; past that a "busy" tail is a
 * failed phone turn (its error is not persisted as an assistant row), and
 * holding the pane prompt forever would be worse than sending. Measured on
 * the LOCAL clock from when this exact tail was first observed, so server
 * clock skew cannot shorten or stretch it.
 */
export const STALE_BUSY_TAIL_MS = 5 * 60_000
