// Telegram-lane identity: which stored Hermes conversation IS the owner's
// Telegram thread right now (plan Stage D, agent-code#1496).
//
// The hard fact this file exists for (t1 F1c, recorded T6): the stored
// conversation behind a Telegram chat CHANGES. `/new` and `/reset` end the
// row (`session_reset`) and start a new one with the same gateway routing
// key; `/resume` and `/handoff` re-point it; compression publishes a child.
// A pane that pinned a stored id kept writing into a dead branch, and Hermes
// REOPENS an ended row on the next prompt (`_reopen_if_finalized`), so the
// thread silently forked. The only stable identity is the gateway's routing
// key, `session_key` (`agent:main:telegram:dm:<chat_id>`, gateway/session.py
// build_session_key), so a lane pane pins THAT and re-resolves the stored id.
//
// Pure functions over `GET /api/sessions` rows, so every rule is testable on
// the recorded REST bodies (T1, T6, T7, T8).

import type { SessionListRow } from '../rest/HermesRest.js'

export type LaneSummary = {
  sessionKey: string
  /** `telegram`, `discord`, …: the row's `source`. */
  platform: string
  /** `dm`, `group`, `channel`, … as the gateway stored it. */
  chatType: string | null
  /** A Telegram DM topic / forum thread: a SEPARATE lane with its own key. */
  threadId: string | null
  displayName: string | null
  /** The stored id the lane points at now, before following compression. */
  headId: string
  /** Most recent activity seen on the lane (seconds, server clock). */
  lastActiveAt: number | null
}

/**
 * Rows that can still be the lane's live conversation.
 *
 * WHY "not ended, OR ended by compression": `order=recent` ranks a compressed
 * lineage by its tip's activity but may list the lineage ROOT, which compression
 * ended with `end_reason='compression'`. That root is still the way to the
 * live tip (`latest-descendant` walks it). Every other end reason
 * (`session_reset` from /new, `session_switch` from /resume or /handoff,
 * suspension) means the lane has MOVED and that row is history.
 */
function isLiveCandidate(row: SessionListRow): boolean {
  return row.ended_at === null || row.ended_at === undefined || row.end_reason === 'compression'
}

function seconds(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0
}

/**
 * The row the lane points at: among live candidates with this key, the one
 * started most recently. Same rule as Hermes' own lookup for a chat's live row
 * (SessionDB.find_session_by_origin: newest `started_at`), so the pane and
 * the gateway agree even when a stale pane revived an old row.
 */
export function pickLaneRow(rows: readonly SessionListRow[], sessionKey: string): SessionListRow | null {
  let best: SessionListRow | null = null
  for (const row of rows) {
    if (row.session_key !== sessionKey || !isLiveCandidate(row)) continue
    if (!best || seconds(row.started_at) > seconds(best.started_at)) best = row
  }
  return best
}

/** Every messaging lane in a listing, most recently active first. */
export function summarizeLanes(rows: readonly SessionListRow[]): LaneSummary[] {
  const byKey = new Map<string, LaneSummary>()
  for (const row of rows) {
    const key = row.session_key
    if (typeof key !== 'string' || !key) continue
    const head = pickLaneRow(rows, key)
    if (!head || byKey.has(key)) continue
    const active = rows
      .filter((candidate) => candidate.session_key === key)
      .reduce((latest, candidate) => Math.max(latest, seconds(candidate.last_active ?? candidate.started_at)), 0)
    byKey.set(key, {
      sessionKey: key,
      platform: String(head.source ?? ''),
      chatType: typeof head.chat_type === 'string' ? head.chat_type : null,
      threadId: head.thread_id !== null && head.thread_id !== undefined && String(head.thread_id) !== '' ? String(head.thread_id) : null,
      displayName: typeof head.display_name === 'string' && head.display_name ? head.display_name : (typeof head.title === 'string' && head.title ? head.title : null),
      headId: head.id,
      lastActiveAt: active || null,
    })
  }
  return [...byKey.values()].sort((a, b) => (b.lastActiveAt ?? 0) - (a.lastActiveAt ?? 0))
}

/**
 * The lane a "Telegram thread" pane opens by default (plan Decision 2,
 * UNCONFIRMED): the owner's direct chat with the bot, not a DM topic and not
 * a group, most recently active first. Null when there is no DM lane.
 */
export function defaultTelegramLane(lanes: readonly LaneSummary[]): LaneSummary | null {
  return lanes.find((lane) => lane.platform === 'telegram' && lane.chatType === 'dm' && lane.threadId === null)
    ?? lanes.find((lane) => lane.platform === 'telegram')
    ?? null
}
