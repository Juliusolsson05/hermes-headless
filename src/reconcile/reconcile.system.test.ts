// The reconciler's oracle: a LIVE fold of a recorded turn must describe the
// same conversation as the rows Hermes returns when that conversation is
// RESUMED later in the same recording (decomposition H6).
//
// Fixtures: research/corpus (recordings of a real `hermes serve` v0.21.7).
// Nothing here is invented: the live side is the event stream the recorder
// saw, the oracle is the server's own resume/history projection.

import { describe, expect, it } from 'vitest'

import { ConversationReconciler, type CommittedRow, type ReconcilerOutput, type SemanticEvent } from './ConversationReconciler.js'
import { classifyFrame, envelopeFromReplay } from '../contract/frames.js'
import type { EventEnvelope, OpenRequestEntry, SessionResumeResult, TranscriptMessage } from '../contract/wire.js'
import { corpusScenarios, inboundFrames, latestRecording, type CorpusEntry } from '../testing/corpus.js'

type Collected = {
  semantic: SemanticEvent[]
  committed: CommittedRow[]
  resets: Array<{ generation: number; reason: string }>
  opened: Array<OpenRequestEntry & { replayed: boolean }>
  closed: Array<{ id: string; reason: string }>
  settled: number
  reclaimed: string[]
  unknown: string[]
}

function collector(): { out: ReconcilerOutput; got: Collected } {
  const got: Collected = { semantic: [], committed: [], resets: [], opened: [], closed: [], settled: 0, reclaimed: [], unknown: [] }
  const out: ReconcilerOutput = {
    semantic: (event) => got.semantic.push(event),
    committed: (row) => got.committed.push(row),
    historyReset: (generation, reason) => got.resets.push({ generation, reason }),
    historyCaughtUp: () => undefined,
    activity: () => undefined,
    session: () => undefined,
    requestOpened: (request) => got.opened.push(request),
    requestClosed: (id, reason) => got.closed.push({ id, reason }),
    turnSettled: () => {
      got.settled += 1
    },
    reclaimed: (reason) => got.reclaimed.push(reason),
    unknown: (shape) => got.unknown.push(shape.eventType),
  }
  return { out, got }
}

/**
 * Every seq'd event for `sid`, merged across connections (live + replay
 * results) and deduplicated by seq, in seq order: what the transport hands
 * the reconciler after its own exactly-once guarantee (tested separately).
 */
function sessionEvents(entries: CorpusEntry[], sid: string): EventEnvelope[] {
  const bySeq = new Map<number, EventEnvelope>()
  const unsequenced: EventEnvelope[] = []
  for (const frame of inboundFrames(entries)) {
    const classified = classifyFrame(frame)
    if (classified.kind === 'event' && classified.event.session_id === sid) {
      if (typeof classified.event.seq === 'number') bySeq.set(classified.event.seq, classified.event)
      else unsequenced.push(classified.event)
    }
    const result = frame.result as { events?: unknown[] } | undefined
    for (const raw of result?.events ?? []) {
      const event = envelopeFromReplay(raw)
      if (event?.session_id === sid && typeof event.seq === 'number') bySeq.set(event.seq, event)
    }
  }
  return [...bySeq.entries()].sort((a, b) => a[0] - b[0]).map(([, event]) => event)
}

/** Runtime → stored id pairs from every create/resume result in a recording. */
function createdSessions(entries: CorpusEntry[]): Array<{ sid: string; stored: string; result: SessionResumeResult }> {
  const out: Array<{ sid: string; stored: string; result: SessionResumeResult }> = []
  for (const frame of inboundFrames(entries)) {
    const result = frame.result as Record<string, unknown> | undefined
    if (result && typeof result.session_id === 'string' && typeof result.stored_session_id === 'string' && Array.isArray(result.messages) && result.message_count === 0) {
      out.push({ sid: result.session_id, stored: result.stored_session_id, result: result as unknown as SessionResumeResult })
    }
  }
  return out
}

/**
 * The last resume/history row list the server returned for a stored id, and
 * whether a turn was running when it was taken. A snapshot taken mid-turn
 * (S10's session.activate during an approval) holds the user row of a turn
 * that has no answer YET; that turn is not comparable and is dropped.
 */
function oracleRows(entries: CorpusEntry[], stored: string, sid: string): { rows: TranscriptMessage[]; running: boolean } | null {
  let found: { rows: TranscriptMessage[]; running: boolean } | null = null
  for (const frame of inboundFrames(entries)) {
    const result = frame.result as Record<string, unknown> | undefined
    if (!result || !Array.isArray(result.messages) || (result.messages as unknown[]).length === 0) continue
    const matches = result.resumed === stored || result.stored_session_id === stored || result.session_id === sid
    const inflight = result.inflight as { streaming?: boolean } | undefined
    if (matches) found = { rows: result.messages as TranscriptMessage[], running: result.running === true || inflight?.streaming === true }
  }
  return found
}

type TurnView = { finalText: string; toolIds: string[] }

/** Turns as the live fold painted them: last text block per turn + tool ids. */
function liveTurns(events: SemanticEvent[]): TurnView[] {
  const turns: TurnView[] = []
  const blocks = new Map<string, Map<number, { kind: string; text: string; toolUseId?: string }>>()
  for (const event of events) {
    const turnId = String(event.turnId ?? '')
    if (event.type === 'turn_started') blocks.set(turnId, new Map())
    const map = blocks.get(turnId)
    if (!map) continue
    if (event.type === 'block_started') map.set(Number(event.blockIndex), { kind: String(event.kind), text: '', toolUseId: event.toolUseId as string | undefined })
    if (event.type === 'text_delta') map.get(Number(event.blockIndex))!.text = String(event.textSoFar)
    if (event.type === 'turn_completed') {
      const ordered = [...map.entries()].sort((a, b) => a[0] - b[0]).map(([, block]) => block)
      const texts = ordered.filter((block) => block.kind === 'text').map((block) => block.text)
      turns.push({ finalText: (texts.at(-1) ?? '').trim(), toolIds: ordered.filter((block) => block.kind === 'tool_use').map((block) => block.toolUseId!) })
    }
  }
  return turns
}

/** Turns as the server persisted them: rows between user rows. */
function committedTurns(rows: TranscriptMessage[]): TurnView[] {
  const turns: TurnView[] = []
  for (const row of rows) {
    if (row.role === 'user') turns.push({ finalText: '', toolIds: [] })
    const turn = turns.at(-1)
    if (!turn) continue
    if (row.role === 'tool' && row.tool_call_id) turn.toolIds.push(row.tool_call_id)
    if (row.role === 'assistant' && typeof row.text === 'string' && row.text.trim()) turn.finalText = row.text.trim()
  }
  return turns
}

function foldSession(entries: CorpusEntry[], session: { sid: string; result: SessionResumeResult }): Collected {
  const { out, got } = collector()
  const reconciler = new ConversationReconciler(out)
  reconciler.applySnapshot(session.result, 'snapshot')
  for (const event of sessionEvents(entries, session.sid)) reconciler.applyEvent(event)
  return got
}

// Scenarios whose recording also holds the server's own view of the
// conversation after a turn completed. S5/S6 never re-read theirs; S10 only
// re-reads mid-turn (its replay + approval path is covered in RpcConnection tests).
const DIFFERENTIAL = ['S3', 'S4', 'S8', 'S9', 'S11', 'S12', 'S16']

describe('live fold vs resumed conversation (H6)', () => {
  for (const scenario of DIFFERENTIAL.filter((name) => corpusScenarios().includes(name))) {
    it(`${scenario}: every completed live turn matches the server's persisted turn`, () => {
      const entries = latestRecording(scenario)
      let compared = 0
      for (const session of createdSessions(entries)) {
        const oracle = oracleRows(entries, session.stored, session.sid)
        if (!oracle) continue
        const live = liveTurns(foldSession(entries, session).semantic)
        const committed = committedTurns(oracle.rows).slice(0, oracle.running ? -1 : undefined)
        // The oracle can be taken mid-conversation; compare the turns both saw.
        const count = Math.min(live.length, committed.length)
        expect(live.slice(0, count)).toEqual(committed.slice(0, count))
        compared += count
      }
      expect(compared).toBeGreaterThan(0)
    })
  }
})

describe('turn content (recorded)', () => {
  it('S4: tool chips carry Hermes call ids and results, and the answer is not duplicated as reasoning', () => {
    const entries = latestRecording('S4')
    const [session] = createdSessions(entries)
    const got = foldSession(entries, session)
    const results = got.semantic.filter((event) => event.type === 'tool_result')
    expect(results.map((event) => event.toolUseId)).toEqual(['call_tools_0_0', 'call_tools_0_1', 'call_tools_1_0'])
    // read_file of a missing path is the recorded failing tool.
    expect(results.map((event) => event.isError)).toEqual([false, true, false])
    // reasoning.available carried the answer verbatim (recorded); it must
    // not become a thinking block next to the same text.
    expect(got.semantic.filter((event) => event.type === 'thinking_delta')).toEqual([])
  })

  it('S6: a post-tool answer segment does not open with the blank lines Hermes prefixes', () => {
    const entries = latestRecording('S6')
    const [session] = createdSessions(entries)
    const texts = foldSession(entries, session).semantic.filter((event) => event.type === 'text_delta').map((event) => String(event.textSoFar))
    expect(texts.length).toBeGreaterThan(0)
    for (const text of texts) expect(text.startsWith('\n')).toBe(false)
  })

  it('S7: an interrupt ends the turn as interrupted and withdraws the open approval', () => {
    const entries = latestRecording('S7')
    const sessions = createdSessions(entries)
    const first = foldSession(entries, sessions[0])
    expect(first.semantic.filter((event) => event.type === 'turn_stopped').map((event) => event.stopReason)).toEqual(['interrupted'])
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(sessions[1].result, 'snapshot')
    const approval = inboundFrames(entries).find((frame) => frame.method === 'approval')!
    reconciler.openRequest({ id: String(approval.id), method: 'approval', params: approval.params as Record<string, unknown>, replayed: false })
    for (const event of sessionEvents(entries, sessions[1].sid)) reconciler.applyEvent(event)
    expect(got.closed).toEqual([{ id: String(approval.id), reason: 'interrupted' }])
  })

  it('S15: a provider error shows Hermes\' explanation and carries the failing layer', () => {
    const entries = latestRecording('S15')
    const [session] = createdSessions(entries)
    const got = foldSession(entries, session)
    const error = got.semantic.find((event) => event.type === 'api_error')!
    expect(error).toMatchObject({ layer: 'provider', retryable: false })
    const text = got.semantic.filter((event) => event.type === 'text_delta').map((event) => String(event.textSoFar)).at(-1) ?? ''
    expect(text).toContain('rejected')
    expect(got.semantic.find((event) => event.type === 'turn_stopped')?.stopReason).toBe('error')
  })

  it('S12: history rewritten by compression becomes a new generation, not 120 duplicate rows', () => {
    const entries = latestRecording('S12')
    const resume = inboundFrames(entries).map((frame) => frame.result as Record<string, unknown> | undefined).filter((result) => result && typeof result.resumed === 'string').at(-1) as unknown as SessionResumeResult
    const [created] = createdSessions(entries)
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(created.result, 'snapshot')
    // Before compression the client held rows 1..120 (row ids as written).
    reconciler.applyHistory(Array.from({ length: 120 }, (_, index) => ({ role: index % 2 ? 'assistant' : 'user', text: 'x', row_id: index + 1 })))
    const before = got.committed.length
    reconciler.applyHistory(resume.messages)
    expect(got.resets.at(-1)?.reason).toBe('rewrite')
    // The rewrite replaced the generation: exactly the resumed rows, once.
    expect(got.committed.length - before).toBe(resume.messages.length)
  })

  it('reports an event type outside the catalog once, by name', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applyEvent({ type: 'brand.new.event', session_id: '' })
    reconciler.applyEvent({ type: 'brand.new.event', session_id: '' })
    expect(got.unknown).toEqual(['brand.new.event'])
  })
})

describe('reconciler guards (survivors from package review A)', () => {
  const s9 = () => {
    const entries = latestRecording('S9')
    const running = inboundFrames(entries).map((frame) => frame.result as Record<string, unknown> | undefined)
      .find((result) => result?.running === true) as unknown as SessionResumeResult
    return running
  }

  it('ignores events for another runtime session on the same connection', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(s9(), 'snapshot', { seedInflight: false })
    const before = got.semantic.length
    reconciler.applyEvent({ type: 'message.delta', session_id: 'someone-else', seq: 1, payload: { text: 'not ours' } })
    expect(got.semantic.length).toBe(before)
  })

  it('commits a row once per generation, however often history repeats it', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    const [created] = createdSessions(latestRecording('S3'))
    reconciler.applySnapshot(created.result, 'snapshot')
    const rows = oracleRows(latestRecording('S3'), created.stored, created.sid)!.rows
    reconciler.applyHistory(rows)
    reconciler.applyHistory(rows)
    expect(got.committed.map((row) => row.key)).toEqual(rows.map((row) => `row:${row.row_id}`))
  })

  it('seeds the running turn from the snapshot when the ring lost its start (recorded S9 running resume)', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    const running = s9()
    reconciler.applySnapshot(running, 'snapshot')
    expect(reconciler.turnInFlight).toBe(true)
    const text = got.semantic.filter((event) => event.type === 'text_delta').map((event) => String(event.textSoFar)).at(-1) ?? ''
    expect(text.length).toBeGreaterThan(0)
    expect(running.inflight?.assistant?.trim().startsWith(text.trim().slice(0, 10))).toBe(true)
  })

  it('closes every open card when a snapshot supersedes the conversation', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(s9(), 'snapshot', { seedInflight: false })
    reconciler.openRequest({ id: 'srq-000000000002', method: 'approval', params: {}, replayed: false })
    reconciler.applySnapshot(s9(), 'snapshot', { seedInflight: false })
    expect(got.closed).toEqual([{ id: 'srq-000000000002', reason: 'superseded' }])
  })

  it('follows session.info to a new stored id (compression rotates the tip)', () => {
    const sessions: string[] = []
    const { out } = collector()
    const reconciler = new ConversationReconciler({ ...out, session: (state) => sessions.push(state.storedSessionId) })
    const running = s9()
    reconciler.applySnapshot(running, 'snapshot', { seedInflight: false })
    reconciler.applyEvent({ type: 'session.info', session_id: running.session_id, payload: { stored_session_id: 'tip-after-compression' } })
    expect(reconciler.currentStoredSessionId).toBe('tip-after-compression')
    expect(sessions.at(-1)).toBe('tip-after-compression')
  })
})
