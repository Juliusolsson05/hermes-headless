// The reconciler's oracle: a LIVE fold of a recorded turn must describe the
// same conversation as the rows Hermes returns when that conversation is
// RESUMED later in the same recording (decomposition H6).
//
// Fixtures: research/corpus (recordings of a real `hermes serve` v0.21.7).
// Nothing here is invented: the live side is the event stream the recorder
// saw, the oracle is the server's own resume/history projection.

import { describe, expect, it } from 'vitest'

import { ConversationReconciler, type CommittedRow, type ReconcilerOutput, type SemanticEvent, type HermesNotice, type HermesSideAnswer } from './ConversationReconciler.js'
import { classifyFrame, envelopeFromReplay } from '../contract/frames.js'
import type { EventEnvelope, OpenRequestEntry, SessionResumeResult, TranscriptMessage } from '../contract/wire.js'
import { corpusScenarios, inboundFrames, latestRecording, recordingFor, type CorpusEntry } from '../testing/corpus.js'

type Collected = {
  semantic: SemanticEvent[]
  committed: CommittedRow[]
  resets: Array<{ generation: number; reason: string }>
  opened: Array<OpenRequestEntry & { replayed: boolean }>
  closed: Array<{ id: string; reason: string }>
  settled: number
  reclaimed: string[]
  unknown: string[]
  notices: HermesNotice[]
  sideAnswers: HermesSideAnswer[]
}

function collector(): { out: ReconcilerOutput; got: Collected } {
  const got: Collected = { semantic: [], committed: [], resets: [], opened: [], closed: [], settled: 0, reclaimed: [], unknown: [], notices: [], sideAnswers: [] }
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
    notice: (notice) => got.notices.push(notice),
    noticeCleared: () => undefined,
    sideAnswer: (answer) => got.sideAnswers.push(answer),
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

describe('durable row identity and tool fallbacks (package review B #3, recorded S4)', () => {
  it('keys user/assistant rows by row_id and tool rows by tool_call_id, as projected', () => {
    const entries = latestRecording('S4')
    const [created] = createdSessions(entries)
    const resumed = inboundFrames(entries).map((frame) => frame.result as Record<string, unknown> | undefined)
      .filter((result) => Array.isArray(result?.messages) && (result!.messages as unknown[]).length > 0).at(-1) as unknown as SessionResumeResult
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(created.result, 'snapshot')
    reconciler.applySnapshot(resumed, 'snapshot', { seedInflight: false })
    expect(got.committed.map((row) => row.key)).toEqual(['row:1', 'tool:call_tools_0_0', 'tool:call_tools_0_1', 'tool:call_tools_1_0', 'row:7'])
  })

  it('opens a chip for a tool.complete whose tool.start it never saw (resume mid-tool)', () => {
    const entries = latestRecording('S4')
    const [session] = createdSessions(entries)
    const complete = sessionEvents(entries, session.sid).find((event) => event.type === 'tool.complete')!
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(session.result, 'snapshot')
    reconciler.applyEvent({ type: 'message.start', session_id: session.sid, seq: 1 })
    reconciler.applyEvent(complete)
    const toolId = (complete.payload as { tool_id: string }).tool_id
    expect(got.semantic.find((event) => event.type === 'block_started' && event.toolUseId === toolId)).toBeDefined()
    expect(got.semantic.find((event) => event.type === 'tool_result' && event.toolUseId === toolId)).toBeDefined()
  })

  it('seals a text segment with block_completed when a tool interrupts it', () => {
    const entries = latestRecording('S6')
    const [session] = createdSessions(entries)
    const got = foldSession(entries, session)
    const textBlocks = new Set(got.semantic.filter((event) => event.type === 'block_started' && event.kind === 'text').map((event) => event.blockIndex))
    const sealed = new Set(got.semantic.filter((event) => event.type === 'block_completed' && event.kind === 'text').map((event) => event.blockIndex))
    expect([...textBlocks].every((index) => sealed.has(index))).toBe(true)
  })

  it('treats `success: false` as a failed tool (justified literal: the documented result flag, 0 occurrences in the corpus)', () => {
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applyEvent({ type: 'message.start', seq: 1 })
    reconciler.applyEvent({ type: 'tool.start', seq: 2, payload: { tool_id: 't1', name: 'web_search', args: {} } })
    reconciler.applyEvent({ type: 'tool.complete', seq: 3, payload: { tool_id: 't1', name: 'web_search', result: { success: false } } })
    expect(got.semantic.find((event) => event.type === 'tool_result')?.isError).toBe(true)
  })
})

describe('reconciler guards (round-1 review c2 survivors)', () => {
  const runtime = (scenario: string) => {
    const frames = inboundFrames(latestRecording(scenario))
    const created = frames.map((frame) => frame.result as Record<string, unknown> | undefined)
      .find((result) => result && typeof result.stored_session_id === 'string' && result.message_count === 0)! as unknown as SessionResumeResult
    return { created, events: sessionEvents(latestRecording(scenario), created.session_id) }
  }

  it('M39: a terminal command that exited non-zero is a failed tool, even with no error text', () => {
    // Mechanical mutation of recorded S4 (terminal result {output, exit_code: 0, error: null}).
    const { created, events } = runtime('S4')
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(created, 'snapshot')
    for (const event of events) {
      const payload = event.payload as { tool_id?: string; result?: Record<string, unknown> } | undefined
      if (event.type === 'tool.complete' && payload?.tool_id === 'call_tools_0_0') {
        reconciler.applyEvent({ ...event, payload: { ...payload, result: { ...payload.result, exit_code: 2 } } })
      } else reconciler.applyEvent(event)
    }
    const result = got.semantic.find((event) => event.type === 'tool_result' && (event as { toolUseId?: string }).toolUseId === 'call_tools_0_0') as { isError?: boolean }
    expect(result.isError).toBe(true)
  })

  it('M40: a new turn over one whose completion never arrived closes the old one as stopped', () => {
    // Mechanical mutation of recorded S3: drop the first turn's message.complete.
    const { created, events } = runtime('S3')
    const firstComplete = events.findIndex((event) => event.type === 'message.complete')
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(created, 'snapshot')
    for (const [index, event] of events.entries()) if (index !== firstComplete) reconciler.applyEvent(event)
    expect(got.semantic.some((event) => event.type === 'turn_stopped' && (event as { stopReason?: string }).stopReason === 'superseded')).toBe(true)
  })

  it('commits a tool row with the result observed live, since Hermes\' history drops it (round-2 x1 #1)', () => {
    // Recorded S4: three tool calls, the second a failed read. The resume
    // projection's tool rows carry no output; the live tool.complete did.
    const { created, events } = runtime('S4')
    const resumed = inboundFrames(latestRecording('S4')).map((frame) => frame.result as { messages?: TranscriptMessage[] } | undefined)
      .filter((result) => Array.isArray(result?.messages) && result!.messages!.length > 0).at(-1)!.messages!
    expect(resumed.filter((row) => row.role === 'tool').every((row) => row.content === undefined || row.content === null)).toBe(true)
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(created, 'snapshot')
    for (const event of events) reconciler.applyEvent(event)
    reconciler.applyHistory(resumed)
    const tools = got.committed.filter((row) => row.row.role === 'tool').map((row) => row.row as TranscriptMessage & { tool_error?: boolean })
    expect(tools.map((row) => [row.tool_call_id, row.tool_error === true])).toEqual([
      ['call_tools_0_0', false], ['call_tools_0_1', true], ['call_tools_1_0', false],
    ])
    expect(tools.every((row) => typeof row.content === 'string' && row.content.length > 0)).toBe(true)
  })
})


// The owner's debug bundle (2026-10-14T16-26-49): after a pane reload, a
// running turn went silent for the rest of the turn. Two guards.
describe('history resets during a running turn (bundle 2026-10-14T16-26-49)', () => {
  it('does not call a narrower history window a rewrite (recorded S12 resume, its tail as the listing)', () => {
    // Pinned to v0.21.7: there /compress was refused (4009 busy), so the
    // resume holds the FULL 120 rows; on v0.22.0 the compression succeeds
    // and the resume is already compacted (Stage 0 report), which is a
    // different fixture, not a narrower window.
    const entries = latestRecording('S12', { release: 'v0.21.7' })
    const resume = inboundFrames(entries).map((frame) => frame.result as Record<string, unknown> | undefined).filter((result) => result && typeof result.resumed === 'string').at(-1) as unknown as SessionResumeResult
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(resume, 'snapshot')
    const resets = got.resets.length
    // Mechanical mutation of the recording: the same rows, minus the oldest
    // half, as a listing that covers a narrower window would return them.
    reconciler.applyHistory(resume.messages.slice(Math.floor(resume.messages.length / 2)))
    expect(got.resets.length).toBe(resets)
  })

  it('re-announces the running turn after a real rewrite, under the same turn id (recorded S4)', () => {
    const entries = latestRecording('S4')
    const [session] = createdSessions(entries)
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot(session.result, 'snapshot')
    // Fold the recorded turn up to its first tool result, i.e. mid-turn.
    const events = sessionEvents(entries, session.sid)
    const firstResult = events.findIndex((event) => event.type === 'tool.complete')
    for (const event of events.slice(0, firstResult + 1)) reconciler.applyEvent(event)
    const turnId = got.semantic.find((event) => event.type === 'turn_started')!.turnId
    const row = (id: number) => ({ role: 'user', text: 'x', row_id: id })
    reconciler.applyHistory([row(1), row(2), row(3)])
    reconciler.applyHistory([row(1), row(3), row(4)]) // row 2 vanished inside the span: a rewrite (the S12 shape)
    expect(got.resets.at(-1)?.reason).toBe('rewrite')
    // The recorded turn started once; the reset must announce it a second time.
    expect(got.semantic.filter((event) => event.type === 'turn_started')).toHaveLength(2)
    const starts = got.semantic.map((event, index) => (event.type === 'turn_started' ? index : -1)).filter((index) => index >= 0)
    const afterReset = got.semantic.slice(starts.at(-1))
    expect(afterReset[0]).toMatchObject({ type: 'turn_started', turnId })
    const toolId = (events[firstResult]!.payload as { tool_id: string }).tool_id
    expect(afterReset.find((event) => event.type === 'tool_result' && event.toolUseId === toolId)).toBeDefined()
  })
})

describe('tool output across a client rebuild (bundle 2026-10-14T16-26-49: 187 of 206 chips lost theirs)', () => {
  it('a second client of the same conversation commits tool rows with the output the first saw live (recorded S4)', () => {
    const entries = latestRecording('S4')
    const [session] = createdSessions(entries)
    const cache = new Map<string, { content: string; isError: boolean }>()
    const first = new ConversationReconciler(collector().out, cache)
    first.applySnapshot(session.result, 'snapshot')
    for (const event of sessionEvents(entries, session.sid)) first.applyEvent(event)
    // Reload Agent: a NEW client resumes the conversation (recorded resume rows).
    const resumed = inboundFrames(entries).map((frame) => frame.result as Record<string, unknown> | undefined)
      .filter((result) => Array.isArray(result?.messages) && (result!.messages as unknown[]).length > 0).at(-1) as unknown as SessionResumeResult
    const { out, got } = collector()
    new ConversationReconciler(out, cache).applySnapshot(resumed, 'snapshot', { seedInflight: false })
    const tools = got.committed.filter((row) => row.row.role === 'tool')
    expect(tools).toHaveLength(3)
    expect(tools.every((row) => typeof row.row.content === 'string' && (row.row.content as string).length > 0)).toBe(true)
    expect(tools.map((row) => row.row.tool_error === true)).toEqual([false, true, false])
  })
})

// hermes-headless#2 review round 1: the span rule alone missed compression,
// where every new row id is ABOVE every known one (both reviewers, replayed).
describe('compression is a rewrite even when every new id is above the old ones', () => {
  function histories(scenario: string) {
    return inboundFrames(recordingFor(scenario, 'v0.22.0'))
      .map((frame) => frame.result as { messages?: TranscriptMessage[]; count?: number } | undefined)
      .filter((result): result is { messages: TranscriptMessage[] } => Array.isArray(result?.messages) && typeof (result as { count?: number }).count === 'number')
      .map((result) => result.messages)
  }
  const ids = (rows: TranscriptMessage[]) => rows.map((row) => row.row_id).filter((id): id is number => typeof id === 'number')

  it('S20: /compress from our own pane (recorded result; the pre-compress rows 1..62 are what its before_messages counted)', () => {
    const frames = inboundFrames(recordingFor('S20', 'v0.22.0'))
    const compressed = frames.map((frame) => frame.result as { before_messages?: number; messages?: TranscriptMessage[] } | undefined)
      .find((result) => (result?.before_messages ?? 0) > 2 && Array.isArray(result?.messages))!
    const { out, got } = collector()
    const reconciler = new ConversationReconciler(out)
    reconciler.applySnapshot({ session_id: 'rt', stored_session_id: 's', messages: [] } as never, 'snapshot')
    // Mechanical stand-in for the 62 rows the pane held: S20 never re-read
    // them, only their count (labelled, not invented content).
    reconciler.applyHistory(Array.from({ length: compressed.before_messages! }, (_, at) => ({ role: at % 2 ? 'assistant' : 'user', text: 'x', row_id: at + 1 })))
    const resetsBefore = got.resets.length
    reconciler.applyHistory(compressed.messages!)
    expect(got.resets.slice(resetsBefore).map((reset) => reset.reason)).toEqual(['rewrite'])
  })

  for (const scenario of ['T7b']) {
    it(`${scenario}: the post-compression history replaces the transcript instead of being appended`, () => {
      const all = histories(scenario)
      const index = all.findIndex((rows, at) => at > 0 && ids(rows).length > 0 && Math.min(...ids(rows)) > Math.max(...ids(all[at - 1]!)))
      expect(index).toBeGreaterThan(0)
      const { out, got } = collector()
      const reconciler = new ConversationReconciler(out)
      reconciler.applySnapshot({ session_id: 'rt', stored_session_id: 's', messages: [] } as never, 'snapshot')
      reconciler.applyHistory(all[index - 1]!)
      const resetsBefore = got.resets.length
      reconciler.applyHistory(all[index]!)
      expect(got.resets.slice(resetsBefore).map((reset) => reset.reason)).toEqual(['rewrite'])
    })
  }
})

describe('a shared tool-result cache keeps conversations apart (recorded S5: one call id, two conversations)', () => {
  it('a reload of conversation A gets A\'s output, not B\'s', () => {
    const entries = recordingFor('S5', 'v0.22.0')
    const frames = inboundFrames(entries)
    const completes = frames.filter((frame) => (frame.params as { type?: string } | undefined)?.type === 'tool.complete')
      .map((frame) => frame.params as { session_id: string; payload: { tool_id: string } })
    const shared = completes.find((a) => completes.some((b) => b.session_id !== a.session_id && b.payload.tool_id === a.payload.tool_id))!
    const other = completes.find((b) => b.session_id !== shared.session_id && b.payload.tool_id === shared.payload.tool_id)!
    const cache = new Map<string, { content: string; isError: boolean }>()
    const fold = (sid: string, stored: string) => {
      const reconciler = new ConversationReconciler(collector().out, cache)
      reconciler.applySnapshot({ session_id: sid, stored_session_id: stored, messages: [] } as never, 'snapshot')
      for (const frame of frames) {
        const params = frame.params as { type?: string; session_id?: string } | undefined
        if (frame.method === 'event' && params?.session_id === sid) reconciler.applyEvent(params as never)
      }
    }
    fold(shared.session_id, 'stored-A')
    fold(other.session_id, 'stored-B')
    // Reload A: a new client commits A's tool row from history.
    const { out, got } = collector()
    new ConversationReconciler(out, cache).applySnapshot({ session_id: 'rt2', stored_session_id: 'stored-A', messages: [{ role: 'tool', tool_call_id: shared.payload.tool_id, name: 'terminal' }] } as never, 'snapshot')
    const row = got.committed.find((entry) => entry.row.role === 'tool')!
    const aLive = completes.filter((complete) => complete.session_id === shared.session_id)
    expect(aLive.length).toBeGreaterThan(0)
    const bText = JSON.stringify((other as unknown as { payload: unknown }).payload)
    expect(bText.includes(String(row.row.content))).toBe(false)
  })
})
