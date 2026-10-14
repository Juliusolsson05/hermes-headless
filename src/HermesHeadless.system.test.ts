// Facade lifecycle on RECORDED frames, with races moved into the windows the
// package review (A, 2026-10-13) showed were unguarded. Each test names the
// finding it pins. Fixtures: research/corpus S3/S8/S9/S10 (recordings); a
// "moved" frame is a mechanical mutation of a recording (same frame, earlier
// arrival), never an invented one.

import { describe, expect, it } from 'vitest'

import { HermesHeadless, type HermesRequest } from './HermesHeadless.js'
import type { SemanticEvent } from './reconcile/ConversationReconciler.js'
import { FakeSocket, READY_FRAME } from './testing/fakeSocket.js'
import { inboundFrames, latestRecording } from './testing/corpus.js'

const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }
const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

type Harness = {
  headless: HermesHeadless
  sockets: FakeSocket[]
  semantic: SemanticEvent[]
  requests: HermesRequest[]
  closed: string[]
  nextSocket: () => Promise<FakeSocket>
}

/** A headless whose dial hands out FakeSockets the test plays the server on. */
function harness(target: ConstructorParameters<typeof HermesHeadless>[0]['target'], dialGate?: Promise<void>): Harness {
  const sockets: FakeSocket[] = []
  const waiters: Array<(socket: FakeSocket) => void> = []
  const headless = new HermesHeadless({
    target,
    heartbeatIntervalMs: 600_000,
    dial: async () => {
      if (dialGate) await dialGate
      const socket = new FakeSocket()
      sockets.push(socket)
      waiters.shift()?.(socket)
      return socket
    },
  })
  const h: Harness = {
    headless,
    sockets,
    semantic: [],
    requests: [],
    closed: [],
    nextSocket: () => new Promise((resolve) => {
      const ready = sockets.find((socket) => !(socket as unknown as { taken?: boolean }).taken)
      if (ready) {
        ;(ready as unknown as { taken: boolean }).taken = true
        resolve(ready)
      } else {
        waiters.push((socket) => {
          ;(socket as unknown as { taken: boolean }).taken = true
          resolve(socket)
        })
      }
    }),
  }
  headless.semantic.on('event', (event: SemanticEvent) => h.semantic.push(event))
  headless.on('request', (request: HermesRequest) => h.requests.push(request))
  headless.on('request-closed', ({ id }: { id: string }) => h.closed.push(id))
  return h
}

/** Server side of the handshake on one socket. */
async function handshake(socket: FakeSocket, epoch = 'epoch-1'): Promise<void> {
  socket.push(READY_FRAME(epoch))
  await socket.answerCall('client.capabilities', CAPS)
}

const eventFrame = (params: Record<string, unknown>) => ({ jsonrpc: '2.0', method: 'event', params })
const ring = (events: Array<Record<string, unknown>>, extra: Record<string, unknown> = {}) => ({
  events,
  latest_seq: events.at(-1)?.seq ?? 0,
  truncated: false,
  count: events.length,
  epoch: 'epoch-1',
  open_requests: [],
  ...extra,
})

/** All seq'd events of one session across every source in a recording, in seq order. */
function sessionEvents(scenario: string): { sid: string; events: Array<Record<string, unknown>>; frames: Array<Record<string, unknown>> } {
  const frames = inboundFrames(latestRecording(scenario))
  const bySeq = new Map<number, Record<string, unknown>>()
  let sid = ''
  for (const frame of frames) {
    const params = frame.params as Record<string, unknown> | undefined
    if (frame.method === 'event' && typeof params?.seq === 'number' && typeof params.session_id === 'string') {
      sid = params.session_id
      bySeq.set(params.seq, params)
    }
    for (const event of ((frame.result as { events?: Array<Record<string, unknown>> } | undefined)?.events ?? [])) {
      if (typeof event.seq === 'number') bySeq.set(event.seq, event)
    }
  }
  return { sid, events: [...bySeq.entries()].sort((a, b) => a[0] - b[0]).map(([, event]) => event), frames }
}

function firstResult(frames: Array<Record<string, unknown>>, match: (result: Record<string, unknown>) => boolean): Record<string, unknown> {
  const found = frames.map((frame) => frame.result as Record<string, unknown> | undefined).find((result) => result && match(result))
  if (!found) throw new Error('recorded result not found')
  return found
}

const finalText = (h: Harness) => h.semantic.filter((event) => event.type === 'text_delta').map((event) => String(event.textSoFar)).at(-1) ?? ''
const allChunks = Array.from({ length: 40 }, (_, index) => `chunk-${String(index + 1).padStart(2, '0')}`).join(' ')

describe('reconnect re-attaches under the acquisition hold (review A #1, recorded S8)', () => {
  it('keeps a delta that arrives between activate and the ring read', async () => {
    const { sid, events, frames } = sessionEvents('S8')
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const first = await h.nextSocket()
    await handshake(first)
    await first.answerCall('session.create', created)
    await first.answerCall('session.events.since', ring([]))
    await starting
    const cut = events.findIndex((event) => event.seq === 9)
    for (const event of events.slice(0, cut + 1)) first.push(eventFrame(event))
    first.close(4000, 'drop')

    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    await handshake(second)
    await second.answerCall('session.activate', created)
    const since = await second.nextSent((frame) => frame.method === 'session.events.since')
    // Moved frame: the backend routes seq 18 to the socket the moment activate
    // rebinds it, before our ring read answers (the review's probe).
    const gapEnd = events.findIndex((event) => event.seq === 17)
    second.push(eventFrame(events[gapEnd + 1]))
    second.push({ jsonrpc: '2.0', id: since.id, result: ring(events.slice(cut + 1, gapEnd + 1)) })
    await reconnecting
    for (const event of events.slice(gapEnd + 2)) second.push(eventFrame(event))
    await flush()
    expect(finalText(h).trim()).toBe(allChunks)
    expect(h.semantic.filter((event) => event.type === 'turn_completed')).toHaveLength(1)
  })
})

describe('resume during a running turn (review A #2, recorded S9)', () => {
  it('rebuilds the turn from the ring so a delta that beat the resume response is not lost', async () => {
    const { sid, events, frames } = sessionEvents('S9')
    const running = firstResult(frames, (result) => result.session_id === sid && result.running === true)
    const stored = String(running.stored_session_id ?? running.resumed)
    const h = harness({ mode: 'resume', storedSessionId: stored })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    const resume = await socket.nextSent((frame) => frame.method === 'session.resume')
    const pivot = events.findIndex((event) => event.seq === 30)
    // Moved frame: seq 30 arrives before the resume response.
    socket.push(eventFrame(events[pivot]))
    socket.push({ jsonrpc: '2.0', id: resume.id, result: running })
    await socket.answerCall('session.events.since', ring(events.slice(0, pivot)))
    await starting
    for (const event of events.slice(pivot + 1)) socket.push(eventFrame(event))
    await flush()
    expect(finalText(h).trim()).toBe(allChunks)
    expect(h.semantic.filter((event) => event.type === 'turn_started')).toHaveLength(1)
    expect(h.semantic.filter((event) => event.type === 'turn_completed')).toHaveLength(1)
  })
})

describe('open requests come from the ring, not the older snapshot (review A #3, recorded S10)', () => {
  it('does not resurrect an approval the ring no longer lists, and closes one cancelled while settling', async () => {
    const { sid, frames } = sessionEvents('S10')
    const approval = frames.find((frame) => frame.method === 'approval')!
    const snapshot: Record<string, unknown> = { ...firstResult(frames, (result) => result.session_id === sid && result.message_count === 0), running: true, open_requests: [{ id: approval.id, method: 'approval', params: approval.params }] }
    // Case 1: the snapshot lists the approval, the (newer) ring does not.
    const h = harness({ mode: 'resume', storedSessionId: String(snapshot.stored_session_id) })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.resume', snapshot)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    expect(h.requests).toEqual([])

    // Case 2: the ring lists it, and its cancel arrived while settling.
    const h2 = harness({ mode: 'resume', storedSessionId: String(snapshot.stored_session_id) })
    const starting2 = h2.headless.start()
    const socket2 = await h2.nextSocket()
    await handshake(socket2)
    await socket2.answerCall('session.resume', snapshot)
    const since = await socket2.nextSent((frame) => frame.method === 'session.events.since')
    socket2.push(eventFrame({ type: 'request.cancel', session_id: sid, seq: 900, payload: { id: approval.id, method: 'approval', reason: 'timeout' } }))
    socket2.push({ jsonrpc: '2.0', id: since.id, result: ring([], { latest_seq: 899, open_requests: [{ id: approval.id, method: 'approval', params: approval.params }] }) })
    await starting2
    expect(h2.requests.map((request) => request.id)).toEqual([approval.id])
    expect(h2.closed).toEqual([approval.id])
  })
})

describe('accepted-but-not-started prompts (review A #4, recorded S8 timing)', () => {
  it('queues a second prompt sent after the first was accepted but before message.start', async () => {
    const { sid, frames } = sessionEvents('S3')
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    const first = h.headless.prompt('one')
    await socket.answerCall('prompt.submit', { status: 'streaming', user_row_id: 1 })
    await first
    const second = h.headless.prompt('two')
    const call = await socket.nextSent((frame) => frame.method === 'prompt.submit' && (frame.params as { text?: string }).text === 'two')
    socket.push({ jsonrpc: '2.0', id: call.id, result: { status: 'queued' } })
    await second
    expect((call.params as { queued?: boolean }).queued).toBe(true)
  })
})

describe('session.reclaimed is a broadcast (review A #5)', () => {
  it('ignores another session being reclaimed and re-attaches when it is this one', async () => {
    const { sid, frames } = sessionEvents('S10')
    const approval = frames.find((frame) => frame.method === 'approval')!
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    socket.push(approval)
    expect(h.requests).toHaveLength(1)
    // Source-derived probe (zero occurrences in the corpus; shape from
    // session_lifecycle.py _announce_session_reclaimed): no outer session id.
    socket.push(eventFrame({ type: 'session.reclaimed', session_id: '', payload: { session_id: 'other-runtime', stored_session_id: 'other-stored', reason: 'ws_orphan_reap' } }))
    expect(h.closed).toEqual([])
    socket.push(eventFrame({ type: 'session.reclaimed', session_id: '', payload: { session_id: sid, stored_session_id: created.stored_session_id, reason: 'lru_evict' } }))
    expect(h.closed).toEqual([String(approval.id)])
    const resume = await socket.nextSent((frame) => frame.method === 'session.resume')
    expect((resume.params as { session_id: string }).session_id).toBe(created.stored_session_id)
  })
})

describe('stop() while a dial is pending (review A #6)', () => {
  it('closes the late socket and never creates a remote session', async () => {
    let releaseDial!: () => void
    const gate = new Promise<void>((resolve) => {
      releaseDial = resolve
    })
    const h = harness({ mode: 'create', source: 'agent-code' }, gate)
    const starting = h.headless.start().catch((error: Error) => error)
    await h.headless.stop()
    releaseDial()
    const outcome = await starting
    expect(outcome).toBeInstanceOf(Error)
    const socket = h.sockets[0]
    expect(socket.readyState).toBe(3)
    expect(socket.sent.some((frame) => frame.method === 'session.create')).toBe(false)
  })
})

describe('after a turn and on stop', () => {
  it('fetches history once the turn settles, and stop() closes the socket without session.close', async () => {
    const { sid, events, frames } = sessionEvents('S3')
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    const firstComplete = events.findIndex((event) => event.type === 'message.complete')
    for (const event of events.slice(0, firstComplete + 1)) socket.push(eventFrame(event))
    const history = await socket.nextSent((frame) => frame.method === 'session.history')
    expect((history.params as { session_id: string }).session_id).toBe(sid)
    await h.headless.stop()
    expect(socket.readyState).toBe(3)
    expect(socket.sent.some((frame) => frame.method === 'session.close')).toBe(false)
  })
})

/** A started create-mode harness on recorded S3, for the command tests below. */
async function started(): Promise<{ h: Harness; socket: FakeSocket; sid: string; created: Record<string, unknown>; events: Array<Record<string, unknown>>; frames: Array<Record<string, unknown>> }> {
  const { sid, events, frames } = sessionEvents('S3')
  const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
  const h = harness({ mode: 'create', source: 'agent-code' })
  const starting = h.headless.start()
  const socket = await h.nextSocket()
  await handshake(socket)
  await socket.answerCall('session.create', created)
  await socket.answerCall('session.events.since', ring([]))
  await starting
  return { h, socket, sid, created, events, frames }
}

describe('commands (package review B #2 survivors)', () => {
  it('falls back to a new conversation when the stored id is gone (4007), and says so', async () => {
    const { frames, sid } = sessionEvents('S3')
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    const h = harness({ mode: 'resume', storedSessionId: 'deleted-conversation' })
    const replaced: unknown[] = []
    h.headless.on('session-replaced', (event: unknown) => replaced.push(event))
    const boundaries: Array<{ type: string; reason?: string }> = []
    h.headless.committed.on('boundary', (boundary: { type: string; reason?: string }) => boundaries.push(boundary))
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    const resume = await socket.nextSent((frame) => frame.method === 'session.resume')
    socket.push({ jsonrpc: '2.0', id: resume.id, error: { code: 4007, message: 'session not found' } })
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    expect(replaced).toEqual([{ previousStoredSessionId: 'deleted-conversation', reason: 'not-found' }])
    expect(h.headless.storedSessionId).toBe(created.stored_session_id)
    // Reset as a different conversation, like reattach()'s fallback.
    expect(boundaries.filter((boundary) => boundary.type === 'reset').map((boundary) => boundary.reason)).toEqual(['new-conversation'])
  })

  it('/new closes the old runtime, creates a new one and resets history; a late history reply for the old one is dropped', async () => {
    const { h, socket, sid, events } = await started()
    const boundaries: Array<{ type: string; reason?: string }> = []
    const rows: string[] = []
    h.headless.committed.on('boundary', (boundary: { type: string; reason?: string }) => boundaries.push(boundary))
    h.headless.committed.on('entry', (row: { storedSessionId: string }) => rows.push(row.storedSessionId))
    const firstComplete = events.findIndex((event) => event.type === 'message.complete')
    for (const event of events.slice(0, firstComplete + 1)) socket.push(eventFrame(event))
    const history = await socket.nextSent((frame) => frame.method === 'session.history')
    const renewing = h.headless.runSlash('/new')
    await socket.answerCall('session.close', { closed: true, messages: [] })
    await socket.answerCall('session.create', { session_id: 'newrt001', stored_session_id: 'new-stored', message_count: 0, messages: [], info: {} })
    await socket.answerCall('session.events.since', ring([]))
    expect(await renewing).toEqual({ kind: 'new-conversation' })
    // The old conversation's history reply arrives only now (review B #1).
    socket.push({ jsonrpc: '2.0', id: history.id, result: { count: 2, messages: [{ role: 'user', text: 'hi', row_id: 1 }, { role: 'assistant', text: 'old', row_id: 2 }] } })
    await flush()
    expect(socket.sent.find((frame) => frame.method === 'session.close')?.params).toEqual({ session_id: sid })
    expect(boundaries.some((boundary) => boundary.type === 'reset' && boundary.reason === 'new-conversation')).toBe(true)
    expect(rows.filter((stored) => stored === 'new-stored')).toEqual([])
  })

  it('closes an answered card locally without waiting for the server', async () => {
    const { h, socket } = await started()
    const approval = inboundFrames(latestRecording('S5')).find((frame) => frame.method === 'approval')!
    socket.push(approval)
    await h.headless.answerRequest(String(approval.id), { choice: 'once' })
    expect(h.closed).toEqual([String(approval.id)])
  })

  it('routes a skill command through command.dispatch and submits its expansion (recorded S13)', async () => {
    const { h, socket } = await started()
    const s13 = inboundFrames(latestRecording('S13'))
    const dispatched = s13.map((frame) => frame.result as Record<string, unknown> | undefined).find((result) => result?.type === 'skill')!
    const running = h.headless.runSlash('/hh-secret')
    const exec = await socket.nextSent((frame) => frame.method === 'slash.exec')
    socket.push({ jsonrpc: '2.0', id: exec.id, error: { code: 4018, message: 'skill command: use command.dispatch for /hh-secret' } })
    await socket.answerCall('command.dispatch', dispatched)
    const submit = await socket.nextSent((frame) => frame.method === 'prompt.submit')
    socket.push({ jsonrpc: '2.0', id: submit.id, result: { status: 'streaming', user_row_id: 3 } })
    expect(await running).toMatchObject({ kind: 'prompt' })
    expect((submit.params as { text: string }).text).toBe(dispatched.message)
  })

  it('re-snapshots by stored id when the reconnect ring is truncated (mechanical mutation of S8)', async () => {
    const { h, socket, created } = await started()
    socket.close(4000, 'drop')
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    await handshake(second)
    await second.answerCall('session.activate', created)
    await second.answerCall('session.events.since', ring([], { truncated: true, latest_seq: 700 }))
    const resume = await second.nextSent((frame) => frame.method === 'session.resume')
    expect((resume.params as { session_id: string }).session_id).toBe(created.stored_session_id)
    second.push({ jsonrpc: '2.0', id: resume.id, result: created })
    await second.answerCall('session.events.since', ring([], { latest_seq: 700 }))
    await second.answerCall('session.history', { count: 0, messages: [] })
    await reconnecting
  })

  it('starts a new conversation when the stored one was deleted while away (4007 on reconnect)', async () => {
    // Round-1 review c1 #1: the conversation is deleted on the server (Hermes
    // Desktop or the CLI) while the socket is down. The resume answers 4007
    // exactly as at start(); before this, reconnect() threw it to the
    // consumer's backoff loop, which then redialled forever.
    const { h, socket, created } = await started()
    const replaced: unknown[] = []
    h.headless.on('session-replaced', (event: unknown) => replaced.push(event))
    const next = firstResult(sessionEvents('S4').frames, (result) => typeof result.stored_session_id === 'string' && result.message_count === 0)
    socket.close(4000, 'drop')
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    // A restarted server (new epoch, recorded S11 shape): straight to resume.
    await handshake(second, 'epoch-2')
    const resume = await second.nextSent((frame) => frame.method === 'session.resume')
    second.push({ jsonrpc: '2.0', id: resume.id, error: { code: 4007, message: 'session not found' } })
    await second.answerCall('session.create', next)
    await second.answerCall('session.events.since', ring([]))
    await reconnecting
    expect(replaced).toEqual([{ previousStoredSessionId: created.stored_session_id, reason: 'not-found' }])
    expect(h.headless.storedSessionId).toBe(next.stored_session_id)
  })
})

// Round-1 review c2 (2026-10-13): probes of findings 1-3 made permanent, and
// tests for the surviving mutants that guard real behaviour.
describe('round-1 review c2', () => {
  function s10() {
    const frames = inboundFrames(latestRecording('S10'))
    const approval = frames.find((frame) => frame.method === 'approval')!
    const sid = String((approval.params as { session_id: string }).session_id)
    const created = firstResult(frames, (result) => result.session_id === sid && result.message_count === 0)
    return { approval, sid, created, open: [{ id: approval.id, method: 'approval', params: approval.params }] }
  }

  it('#1 keeps an approval that arrives while a resume is attaching (no close, no flicker)', async () => {
    const { approval, created, open } = s10()
    const h = harness({ mode: 'resume', storedSessionId: String(created.stored_session_id) })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    const resume = await socket.nextSent((frame) => frame.method === 'session.resume')
    // Hermes routes the running turn's approval here the moment the resume rebinds.
    socket.push(approval)
    socket.push({ jsonrpc: '2.0', id: resume.id, result: { ...created, running: true } })
    await socket.answerCall('session.events.since', ring([], { latest_seq: 5, open_requests: open }))
    await starting
    expect(h.requests.map((request) => request.id)).toEqual([approval.id])
    expect(h.closed).toEqual([])
  })

  it('#1 keeps an open approval across a truncated reconnect (activate, then resume on the same socket)', async () => {
    const { approval, sid, created, open } = s10()
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    socket.push(eventFrame({ type: 'message.start', session_id: sid, seq: 1 }))
    socket.push(approval)
    socket.close(4000, 'drop')
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    await handshake(second)
    await second.answerCall('session.activate', created)
    await second.answerCall('session.events.since', ring([], { truncated: true, latest_seq: 900, open_requests: open }))
    await second.answerCall('session.resume', { ...created, running: true })
    await second.answerCall('session.events.since', ring([], { latest_seq: 900, open_requests: open }))
    await second.answerCall('session.history', { count: 0, messages: [] })
    await reconnecting
    // Closed as superseded by the resume snapshot, reopened from the ring:
    // opened once more than it was closed, so it is open at the end.
    const opened = h.requests.filter((request) => request.id === approval.id).length
    const closed = h.closed.filter((id) => id === approval.id).length
    expect(opened - closed).toBe(1)
  })

  it('#1 / M36: a card still open across an activate reconnect is not shown twice', async () => {
    const { approval, sid, created, open } = s10()
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.create', created)
    await socket.answerCall('session.events.since', ring([]))
    await starting
    socket.push(eventFrame({ type: 'message.start', session_id: sid, seq: 1 }))
    socket.push(approval)
    socket.close(4000, 'drop')
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    await handshake(second)
    await second.answerCall('session.activate', created)
    await second.answerCall('session.events.since', ring([], { latest_seq: 1, open_requests: open }))
    await second.answerCall('session.history', { count: 0, messages: [] })
    await reconnecting
    expect(h.requests.map((request) => request.id)).toEqual([approval.id])
  })

  it('#2 replacing its own socket is not reported as a disconnect', async () => {
    const { h, socket } = await started()
    const states: string[] = []
    h.headless.on('connection', (state: { kind: string }) => states.push(state.kind))
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    expect(socket.readyState).toBe(3)
    await handshake(second)
    await second.answerCall('session.activate', firstResult(sessionEvents('S3').frames, (result) => result.message_count === 0))
    await second.answerCall('session.events.since', ring([]))
    await second.answerCall('session.history', { count: 0, messages: [] })
    await reconnecting
    expect(states).toEqual(['connecting', 'ready'])
  })

  it('#3 settles a long turn whose start was evicted but which completed before the ring read (recorded S9)', async () => {
    const frames = inboundFrames(latestRecording('S9'))
    const running = firstResult(frames, (result) => result.running === true)
    const sid = String(running.session_id)
    const events = frames.filter((frame) => frame.method === 'event' && (frame.params as { session_id?: string }).session_id === sid).map((frame) => frame.params as Record<string, unknown>)
    const complete = events.find((event) => event.type === 'message.complete')!
    const lastDelta = events.filter((event) => event.type === 'message.delta').at(-1)!
    const h = harness({ mode: 'resume', storedSessionId: String(running.stored_session_id) })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.resume', running)
    // Mechanical mutation of S9: the ring lost the turn's start (> 512 events).
    await socket.answerCall('session.events.since', ring([lastDelta, complete], { truncated: true, latest_seq: complete.seq }))
    await starting
    expect(h.headless.turnInFlight).toBe(false)
    expect(h.semantic.some((event) => event.type === 'turn_completed')).toBe(true)
  })

  it('M27: declining sends each request\'s documented "no", never an approval', async () => {
    const { h, socket } = await started()
    const approval = inboundFrames(latestRecording('S5')).find((frame) => frame.method === 'approval')!
    socket.push(approval)
    await h.headless.declineRequest({ id: String(approval.id), method: 'approval' })
    expect(socket.sent.at(-1)).toEqual({ jsonrpc: '2.0', id: approval.id, result: { choice: 'deny' } })
    for (const [method, result] of [['clarify', {}], ['sudo', { value: '' }], ['secret', { value: '' }]] as const) {
      const frame = inboundFrames(latestRecording('S6')).find((entry) => entry.method === method)!
      socket.push(frame)
      await h.headless.declineRequest({ id: String(frame.id), method })
      expect(socket.sent.at(-1)).toEqual({ jsonrpc: '2.0', id: frame.id, result })
    }
  })

  it('M30: a TRANSIENT resume error fails the start; it never abandons the conversation for a new one', async () => {
    const h = harness({ mode: 'resume', storedSessionId: 'my-conversation' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    const resume = await socket.nextSent((frame) => frame.method === 'session.resume')
    socket.push({ jsonrpc: '2.0', id: resume.id, error: { code: 5036, message: 'database unavailable' } })
    await expect(starting).rejects.toThrow(/database unavailable/)
    expect(socket.sent.some((frame) => frame.method === 'session.create')).toBe(false)
  })

  it('M24: stop() during the handshake leaves no live connection', async () => {
    const h = harness({ mode: 'create', source: 'agent-code' })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    socket.push(READY_FRAME())
    await h.headless.stop()
    await socket.answerCall('client.capabilities', CAPS).catch(() => undefined)
    await expect(starting).rejects.toThrow()
    expect(socket.readyState).toBe(3)
    expect(h.headless.isConnected).toBe(false)
  })

  it('M18/M19: a steered or refused prompt does not leave a phantom turn in flight', async () => {
    const { h, socket } = await started()
    const steering = h.headless.prompt('fold this in')
    await socket.answerCall('prompt.submit', { status: 'steered' })
    await steering
    expect(h.headless.turnInFlight).toBe(false)
    const refusing = h.headless.prompt('not now')
    const submit = await socket.nextSent((frame) => frame.method === 'prompt.submit' && (frame.params as { text?: string }).text === 'not now')
    socket.push({ jsonrpc: '2.0', id: submit.id, error: { code: 4009, message: 'busy' } })
    await expect(refusing).rejects.toThrow()
    expect(h.headless.turnInFlight).toBe(false)
  })

  it('M20 / M29: a resumed running turn is in flight; an activate reconnect fetches the history it missed', async () => {
    const frames = inboundFrames(latestRecording('S9'))
    const running = firstResult(frames, (result) => result.running === true)
    const h = harness({ mode: 'resume', storedSessionId: String(running.stored_session_id) })
    const starting = h.headless.start()
    const socket = await h.nextSocket()
    await handshake(socket)
    await socket.answerCall('session.resume', running)
    await socket.answerCall('session.events.since', ring([], { latest_seq: 0 }))
    await starting
    expect(h.headless.turnInFlight).toBe(true)
    await socket.answerCall('session.history', { count: 0, messages: [] })
    socket.close(4000, 'drop')
    const reconnecting = h.headless.reconnect()
    const second = await h.nextSocket()
    await handshake(second)
    await second.answerCall('session.activate', running)
    await second.answerCall('session.events.since', ring([]))
    const history = await second.nextSent((frame) => frame.method === 'session.history')
    second.push({ jsonrpc: '2.0', id: history.id, result: { count: 0, messages: [] } })
    await reconnecting
  })
})

