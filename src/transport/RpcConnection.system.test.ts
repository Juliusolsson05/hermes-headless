// Exactly-once delivery across reconnects, on RECORDED frames.
//
// Fixtures: research/corpus/S8 (a real <20 s socket drop mid-turn, recording),
// S10 (an approval re-delivered through open_requests), S11 (backend restart,
// new epoch). The cut-point test is a mechanical mutation of S8: it replays the
// turn's real event sequence with the drop moved to every possible position and
// with 0-3 live frames racing the replay response.

import { describe, expect, it } from 'vitest'

import { RpcConnection, type ServerRequest } from './RpcConnection.js'
import type { EventEnvelope } from '../contract/wire.js'
import { FakeSocket, READY_FRAME } from '../testing/fakeSocket.js'
import { inboundFrames, latestRecording } from '../testing/corpus.js'

const CAPABILITIES = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }

async function openConnection(watermarks: Map<string, number>, epoch = 'epoch-1', canAnswer = (method: string) => ['approval', 'clarify', 'sudo', 'secret'].includes(method)) {
  const socket = new FakeSocket()
  const opening = RpcConnection.open(socket, { canAnswer, watermarks, heartbeatIntervalMs: 60_000 })
  socket.push(READY_FRAME(epoch))
  await socket.answerCall('client.capabilities', CAPABILITIES)
  const connection = await opening
  const events: EventEnvelope[] = []
  const requests: ServerRequest[] = []
  connection.on('event', (event: EventEnvelope) => events.push(event))
  connection.on('request', (request: ServerRequest) => requests.push(request))
  return { socket, connection, events, requests }
}

/** Every seq'd event of the S8 session, from all three recorded sources, in seq order. */
function s8TurnEvents(): { sid: string; events: Array<Record<string, unknown>> } {
  const entries = latestRecording('S8')
  const bySeq = new Map<number, Record<string, unknown>>()
  let sid = ''
  for (const frame of inboundFrames(entries)) {
    if (frame.method === 'event') {
      const params = frame.params as Record<string, unknown>
      if (typeof params.seq === 'number' && typeof params.session_id === 'string') {
        sid = params.session_id
        bySeq.set(params.seq, params)
      }
    }
    const result = frame.result as { events?: Array<Record<string, unknown>> } | undefined
    for (const event of result?.events ?? []) {
      if (typeof event.seq === 'number') bySeq.set(event.seq, event)
    }
  }
  return { sid, events: [...bySeq.entries()].sort((a, b) => a[0] - b[0]).map(([, event]) => event) }
}

const eventFrame = (params: Record<string, unknown>) => ({ jsonrpc: '2.0', method: 'event', params })

describe('replay across a dropped socket (recorded S8)', () => {
  it('recorded the whole turn with no seq gap (the oracle for the cut-point test)', () => {
    const { events } = s8TurnEvents()
    const seqs = events.map((event) => event.seq as number)
    expect(seqs.length).toBeGreaterThan(30)
    for (let index = 1; index < seqs.length; index += 1) expect(seqs[index]).toBe(seqs[index - 1] + 1)
    // session.title / session.info follow message.complete (recorded), so the
    // completion is inside the turn's events, not necessarily last.
    expect(events.some((event) => event.type === 'message.complete')).toBe(true)
  })

  it('delivers every event exactly once in seq order for every cut point, gap and race', async () => {
    // Model of the real race: when the client reconnects, the server's ring
    // holds the gap (cut .. cut+gapLength); the turn keeps streaming, so
    // frames NEWER than the gap can arrive on the new socket before the
    // replay reply does. Those are the dangerous ones: dispatched first, they
    // would move the watermark past the whole gap.
    const { sid, events } = s8TurnEvents()
    for (let cut = 0; cut < events.length; cut += 1) {
      for (const gapLength of [0, 1, 4]) {
        for (const race of [0, 1, 3]) {
          const gap = events.slice(cut, cut + gapLength)
          const newer = events.slice(cut + gapLength, cut + gapLength + race)
          const rest = events.slice(cut + gapLength + race)
          const watermarks = new Map<string, number>()
          const first = await openConnection(watermarks)
          for (const event of events.slice(0, cut)) first.socket.push(eventFrame(event))
          first.socket.close(4000, 'drop')

          const second = await openConnection(watermarks)
          const replaying = second.connection.replay(sid, 'epoch-1')
          const call = await second.socket.nextSent((frame) => frame.method === 'session.events.since')
          for (const event of newer) second.socket.push(eventFrame(event))
          second.socket.push({
            jsonrpc: '2.0',
            id: call.id,
            result: { events: gap, latest_seq: gap.at(-1)?.seq ?? 0, truncated: false, count: gap.length, epoch: 'epoch-1', open_requests: [] },
          })
          await replaying
          for (const event of rest) second.socket.push(eventFrame(event))
          const delivered = [...first.events, ...second.events].map((event) => event.seq)
          expect({ cut, gapLength, race, delivered }).toEqual({ cut, gapLength, race, delivered: events.map((event) => event.seq) })
        }
      }
    }
  })

  it('without the replay hold, a racing live frame would win and the gap would be lost (guards the hold)', async () => {
    // Direct check of the property the hold protects: a live frame newer than
    // the gap, dispatched first, would move the watermark past the gap.
    const { sid, events } = s8TurnEvents()
    const watermarks = new Map<string, number>([[sid, events[5].seq as number]])
    const second = await openConnection(watermarks)
    const replaying = second.connection.replay(sid, 'epoch-1')
    const call = await second.socket.nextSent((frame) => frame.method === 'session.events.since')
    second.socket.push(eventFrame(events[10]))
    expect(second.events).toEqual([])
    second.socket.push({ jsonrpc: '2.0', id: call.id, result: { events: events.slice(6, 11), latest_seq: events[10].seq, truncated: false, count: 5, epoch: 'epoch-1', open_requests: [] } })
    await replaying
    expect(second.events.map((event) => event.seq)).toEqual(events.slice(6, 11).map((event) => event.seq))
  })
})

describe('backend restart (recorded S11)', () => {
  it('reports epoch-changed instead of "nothing missed" when the backend process changed', async () => {
    const entries = latestRecording('S11')
    const since = inboundFrames(entries).find((frame) => {
      const result = frame.result as { epoch?: string; events?: unknown[] } | undefined
      return result?.epoch !== undefined && Array.isArray(result?.events)
    })!.result as Record<string, unknown>
    expect(since.events).toEqual([])
    const watermarks = new Map([['7a2de5b8', 13]])
    const { socket, connection } = await openConnection(watermarks, String(since.epoch))
    const outcome = connection.replay('7a2de5b8', 'epoch-of-the-old-process')
    await socket.answerCall('session.events.since', since)
    expect(await outcome).toEqual({ kind: 'epoch-changed', epoch: since.epoch })
    expect(watermarks.has('7a2de5b8')).toBe(false)
  })
})

describe('server requests', () => {
  it('re-delivers the open approval from the recorded S10 replay and answers it through request.answer', async () => {
    const entries = latestRecording('S10')
    const since = inboundFrames(entries).find((frame) => Array.isArray((frame.result as { open_requests?: unknown[] } | undefined)?.open_requests))!.result as Record<string, unknown>
    const open = since.open_requests as Array<{ id: string; method: string }>
    expect(open.map((request) => request.method)).toEqual(['approval'])
    const sid = String((open[0] as { params?: { session_id?: string } }).params?.session_id ?? 'x')
    const { socket, connection, requests } = await openConnection(new Map([[sid, 8]]))
    const outcome = connection.replay(sid, 'epoch-1')
    await socket.answerCall('session.events.since', { ...since, epoch: 'epoch-1' })
    await outcome
    expect(requests).toHaveLength(1)
    expect(requests[0]).toMatchObject({ id: open[0].id, method: 'approval', replayed: true })
    const answering = connection.answer(open[0].id, { choice: 'once' })
    const call = await socket.answerCall('request.answer', { status: 'ok' })
    await answering
    expect(call.params).toEqual({ id: open[0].id, result: { choice: 'once' } })
  })

  it('answers a live request with a plain response carrying its id', async () => {
    const entries = latestRecording('S5')
    const live = inboundFrames(entries).find((frame) => frame.method === 'approval')!
    const { socket, connection, requests } = await openConnection(new Map())
    socket.push(live)
    expect(requests[0]).toMatchObject({ id: live.id, replayed: false })
    await connection.answer(String(live.id), { choice: 'deny' })
    expect(socket.sent.at(-1)).toEqual({ jsonrpc: '2.0', id: live.id, result: { choice: 'deny' } })
  })

  it('declines a request no consumer can answer at once instead of letting the agent wait (H11)', async () => {
    // Contract probe: `preview.read` is a Desktop GUI bridge (server_requests.py)
    // that never occurred in the corpus. Its frame shape is the S5 approval's.
    const { socket, requests } = await openConnection(new Map())
    socket.push({ jsonrpc: '2.0', id: 'srq-000000000001', method: 'preview.read', params: { session_id: 'abc' } })
    expect(requests).toEqual([])
    expect(socket.sent.at(-1)).toMatchObject({ id: 'srq-000000000001', error: { code: -32601 } })
  })

  it('surfaces a request only once even when the live frame and open_requests both carry it', async () => {
    const entries = latestRecording('S5')
    const live = inboundFrames(entries).find((frame) => frame.method === 'approval')!
    const { socket, connection, requests } = await openConnection(new Map())
    socket.push(live)
    connection.surfaceOpenRequests([{ id: String(live.id), method: 'approval', params: live.params as Record<string, unknown> }])
    expect(requests).toHaveLength(1)
  })
})

describe('closing', () => {
  it('fails outstanding calls with ConnectionClosedError (effect unknown) when the socket drops', async () => {
    const { socket, connection } = await openConnection(new Map())
    const pending = connection.call('prompt.submit', { session_id: 'x', text: 'hi' })
    socket.close(1006, '')
    await expect(pending).rejects.toMatchObject({ name: 'ConnectionClosedError', method: 'prompt.submit', code: 1006 })
  })
})

describe('heartbeat (package review B survivor)', () => {
  it('closes a socket that has been silent past the deadline, and keeps one that streams', async () => {
    // A fake clock with real clearTimer semantics: the ready timeout and
    // per-call timeouts must be cancellable, or they fire and close the socket.
    const timers = new Map<number, { fn: () => void; at: number }>()
    let nextHandle = 1
    let now = 0
    const realNow = Date.now
    Date.now = () => now
    try {
      const socket = new FakeSocket()
      const opening = RpcConnection.open(socket, {
        canAnswer: () => true,
        watermarks: new Map(),
        heartbeatIntervalMs: 15_000,
        heartbeatDeadlineMs: 45_000,
        setTimer: (fn, ms) => {
          const handle = nextHandle++
          timers.set(handle, { fn, at: now + ms })
          return handle
        },
        clearTimer: (handle) => {
          timers.delete(handle as number)
        },
      })
      socket.push(READY_FRAME())
      await socket.answerCall('client.capabilities', CAPABILITIES)
      const connection = await opening
      const advance = (ms: number) => {
        now += ms
        for (const [handle, timer] of [...timers.entries()].filter(([, entry]) => entry.at <= now)) {
          timers.delete(handle)
          timer.fn()
        }
      }
      advance(15_000)
      advance(15_000)
      // Any inbound frame is liveness (Hermes' own client): a streaming turn
      // keeps the socket alive even without pong replies.
      socket.push({ jsonrpc: '2.0', method: 'event', params: { type: 'sessions.changed', payload: {} } })
      advance(15_000)
      expect(connection.isOpen).toBe(true)
      advance(15_000)
      advance(15_000)
      advance(15_000)
      expect(connection.isOpen).toBe(false)
    } finally {
      Date.now = realNow
    }
  })
})
