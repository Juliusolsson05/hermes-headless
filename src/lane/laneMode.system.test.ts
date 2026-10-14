// HermesHeadless lane mode on RECORDED Telegram-lane traffic (Hermes v0.22.0
// with its real Telegram adapter, research/corpus T4/T5/T6). The server side
// is played from the recording: REST listings, resume and history results,
// and the lease status frames, in the order the recorded pane saw them.

import { describe, expect, it } from 'vitest'

import { HermesHeadless, type HermesLaneState, type LaneMove } from '../HermesHeadless.js'
import type { HermesRest, LatestDescendant, SessionListRow } from '../rest/HermesRest.js'
import { FakeSocket, READY_FRAME } from '../testing/fakeSocket.js'
import { recordingFor, type CorpusEntry } from '../testing/corpus.js'

const DM = 'agent:main:telegram:dm:111'
const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }
const flush = () => new Promise((resolve) => setTimeout(resolve, 0))
const RING = { events: [], latest_seq: 0, truncated: false, count: 0, epoch: 'epoch-1', open_requests: [] }

/** Inbound WS result of the recorded request `id`. */
function resultOf(entries: CorpusEntry[], id: string): Record<string, unknown> {
  const frame = entries.find((entry) => entry.dir === 'in' && entry.kind === 'ws' && entry.frame.id === id)!.frame
  return frame.result as Record<string, unknown>
}

function listings(entries: CorpusEntry[]): SessionListRow[][] {
  return entries
    .filter((entry) => entry.dir === 'in' && entry.kind === 'http' && /\/api\/sessions\?/.test(String(entry.frame.url)) && entry.frame.status === 200)
    .map((entry) => (entry.frame.body as { sessions: SessionListRow[] }).sessions)
}

/** A REST stand-in serving the recorded listing that `phase()` names. */
function recordedRest(listing: () => SessionListRow[]): HermesRest {
  return {
    listSessions: async () => ({ sessions: listing() }),
    latestDescendant: async (id: string): Promise<LatestDescendant> => ({ requested_session_id: id, session_id: id, path: [id], changed: false }),
    restartGateway: async () => ({ ok: true }),
  } as unknown as HermesRest
}

/** Answer every client call on `socket` with `answer(method, params)` (undefined = leave unanswered). */
function playServer(socket: FakeSocket, answer: (method: string, params: Record<string, unknown>) => unknown): void {
  const answered = new Set<string>()
  const loop = async () => {
    for (;;) {
      const call = await socket.nextSent((frame) => typeof frame.method === 'string' && frame.id !== undefined && !answered.has(String(frame.id)))
      answered.add(String(call.id))
      const result = answer(String(call.method), (call.params ?? {}) as Record<string, unknown>)
      if (result !== undefined) socket.push({ jsonrpc: '2.0', id: call.id, result })
    }
  }
  void loop()
}

async function waitFor(check: () => boolean, label: string): Promise<void> {
  for (let tick = 0; tick < 400 && !check(); tick += 1) await flush()
  if (!check()) throw new Error(`timed out waiting for ${label}`)
}

describe('Telegram lane mode (recorded T5: hold, then send)', () => {
  it('holds a pane prompt while the phone turn runs and sends it exactly once after the phone reply lands', async () => {
    const entries = recordingFor('T5', 'v0.22.0')
    const [listing] = listings(entries)
    // History as the recorded pane saw it: idle (r3), phone turn running
    // (r4: the phone's user row 4 is the tail), phone reply landed (r44).
    let history = resultOf(entries, 'r3')
    // The clock of the recording: the busy rule ages a tail by Hermes' own
    // row timestamp, so replaying a recording hours later must not make the
    // phone turn look long dead. 2 s after the phone's user row was written.
    const phoneRow = (resultOf(entries, 'r4').messages as Array<{ timestamp?: number }>).at(-1)!
    const now = () => phoneRow.timestamp! * 1000 + 2_000
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(() => listing!), dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000, now })
    const lanes: HermesLaneState[] = []
    headless.on('lane', (state: HermesLaneState) => lanes.push(state))
    playServer(socket, (method) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') return resultOf(entries, 'r2')
      if (method === 'session.events.since') return RING
      if (method === 'session.history') return history
      if (method === 'prompt.submit') return { status: 'streaming', user_row_id: 6 }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting

    history = resultOf(entries, 'r4')
    const outcome = await headless.prompt('Pane prompt during the phone turn.')
    expect(outcome.status).toBe('held')
    expect(lanes.at(-1)).toMatchObject({ busy: true, held: [{ text: 'Pane prompt during the phone turn.' }] })
    expect(socket.sent.some((frame) => frame.method === 'prompt.submit')).toBe(false)

    // The phone reply commits; Hermes broadcasts the change signal.
    history = resultOf(entries, 'r44')
    socket.push({ jsonrpc: '2.0', method: 'event', params: { type: 'sessions.changed', session_id: '', payload: {} } })
    await waitFor(() => socket.sent.some((frame) => frame.method === 'prompt.submit'), 'the held prompt to be sent')
    const submits = socket.sent.filter((frame) => frame.method === 'prompt.submit')
    expect(submits).toHaveLength(1)
    expect((submits[0]!.params as { text: string; queued: boolean })).toMatchObject({ text: 'Pane prompt during the phone turn.', queued: true })
    expect(lanes.at(-1)?.held).toEqual([])
    await headless.stop()
  })
})

describe('Telegram lane mode (recorded T6: Telegram /new)', () => {
  it('moves to the new conversation as a new conversation, and never prompts the ended one', async () => {
    const entries = recordingFor('T6', 'v0.22.0')
    const all = listings(entries)
    let listing = all[0]!
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(() => listing), dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000 })
    const moves: LaneMove[] = []
    headless.on('lane-moved', (move: LaneMove) => moves.push(move))
    const resumes: string[] = []
    playServer(socket, (method, params) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') {
        resumes.push(String(params.session_id))
        // r2: the original conversation; r12: the one Telegram /new created.
        return resultOf(entries, resumes.length === 1 ? 'r2' : 'r12')
      }
      if (method === 'session.events.since') return RING
      if (method === 'session.history') return resultOf(entries, resumes.length <= 1 ? 'r3' : 'r13')
      if (method === 'prompt.submit') return { status: 'streaming', user_row_id: 9 }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting
    const original = headless.storedSessionId

    // Telegram /new: the next listing names the new row.
    listing = all.at(-1)!
    await headless.prompt('After the reset.')
    expect(moves).toEqual([{ from: original, to: expect.not.stringMatching(original), reason: 'new-conversation' }])
    expect(headless.storedSessionId).toBe(moves[0]!.to)
    const submit = socket.sent.find((frame) => frame.method === 'prompt.submit')!
    // Submitted on the NEW conversation's runtime id, never the ended one's.
    expect((submit.params as { session_id: string }).session_id).toBe(String(resultOf(entries, 'r12').session_id))
    await headless.stop()
  })
})

describe('Telegram lane mode (recorded T4: our turn waits on the phone\'s lease)', () => {
  it('reports the lane busy from "Another Hermes process" until "Session is free"', async () => {
    const entries = recordingFor('T4', 'v0.22.0')
    const status = (text: RegExp) => entries.find((entry) => entry.dir === 'in' && entry.kind === 'ws' && text.test(JSON.stringify(entry.frame)) && (entry.frame.params as { type?: string } | undefined)?.type === 'status.update')!.frame
    const waiting = status(/Another Hermes process/)
    const free = status(/Session is free/)
    const sid = String((waiting.params as { session_id: string }).session_id)
    const [listing] = listings(entries)
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(() => listing!), dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000 })
    const lanes: HermesLaneState[] = []
    headless.on('lane', (state: HermesLaneState) => lanes.push(state))
    const resume = entries.find((entry) => entry.dir === 'in' && entry.kind === 'ws' && (entry.frame.result as { session_id?: string } | undefined)?.session_id === sid && Array.isArray((entry.frame.result as { messages?: unknown }).messages))!.frame.result
    playServer(socket, (method) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') return resume
      if (method === 'session.events.since') return RING
      // The tail is idle as far as rows go (the T4 blind spot): our own row.
      if (method === 'session.history') return { count: 0, messages: [] }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting
    socket.push(waiting)
    await waitFor(() => lanes.at(-1)?.busy === true, 'busy from the lease status')
    socket.push(free)
    await waitFor(() => lanes.at(-1)?.busy === false, 'idle after Session is free')
    await headless.stop()
  })
})

describe('a phone turn that died long ago (review: stale by the row\'s own age)', () => {
  it('does not hold a pane prompt behind a phone message from hours ago (recorded T5 tail, replayed 6 h later)', async () => {
    const entries = recordingFor('T5', 'v0.22.0')
    const [listing] = listings(entries)
    const phoneRow = (resultOf(entries, 'r4').messages as Array<{ timestamp?: number }>).at(-1)!
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(() => listing!), dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000, now: () => phoneRow.timestamp! * 1000 + 6 * 3_600_000 })
    playServer(socket, (method) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') return resultOf(entries, 'r2')
      if (method === 'session.events.since') return RING
      if (method === 'session.history') return resultOf(entries, 'r4')
      if (method === 'prompt.submit') return { status: 'streaming', user_row_id: 6 }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting
    expect((await headless.prompt('Hours later.')).status).toBe('streaming')
    await headless.stop()
  })
})

describe('fail closed (review: a lane prompt never goes out when the tip check failed)', () => {
  it('holds the prompt when the thread listing errors, instead of sending to a possibly ended conversation', async () => {
    const entries = recordingFor('T5', 'v0.22.0')
    const [listing] = listings(entries)
    let failing = false
    const rest = recordedRest(() => {
      if (failing) throw new Error('502 Bad Gateway')
      return listing!
    })
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest, dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000 })
    playServer(socket, (method) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') return resultOf(entries, 'r2')
      if (method === 'session.events.since') return RING
      if (method === 'session.history') return resultOf(entries, 'r3')
      if (method === 'prompt.submit') return { status: 'streaming', user_row_id: 6 }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting
    failing = true
    expect((await headless.prompt('While REST is down.')).status).toBe('held')
    expect(socket.sent.some((frame) => frame.method === 'prompt.submit')).toBe(false)
    // REST is back; the next change signal resolves the tip and sends it.
    failing = false
    socket.push({ jsonrpc: '2.0', method: 'event', params: { type: 'sessions.changed', session_id: '', payload: {} } })
    await waitFor(() => socket.sent.some((frame) => frame.method === 'prompt.submit'), 'the held prompt after REST recovers')
    await headless.stop()
  })
})

describe('Telegram lane mode (recorded T7: rotating compression)', () => {
  it('follows the compression tip as the SAME conversation, with no new-conversation reset', async () => {
    const entries = recordingFor('T7', 'v0.22.0')
    const all = listings(entries)
    // latest-descendant as recorded: before compression the root is its own
    // tip; after it the root's descendant is the child (T7:289/309).
    const descendants = entries
      .filter((entry) => entry.dir === 'in' && entry.kind === 'http' && /latest-descendant/.test(String(entry.frame.url)) && entry.frame.status === 200)
      .map((entry) => entry.frame.body as LatestDescendant)
    const moved = descendants.find((body) => body.changed)!
    let listing = all[0]!
    const rest = {
      listSessions: async () => ({ sessions: listing }),
      latestDescendant: async (id: string): Promise<LatestDescendant> => (id === moved.requested_session_id && listing === all.at(-1) ? moved : { requested_session_id: id, session_id: id, path: [id], changed: false }),
    } as unknown as HermesRest
    const resumed = entries.filter((entry) => entry.dir === 'in' && entry.kind === 'ws' && Array.isArray((entry.frame.result as { messages?: unknown } | undefined)?.messages) && (entry.frame.result as { session_id?: string }).session_id)
      .map((entry) => entry.frame.result as Record<string, unknown>)
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest, dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000 })
    const moves: LaneMove[] = []
    headless.on('lane-moved', (move: LaneMove) => moves.push(move))
    const resets: string[] = []
    headless.committed.on('boundary', (boundary: { type: string; reason?: string }) => { if (boundary.type === 'reset') resets.push(String(boundary.reason)) })
    playServer(socket, (method, params) => {
      if (method === 'client.capabilities') return CAPS
      if (method === 'session.resume') return resumed.find((result) => result.stored_session_id === params.session_id || result.resumed === params.session_id) ?? resumed[0]
      if (method === 'session.events.since') return RING
      if (method === 'session.history') return { count: 0, messages: [] }
      if (method === 'prompt.submit') return { status: 'streaming', user_row_id: 99 }
      return {}
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting
    listing = all.at(-1)!
    await headless.prompt('After compression.')
    expect(moves.map((move) => move.reason)).toEqual(['compression'])
    expect(resets).not.toContain('new-conversation')
    await headless.stop()
  })
})
