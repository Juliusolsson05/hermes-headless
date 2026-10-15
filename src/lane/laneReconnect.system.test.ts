// A held lane prompt across a dropped socket, and a submit that hits a reaped
// runtime (agent-code#1527).
//
// Fixtures: the recorded T5 Telegram-lane traffic (Hermes v0.22.0), with two
// MECHANICAL mutations taken from the server, not imagined:
//   - every session-scoped call on a runtime id the server no longer holds is
//     answered `{code: 4001, message: 'session not found'}`, the exact reply
//     of `_sess_nowait` (vendor/hermes-agent-src tui_gateway/server.py);
//   - the resume after the drop hands out a NEW runtime id, as a re-resumed
//     conversation does (the old one was detached on the drop).
// The incident this pins (server log, 2026-10-15 15:50-15:51 UTC): socket
// dropped, `session.activate` on the old runtime → 4001, then `session.history`
// and `prompt.submit` on the OLD runtime → 4001, while the fallback
// `session.resume` was still in flight. The pane lost the held message.

import { describe, expect, it } from 'vitest'

import { HermesHeadless, type HermesLaneState } from '../HermesHeadless.js'
import type { HermesRest, LatestDescendant, SessionListRow } from '../rest/HermesRest.js'
import { FakeSocket, READY_FRAME } from '../testing/fakeSocket.js'
import { recordingFor, type CorpusEntry } from '../testing/corpus.js'
import { STALE_BUSY_TAIL_MS } from './laneBusy.js'

const DM = 'agent:main:telegram:dm:111'
const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }
const RING = { events: [], latest_seq: 0, truncated: false, count: 0, epoch: 'epoch-1', open_requests: [] }
const NOT_IN_MEMORY = { code: 4001, message: 'session not found' }
const NEW_RUNTIME = 'rt-after-reconnect'
const flush = () => new Promise((resolve) => setTimeout(resolve, 0))

function resultOf(entries: CorpusEntry[], id: string): Record<string, unknown> {
  return entries.find((entry) => entry.dir === 'in' && entry.kind === 'ws' && entry.frame.id === id)!.frame.result as Record<string, unknown>
}

function firstListing(entries: CorpusEntry[]): SessionListRow[] {
  const entry = entries.find((e) => e.dir === 'in' && e.kind === 'http' && /\/api\/sessions\?/.test(String(e.frame.url)) && e.frame.status === 200)!
  return (entry.frame.body as { sessions: SessionListRow[] }).sessions
}

function recordedRest(listing: SessionListRow[]): HermesRest {
  return {
    listSessions: async () => ({ sessions: listing }),
    latestDescendant: async (id: string): Promise<LatestDescendant> => ({ requested_session_id: id, session_id: id, path: [id], changed: false }),
  } as unknown as HermesRest
}

type Reply = { result: unknown } | { error: { code: number; message: string } } | { defer: (send: (reply: Reply) => void) => void }

/** Answer every client call on `socket`; `answer` returns a result, an error, or defers. */
function playServer(socket: FakeSocket, answer: (method: string, params: Record<string, unknown>) => Reply | undefined): void {
  const answered = new Set<string>()
  const respond = (id: unknown, reply: Reply) => {
    if ('defer' in reply) reply.defer((later) => respond(id, later))
    else socket.push({ jsonrpc: '2.0', id, ...reply })
  }
  void (async () => {
    for (;;) {
      const call = await socket.nextSent((frame) => typeof frame.method === 'string' && frame.id !== undefined && !answered.has(String(frame.id)))
      answered.add(String(call.id))
      const reply = answer(String(call.method), (call.params ?? {}) as Record<string, unknown>)
      if (reply) respond(call.id, reply)
    }
  })()
}

async function waitFor(check: () => boolean, label: string): Promise<void> {
  for (let tick = 0; tick < 400 && !check(); tick += 1) await flush()
  if (!check()) throw new Error(`timed out waiting for ${label}`)
}

const sessionCalls = (socket: FakeSocket, method: string) =>
  socket.sent.filter((frame) => frame.method === method).map((frame) => (frame.params as { session_id?: string }).session_id)

describe('a held lane prompt across a dropped socket (agent-code#1527)', () => {
  it('never reads history or submits on the detached runtime, and sends the held prompt once on the resumed one', async () => {
    const entries = recordingFor('T5', 'v0.22.0')
    const resumed = resultOf(entries, 'r2')
    const oldRuntime = String(resumed.session_id)
    const phoneRow = (resultOf(entries, 'r4').messages as Array<{ timestamp?: number }>).at(-1)!
    // The recording's clock: 2 s into the phone turn. Moved past the stale
    // limit while the socket is down, exactly as the 40-minute phone turn of
    // the incident did, so the lane counts as free when the pane comes back.
    let clock = phoneRow.timestamp! * 1000 + 2_000
    const busyHistory = resultOf(entries, 'r4')

    const first = new FakeSocket()
    const second = new FakeSocket()
    const sockets = [first, second]
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(firstListing(entries)), dial: async () => sockets.shift()!, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000, now: () => clock })
    const failed: string[] = []
    headless.on('hold-failed', ({ text }: { text: string }) => failed.push(text))
    const lanes: HermesLaneState[] = []
    headless.on('lane', (state: HermesLaneState) => lanes.push(state))

    let history = resultOf(entries, 'r3')
    playServer(first, (method) => {
      if (method === 'client.capabilities') return { result: CAPS }
      if (method === 'session.resume') return { result: resumed }
      if (method === 'session.events.since') return { result: RING }
      if (method === 'session.history') return { result: history }
      return { result: {} }
    })
    const starting = headless.start()
    await flush()
    first.push(READY_FRAME())
    await starting

    history = busyHistory
    expect((await headless.prompt('Held while the phone works.')).status).toBe('held')

    // The drop. The server detached the old runtime; the second socket only
    // knows NEW_RUNTIME once `session.resume` answers, and that answer is held
    // back so a change signal lands in the middle of the re-attach.
    let releaseResume: (() => void) | null = null
    playServer(second, (method, params) => {
      if (method === 'client.capabilities') return { result: CAPS }
      const sid = params.session_id
      if (method === 'session.activate') return { error: NOT_IN_MEMORY }
      if (method === 'session.resume') return { defer: (send) => { releaseResume = () => send({ result: { ...resumed, session_id: NEW_RUNTIME } }) } }
      if (sid !== NEW_RUNTIME) return { error: NOT_IN_MEMORY }
      if (method === 'session.events.since') return { result: RING }
      if (method === 'session.history') return { result: busyHistory }
      if (method === 'prompt.submit') return { result: { status: 'streaming', user_row_id: 6 } }
      return { result: {} }
    })
    first.close(1006, '')
    clock += STALE_BUSY_TAIL_MS + 60_000
    const reconnecting = headless.reconnect()
    await flush()
    second.push(READY_FRAME())
    await waitFor(() => releaseResume !== null, 'the fallback session.resume')
    // Hermes' change signal while the resume is still pending.
    second.push({ jsonrpc: '2.0', method: 'event', params: { type: 'sessions.changed', session_id: '', payload: {} } })
    for (let tick = 0; tick < 20; tick += 1) await flush()
    releaseResume!()
    await reconnecting

    await waitFor(() => sessionCalls(second, 'prompt.submit').length > 0 || failed.length > 0, 'the held prompt to go out')
    for (let tick = 0; tick < 20; tick += 1) await flush()
    expect(failed).toEqual([])
    expect(sessionCalls(second, 'session.history')).not.toContain(oldRuntime)
    expect(sessionCalls(second, 'prompt.submit')).toEqual([NEW_RUNTIME])
    expect(lanes.at(-1)?.held).toEqual([])
    await headless.stop()
  })
})

describe('a submit that hits a reaped runtime (agent-code#1527)', () => {
  it('re-attaches by stored id once and resubmits on the new runtime', async () => {
    const entries = recordingFor('T5', 'v0.22.0')
    const resumed = resultOf(entries, 'r2')
    const oldRuntime = String(resumed.session_id)
    const idle = resultOf(entries, 'r3')
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'lane', sessionKey: DM }, rest: recordedRest(firstListing(entries)), dial: async () => socket, heartbeatIntervalMs: 600_000, laneDebounceMs: 1, laneBackstopMs: 600_000 })
    // Server-side idle TTL reaped the runtime while the socket stayed up: the
    // first resume hands out the old runtime, later ones a fresh one.
    let resumes = 0
    playServer(socket, (method, params) => {
      if (method === 'client.capabilities') return { result: CAPS }
      if (method === 'session.resume') {
        resumes += 1
        return { result: resumes === 1 ? resumed : { ...resumed, session_id: NEW_RUNTIME } }
      }
      if (method === 'session.events.since') return { result: RING }
      if (method === 'session.history') return { result: idle }
      if (method === 'prompt.submit') return params.session_id === NEW_RUNTIME ? { result: { status: 'streaming', user_row_id: 6 } } : { error: NOT_IN_MEMORY }
      return { result: {} }
    })
    const starting = headless.start()
    await flush()
    socket.push(READY_FRAME())
    await starting

    const outcome = await headless.prompt('After the runtime was reaped.')
    expect(outcome.status).toBe('streaming')
    expect(sessionCalls(socket, 'prompt.submit')).toEqual([oldRuntime, NEW_RUNTIME])
    expect(resumes).toBe(2)
    await headless.stop()
  })
})
