// Plan Stage B (agent-code#1495/#1497) on RECORDED frames from real Hermes
// servers: S25 (v0.20.4), S18/S19/S20/S22 (v0.22.0). Each test names the
// behaviour it pins and fails on the code before Stage B.

import { describe, expect, it } from 'vitest'

import { HermesIncompatibleBackendError, HermesStartError } from './errors.js'
import { HermesHeadless } from './HermesHeadless.js'
import { ConversationReconciler, type HermesSideAnswer } from './reconcile/ConversationReconciler.js'
import type { OpenRequestEntry } from './contract/wire.js'
import { RpcConnection } from './transport/RpcConnection.js'
import { FakeSocket, READY_FRAME } from './testing/fakeSocket.js'
import { inboundFrames, recordingFor, type CorpusEntry } from './testing/corpus.js'

/** Client → server WS frames of a recording (the corpus helper only exposes inbound ones). */
function outboundFrames(entries: CorpusEntry[]): Array<Record<string, unknown>> {
  return entries.filter((entry) => entry.dir === 'out' && entry.kind === 'ws').map((entry) => entry.frame as Record<string, unknown>)
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0))
const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }

describe('S25: a v0.20.4 server (desktop contract 6)', () => {
  it('fails the handshake as an incompatible backend and closes the socket', async () => {
    const frames = inboundFrames(recordingFor('S25', 'v0.20.4'))
    const ready = frames.find((frame) => (frame.params as { type?: string } | undefined)?.type === 'gateway.ready')!
    const refusal = frames.find((frame) => (frame.error as { code?: number } | undefined)?.code === -32601)!
    const socket = new FakeSocket()
    const opening = RpcConnection.open(socket, { canAnswer: () => true, watermarks: new Map(), heartbeatIntervalMs: 600_000 })
    socket.push(ready)
    const call = await socket.nextSent((frame) => frame.method === 'client.capabilities')
    socket.push({ ...refusal, id: call.id })
    await expect(opening).rejects.toBeInstanceOf(HermesIncompatibleBackendError)
    await expect(opening).rejects.toMatchObject({ method: 'client.capabilities', code: -32601 })
    expect(socket.readyState).toBe(3)
  })

  it('reports it from start() as a handshake-stage failure', async () => {
    const frames = inboundFrames(recordingFor('S25', 'v0.20.4'))
    const ready = frames.find((frame) => (frame.params as { type?: string } | undefined)?.type === 'gateway.ready')!
    const refusal = frames.find((frame) => (frame.error as { code?: number } | undefined)?.code === -32601)!
    const socket = new FakeSocket()
    const headless = new HermesHeadless({ target: { mode: 'create', source: 'agent-code' }, heartbeatIntervalMs: 600_000, dial: async () => socket })
    const starting = headless.start()
    await flush()
    socket.push(ready)
    const call = await socket.nextSent((frame) => frame.method === 'client.capabilities')
    socket.push({ ...refusal, id: call.id })
    const error = await starting.catch((caught: unknown) => caught)
    expect(error).toBeInstanceOf(HermesStartError)
    expect((error as HermesStartError).stage).toBe('handshake')
    expect((error as HermesStartError).cause).toBeInstanceOf(HermesIncompatibleBackendError)
  })
})

/** A headless attached to the recorded session of `scenario` on a FakeSocket. */
async function attached(scenario: string) {
  const frames = inboundFrames(recordingFor(scenario, 'v0.22.0'))
  const created = frames.map((frame) => frame.result as Record<string, unknown> | undefined)
    .find((result) => result && typeof result.stored_session_id === 'string' && result.message_count === 0)!
  const socket = new FakeSocket()
  const headless = new HermesHeadless({ target: { mode: 'create', source: 'agent-code' }, heartbeatIntervalMs: 600_000, dial: async () => socket })
  const answers: HermesSideAnswer[] = []
  headless.on('side-answer', (answer: HermesSideAnswer) => answers.push(answer))
  const starting = headless.start()
  await flush()
  socket.push(READY_FRAME())
  await socket.answerCall('client.capabilities', CAPS)
  await socket.answerCall('session.create', created)
  await socket.answerCall('session.events.since', { events: [], latest_seq: 0, truncated: false, count: 0, epoch: 'epoch-1', open_requests: [] })
  await starting
  return { frames, created, socket, headless, answers, sid: String(created.session_id) }
}

describe('S18 / S19: /bg and /btw get their answers (r4 F6)', () => {
  for (const [scenario, typed, method, event, kind] of [
    ['S18', '/bg summarise the repo', 'prompt.background', 'background.complete', 'background'],
    ['S19', '/btw what model are you?', 'prompt.btw', 'btw.complete', 'btw'],
  ] as const) {
    it(`${scenario}: ${typed.split(' ')[0]} goes to ${method}, and ${event} surfaces as a side answer`, async () => {
      const { frames, socket, headless, answers, sid } = await attached(scenario)
      const recordedTask = frames.map((frame) => frame.result as { task_id?: string } | undefined).find((result) => typeof result?.task_id === 'string')!
      const running = headless.runSlash(typed)
      const call = await socket.nextSent((frame) => frame.method === method)
      expect((call.params as { text: string }).text).toBe(typed.replace(/^\/\S+\s+/, ''))
      socket.push({ jsonrpc: '2.0', id: call.id, result: recordedTask })
      expect(await running).toMatchObject({ kind: 'side-task', taskId: recordedTask.task_id })
      // Never via slash.exec: that path printed the answer after its capture window.
      expect(socket.sent.some((frame) => frame.method === 'slash.exec')).toBe(false)
      const complete = frames.find((frame) => (frame.params as { type?: string } | undefined)?.type === event)!
      socket.push({ ...complete, params: { ...(complete.params as object), session_id: sid } })
      await flush()
      expect(answers).toHaveLength(1)
      expect(answers[0]).toMatchObject({ kind, taskId: recordedTask.task_id })
      expect(answers[0]!.text.length).toBeGreaterThan(0)
    })
  }
})

describe('S20: /compress uses session.compress', () => {
  it('calls the session RPC with the focus topic and reports the recorded result', async () => {
    const { frames, socket, headless } = await attached('S20')
    const recorded = outboundFrames(recordingFor('S20', 'v0.22.0')).find((frame) => frame.method === 'session.compress')!
    const result = frames.find((frame) => frame.id === recorded.id && frame.result)!.result
    const running = headless.runSlash('/compress the tests')
    const call = await socket.nextSent((frame) => frame.method === 'session.compress')
    expect((call.params as { focus_topic?: string }).focus_topic).toBe('the tests')
    socket.push({ jsonrpc: '2.0', id: call.id, result })
    const outcome = await running
    expect(outcome.kind).toBe('output')
    expect(socket.sent.some((frame) => frame.method === 'slash.exec')).toBe(false)
  })
})

describe('S22: a card answered by ANOTHER client (r4 F4)', () => {
  it('closes when the turn ends, though Hermes never tells this client', () => {
    const frames = inboundFrames(recordingFor('S22', 'v0.22.0'))
    const approval = frames.find((frame) => frame.method === 'approval')!
    const sid = String((approval.params as { session_id: string }).session_id)
    const closed: Array<{ id: string; reason: string }> = []
    const noop = () => undefined
    const reconciler = new ConversationReconciler({
      semantic: noop, committed: noop, historyReset: noop, historyCaughtUp: noop, activity: noop, session: noop,
      requestOpened: noop, requestClosed: (id, reason) => closed.push({ id, reason }), turnSettled: noop, reclaimed: noop,
      unknown: noop, notice: noop, noticeCleared: noop, sideAnswer: noop,
    })
    reconciler.applySnapshot({ session_id: sid, stored_session_id: 's', messages: [] } as never, 'snapshot')
    // Client A sees the request (it never answers: client B does) …
    reconciler.openRequest({ id: String(approval.id), method: 'approval', params: approval.params as Record<string, unknown>, replayed: false } as OpenRequestEntry & { replayed: boolean })
    // … then exactly the events A received after it, through message.complete.
    const after = frames.slice(frames.indexOf(approval) + 1)
    const seen = new Set<number>()
    for (const frame of after) {
      const params = frame.params as { type?: string; seq?: number; session_id?: string } | undefined
      if (frame.method !== 'event' || params?.session_id !== sid) continue
      if (typeof params.seq === 'number') {
        if (seen.has(params.seq)) continue
        seen.add(params.seq)
      }
      reconciler.applyEvent(params as never)
      if (params.type === 'message.complete') break
    }
    // No request.cancel / approval.cancelled ever reached A (recorded).
    expect(after.some((frame) => ['request.cancel', 'approval.cancelled'].includes(String((frame.params as { type?: string } | undefined)?.type)))).toBe(false)
    expect(closed).toEqual([{ id: String(approval.id), reason: 'settled' }])
  })
})
