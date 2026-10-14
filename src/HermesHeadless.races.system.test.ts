// Attach races between Hermes' ring reply and live frames (round-2 review c1,
// kept from the reviewer's probes). Each reproduces an upstream interleaving
// (tui_gateway methods_session.py session.events.since builds its event list,
// head and open-request list under separate locks, then writes its reply
// later) on recorded S3/S8/S9/S10/S16 frames, and each failed before the fix.
import { describe, expect, it, vi } from 'vitest'
import { HermesHeadless } from './HermesHeadless.js'
import { FakeSocket, READY_FRAME } from './testing/fakeSocket.js'
import { inboundFrames, latestRecording } from './testing/corpus.js'
const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }
const event = (params: any) => ({ jsonrpc: '2.0', method: 'event', params })
const ring = (events: any[], extras: any = {}) => ({ events, latest_seq: events.at(-1)?.seq ?? 0, count: events.length, truncated: false, epoch: 'epoch-1', open_requests: [], ...extras })
const recorded = (s: string) => inboundFrames(latestRecording(s))
const result = (frames: any[], pred: (r: any) => boolean) => frames.map(f => f.result).find(r => r && pred(r))
const events = (frames: any[], sid: string) => {
  const bySeq = new Map<number, any>()
  for (const f of frames) {
    if (f.method === 'event' && f.params.session_id === sid && typeof f.params.seq === 'number') bySeq.set(f.params.seq, f.params)
    for (const e of f.result?.events ?? []) if (e.session_id === sid) bySeq.set(e.seq, e)
  }
  return [...bySeq.values()].sort((a,b) => a.seq-b.seq)
}
async function begin(target: any) {
  const socket = new FakeSocket()
  const headless = new HermesHeadless({target, dial: async () => socket, heartbeatIntervalMs: 600_000})
  const requests: any[] = [], closed: any[] = [], semantic: any[] = []
  headless.on('request', r => requests.push(r))
  headless.on('request-closed', r => closed.push(r))
  headless.semantic.on('event', e => semantic.push(e))
  const starting = headless.start()
  await Promise.resolve()
  socket.push(READY_FRAME())
  await socket.answerCall('client.capabilities', CAPS)
  return {socket, headless, starting, requests, closed, semantic}
}

describe('attach races against the ring reply', () => {
  it('R1: shows an approval registered after the ring snapshot but delivered before its response', async () => {
    const f = recorded('S10')
    const approval = f.find(f => f.method === 'approval')!
    const created = result(f, r => r.session_id === (approval.params as any).session_id && r.message_count === 0)
    const h = await begin({mode: 'resume', storedSessionId: created.stored_session_id})
    try {
      await h.socket.answerCall('session.resume', {...created, running: true})
      const since = await h.socket.nextSent(f => f.method === 'session.events.since')
      // methods_session.py:2348 builds open_requests; ws.py:330 awaits
      // to_thread(dispatch). An agent thread can register/write an approval
      // before ws.py:337 writes the already-built ring response.
      const response = ring([], {latest_seq: 9})
      h.socket.push(approval)
      h.socket.push({jsonrpc:'2.0', id: since.id, result: response})
      await h.starting
      expect(h.requests.map(r => r.id)).toContain(approval.id)
    } finally { await h.headless.stop() }
  })

  it('R2: a seq stamped after events_since but before latest_seq still settles the live turn', async () => {
    const f = recorded('S8')
    const created = result(f, r => typeof r.stored_session_id === 'string' && r.message_count === 0)
    const evs = events(f, created.session_id)
    const end = evs.find(e => e.type === 'message.complete')
    const prefix = evs.filter(e => e.seq < end.seq)
    const h = await begin({mode:'resume', storedSessionId:created.stored_session_id})
    try {
      await h.socket.answerCall('session.resume', {...created, running:true})
      const since = await h.socket.nextSent(f => f.method === 'session.events.since')
      // Upstream reads events_since and latest_seq under separate locks.
      // The completion is live and reflected in latest_seq, but missing from
      // the earlier events copy. Its held copy is the only copy available.
      h.socket.push(event(end))
      h.socket.push({jsonrpc:'2.0',id:since.id,result:ring(prefix,{latest_seq:end.seq})})
      await h.starting
      expect(h.headless.turnInFlight).toBe(false)
    } finally { await h.headless.stop() }
  })

  it('R3: a running prompt before message.start must not be settled by the previous turn completion', async () => {
    const f = recorded('S3')
    const created = result(f, r => typeof r.stored_session_id === 'string' && r.message_count === 0)
    const evs = events(f, created.session_id)
    const oldEnd = evs.find(e => e.type === 'message.complete')
    const priorTurn = evs.filter(e => e.seq <= oldEnd.seq)
    const running = result(recorded('S9'), r=>r.running === true)
    const h = await begin({mode:'resume', storedSessionId:created.stored_session_id})
    try {
      // methods_prompt.py:722 sets running and seeds an empty inflight at
      // admission; prompt_turn.py emits message.start later. Move the real
      // resume to S3's next accepted turn (t3257..3261), preserving its shape.
      const history = result(f, r => Array.isArray(r.messages) && r.messages.length > 0).messages.slice(0, 2)
      await h.socket.answerCall('session.resume', {...created, messages: history, message_count: history.length, running:true, inflight:{...running.inflight, assistant:'', user:'And again.', streaming:true}})
      await h.socket.answerCall('session.events.since',ring(priorTurn))
      await h.starting
      const busy = h.headless.turnInFlight
      const prompt = h.headless.prompt('another queued message')
      const submitted = await h.socket.answerCall('prompt.submit',{status:'queued'})
      await prompt
      expect({busy,queued:(submitted.params as any).queued}).toEqual({busy:true,queued:true})
    } finally { await h.headless.stop() }
  })

  it('R4: an acquire timeout must not open a request after its held cancellation', async () => {
    vi.useFakeTimers()
    const f = recorded('S5')
    const cancel = f.find(f => f.method==='event' && (f.params as any).type==='request.cancel')!
    const approval = f.find(f => f.method === 'approval' && f.id === (cancel.params as any).payload.id)!
    const created = result(f,r=>r.session_id===(approval.params as any).session_id && r.message_count===0)
    const h = await begin({mode:'resume', storedSessionId:created.stored_session_id})
    try {
      await h.socket.answerCall('session.resume',created)
      const since = await h.socket.nextSent(f=>f.method==='session.events.since')
      h.socket.push(approval)
      h.socket.push(cancel)
      const failure = expect(h.starting).rejects.toThrow('session.events.since timed out')
      await vi.advanceTimersByTimeAsync(10_001)
      await failure
      expect(h.requests.filter(r=>!h.closed.some(c=>c.id===r.id))).toEqual([])
    } finally {await h.headless.stop(); vi.useRealTimers()}
  })

  it('R5: stopping during acquire must not publish a held approval after stopped', async () => {
    const f = recorded('S10')
    const approval = f.find(f=>f.method==='approval')!
    const created = result(f,r=>r.session_id===(approval.params as any).session_id && r.message_count===0)
    const h = await begin({mode:'resume',storedSessionId:created.stored_session_id})
    await h.socket.answerCall('session.resume',created)
    await h.socket.nextSent(f=>f.method==='session.events.since')
    h.socket.push(approval)
    const order:string[]=[]
    h.headless.on('connection',s=>order.push(s.kind))
    h.headless.on('request',()=>order.push('request'))
    const failed=expect(h.starting).rejects.toThrow('client-stop')
    await h.headless.stop()
    await failed
    expect(order).toEqual(['stopped'])
  })
})
