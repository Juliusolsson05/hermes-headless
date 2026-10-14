// Fallback and resume guards whose mutants survived round 2 (review c1,
// kept from the reviewer's probes on recorded frames).
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


describe('resume fallbacks', () => {
  it('N12: an idle resume never paints another turn from its old completion', async () => {
    const {sid, events, frames} = sessionEvents('S3')
    const completed=firstResult(frames,r=>Array.isArray(r.messages) && Number(r.message_count)>0)
    const created=firstResult(frames,r=>r.message_count===0)
    const h=harness({mode:'resume',storedSessionId:String(created.stored_session_id)})
    try {
      const starting=h.headless.start();const socket=await h.nextSocket();await handshake(socket)
      await socket.answerCall('session.resume',{...completed,session_id:sid,running:false,inflight:null})
      await socket.answerCall('session.events.since',ring(events))
      await starting
      expect(h.semantic.filter(e=>e.type==='turn_completed' || e.type==='text_delta')).toEqual([])
    } finally { await h.headless.stop() }
  })

  it('N14: a transient error during reattach must not abandon the stored conversation', async () => {
    const {h,socket,created}=await started()
    try {
      socket.close(4000,'drop')
      const reconnecting=h.headless.reconnect().then(()=>({success:true}),error=>({error:error.message}))
      const second=await h.nextSocket(); await handshake(second,'epoch-2')
      // Answer the mutant's unexpected fallback too, so the assertion tests
      // identity preservation without relying on a test timeout to kill it.
      const send=second.send.bind(second)
      second.send=(data:string)=>{
        send(data); const frame=JSON.parse(data)
        if(frame.method==='session.create') queueMicrotask(()=>second.push({jsonrpc:'2.0',id:frame.id,result:created}))
        if(frame.method==='session.events.since') queueMicrotask(()=>second.push({jsonrpc:'2.0',id:frame.id,result:ring([])}))
      }
      const resume=await second.nextSent(f=>f.method==='session.resume')
      second.push({jsonrpc:'2.0',id:resume.id,error:{code:5036,message:'database unavailable'}})
      expect(await reconnecting).toEqual({error:'database unavailable'})
      expect(second.sent.some(f=>f.method==='session.create')).toBe(false)
    } finally { await h.headless.stop() }
  })

  it('N17: a replacement conversation forgets pending work from the deleted one', async () => {
    const {h,socket}=await started()
    try {
      const sending=h.headless.prompt('pending old turn')
      await socket.answerCall('prompt.submit',{status:'streaming'});await sending
      socket.close(4000,'drop')
      const reconnecting=h.headless.reconnect()
      const second=await h.nextSocket();await handshake(second,'epoch-2')
      const resume=await second.nextSent(f=>f.method==='session.resume')
      second.push({jsonrpc:'2.0',id:resume.id,error:{code:4007,message:'session not found'}})
      const next=firstResult(sessionEvents('S4').frames,r=>r.message_count===0)
      await second.answerCall('session.create',next);await second.answerCall('session.events.since',ring([]))
      await reconnecting
      expect(h.headless.turnInFlight).toBe(false)
    } finally { await h.headless.stop() }
  })

  it('N18: fallback preserves the caller chosen source', async () => {
    const created=firstResult(sessionEvents('S3').frames,r=>r.message_count===0)
    const h=harness({mode:'create',source:'agent-code-review'})
    try {
      const starting=h.headless.start();const socket=await h.nextSocket();await handshake(socket)
      await socket.answerCall('session.create',created);await socket.answerCall('session.events.since',ring([]));await starting
      socket.close(4000,'drop');const reconnecting=h.headless.reconnect()
      const second=await h.nextSocket();await handshake(second,'epoch-2')
      const resume=await second.nextSent(f=>f.method==='session.resume')
      second.push({jsonrpc:'2.0',id:resume.id,error:{code:4007,message:'session not found'}})
      const call=await second.answerCall('session.create',created);await second.answerCall('session.events.since',ring([]));await reconnecting
      expect((call.params as {source:string}).source).toBe('agent-code-review')
    } finally { await h.headless.stop() }
  })
})
