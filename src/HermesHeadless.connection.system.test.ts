// Connection operations (manage_connections cards) on RECORDED frames
// (research/corpus S26: a catalog MCP install answered "skipped"; S27: an
// OAuth catalog install answered "skipped"; both v0.22.0). Before this the
// request became a one-line notice and the turn waited up to 300 s with no
// way to answer it from Agent Code (owner, 2026-10-14).

import { describe, expect, it } from 'vitest'

import { HermesHeadless, type HermesConnectionOperation } from './index.js'
import { FakeSocket, READY_FRAME } from './testing/fakeSocket.js'
import { latestRecording, type CorpusEntry } from './testing/corpus.js'

const CAPS = { server_requests: ['approval', 'clarify', 'sudo', 'secret'], declines_not_shown: true }
const ring = { events: [], latest_seq: 0, truncated: false, count: 0, epoch: 'epoch-1', open_requests: [] }

function recorded(scenario: string) {
  const entries: CorpusEntry[] = latestRecording(scenario, { release: 'v0.22.0' }).filter((entry) => entry.kind === 'ws')
  const results = entries.filter((entry) => entry.dir === 'in' && 'result' in entry.frame).map((entry) => entry.frame.result as Record<string, unknown>)
  const created = results.find((result) => typeof result.stored_session_id === 'string' && result.message_count === 0)!
  const connectionFrames = entries.filter((entry) => entry.dir === 'in' && String((entry.frame.params as { type?: string } | undefined)?.type ?? '').startsWith('connection.')).map((entry) => entry.frame)
  const respond = entries.find((entry) => entry.dir === 'out' && entry.frame.method === 'connection.respond')!.frame
  return { created, connectionFrames, respond }
}

async function started(scenario: string) {
  const { created, connectionFrames, respond } = recorded(scenario)
  let socket!: FakeSocket
  const headless = new HermesHeadless({
    target: { mode: 'create', source: 'agent-code' },
    heartbeatIntervalMs: 600_000,
    dial: async () => (socket = new FakeSocket()),
  })
  const operations: HermesConnectionOperation[] = []
  headless.on('connection-operation', (operation: HermesConnectionOperation) => operations.push(operation))
  const starting = headless.start()
  await new Promise((resolve) => setTimeout(resolve, 0))
  socket.push(READY_FRAME())
  await socket.answerCall('client.capabilities', CAPS)
  await socket.answerCall('session.create', created)
  await socket.answerCall('session.events.since', ring)
  await starting
  return { headless, socket: () => socket, operations, connectionFrames, respond, created }
}

describe('a connection operation (recorded S26)', () => {
  it('surfaces the request as an operation the consumer can draw, with the card\'s own text', async () => {
    const h = await started('S26')
    h.socket().push(h.connectionFrames[0])
    expect(h.operations).toHaveLength(1)
    const [operation] = h.operations
    expect(operation).toMatchObject({ opId: '795252ea5041', settled: false, toolCallId: 'call_connect-form_0_0' })
    // Epoch SECONDS on the wire, ms here (the card counts down to it).
    expect(operation!.deadlineAt).toBe(Math.round(1792016754.376252 * 1000))
    expect(operation!.targets).toEqual([expect.objectContaining({ name: 'n8n-official', kind: 'mcp', action: 'install', state: 'pending', resolved: false, connectUrl: null })])
    // The credential the card must ask for, as recorded (not a secret: a URL).
    expect(operation!.targets[0]!.requiredEnv).toEqual([{ name: 'N8N_MCP_SERVER_URL', prompt: 'MCP Server URL (n8n Settings > Instance-level MCP > Connect; ends in /mcp-server/http)', required: true, secret: false, default: '' }])
    expect(operation!.targets[0]!.instructions).toContain('In n8n Settings > Instance-level MCP')
    await h.headless.stop()
  })

  it('answers through connection.respond exactly as the recorded client did, and follows the updates to settled', async () => {
    const h = await started('S26')
    h.socket().push(h.connectionFrames[0])
    const answering = h.headless.respondConnection('795252ea5041', { targets: [{ name: 'n8n-official', status: 'skipped' }] })
    const call = await h.socket().nextSent((frame) => frame.method === 'connection.respond')
    // Same params as the recording, with this connection's runtime id as owner.
    expect(call.params).toEqual({ ...(h.respond.params as object), owner: { session_id: h.created.session_id, type: 'session' } })
    h.socket().push({ jsonrpc: '2.0', id: call.id, result: { status: 'ok', settled: false } })
    expect(await answering).toEqual({ settled: false })

    for (const frame of h.connectionFrames.slice(1)) h.socket().push(frame)
    expect(h.operations.map((operation) => [operation.seq, operation.targets[0]!.state, operation.settled])).toEqual([
      [0, 'pending', false],
      [1, 'skipped', false],
      [2, 'skipped', true],
    ])
    expect(h.operations.at(-1)!.settledBy).toBe('all_resolved')
    await h.headless.stop()
  })
})

describe('an OAuth catalog install (recorded S27)', () => {
  it('carries its browser instructions and no link (a catalog OAuth runs on the server, not via a minted URL)', async () => {
    const h = await started('S27')
    h.socket().push(h.connectionFrames[0])
    expect(h.operations[0]!.targets[0]).toMatchObject({ name: 'airtable', action: 'install', connectUrl: null })
    expect(h.operations[0]!.targets[0]!.instructions).toContain('authorize with\nAirtable')
    await h.headless.stop()
  })
})
