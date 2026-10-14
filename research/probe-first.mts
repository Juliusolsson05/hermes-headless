// One-off first contact probe: sign in, open the socket, create a session,
// send one prompt, and log everything. Used once to calibrate the scripted
// model and the scenario list before record-wire.mts exists.
import { WireTap } from './tap.js'
import { RawPeer, mintTicket, passwordNativeLogin, probe } from './driver.js'

const [baseUrl, out] = process.argv.slice(2)
const tap = new WireTap(out)
await probe(tap, baseUrl)
const tokens = await passwordNativeLogin(tap, baseUrl, process.env.HH_USER ?? '', process.env.HH_PASS ?? '')
const ticket = await mintTicket(tap, baseUrl, tokens.access_token)
const peer = new RawPeer(tap)
const ws = new URL('/api/ws', baseUrl)
ws.protocol = ws.protocol === 'https:' ? 'wss:' : 'ws:'
ws.searchParams.set('ticket', ticket)
await peer.open(ws.toString())
await peer.waitEvent('gateway.ready', 15_000)
await peer.call('client.capabilities', { server_requests: true })
const created = await peer.call<Record<string, unknown>>('session.create', { source: 'agent-code', cols: 96 })
const sid = created.session_id as string
await peer.call('prompt.submit', { session_id: sid, text: 'Say hello.' })
await peer.waitEvent('message.complete', 90_000)
await new Promise((resolve) => setTimeout(resolve, 1500))
peer.close()
await peer.closed
