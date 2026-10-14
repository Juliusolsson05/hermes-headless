// An in-memory WebSocket for driving RpcConnection with recorded frames.
// The test plays the server: it pushes recorded inbound frames and answers the
// client's calls with recorded results, mapping ids (the client mints its own).

import type { WebSocketLike } from '../transport/RpcConnection.js'

type Listener = (event: never) => void

export class FakeSocket implements WebSocketLike {
  readyState = 1
  readonly sent: Array<Record<string, unknown>> = []
  private readonly listeners = new Map<string, Listener[]>()
  private readonly sendWaiters: Array<{ match: (frame: Record<string, unknown>) => boolean; resolve: (frame: Record<string, unknown>) => void }> = []

  addEventListener(type: string, listener: Listener): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener])
  }

  send(data: string): void {
    if (this.readyState !== 1) throw new Error('socket closed')
    const frame = JSON.parse(data) as Record<string, unknown>
    this.sent.push(frame)
    for (let index = this.sendWaiters.length - 1; index >= 0; index -= 1) {
      if (this.sendWaiters[index].match(frame)) this.sendWaiters.splice(index, 1)[0].resolve(frame)
    }
  }

  close(code = 1000, reason = ''): void {
    if (this.readyState === 3) return
    this.readyState = 3
    this.fire('close', { code, reason })
  }

  /** Server → client: deliver one frame. */
  push(frame: unknown): void {
    this.fire('message', { data: JSON.stringify(frame) })
  }

  /** Resolve when the client sends a frame matching `match` (or one already sent). */
  nextSent(match: (frame: Record<string, unknown>) => boolean): Promise<Record<string, unknown>> {
    const already = this.sent.find(match)
    if (already) return Promise.resolve(already)
    return new Promise((resolve) => this.sendWaiters.push({ match, resolve }))
  }

  /** Wait for the client's call to `method` and answer it with `result`. */
  async answerCall(method: string, result: unknown): Promise<Record<string, unknown>> {
    const call = await this.nextSent((frame) => frame.method === method && !this.answered.has(String(frame.id)))
    this.answered.add(String(call.id))
    this.push({ jsonrpc: '2.0', id: call.id, result })
    return call
  }

  private readonly answered = new Set<string>()

  private fire(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) (listener as (event: unknown) => void)(event)
  }
}

export const READY_FRAME = (epoch = 'epoch-1') => ({
  jsonrpc: '2.0',
  method: 'event',
  params: { type: 'gateway.ready', payload: { change_events: true, heartbeat: true, replay_epoch: epoch } },
})
