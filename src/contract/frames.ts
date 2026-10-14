// Classify one inbound JSON-RPC frame. Pure; no I/O.
//
// The rule Hermes' own client uses (apps/shared/src/json-rpc-channel.ts:61-62)
// is the rule here: a frame with a string `id` AND a string `method` other
// than "event" is a server→client REQUEST; `method === "event"` is a
// notification; anything with an `id` and `result`/`error` is a response.
//
// WHY `invalid` is a value and not a throw: one malformed frame from a remote
// backend must not tear down a socket that is otherwise healthy. The transport
// reports it (as an unknown shape) and keeps reading.

import type { EventEnvelope, RawFrame, RpcErrorBody } from './wire.js'

export type ClassifiedFrame =
  | { kind: 'response'; id: string; result?: unknown; error?: RpcErrorBody }
  | { kind: 'event'; event: EventEnvelope }
  | { kind: 'request'; id: string; method: string; params: Record<string, unknown> }
  | { kind: 'invalid'; reason: string; raw: unknown }

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function parseFrameText(text: string): ClassifiedFrame {
  let raw: unknown
  try {
    raw = JSON.parse(text)
  } catch (error) {
    return { kind: 'invalid', reason: `not JSON: ${error instanceof Error ? error.message : String(error)}`, raw: text.slice(0, 200) }
  }
  return classifyFrame(raw)
}

export function classifyFrame(raw: unknown): ClassifiedFrame {
  if (!isRecord(raw)) return { kind: 'invalid', reason: 'frame is not an object', raw }
  const frame = raw as RawFrame
  if (frame.method === 'event') {
    const params = frame.params
    if (!isRecord(params) || typeof params.type !== 'string') {
      return { kind: 'invalid', reason: 'event without params.type', raw }
    }
    const event: EventEnvelope = { type: params.type }
    if (typeof params.session_id === 'string' && params.session_id) event.session_id = params.session_id
    if ('payload' in params) event.payload = params.payload
    if (typeof params.seq === 'number') event.seq = params.seq
    return { kind: 'event', event }
  }
  if (typeof frame.id === 'string' && typeof frame.method === 'string') {
    return { kind: 'request', id: frame.id, method: frame.method, params: isRecord(frame.params) ? frame.params : {} }
  }
  if (frame.id !== undefined && frame.id !== null && ('result' in frame || 'error' in frame)) {
    const id = String(frame.id)
    if ('error' in frame && isRecord(frame.error)) {
      const error = frame.error as Record<string, unknown>
      return {
        kind: 'response',
        id,
        error: { code: typeof error.code === 'number' ? error.code : -32603, message: String(error.message ?? 'unknown error'), data: error.data },
      }
    }
    return { kind: 'response', id, result: frame.result }
  }
  return { kind: 'invalid', reason: 'unrecognised JSON-RPC frame', raw }
}

/** Replayed events arrive as bare envelope objects inside `session.events.since`. */
export function envelopeFromReplay(raw: unknown): EventEnvelope | null {
  if (!isRecord(raw) || typeof raw.type !== 'string') return null
  const event: EventEnvelope = { type: raw.type }
  if (typeof raw.session_id === 'string' && raw.session_id) event.session_id = raw.session_id
  if ('payload' in raw) event.payload = raw.payload
  if (typeof raw.seq === 'number') event.seq = raw.seq
  return event
}
