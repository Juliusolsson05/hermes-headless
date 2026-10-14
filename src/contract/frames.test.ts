import { describe, expect, it } from 'vitest'

import { classifyFrame } from './frames.js'

describe('classifyFrame (rule from apps/shared json-rpc-channel.ts:61-62)', () => {
  it('rejects an event frame without a type instead of dispatching it', () => {
    expect(classifyFrame({ jsonrpc: '2.0', method: 'event', params: { session_id: 'x' } }).kind).toBe('invalid')
  })

  it('tells a server request (string id + method) from a response and an event', () => {
    expect(classifyFrame({ jsonrpc: '2.0', id: 'srq-1', method: 'approval', params: {} }).kind).toBe('request')
    expect(classifyFrame({ jsonrpc: '2.0', id: 'hh1', result: {} }).kind).toBe('response')
    expect(classifyFrame({ jsonrpc: '2.0', method: 'event', params: { type: 'message.start' } }).kind).toBe('event')
  })
})
