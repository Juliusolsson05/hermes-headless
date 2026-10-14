import { describe, expect, it } from 'vitest'

import { backendCompatibility } from './version.js'
import { inboundFrames, latestRecording } from './testing/corpus.js'

describe('backendCompatibility', () => {
  it('reads the recorded v0.21.7 backend as tested (desktop_contract 8, recorded S3)', () => {
    const created = inboundFrames(latestRecording('S3')).map((frame) => frame.result as { info?: { desktop_contract?: unknown } } | undefined).find((result) => result?.info?.desktop_contract !== undefined)
    expect(backendCompatibility(created?.info?.desktop_contract)).toEqual({ kind: 'tested', contract: 8 })
  })

  it('warns on a newer contract and refuses one older than server→client requests (v7)', () => {
    expect(backendCompatibility(9)).toEqual({ kind: 'skew', contract: 9 })
    expect(backendCompatibility('7')).toEqual({ kind: 'skew', contract: 7 })
    expect(backendCompatibility(6)).toEqual({ kind: 'unsupported', contract: 6 })
    expect(backendCompatibility(null)).toEqual({ kind: 'unsupported', contract: null })
  })
})
