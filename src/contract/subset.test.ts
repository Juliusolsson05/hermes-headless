// H14 (decomposition §7): the recorded wire matches the pinned contract subset.
//
// Fixtures: every recording in research/corpus (label: recording, real
// `hermes serve` v0.21.7 behind the auth gate, scripted model). If Hermes
// starts sending a new event type, this test fails and names it. The fix is
// a decision recorded in catalog.ts (handle it or ignore it with a reason),
// never a looser test.

import { describe, expect, it } from 'vitest'

import { classifyFrame } from './frames.js'
import { isAnswerableRequest, isHandledEvent, isIgnoredEvent } from './catalog.js'
import { corpusScenarios, inboundFrames, loadRecording, recordingFiles } from '../testing/corpus.js'

/**
 * Server→client requests Hermes advertised that this package declines with
 * -32601 (the Desktop GUI bridges and vault prompts). Listed so a request
 * that is neither answerable nor knowingly declined fails the test.
 */
const DECLINED_REQUESTS = new Set([
  'display.install.sudo', 'preview.act', 'preview.read', 'setup_choose', 'setup_beat', 'terminal.read',
  'tour', 'vault.code', 'vault.save_login', 'vault.unlock_prompt', 'window.read', 'theme.apply', 'theme.pick',
])

const files = corpusScenarios().flatMap((scenario) => recordingFiles(scenario))

describe('recorded wire vs contract subset', () => {
  it('has a corpus to check', () => {
    expect(files.length).toBeGreaterThan(0)
  })

  it('classifies every inbound frame (no invalid frames on the real wire)', () => {
    const invalid: string[] = []
    for (const file of files) {
      for (const frame of inboundFrames(loadRecording(file))) {
        const classified = classifyFrame(frame)
        if (classified.kind === 'invalid') invalid.push(`${file}: ${classified.reason}`)
      }
    }
    expect(invalid).toEqual([])
  })

  it('assigns every recorded event type an owner or an explicit ignore reason', () => {
    const unowned = new Set<string>()
    for (const file of files) {
      for (const frame of inboundFrames(loadRecording(file))) {
        const classified = classifyFrame(frame)
        if (classified.kind !== 'event') continue
        const type = classified.event.type
        if (!isHandledEvent(type) && !isIgnoredEvent(type)) unowned.add(type)
      }
    }
    expect([...unowned]).toEqual([])
  })

  it('knows every server request method it can receive', () => {
    const unknown = new Set<string>()
    for (const file of files) {
      for (const frame of inboundFrames(loadRecording(file))) {
        const classified = classifyFrame(frame)
        if (classified.kind !== 'request') continue
        if (!isAnswerableRequest(classified.method) && !DECLINED_REQUESTS.has(classified.method)) unknown.add(classified.method)
      }
    }
    expect([...unknown]).toEqual([])
  })

  it('carries the fields the reconciler reads on every handled event', () => {
    // A payload missing a field the reconciler depends on is a contract break
    // that would otherwise surface as a blank bubble or a stuck tool chip.
    const required: Record<string, string[]> = {
      'message.delta': ['text'],
      'message.complete': [],
      'tool.start': ['tool_id', 'name'],
      'tool.complete': ['tool_id', 'name'],
      'request.cancel': ['id', 'method', 'reason'],
      'session.title': ['session_id', 'title'],
    }
    const missing: string[] = []
    for (const file of files) {
      for (const frame of inboundFrames(loadRecording(file))) {
        const classified = classifyFrame(frame)
        if (classified.kind !== 'event') continue
        const keys = required[classified.event.type]
        if (!keys) continue
        const payload = (classified.event.payload ?? {}) as Record<string, unknown>
        for (const key of keys) if (!(key in payload)) missing.push(`${classified.event.type}.${key} in ${file}`)
      }
    }
    expect(missing).toEqual([])
  })
})
