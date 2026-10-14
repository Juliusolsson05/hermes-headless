// Lane identity on REAL REST bodies from Hermes v0.22.0 with upstream's
// Telegram adapter against its Bot API stand-in (research/corpus T1/T6/T7/T8,
// plan Stage 0). The function under test must pick the conversation the
// gateway itself routes the Telegram chat to.

import { describe, expect, it } from 'vitest'

import type { SessionListRow } from '../rest/HermesRest.js'
import { recordingFor, type CorpusEntry } from '../testing/corpus.js'
import { defaultTelegramLane, pickLaneRow, summarizeLanes } from './lanes.js'

const DM = 'agent:main:telegram:dm:111'

/** Every recorded `GET /api/sessions` listing (200 only), in order. */
function listings(entries: CorpusEntry[]): SessionListRow[][] {
  return entries
    .filter((entry) => entry.dir === 'in' && entry.kind === 'http' && /\/api\/sessions\?/.test(String(entry.frame.url)) && entry.frame.status === 200)
    .map((entry) => (entry.frame.body as { sessions: SessionListRow[] }).sessions)
}

/** The recorded ground truth the scenario wrote down ("tip after …" notes). */
function notedTip(entries: CorpusEntry[], text: string): string {
  const note = entries.find((entry) => entry.kind === 'note' && String(entry.frame.text).startsWith(text))!
  return String((note.frame.data as { tip: string }).tip)
}

describe('which conversation is the Telegram thread (recorded)', () => {
  it('T1: the phone\'s first message creates the DM lane, and it is picked', () => {
    const [first] = listings(recordingFor('T1', 'v0.22.0'))
    expect(pickLaneRow(first!, DM)?.source).toBe('telegram')
    expect(summarizeLanes(first!)).toHaveLength(1)
  })

  it('T6: after Telegram /new the lane moves to the NEW row, never back to the ended one', () => {
    const entries = recordingFor('T6', 'v0.22.0')
    const all = listings(entries)
    const tip = notedTip(entries, 'tip after /new')
    // Every listing taken after the /new must resolve to the new row.
    const afterNew = all.slice(1)
    expect(afterNew.length).toBeGreaterThan(0)
    for (const listing of afterNew) expect(pickLaneRow(listing, DM)?.id).toBe(tip)
  })

  it('T7: after compression the listing names the tip of the lineage', () => {
    const all = listings(recordingFor('T7', 'v0.22.0'))
    const before = pickLaneRow(all[0]!, DM)!.id
    const after = pickLaneRow(all.at(-1)!, DM)!.id
    expect(after).not.toBe(before)
  })

  it('T8: a DM topic is its own lane, and the default is the plain DM', () => {
    const all = listings(recordingFor('T8', 'v0.22.0'))
    const lanes = summarizeLanes(all.at(-1)!)
    expect(lanes.map((lane) => lane.sessionKey).sort()).toEqual([DM, `${DM}:7`])
    expect(lanes.find((lane) => lane.sessionKey === `${DM}:7`)?.threadId).toBe('7')
    expect(defaultTelegramLane(lanes)?.sessionKey).toBe(DM)
  })
})
