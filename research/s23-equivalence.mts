// S23 verdict (plan H2, r4 open question 1): is an idle `prompt.submit` with
// `queued: true` the same as a plain idle submit?
//
// Usage: node --import tsx research/s23-equivalence.mts [path/to/S23.jsonl]
// (default: the newest v0.22.0 S23 recording).
//
// HOW "the same" is judged: the server frames of each variant (from its
// `variant` note to its `variant finished` note) are reduced to a sequence of
// (kind, normalised body), where normalising removes only values that differ
// by construction between two sessions: runtime/stored session ids, RPC ids,
// timestamps, `seq`, durations, rates, global message row ids, and the random
// thinking-spinner text; global `*.changed` signals are set aside (below).
// Everything else — event
// order, event types, payload keys, texts, statuses, row ids — must match.
// Usage counters are per session (both variants report calls: 1), so they
// are compared as values; only avg latency/tps (timing) are volatile.

import { recordingFor, loadRecording, type CorpusEntry } from '../src/testing/corpus.js'

const path = process.argv[2]
const entries: CorpusEntry[] = path ? loadRecording(path) : recordingFor('S23', 'v0.22.0')

const VOLATILE_KEYS = new Set([
  'session_id', 'stored_session_id', 'id', 'timestamp', 'started_at', 'ended_at', 'seq', 'duration_s',
  'avg_latency_s', 'avg_tps', 'replay_epoch', 'task_id', 'last_activity_at', 'resumed',
  // state.db message ids are global autoincrement: the second session's rows
  // are numbered after the first's, so they differ by construction.
  'user_row_id', 'row_id', 'row_ids', 'final_assistant_row_id', 'turn_started_at',
])
function normalise(value: unknown, key?: string): unknown {
  if (key && VOLATILE_KEYS.has(key)) return '<volatile>'
  if (Array.isArray(value)) return value.map((item) => normalise(item))
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const [inner, child] of Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b))) out[inner] = normalise(child, inner)
    return out
  }
  return value
}

type Variant = { queued: boolean; frames: string[]; raw: Array<Record<string, unknown>>; globalSignals: string[] }

const variants: Variant[] = []
let current: Variant | undefined
for (const entry of entries) {
  if (entry.kind === 'note') {
    const text = String(entry.frame.text)
    const data = entry.frame.data as { queued?: boolean } | undefined
    if (text === 'variant') {
      current = { queued: Boolean(data?.queued), frames: [], raw: [], globalSignals: [] }
      variants.push(current)
    } else if (text === 'variant finished') {
      current = undefined
    }
    continue
  }
  if (!current || entry.kind !== 'ws' || entry.dir !== 'in') continue
  const frame = entry.frame
  const label = frame.method === 'event'
    ? `event ${(frame.params as { type: string }).type}`
    : 'result' in frame ? 'result' : 'error' in frame ? 'error' : `request ${String(frame.method)}`
  // `sessions.changed` / `projects.changed` are GLOBAL, payload-free change
  // signals (debounced, trailing edge; tui_gateway/change_watcher.py): whether
  // one lands inside a variant's window depends on what the PREVIOUS variant
  // wrote, not on this submit. The first run of this script compared them and
  // a lone `projects.changed` shifted every later position. Excluded from the
  // strict comparison and counted separately.
  if (/^event [a-z_]+\.changed$/.test(label)) {
    current.globalSignals.push(label.slice('event '.length))
    continue
  }
  current.raw.push(frame)
  let body = frame.method === 'event' ? (frame.params as { payload?: unknown }).payload ?? null : frame.result ?? frame.error ?? frame.params
  // The spinner text of `thinking.delta` is a random kaomoji per frame
  // ("(¬‿¬) reflecting...") — cosmetic, never contract.
  if (label === 'event thinking.delta') body = { text: '<spinner>' }
  // Hermes de-duplicates session titles across the profile: the SECOND
  // session to earn the same auto-title gets " #2" (it would whichever
  // variant ran second). Order-of-sessions, not queued-vs-plain.
  if (label === 'event session.title') {
    const payload = body as { title?: string }
    body = { ...payload, title: String(payload.title ?? '').replace(/ #\d+$/, '') }
  }
  current.frames.push(`${label} ${JSON.stringify(normalise(body))}`)
}

if (variants.length !== 2) throw new Error(`expected 2 variants, found ${variants.length}`)
const [queued, plain] = variants[0].queued ? [variants[0], variants[1]] : [variants[1], variants[0]]

const diffs: string[] = []
const length = Math.max(queued.frames.length, plain.frames.length)
for (let index = 0; index < length; index += 1) {
  if (queued.frames[index] !== plain.frames[index]) {
    diffs.push(`#${index}\n  queued: ${(queued.frames[index] ?? '<none>').slice(0, 400)}\n  plain:  ${(plain.frames[index] ?? '<none>').slice(0, 400)}`)
  }
}

function lastUsage(raw: Array<Record<string, unknown>>): Record<string, number> | undefined {
  for (let index = raw.length - 1; index >= 0; index -= 1) {
    const params = raw[index].params as { type?: string; payload?: { usage?: Record<string, number> } } | undefined
    if (params?.type === 'message.complete' && params.payload?.usage) return params.payload.usage
  }
  return undefined
}
const usageQ = lastUsage(queued.raw)
const usageP = lastUsage(plain.raw)

console.log(JSON.stringify({
  frames: { queued: queued.frames.length, plain: plain.frames.length },
  globalSignalsExcluded: { queued: queued.globalSignals, plain: plain.globalSignals },
  differingPositions: diffs.length,
  // Per-variant usage as reported by message.complete (per session; avg
  // latency/tps are timing and differ run to run).
  usage: { queued: usageQ, plain: usageP },
  verdict: diffs.length === 0 ? 'EQUIVALENT apart from volatile ids/timestamps/counters' : 'DIFFERENT',
}, null, 2))
for (const diff of diffs) console.log(diff)
