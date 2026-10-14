// Assertions and hypothesis checks over the lane recordings (T1–T8), carried
// over from t2's analyze.py (temp/hermes-research/scratch-t2/analyze.py) and
// extended with the plan's H3 busy-rule check.
//
// Usage: node --import tsx research/lane-analyze.mts [--release v0.22.0] [--json out.json]
//
// WHY a separate script and not package tests yet: Stage 0 produces evidence;
// the lane tests (Stage D) will assert on the same recordings through the
// package's own code. This script only reads recordings — it never re-derives
// a fact from source reading — and prints every check with the file line it
// rests on, so the plan's §13 verdicts cite lines, not impressions.

import { writeFileSync } from 'node:fs'

import { recordingFilesFor, loadRecording, type CorpusEntry } from '../src/testing/corpus.js'
import { parseReleaseArgs } from './provenance.js'

type Row = { role: string; text?: string; row_id?: number; name?: string }
type ModelRequest = {
  index: number
  startedAt: number
  finishedAt: number | null
  stream: boolean
  label: string | null
  markerCounts: Record<string, number>
}

const argv = process.argv.slice(2)
const jsonIndex = argv.indexOf('--json')
const jsonOut = jsonIndex >= 0 ? argv[jsonIndex + 1] : undefined
const { release } = parseReleaseArgs(argv.filter((_, index) => index !== jsonIndex && index !== jsonIndex + 1), 'v0.22.0')

const report: Record<string, unknown> = { release }
const checks: Array<{ scenario: string; check: string; ok: boolean; evidence: string }> = []

function check(scenario: string, name: string, ok: boolean, evidence: string): void {
  checks.push({ scenario, check: name, ok, evidence })
}

function file(scenario: string): { path: string; entries: CorpusEntry[] } | undefined {
  const files = recordingFilesFor(scenario, release)
  if (!files.length) return undefined
  const path = files[files.length - 1]
  return { path, entries: loadRecording(path) }
}

const lineOf = (entries: CorpusEntry[], entry: CorpusEntry) => entries.indexOf(entry) + 1
const notes = (entries: CorpusEntry[], text: string) => entries.filter((entry) => entry.kind === 'note' && entry.frame.text === text)
const eventsOf = (entries: CorpusEntry[], type: string) => entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in' && entry.frame.method === 'event' && (entry.frame.params as { type: string }).type === type)

function modelRequests(entries: CorpusEntry[]): ModelRequest[] {
  return (notes(entries, 'model-requests')[0]?.frame.data as ModelRequest[] | undefined) ?? []
}

/** The reply frame (and its line) for an RPC id sent on the lane pane. */
function replyFor(entries: CorpusEntry[], rpcId: string, before: number): { entry: CorpusEntry; result: Record<string, unknown> } | undefined {
  // RPC ids restart per socket; take the LAST reply with this id at or before
  // the note that named it (the note is written right after the reply).
  for (let index = before; index >= 0; index -= 1) {
    const entry = entries[index]
    if (entry.kind === 'ws' && entry.dir === 'in' && entry.frame.id === rpcId && 'result' in entry.frame) {
      return { entry, result: entry.frame.result as Record<string, unknown> }
    }
  }
  return undefined
}

const LEASE_TEXT = 'Another Hermes process is using this session'

/**
 * The plan's busy rule (Stage D, src/lane/laneBusy.ts to be), verbatim:
 * busy when the last visible row is a user row we did not submit, a tool row,
 * or an assistant row carrying tool_calls with no later row; also busy while a
 * lease-wait status was seen < 5 s ago; otherwise idle.
 */
function busyRule(rows: Row[], ours: number[], lastLeaseAt: number | undefined, at: number): { busy: boolean; why: string } {
  const tail = rows.at(-1)
  if (lastLeaseAt !== undefined && at - lastLeaseAt < 5000) return { busy: true, why: 'lease status < 5 s ago' }
  if (!tail) return { busy: false, why: 'empty history' }
  if (tail.role === 'user' && !(typeof tail.row_id === 'number' && ours.includes(tail.row_id))) return { busy: true, why: `tail user row ${tail.row_id} not ours` }
  if (tail.role === 'tool') return { busy: true, why: 'tail tool row' }
  if (tail.role === 'assistant' && (tail as { tool_calls?: unknown[] }).tool_calls?.length) return { busy: true, why: 'tail assistant with tool_calls' }
  return { busy: false, why: `tail ${tail.role}${tail.row_id === undefined ? '' : ` row ${tail.row_id}`}` }
}

/**
 * Turn windows from the model-request ground truth. Each streamed request
 * with a marker label is one window [start, finish]; a request whose input
 * ends in a `tool` message continues the previous window of the same label
 * (the gap between them is the tool running, still the same turn).
 *
 * WHY not "[first start, last finish] per label" (the first version): after
 * a compression the gateway's next request carries the summary, and the
 * summary repeats earlier markers, so a label's "last finish" jumped minutes
 * ahead and every read in between was scored phone-busy (T7 run 2).
 * Non-streamed requests are auxiliary (titles, the compression summary) and
 * never count as the phone turn running.
 */
function turnWindows(requests: ModelRequest[]): Array<{ label: string; start: number; end: number }> {
  const windows: Array<{ label: string; start: number; end: number }> = []
  const streamed = requests.filter((request) => request.stream && request.label).sort((a, b) => a.startedAt - b.startedAt)
  for (const request of streamed) {
    const end = request.finishedAt ?? Number.POSITIVE_INFINITY
    const roles = (request as ModelRequest & { roles?: string[] }).roles ?? []
    const previous = [...windows].reverse().find((window) => window.label === request.label)
    if (previous && roles.at(-1) === 'tool') previous.end = Math.max(previous.end, end)
    else windows.push({ label: request.label!, start: request.startedAt, end })
  }
  return windows
}

/**
 * A REFINED candidate, computed beside the plan's rule so Stage 0 can say
 * whether the change helps (it is not adopted here; the plan changes first):
 * - the lease-wait status makes the lane busy until Hermes says "Session is
 *   free" (the wait status is re-sent only every 15 s, so a fixed 5 s timer
 *   expires in the middle of a long phone turn);
 * - a tail user row that IS ours and has no `message.complete` yet is our own
 *   pending turn: the pane knows that from its own stream, so it is reported
 *   as `own-pending` and not scored.
 */
function refinedRule(rows: Row[], ours: number[], leaseWaiting: boolean, ownPending: boolean): { busy: boolean | null; why: string } {
  if (leaseWaiting) return { busy: true, why: 'lease wait not yet "Session is free"' }
  const tail = rows.at(-1)
  if (tail?.role === 'user' && typeof tail.row_id === 'number' && ours.includes(tail.row_id) && ownPending) return { busy: null, why: 'own pending turn' }
  const plain = busyRule(rows, ours, undefined, 0)
  return { busy: plain.busy, why: plain.why }
}

type Evaluation = {
  scenario: string
  line: number
  t: number
  trigger: string
  rule: 'busy' | 'idle'
  why: string
  truth: 'phone-busy' | 'own-busy' | 'idle'
  agrees: boolean | null
  edgeMs: number
  refined: 'busy' | 'idle' | 'own-pending'
  refinedWhy: string
  refinedAgrees: boolean | null
}

const evaluations: Evaluation[] = []

function evaluateBusy(scenario: string, entries: CorpusEntry[]): void {
  const windows = turnWindows(modelRequests(entries))
  const leaseAts = entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in' && entry.frame.method === 'event'
    && (entry.frame.params as { type: string }).type === 'status.update'
    && String(((entry.frame.params as { payload?: { text?: string } }).payload?.text) ?? '').includes(LEASE_TEXT)).map((entry) => entry.t)
  const statusAt = (needle: string) => entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in' && entry.frame.method === 'event'
    && (entry.frame.params as { type: string }).type === 'status.update'
    && String(((entry.frame.params as { payload?: { text?: string } }).payload?.text) ?? '').includes(needle)).map((entry) => entry.t)
  const freeAts = statusAt('Session is free')
  const submits = notes(entries, 'pane-submitted').map((entry) => entry.t)
  const completes = eventsOf(entries, 'message.complete').map((entry) => entry.t)
  entries.forEach((entry, index) => {
    if (entry.kind !== 'note' || entry.frame.text !== 'busy-eval') return
    const data = entry.frame.data as { trigger: string; rpcId: string; ours: number[] }
    const reply = replyFor(entries, data.rpcId, index)
    if (!reply) return
    const t = reply.entry.t
    const rows = (reply.result.messages as Row[] | undefined) ?? []
    const lastLeaseAt = leaseAts.filter((at) => at <= t).at(-1)
    const verdict = busyRule(rows, data.ours, lastLeaseAt, t)
    const inside = windows.filter((window) => t >= window.start && t <= window.end)
    const lastSubmitAt = submits.filter((at) => at <= t).at(-1)
    const ownPendingTruth = lastSubmitAt !== undefined && !completes.some((at) => at > lastSubmitAt && at <= t)
    // A pane prompt that was submitted and has not completed is the pane's
    // OWN pending turn (T4: it waits on the lease for seconds after the phone
    // turn ends); the pane knows that from its own submit, so it is not a
    // phone-busy/idle question and is not scored.
    const truth = inside.some((window) => window.label.startsWith('phone')) ? 'phone-busy'
      : inside.some((window) => window.label.startsWith('pane')) || ownPendingTruth ? 'own-busy' : 'idle'
    const edgeMs = Math.min(...windows.flatMap((window) => [Math.abs(t - window.start), Math.abs(t - window.end)]), Number.POSITIVE_INFINITY)
    const lastFree = freeAts.filter((at) => at <= t).at(-1)
    const leaseWaiting = lastLeaseAt !== undefined && (lastFree === undefined || lastFree < lastLeaseAt)
    const lastSubmit = submits.filter((at) => at <= t).at(-1)
    const ownPending = lastSubmit !== undefined && !completes.some((at) => at > lastSubmit && at <= t)
    const refined = refinedRule(rows, data.ours, leaseWaiting, ownPending)
    evaluations.push({
      scenario,
      line: lineOf(entries, reply.entry),
      t,
      trigger: data.trigger,
      rule: verdict.busy ? 'busy' : 'idle',
      why: verdict.why,
      truth,
      // The rule is only meant to detect PHONE turns; during our own turn the
      // pane knows it is busy from its own stream, so those are not scored.
      agrees: truth === 'own-busy' ? null : (verdict.busy === (truth === 'phone-busy')),
      edgeMs: Number.isFinite(edgeMs) ? edgeMs : -1,
      refined: refined.busy === null ? 'own-pending' : refined.busy ? 'busy' : 'idle',
      refinedWhy: refined.why,
      refinedAgrees: refined.busy === null || truth === 'own-busy' ? null : (refined.busy === (truth === 'phone-busy')),
    })
  })
}

function historyRepliesContaining(entries: CorpusEntry[], trigger: string, needle: string): CorpusEntry[] {
  const hits: CorpusEntry[] = []
  entries.forEach((entry, index) => {
    if (entry.kind !== 'note' || entry.frame.text !== 'busy-eval') return
    const data = entry.frame.data as { trigger: string; rpcId: string }
    if (data.trigger !== trigger) return
    const reply = replyFor(entries, data.rpcId, index)
    const rows = (reply?.result.messages as Row[] | undefined) ?? []
    if (reply && rows.some((row) => String(row.text ?? '').includes(needle))) hits.push(reply.entry)
  })
  return hits
}

function httpReplies(entries: CorpusEntry[], pathPart: string): CorpusEntry[] {
  return entries.filter((entry) => entry.kind === 'http' && entry.dir === 'in' && String(entry.frame.url).includes(pathPart))
}

// ---------- per scenario ----------

for (const scenario of ['T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T7b', 'T8']) {
  const loaded = file(scenario)
  if (!loaded) {
    check(scenario, 'recording exists', false, `no ${release} recording`)
    continue
  }
  const { path, entries } = loaded
  const short = path.split('research/corpus/')[1]
  const failed = notes(entries, 'scenario failed')[0]
  check(scenario, 'scenario finished', !failed && notes(entries, 'scenario finished').length === 1, failed ? `${short}:${lineOf(entries, failed)} ${JSON.stringify(failed.frame.data)}` : short)
  report[scenario] = { file: short, entries: entries.length, wsFrames: entries.filter((entry) => entry.kind === 'ws').length }
  evaluateBusy(scenario, entries)
  const requests = modelRequests(entries)
  const sessionsList = httpReplies(entries, '/api/sessions?source=telegram')

  if (scenario === 'T1') {
    const unauth = sessionsList.find((entry) => entry.frame.status === 401)
    const authed = sessionsList.find((entry) => entry.frame.status === 200)
    const row = ((authed?.frame.body as { sessions?: Array<Record<string, unknown>> } | undefined)?.sessions ?? [])[0]
    check(scenario, 'H1: REST without bearer is refused (401)', Boolean(unauth), unauth ? `${short}:${lineOf(entries, unauth)}` : 'missing')
    const cols = ['session_key', 'chat_type', 'thread_id', 'ended_at', 'end_reason', 'parent_session_id', 'chat_id']
    check(scenario, 'H1: REST with the native bearer returns session_key, chat_type, thread_id, ended_at', Boolean(row) && cols.every((col) => col in (row ?? {})),
      authed ? `${short}:${lineOf(entries, authed)} session_key=${row?.session_key} chat_type=${row?.chat_type} thread_id=${row?.thread_id} ended_at=${row?.ended_at}` : 'missing')
    const desc = httpReplies(entries, '/latest-descendant')[0]
    check(scenario, 'latest-descendant shape {requested_session_id, session_id, path, changed}', Boolean(desc) && ['requested_session_id', 'session_id', 'path', 'changed'].every((key) => key in (desc!.frame.body as object)),
      desc ? `${short}:${lineOf(entries, desc)}` : 'missing')
    const list = entries.find((entry) => entry.kind === 'ws' && entry.dir === 'in' && Array.isArray((entry.frame.result as { sessions?: unknown[] } | undefined)?.sessions))
    const wsRow = ((list?.frame.result as { sessions?: Array<Record<string, unknown>> } | undefined)?.sessions ?? [])[0]
    check(scenario, 'WS session.list has no session_key (REST is required)', Boolean(wsRow) && !('session_key' in (wsRow ?? {})), list ? `${short}:${lineOf(entries, list)}` : 'missing')
  }

  if (scenario === 'T2') {
    const changed = eventsOf(entries, 'sessions.changed')
    const phoneSends = notes(entries, 'phone')
    report.T2sessionsChanged = changed.map((entry) => ({ line: lineOf(entries, entry), t: entry.t }))
    for (const label of ['phone-live', 'phone-tooled']) {
      // H5 as lane mode needs it: after the phone turn's last model call
      // finished (its final row is committed right after), a sessions.changed
      // arrives, and the first history read after that signal shows the row.
      // Read from frames, not from the recorder's trigger labels: the first
      // recording's single-flight refetch mislabelled a signal-driven read
      // as "probe" (fixed in record-lane.mts).
      const lastFinish = Math.max(...requests.filter((request) => request.stream && request.label === label).map((request) => request.finishedAt ?? 0))
      const signal = changed.find((entry) => entry.t >= lastFinish)
      const after = signal ? entries.slice(entries.indexOf(signal)).find((entry) => entry.kind === 'ws' && entry.dir === 'in'
        && Array.isArray((entry.frame.result as { messages?: unknown[] } | undefined)?.messages)) : undefined
      const shows = Boolean(after && ((after.frame.result as { messages: Row[] }).messages).some((row) => String(row.text ?? '').includes(`ANSWER ${label}`)))
      check(scenario, `H5: a sessions.changed follows the final commit of ${label} and the next history read shows it`, Boolean(signal) && shows,
        signal ? `${short}:${lineOf(entries, signal)} (signal t=${signal.t}, last model call finished t=${lastFinish}, delay ${signal.t - lastFinish} ms); history ${after ? `${short}:${lineOf(entries, after)}` : 'none'}` : 'no signal')
    }
    const contentEvents = entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in' && entry.frame.method === 'event'
      && /^message\./.test((entry.frame.params as { type: string }).type))
    check(scenario, 'phone turns produce no message.* events on the pane socket', contentEvents.length === 0, `${contentEvents.length} message.* events`)
    // Per phone send: how many sessions.changed arrived before the next send.
    const perSend = phoneSends.map((send, index) => {
      const next = phoneSends[index + 1]?.t ?? Number.POSITIVE_INFINITY
      return { phone: (send.frame.data as { text: string }).text.slice(0, 40), t: send.t, sessionsChanged: changed.filter((entry) => entry.t > send.t && entry.t < next).map((entry) => entry.t) }
    })
    report.T2perPhoneSend = perSend
    // DB message_count transitions vs signals.
    report.T2db = notes(entries, 'db').map((entry) => ({ line: lineOf(entries, entry), label: (entry.frame.data as { label: string }).label, counts: ((entry.frame.data as { sessions: Array<{ id: string; message_count: number }> }).sessions).map((row) => row.message_count) }))
  }

  if (scenario === 'T3') {
    const tg = notes(entries, 'telegram after pane turn')[0]
    const visible = (tg?.frame.data as { visible?: string[] } | undefined)?.visible ?? []
    check(scenario, 'pane reply is NOT delivered to the phone', Boolean(tg) && !visible.some((text) => text.replace(/\\(.)/g, '$1').includes('ANSWER pane-turn')),
      tg ? `${short}:${lineOf(entries, tg)} visible=${JSON.stringify(visible)}` : 'missing')
    const complete = eventsOf(entries, 'message.complete')[0]
    check(scenario, 'pane turn streamed and completed', Boolean(complete) && String((complete?.frame.params as { payload?: { text?: string } }).payload?.text).includes('ANSWER pane-turn'),
      complete ? `${short}:${lineOf(entries, complete)}` : 'missing')
  }

  if (scenario === 'T4' || scenario === 'T5') {
    const paneLabel = scenario === 'T4' ? 'pane-race' : 'pane-held'
    const phoneLabel = scenario === 'T4' ? 'phone-race' : 'phone-held'
    const pane = requests.filter((request) => request.stream && request.label === paneLabel)
    const phone = requests.filter((request) => request.stream && request.label === phoneLabel)
    const occurrences = pane.map((request) => request.markerCounts[paneLabel] ?? 0)
    const note = notes(entries, 'model-requests')[0]
    const ev = note ? `${short}:${lineOf(entries, note)}` : 'missing'
    if (scenario === 'T4') {
      check(scenario, 'T4 (negative fixture): the pane model request contains the pane prompt TWICE', occurrences.length === 1 && occurrences[0] === 2, `${ev} occurrences=${JSON.stringify(occurrences)}`)
    } else {
      check(scenario, 'T5 (positive fixture): no model request contains the pane prompt twice', requests.every((request) => (request.markerCounts[paneLabel] ?? 0) <= 1) && occurrences.length === 1, `${ev} occurrences=${JSON.stringify(occurrences)}`)
      const released = notes(entries, 'hold released: tail is the phone turn final assistant row')[0]
      const submitted = notes(entries, 'pane-submitted')[0]
      check(scenario, 'T5: submit sent only after the phone turn\'s final row was in history', Boolean(released && submitted && released.t <= submitted.t),
        released ? `${short}:${lineOf(entries, released)}` : 'missing')
    }
    if (pane[0] && phone.length) {
      const phoneEnd = Math.max(...phone.map((request) => request.finishedAt ?? 0))
      check(scenario, 'model calls do not overlap (lease serialises gateway and serve)', pane[0].startedAt >= phoneEnd, `${ev} phone ends ${phoneEnd}, pane starts ${pane[0].startedAt} (gap ${pane[0].startedAt - phoneEnd} ms)`)
    }
    const lease = entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in' && entry.frame.method === 'event'
      && String(((entry.frame.params as { payload?: { text?: string } }).payload?.text) ?? '').match(/Another Hermes process|Session is free/))
    report[`${scenario}leaseStatuses`] = lease.map((entry) => ({ line: lineOf(entries, entry), t: entry.t, text: ((entry.frame.params as { payload?: { text?: string } }).payload?.text) }))
    const submitNote = notes(entries, 'pane-submitted')[0]
    report[`${scenario}submit`] = submitNote ? { line: lineOf(entries, submitNote), t: submitNote.t, data: submitNote.frame.data } : null
  }

  if (scenario === 'T6') {
    const after = notes(entries, 'db').find((entry) => (entry.frame.data as { label: string }).label === 'after-new-message')
    const sessions = (after?.frame.data as { sessions?: Array<Record<string, unknown>> } | undefined)?.sessions ?? []
    const ended = sessions.find((row) => row.end_reason === 'session_reset')
    const fresh = sessions.find((row) => row.ended_at == null && row.session_key === 'agent:main:telegram:dm:111')
    check(scenario, 'old row ended with end_reason session_reset; a new open row has the same session_key', Boolean(ended && fresh && ended.session_key === fresh.session_key),
      after ? `${short}:${lineOf(entries, after)} ended=${ended?.id} new=${fresh?.id} parent=${fresh?.parent_session_id}` : 'missing')
    const tipNote = notes(entries, 'tip after /new (REST)')[0]
    report.T6tipImmediatelyAfterNew = tipNote ? { line: lineOf(entries, tipNote), data: tipNote.frame.data } : null
    const desc = httpReplies(entries, '/latest-descendant')[0]
    report.T6latestDescendantOfOld = desc ? { line: lineOf(entries, desc), body: desc.frame.body } : null
    const stale = entries.findIndex((entry) => entry.kind === 'note' && entry.frame.text === 'busy-eval' && (entry.frame.data as { trigger: string }).trigger === 'stale-old-id')
    const staleReply = stale >= 0 ? replyFor(entries, (entries[stale].frame.data as { rpcId: string }).rpcId, stale) : undefined
    check(scenario, 'the pre-/new id still resumes and returns its history (the stale-id trap)', Boolean(staleReply && ((staleReply.result.messages as Row[]) ?? []).length > 0),
      staleReply ? `${short}:${lineOf(entries, staleReply.entry)}` : 'missing')
    const newTip = entries.findIndex((entry) => entry.kind === 'note' && entry.frame.text === 'busy-eval' && (entry.frame.data as { trigger: string }).trigger === 'new-tip')
    const newReply = newTip >= 0 ? replyFor(entries, (entries[newTip].frame.data as { rpcId: string }).rpcId, newTip) : undefined
    const newRows = ((newReply?.result.messages as Row[] | undefined) ?? [])
    check(scenario, 'H4: history of the new tip has no pre-/new rows', Boolean(newReply) && !newRows.some((row) => String(row.text ?? '').includes('phone-seed')),
      newReply ? `${short}:${lineOf(entries, newReply.entry)} rows=${JSON.stringify(newRows.map((row) => `${row.role}:${String(row.text ?? row.name ?? '').slice(0, 30)}`))}` : 'missing')
  }

  if (scenario === 'T7' || scenario === 'T7b') {
    const afterDb = notes(entries, 'db').find((entry) => (entry.frame.data as { label: string }).label === 'after-compress')
    const sessions = (afterDb?.frame.data as { sessions?: Array<Record<string, unknown>> } | undefined)?.sessions ?? []
    report[`${scenario}sessionsAfterCompress`] = { line: afterDb ? lineOf(entries, afterDb) : null, sessions: sessions.map((row) => ({ id: row.id, ended_at: row.ended_at, end_reason: row.end_reason, parent: row.parent_session_id, key: row.session_key, count: row.message_count })) }
    const descs = httpReplies(entries, '/latest-descendant').map((entry) => ({ line: lineOf(entries, entry), body: entry.frame.body }))
    report[`${scenario}latestDescendant`] = descs
    const phoneAfter = notes(entries, 'phone after /compress')[0]
    report[`${scenario}phoneAfterCompress`] = phoneAfter?.frame.data
    const resumeEval = entries.findIndex((entry) => entry.kind === 'note' && entry.frame.text === 'busy-eval' && (entry.frame.data as { trigger: string }).trigger === 'after-compress-resume-root')
    const resumeReply = resumeEval >= 0 ? replyFor(entries, (entries[resumeEval].frame.data as { rpcId: string }).rpcId, resumeEval) : undefined
    const resumeCall = entries.slice(0, resumeEval).reverse().find((entry) => entry.kind === 'ws' && entry.dir === 'in' && (entry.frame.result as { resumed?: string } | undefined)?.resumed !== undefined)
    report[`${scenario}resumeRoot`] = resumeCall ? { line: lineOf(entries, resumeCall), resumed: (resumeCall.frame.result as { resumed?: string }).resumed, session_id: (resumeCall.frame.result as { session_id?: string }).session_id } : null
    const rows = ((resumeReply?.result.messages as Row[] | undefined) ?? [])
    report[`${scenario}historyAfterResume`] = { line: resumeReply ? lineOf(entries, resumeReply.entry) : null, rows: rows.map((row) => `${row.role}:${String(row.text ?? row.name ?? '').slice(0, 40)}`) }
    const rotated = sessions.some((row) => row.end_reason === 'compression')
    if (scenario === 'T7') {
      check(scenario, 'compression (in_place:false) rotated the lane: parent ended "compression", child holds the key', rotated, afterDb ? `${short}:${lineOf(entries, afterDb)}` : 'missing')
      check(scenario, 'latest-descendant of the pre-compression id moves (changed:true)', descs.some((desc) => (desc.body as { changed?: boolean }).changed === true), JSON.stringify(descs.map((desc) => desc.line)))
      check(scenario, 'H4: history after resuming the pre-compression id includes pre-compression rows', rows.some((row) => String(row.text ?? '').includes('phone-seed') || String(row.text ?? '').includes('phone-c1')),
        resumeReply ? `${short}:${lineOf(entries, resumeReply.entry)}` : 'missing')
    } else {
      check(scenario, 'default in-place compression keeps the same id (no rotation)', !rotated, afterDb ? `${short}:${lineOf(entries, afterDb)}` : 'missing')
    }
  }

  if (scenario === 'T8') {
    const lanes = notes(entries, 'lanes over REST')[0]
    const keys = ((lanes?.frame.data as { keys?: Array<{ session_key: string; thread_id: unknown }> } | undefined)?.keys ?? [])
    check(scenario, 'a DM topic is a second lane: session_key …:dm:111:7 beside …:dm:111', keys.some((key) => key.session_key === 'agent:main:telegram:dm:111:7') && keys.some((key) => key.session_key === 'agent:main:telegram:dm:111'),
      lanes ? `${short}:${lineOf(entries, lanes)} ${JSON.stringify(keys)}` : 'missing')
    const pick = notes(entries, 'auto-pick (dm, no thread)')[0]
    report.T8autoPick = pick ? { line: lineOf(entries, pick), data: pick.frame.data } : null
    const threads = notes(entries, 'sendMessage thread ids')[0]
    report.T8sendThreads = threads?.frame.data
  }
}

// ---------- H3 summary ----------
const scored = evaluations.filter((evaluation) => evaluation.agrees !== null)
const disagreements = scored.filter((evaluation) => !evaluation.agrees)
report.H3 = {
  evaluations: evaluations.length,
  scored: scored.length,
  ownTurnNotScored: evaluations.length - scored.length,
  disagreements,
  byTrigger: Object.fromEntries([...new Set(evaluations.map((evaluation) => evaluation.trigger))].map((trigger) => {
    const subset = scored.filter((evaluation) => evaluation.trigger === trigger)
    return [trigger, { scored: subset.length, wrong: subset.filter((evaluation) => !evaluation.agrees).length }]
  })),
}
const refinedScored = evaluations.filter((evaluation) => evaluation.refinedAgrees !== null)
report.H3refined = {
  scored: refinedScored.length,
  disagreements: refinedScored.filter((evaluation) => !evaluation.refinedAgrees),
}
report.evaluations = evaluations
report.checks = checks

for (const item of checks) console.log(`${item.ok ? 'PASS' : 'FAIL'} ${item.scenario} ${item.check}\n     ${item.evidence}`)
console.log(`\nH3: ${evaluations.length} evaluations, ${scored.length} scored, ${disagreements.length} disagreements`)
for (const item of disagreements) console.log(`  ${item.scenario} line ${item.line} t=${item.t} trigger=${item.trigger}: rule=${item.rule} (${item.why}) truth=${item.truth} edge=${item.edgeMs} ms`)
const refinedWrong = refinedScored.filter((evaluation) => !evaluation.refinedAgrees)
console.log(`\nH3 refined candidate: ${refinedScored.length} scored, ${refinedWrong.length} disagreements`)
for (const item of refinedWrong) console.log(`  ${item.scenario} line ${item.line} t=${item.t} trigger=${item.trigger}: refined=${item.refined} (${item.refinedWhy}) truth=${item.truth} edge=${item.edgeMs} ms`)
if (jsonOut) writeFileSync(jsonOut, JSON.stringify(report, null, 2))
