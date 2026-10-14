// Stage 0 LANE recorder (plan 2026-10-14-hermes-telegram-thread-and-commands
// §5 Stage 0, T1–T8): a real `hermes gateway run` with upstream's real
// Telegram adapter pointed at upstream's Telegram Bot API stand-in, plus a real
// `hermes serve` on the same home, driven by the naive pane driver
// (research/driver.ts). One file per scenario under
// research/corpus/<T…>/<release>-<run id>.jsonl, in the same tap format as
// record-wire.mts, so the same corpus loaders read it.
//
// Usage:
//   HERMES_LIVE_RUNTIME=<runtime with python-telegram-bot + aiohttp> \
//     node --import tsx research/record-lane.mts --release v0.22.0 [T1 …]
//
// The runtime needs the Telegram extra installed into its venv (the core lock
// export leaves it out; t2 installed the tag's pins):
//   uv pip install --python <runtime>/venv/bin/python 'python-telegram-bot[webhooks]==22.8' aiohttp==3.14.3
//
// WHAT a lane recording contains beyond the WS frames:
// - every REST exchange (bearer-authenticated `/api/sessions…`), via the tap;
// - `phone` notes: each simulated phone message, at the time it was sent;
// - `busy-eval` notes: each point where the future pane would evaluate the
//   busy rule (after each `sessions.changed`, before each submit, and 1 s
//   probes during phone turns), naming the RPC id of the `session.history`
//   reply it was computed from — research/lane-analyze.mts recomputes the
//   rule from that reply, never from anything the recorder concluded;
// - `db` notes: state.db rows at key points (fake content);
// - one `model-requests` note at the end: every chat request Hermes made, in
//   tap time, with which marker each user message carried. This is the
//   ground truth for "was a phone turn running at time t" (H3) and for "did
//   the model see the pane prompt twice" (T4/T5), as in t2's analyze.py.
// - one `telegram` note at the end: what the stand-in's phone shows, which
//   proves a pane reply was never delivered (T3).
//
// Marker convention (src/testing/scripted-model: say-/slow-/toolslow-):
// phone texts carry labels starting `phone`, pane texts `pane`, so a model
// request's owner is read from its last user message without guessing.

import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { startScriptedModel, type ScriptedModelServer } from '../src/testing/scripted-model/server.js'
import { removeLiveHome } from '../src/testing/live-hermes/harness.js'
import { LANE_USERS, startLaneHermes, type LaneHermes } from '../src/testing/live-hermes/lane.js'
import { RawPeer, eventType, mintTicket, passwordNativeLogin, probe, restCall, type Tokens } from './driver.js'
import { parseReleaseArgs, readProvenance, recordingFileName } from './provenance.js'
import { WireTap } from './tap.js'

const corpusRoot = fileURLToPath(new URL('./corpus/', import.meta.url))
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Ctx = {
  lane: LaneHermes
  tap: WireTap
  tokens: Tokens
  model: ScriptedModelServer
}

async function until<T>(fn: () => Promise<T | undefined | false> | T | undefined | false, label: string, ms = 60_000): Promise<T> {
  const end = Date.now() + ms
  while (Date.now() < end) {
    const value = await fn()
    if (value) return value
    await sleep(150)
  }
  throw new Error(`timed out: ${label}`)
}

/** Marker label of a chat message's text, e.g. `phone-seed` from `[[hh:say-phone-seed]]`. */
function markerLabel(text: string): string | undefined {
  const match = /\[\[hh:(?:say|slow|toolslow)-([a-z0-9-]+)\]\]/.exec(text)
  return match?.[1]
}

function textOf(content: unknown): string {
  if (typeof content === 'string') return content
  if (Array.isArray(content)) return content.map((part) => (part && typeof part === 'object' && 'text' in part ? String((part as { text: unknown }).text) : '')).join('')
  return ''
}

/**
 * The pane, as the plan's lane mode would drive it, minus all cleverness:
 * resume a stored id, refetch `session.history` on every `sessions.changed`
 * (single-flight, one trailing refetch), and say where the busy rule would be
 * evaluated. Nothing here decides "busy": the analysis does, from the replies.
 */
class Pane {
  peer!: RawPeer
  sid = ''
  stored = ''
  /** user_row_id of every prompt this pane submitted (the busy rule's "ours"). */
  readonly submitted: number[] = []
  private inFlight = false
  private dirty = false
  private pendingTrigger: string | undefined
  private probeTimer: ReturnType<typeof setInterval> | undefined
  private heartbeat: ReturnType<typeof setInterval> | undefined
  followChanges = true

  constructor(private readonly ctx: Ctx) {}

  async connect(): Promise<void> {
    const { tap, lane } = this.ctx
    const ticket = await mintTicket(tap, lane.serve.baseUrl, this.ctx.tokens.access_token)
    this.peer = new RawPeer(tap)
    const url = new URL('/api/ws', lane.serve.baseUrl)
    url.protocol = 'ws:'
    url.searchParams.set('ticket', ticket)
    await this.peer.open(url.toString())
    await this.peer.waitFor((frame) => eventType(frame) === 'gateway.ready', 15_000, true)
    await this.peer.call('client.capabilities', { server_requests: true })
    // t2 kept the socket alive with a ping every 10 s; the pane does the same.
    this.heartbeat = setInterval(() => {
      void this.peer.call('gateway.ping').catch(() => undefined)
    }, 10_000)
    this.peer.listeners.push((frame) => {
      if (eventType(frame) === 'sessions.changed' && this.followChanges && this.sid) void this.refetch('sessions.changed')
    })
  }

  async resume(stored: string): Promise<Record<string, unknown>> {
    const result = await this.peer.call<Record<string, unknown>>('session.resume', { session_id: stored, cols: 96 })
    this.sid = String(result.session_id)
    this.stored = String(result.stored_session_id ?? stored)
    return result
  }

  /** One `session.history` read, labelled as a busy-rule evaluation point. */
  async history(trigger: string): Promise<Record<string, unknown>> {
    const promise = this.peer.call<Record<string, unknown>>('session.history', { session_id: this.sid })
    const rpcId = this.peer.lastId
    const result = await promise
    this.ctx.tap.note('busy-eval', { trigger, rpcId, sid: this.sid, ours: [...this.submitted] })
    return result
  }

  private async refetch(trigger: string): Promise<void> {
    if (this.inFlight) {
      // The trailing refetch is labelled with the trigger that asked for it.
      // (First T2 run: a `sessions.changed` that landed during a probe read
      // was re-labelled "probe", hiding that the signal alone surfaced the
      // phone turn's final row.)
      this.dirty = true
      this.pendingTrigger = trigger
      return
    }
    this.inFlight = true
    try {
      let next = trigger
      do {
        this.dirty = false
        const current = next
        await this.history(current).catch((error: Error) => this.ctx.tap.note('history failed', { trigger: current, message: error.message }))
        next = this.pendingTrigger ?? trigger
        this.pendingTrigger = undefined
      } while (this.dirty)
    } finally {
      this.inFlight = false
    }
  }

  startProbe(everyMs = 1000): void {
    this.stopProbe()
    this.probeTimer = setInterval(() => void this.refetch('probe'), everyMs)
  }

  stopProbe(): void {
    if (this.probeTimer) clearInterval(this.probeTimer)
    this.probeTimer = undefined
  }

  /** `prompt.submit` preceded by a fresh history read (the "before submit" evaluation point). */
  async submit(text: string, extra: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    await this.history('before-submit')
    const result = await this.peer.call<Record<string, unknown>>('prompt.submit', { session_id: this.sid, text, ...extra })
    if (typeof result.user_row_id === 'number') this.submitted.push(result.user_row_id)
    this.ctx.tap.note('pane-submitted', { text, result: { status: result.status, user_row_id: result.user_row_id } })
    return result
  }

  complete(timeoutMs = 120_000): Promise<Record<string, unknown>> {
    return this.peer.waitFor((frame) => eventType(frame) === 'message.complete' && (frame.params as { session_id?: string }).session_id === this.sid, timeoutMs)
  }

  close(): void {
    this.stopProbe()
    if (this.heartbeat) clearInterval(this.heartbeat)
    this.peer?.close()
  }
}

async function phone(ctx: Ctx, text: string, opts: { userId?: number; threadId?: number } = {}): Promise<void> {
  // A note, not an `http` entry: the phone talks to the stand-in, never to
  // serve, and nothing a test replays should mistake it for a Hermes exchange.
  ctx.tap.note('phone', { text, user_id: opts.userId ?? LANE_USERS.owner, ...(opts.threadId === undefined ? {} : { thread_id: opts.threadId }) })
  await ctx.lane.dm(text, opts.userId, opts.threadId)
}

async function phoneAnswer(ctx: Ctx, label: string, opts: { userId?: number; timeoutMs?: number } = {}): Promise<void> {
  await ctx.lane.waitPhoneText(`ANSWER ${label}`, opts.userId, opts.timeoutMs ?? 60_000)
  ctx.tap.note('phone shows answer', { label })
}

async function dbNote(ctx: Ctx, label: string): Promise<{ sessions: Array<Record<string, unknown>>; messages: Array<Record<string, unknown>> }> {
  const snap = await ctx.lane.db()
  ctx.tap.note('db', {
    label,
    sessions: snap.sessions,
    messages: snap.messages.map((row) => ({ ...row, content: typeof row.content === 'string' && row.content.length > 300 ? `${row.content.slice(0, 300)}…` : row.content })),
  })
  return snap
}

/** Wait for a confirmation card on the phone and tap "Approve Once", as the owner would. */
async function confirmOnPhone(ctx: Ctx, command: string, timeoutMs = 30_000): Promise<void> {
  const pressed = await until(async () => {
    try {
      return await ctx.lane.press('Approve Once')
    } catch {
      return undefined
    }
  }, `confirmation card for ${command}`, timeoutMs)
  ctx.tap.note('phone taps confirmation', { command, ...pressed })
}

/** REST lane discovery, exactly as LaneResolver will do it (plan Stage D). */
async function listTelegram(ctx: Ctx): Promise<Array<Record<string, unknown>>> {
  const reply = await restCall(ctx.tap, ctx.lane.serve.baseUrl, '/api/sessions?source=telegram&order=recent&limit=50', ctx.tokens.access_token)
  const body = reply.body as { sessions?: Array<Record<string, unknown>> } | Array<Record<string, unknown>>
  return Array.isArray(body) ? body : body.sessions ?? []
}

async function latestDescendant(ctx: Ctx, id: string): Promise<Record<string, unknown>> {
  const reply = await restCall(ctx.tap, ctx.lane.serve.baseUrl, `/api/sessions/${encodeURIComponent(id)}/latest-descendant`, ctx.tokens.access_token)
  return { status: reply.status, ...(reply.body as Record<string, unknown>) }
}

const laneKey = (userId: number, threadId?: number) => `agent:main:telegram:dm:${userId}${threadId === undefined ? '' : `:${threadId}`}`

/** Live tip of a lane from the REST list (newest open row with this key). */
async function resolveTip(ctx: Ctx, key: string): Promise<string | undefined> {
  const rows = (await listTelegram(ctx)).filter((row) => row.session_key === key && (row.ended_at === null || row.ended_at === undefined))
  rows.sort((a, b) => Number(b.started_at ?? 0) - Number(a.started_at ?? 0))
  const top = rows[0]
  if (!top) return undefined
  const tip = await latestDescendant(ctx, String(top.id))
  return String(tip.session_id ?? top.id)
}

/** Phone seed → wait → discover via REST → resume. The common T-scenario prelude. */
async function seedAndAttach(ctx: Ctx, pane: Pane, label = 'phone-seed'): Promise<string> {
  await phone(ctx, `[[hh:say-${label}]] First message from the simulated phone.`)
  await phoneAnswer(ctx, label)
  const tip = await until(() => resolveTip(ctx, laneKey(LANE_USERS.owner)), 'lane row visible over REST', 20_000)
  await pane.resume(tip)
  await pane.history('attach')
  return tip
}

type Scenario = {
  id: string
  title: string
  /** Extra config.yaml keys for this scenario's home (T7: compression.in_place). */
  config?: Record<string, unknown>
  run(ctx: Ctx, pane: Pane): Promise<void>
}

const scenarios: Scenario[] = [
  {
    id: 'T1',
    title: 'lane discovery: phone seed, REST list with bearer, latest-descendant, resume, history',
    async run(ctx, pane) {
      await phone(ctx, '[[hh:say-phone-seed]] First message from the simulated phone.')
      await phoneAnswer(ctx, 'phone-seed')
      await dbNote(ctx, 'after-seed')
      // H1's negative half: the same route without a bearer must be refused.
      await restCall(ctx.tap, ctx.lane.serve.baseUrl, '/api/sessions?source=telegram&order=recent&limit=50', undefined)
      const rows = await until(async () => {
        const list = await listTelegram(ctx)
        return list.length ? list : undefined
      }, 'telegram rows over REST', 20_000)
      const lane = rows.find((row) => row.session_key === laneKey(LANE_USERS.owner))
      if (!lane) throw new Error(`no row with session_key ${laneKey(LANE_USERS.owner)} in ${JSON.stringify(rows)}`)
      const tip = await latestDescendant(ctx, String(lane.id))
      // WS session.list for comparison: it has no session_key (t1 F1b).
      await pane.peer.call('session.list', { limit: 50 })
      await pane.resume(String(tip.session_id ?? lane.id))
      await pane.history('attach')
    },
  },
  {
    id: 'T2',
    title: 'phone turns while the pane is attached: sessions.changed → session.history (plain and tool-using)',
    async run(ctx, pane) {
      await seedAndAttach(ctx, pane)
      for (const label of ['phone-live', 'phone-tooled']) {
        const kind = label === 'phone-tooled' ? 'toolslow' : 'say'
        pane.startProbe()
        await phone(ctx, `[[hh:${kind}-${label}]] Phone message while the pane is attached.`)
        await phoneAnswer(ctx, label, { timeoutMs: 90_000 })
        // Past the change watcher's 2 s debounce, so the final signal lands.
        await sleep(4000)
        pane.stopProbe()
        await pane.history('settled')
        await dbNote(ctx, `after-${label}`)
      }
    },
  },
  {
    id: 'T3',
    title: 'pane turn on the lane: prompt.submit → stream → complete → history; nothing reaches the phone',
    async run(ctx, pane) {
      await seedAndAttach(ctx, pane)
      const done = pane.complete()
      await pane.submit('[[hh:say-pane-turn]] Message from the pane.', { queued: true })
      await done
      await sleep(3000)
      await pane.history('after-own-turn')
      await dbNote(ctx, 'after-pane-turn')
      // Long enough for any delivery to have happened.
      await sleep(3000)
      const snap = await ctx.lane.snapshot()
      ctx.tap.note('telegram after pane turn', { visible: snap.visible.map((entry) => entry.text) })
    },
  },
  {
    id: 'T4',
    title: 'phone-first race: pane prompt.submit during a running phone turn (the duplicated-context negative fixture)',
    async run(ctx, pane) {
      await seedAndAttach(ctx, pane)
      const before = ctx.model.requests.length
      pane.startProbe()
      await phone(ctx, '[[hh:slow-phone-race]] Phone starts the concurrent race.')
      await until(() => ctx.model.requests.slice(before).some((request) => textOf(lastUser(request.body)?.content).includes('slow-phone-race')), 'gateway model request started', 30_000)
      const done = pane.complete(60_000)
      await pane.submit('[[hh:say-pane-race]] Pane submits during phone turn.', { queued: true })
      await phoneAnswer(ctx, 'phone-race', { timeoutMs: 60_000 })
      await done
      await sleep(4000)
      pane.stopProbe()
      await pane.history('settled')
      await dbNote(ctx, 'after-race')
    },
  },
  {
    id: 'T5',
    title: 'hold then send: wait for the phone turn\'s final assistant row in history, then submit',
    async run(ctx, pane) {
      await seedAndAttach(ctx, pane)
      const before = ctx.model.requests.length
      pane.startProbe()
      await phone(ctx, '[[hh:slow-phone-held]] Phone turn the pane must wait for.')
      await until(() => ctx.model.requests.slice(before).some((request) => textOf(lastUser(request.body)?.content).includes('slow-phone-held')), 'gateway model request started', 30_000)
      ctx.tap.note('hold: phone turn running; pane holds its prompt')
      // The hold: the pane reads history (sessions.changed + probes) until the
      // tail is the phone turn's final assistant row. Decided here from the
      // reply only, the same information the package will have.
      await until(async () => {
        const result = await pane.history('hold-check')
        const messages = (result.messages as Array<Record<string, unknown>> | undefined) ?? []
        const tail = messages.at(-1)
        return tail?.role === 'assistant' && String(tail.text ?? '').includes('ANSWER phone-held')
      }, 'phone turn final assistant row in history', 60_000)
      ctx.tap.note('hold released: tail is the phone turn final assistant row')
      const done = pane.complete(60_000)
      await pane.submit('[[hh:say-pane-held]] Pane prompt sent after the phone turn.', { queued: true })
      await done
      await sleep(4000)
      pane.stopProbe()
      await pane.history('settled')
      await dbNote(ctx, 'after-held')
    },
  },
  {
    id: 'T6',
    title: 'Telegram /new: old row ends session_reset, a new row takes the lane key; the old id still resumes',
    async run(ctx, pane) {
      const old = await seedAndAttach(ctx, pane)
      pane.startProbe(2000)
      await phone(ctx, '/new')
      // v0.22.0 answers /new with a "Confirm /new" card (Approve Once /
      // Always Approve / Cancel); the reset happens only once it is tapped.
      await confirmOnPhone(ctx, '/new')
      await until(async () => {
        const snap = await ctx.lane.db()
        return snap.sessions.find((row) => row.id === old)?.ended_at != null
      }, 'old lane row ended', 30_000)
      await sleep(4000)
      pane.stopProbe()
      await dbNote(ctx, 'after-new')
      await listTelegram(ctx)
      await latestDescendant(ctx, old)
      const tipAfterNew = await resolveTip(ctx, laneKey(LANE_USERS.owner))
      ctx.tap.note('tip after /new (REST)', { old, tip: tipAfterNew ?? null })
      await phone(ctx, '[[hh:say-phone-after-new]] First message after /new.')
      await phoneAnswer(ctx, 'phone-after-new')
      await sleep(3000)
      await dbNote(ctx, 'after-new-message')
      const tip = await until(() => resolveTip(ctx, laneKey(LANE_USERS.owner)), 'new lane tip over REST', 20_000)
      ctx.tap.note('tip after the first post-/new message (REST)', { old, tip })
      // The trap (t1 F2e): the old id still resumes. Only read it; a prompt
      // here would reopen the ended row (`_reopen_if_finalized`) and fork
      // the lane, which is exactly what lane mode must never do.
      await pane.resume(old)
      await pane.history('stale-old-id')
      // H4: the new tip's history starts at the reset, without the old rows.
      await pane.resume(tip)
      await pane.history('new-tip')
    },
  },
  {
    id: 'T7',
    title: 'compression on the lane (compression.in_place: false): the tip moves; resume and latest-descendant follow',
    config: { compression: { in_place: false } },
    async run(ctx, pane) {
      await compressionRun(ctx, pane)
    },
  },
  {
    id: 'T7b',
    title: 'compression on the lane with the DEFAULT compression.in_place: true (what a stock server does)',
    async run(ctx, pane) {
      await compressionRun(ctx, pane)
    },
  },
  {
    id: 'T8',
    title: 'DM topic lane (message_thread_id + is_topic_message) beside the plain DM lane: two session_keys',
    async run(ctx, pane) {
      await seedAndAttach(ctx, pane)
      await phone(ctx, '[[hh:say-phone-topic]] Message inside a DM topic.', { threadId: 7 })
      await phoneAnswer(ctx, 'phone-topic')
      await sleep(3000)
      await dbNote(ctx, 'after-topic')
      const rows = await listTelegram(ctx)
      ctx.tap.note('lanes over REST', { keys: rows.map((row) => ({ id: row.id, session_key: row.session_key, chat_type: row.chat_type, thread_id: row.thread_id, last_active: row.last_active })) })
      // The auto-pick rule (plan Decision 2): chat_type dm, no thread, most
      // recent. The topic lane is newer, so a "most recent telegram row"
      // rule would pick the wrong lane; this records the rows that decide it.
      const tip = await resolveTip(ctx, laneKey(LANE_USERS.owner))
      ctx.tap.note('auto-pick (dm, no thread)', { tip: tip ?? null })
      const sends = (await ctx.lane.snapshot()).sends.filter((call) => call.method === 'sendMessage')
      ctx.tap.note('sendMessage thread ids', { calls: sends.map((call) => ({ thread: call.params.message_thread_id ?? null, reply_to: call.params.reply_to_message_id ?? null })) })
      await pane.peer.call('session.list', { limit: 50 })
    },
  },
]

/**
 * 25 phone exchanges (enough for `/compress` to remove something: ≥ 4
 * messages and a middle worth more than the summary), the pane attached the whole time, then a
 * phone `/compress`. Records the tip (REST), resume of the PRE-compression id,
 * and history (H4: ancestors included), then one more phone turn.
 */
async function compressionRun(ctx: Ctx, pane: Pane): Promise<void> {
  const root = await seedAndAttach(ctx, pane)
  // 24 turns, not 5: the first T7 run with 5 phone turns came back
  // "Compression refused (summary would grow the conversation): 12 messages
  // preserved" — the same tail-budget effect S20 hit (see there).
  for (let turn = 1; turn <= 24; turn += 1) {
    await phone(ctx, `[[hh:say-phone-c${turn}]] Phone turn ${turn} before compression.`)
    await phoneAnswer(ctx, `phone-c${turn}`)
  }
  await sleep(3000)
  await dbNote(ctx, 'before-compress')
  pane.startProbe(2000)
  await phone(ctx, '/compress')
  await confirmOnPhone(ctx, '/compress', 15_000).catch((error: Error) => ctx.tap.note('no confirmation card for /compress', { message: error.message }))
  // The gateway answers /compress with a status message; wait for the DB to
  // change instead of matching its wording.
  const before = await ctx.lane.db()
  const beforeCount = before.sessions.length
  const beforeEnded = before.sessions.filter((row) => row.ended_at != null).length
  await until(async () => {
    const snap = await ctx.lane.db()
    const compacted = (snap.messages as Array<Record<string, unknown>>).length
    return snap.sessions.length !== beforeCount || snap.sessions.filter((row) => row.ended_at != null).length !== beforeEnded || compacted < before.messages.length
  }, 'compression visible in state.db', 60_000).catch((error: Error) => ctx.tap.note('compression not visible in sessions/messages', { message: error.message }))
  await sleep(5000)
  pane.stopProbe()
  await dbNote(ctx, 'after-compress')
  const telegramAfter = await ctx.lane.snapshot()
  ctx.tap.note('phone after /compress', { visible: telegramAfter.visible.map((entry) => entry.text).slice(-3) })
  await listTelegram(ctx)
  await latestDescendant(ctx, root)
  // Resume the id the pane held BEFORE compression: does it follow?
  await pane.resume(root)
  await pane.history('after-compress-resume-root')
  await phone(ctx, '[[hh:say-phone-after-compress]] Phone turn after compression.')
  await phoneAnswer(ctx, 'phone-after-compress')
  await sleep(3000)
  await dbNote(ctx, 'after-compress-turn')
  await latestDescendant(ctx, root)
  await pane.history('after-compress-turn')
}

function lastUser(body: unknown): { content?: unknown } | undefined {
  const messages = ((body as { messages?: Array<{ role: string; content?: unknown }> } | null)?.messages) ?? []
  for (let index = messages.length - 1; index >= 0; index -= 1) if (messages[index].role === 'user') return messages[index]
  return undefined
}

/** The model-request ground truth, in tap time (see the header). */
function modelRequestsNote(ctx: Ctx): void {
  const rows = ctx.model.requests
    .filter((request) => request.path.endsWith('/chat/completions'))
    .map((request, index) => {
      const body = (request.body ?? {}) as { messages?: Array<{ role: string; content?: unknown; tool_calls?: unknown[] }>; stream?: boolean }
      const messages = body.messages ?? []
      const last = lastUser(body)
      const users = messages.filter((message) => message.role === 'user').map((message) => textOf(message.content))
      const markerCounts: Record<string, number> = {}
      for (const text of users) {
        for (const match of text.matchAll(/\[\[hh:(?:say|slow|toolslow)-([a-z0-9-]+)\]\]/g)) markerCounts[match[1]] = (markerCounts[match[1]] ?? 0) + 1
      }
      return {
        index,
        startedAt: request.startedAt - ctx.tap.startedAt,
        finishedAt: request.finishedAt ? request.finishedAt - ctx.tap.startedAt : null,
        stream: body.stream === true,
        label: markerLabel(textOf(last?.content)) ?? null,
        roles: messages.map((message) => (message.role === 'assistant' && message.tool_calls?.length ? 'assistant+tool_calls' : message.role)),
        markerCounts,
        // The conversation the model saw, minus the system prompt (long and
        // not lane contract); user/assistant text truncated.
        conversation: messages.filter((message) => message.role !== 'system').map((message) => ({
          role: message.role,
          text: textOf(message.content).slice(0, 200),
          ...(message.tool_calls?.length ? { tool_calls: message.tool_calls.length } : {}),
        })),
      }
    })
  ctx.tap.note('model-requests', rows)
}

const { release, rest: wanted } = parseReleaseArgs(process.argv.slice(2), 'v0.22.0')
const provenance = readProvenance(release)
const selected = wanted.length ? scenarios.filter((scenario) => wanted.includes(scenario.id)) : scenarios
if (wanted.length && selected.length !== wanted.length) throw new Error(`unknown lane scenario(s): ${wanted.join(', ')}`)
const runId = new Date().toISOString().replace(/[:.]/g, '-')
const evidenceDir = process.env.LANE_EVIDENCE_DIR
for (const scenario of selected) {
  const model = await startScriptedModel()
  const dir = join(corpusRoot, scenario.id)
  mkdirSync(dir, { recursive: true })
  const tap = new WireTap(join(dir, recordingFileName(release, runId)))
  tap.note(scenario.title, {
    scenario: scenario.id,
    release: provenance.release,
    sourceCommit: provenance.sourceCommit,
    hermesSource: `${provenance.release} ${provenance.sourceCommit.slice(0, 8)}`,
    recordedAt: new Date().toISOString(),
    topology: 'hermes gateway run (real Telegram adapter → upstream TelegramStandin) + hermes serve, one HERMES_HOME',
    phone: { user_id: LANE_USERS.owner, lane_key: laneKey(LANE_USERS.owner) },
    config: scenario.config ?? {},
  })
  const started = Date.now()
  let lane: LaneHermes | undefined
  let pane: Pane | undefined
  const ctx = { tap, model } as Ctx
  try {
    lane = await startLaneHermes({ modelUrl: model.url, config: scenario.config })
    ctx.lane = lane
    tap.note('gateway connected to the stand-in; serve ready')
    await probe(tap, lane.serve.baseUrl)
    ctx.tokens = await passwordNativeLogin(tap, lane.serve.baseUrl, lane.serve.username, lane.serve.password)
    pane = new Pane(ctx)
    await pane.connect()
    await scenario.run(ctx, pane)
    tap.note('scenario finished')
    console.log(`${scenario.id} ok (${Math.round((Date.now() - started) / 1000)} s)`)
  } catch (error) {
    tap.note('scenario failed', { message: error instanceof Error ? error.message : String(error) })
    console.log(`${scenario.id} FAILED: ${error instanceof Error ? error.message : String(error)}`)
    process.exitCode = 1
  } finally {
    pane?.close()
    await sleep(300)
    if (lane) {
      const snap = await lane.snapshot().catch(() => undefined)
      if (snap) tap.note('telegram', { visible: snap.visible.map((entry) => entry.text), sends: snap.sends.length })
      await dbNote(ctx, 'final').catch(() => undefined)
    }
    modelRequestsNote(ctx)
    if (lane) {
      await lane.stop()
      if (evidenceDir) {
        // The gateway log is evidence for the report, never corpus: it holds
        // host paths and the stand-in token in URLs.
        const { copyFileSync, existsSync } = await import('node:fs')
        mkdirSync(evidenceDir, { recursive: true })
        if (existsSync(lane.gatewayLog)) copyFileSync(lane.gatewayLog, join(evidenceDir, `${scenario.id}-${release}-gateway.log`))
      }
      removeLiveHome(lane.home)
    }
    await model.close()
  }
}
