// A scripted OpenAI-compatible model endpoint for driving a REAL `hermes serve`.
//
// WHY this exists (plan Ruling 2026-10-13): what Agent Code must get right is
// the wire between itself and Hermes. The model behind Hermes is irrelevant to
// that wire, but a real model makes every turn non-deterministic and needs a
// paid key. Pointing Hermes' `provider: custom` at this server keeps Hermes
// 100% real (agent loop, tools, approvals, persistence, replay) while making
// each scenario repeatable: the same prompt always yields the same tool calls.
//
// HOW a scenario is chosen: the newest user message carries a marker
// `[[hh:<scenario>]]`. The step inside a scenario is derived from the request
// itself (how many assistant turns follow that user message), so the server is
// stateless and two concurrent sessions can never cross scripts.
//
// WHY tool calls name real Hermes tools (terminal, clarify, skill_view): the
// approval / clarify / sudo / secret prompts are produced by Hermes' own tool
// implementations. Calling them is the only honest way to get real prompts.

import http from 'node:http'
import type { AddressInfo } from 'node:net'

type ChatMessage = { role: string; content?: unknown; tool_calls?: unknown[] }
type ToolCall = { name: string; arguments: Record<string, unknown> }
type Step =
  // `headers`: extra HTTP response headers (S21's credits headers).
  | { kind: 'text'; chunks: string[]; delayMs?: number; headers?: Record<string, string> }
  | { kind: 'tools'; calls: ToolCall[] }
  | { kind: 'http-error'; status: number; body: unknown }

/**
 * The scenario table. Each entry lists the steps in order; step N answers the
 * request that follows N assistant turns after the marked user message. A
 * request past the last step gets the final text step again.
 */
export const SCENARIOS: Record<string, Step[]> = {
  plain: [{ kind: 'text', chunks: ['Hello', ' from', ' the', ' scripted', ' model.'], delayMs: 40 }],
  // Long enough to drop and resume the socket mid-stream (S8/S9) and to
  // interrupt it (S7): ~40 chunks over ~24 s.
  slow: [{
    kind: 'text',
    chunks: Array.from({ length: 40 }, (_, index) => `chunk-${String(index + 1).padStart(2, '0')} `),
    delayMs: 600,
  }],
  tools: [
    {
      kind: 'tools',
      calls: [
        { name: 'terminal', arguments: { command: 'echo hh-tools-ok' } },
        { name: 'read_file', arguments: { path: '/nonexistent/hh-missing.txt' } },
      ],
    },
    { kind: 'tools', calls: [{ name: 'terminal', arguments: { command: 'sleep 3 && echo hh-long-ok' } }] },
    { kind: 'text', chunks: ['All', ' three', ' tools', ' ran.'], delayMs: 30 },
  ],
  // `rm -r…` matches DANGEROUS_PATTERNS ("recursive delete") in
  // tools/approval_detection.py, so the terminal tool asks for approval.
  // The path is a throwaway under the recorder's own temp root.
  approval: [
    { kind: 'tools', calls: [{ name: 'terminal', arguments: { command: 'rm -r /tmp/hh-approval-probe-dir-does-not-exist' } }] },
    { kind: 'text', chunks: ['Approval', ' path', ' finished.'], delayMs: 30 },
  ],
  clarify: [
    {
      kind: 'tools',
      calls: [{
        name: 'clarify',
        arguments: {
          questions: [
            { question: 'Which color?', choices: ['red', 'blue'] },
            { question: 'Which letters?', choices: ['a', 'b', 'c'], multi_select: true },
            { question: 'Anything else to add?' },
          ],
        },
      }],
    },
    { kind: 'text', chunks: ['Thanks', ' for', ' answering.'], delayMs: 30 },
  ],
  sudo: [
    { kind: 'tools', calls: [{ name: 'terminal', arguments: { command: 'sudo echo hh-sudo-probe' } }] },
    { kind: 'text', chunks: ['Sudo', ' path', ' finished.'], delayMs: 30 },
  ],
  // `hh-secret` is a skill the harness plants in HERMES_HOME/skills whose
  // frontmatter requires an unset environment variable; viewing it makes
  // tools/skills_tool_setup.py ask the client for the secret.
  secret: [
    { kind: 'tools', calls: [{ name: 'skill_view', arguments: { name: 'hh-secret' } }] },
    { kind: 'text', chunks: ['Secret', ' path', ' finished.'], delayMs: 30 },
  ],
  // A non-retryable provider failure (S15).
  error: [{ kind: 'http-error', status: 400, body: { error: { message: 'scripted bad request', type: 'invalid_request_error', code: 'scripted_error' } } }],
  // S21 notification.show / notification.clear. Hermes reads Nous credit
  // headers off ANY streamed chat response (agent/chat_completion_helpers.py
  // `_capture_credits`; agent/credits_tracker.py checks no provider), and a
  // depleted → restored transition is the only notification a scripted
  // local turn can produce in v0.22.0 (the other emitters are a >30 s agent
  // build and a dropped mid-turn model switch). Values from upstream's own
  // tests/agent/test_credits_tracker.py DEPLETED_HEADERS / _base_headers.
  'credits-out': [{ kind: 'text', chunks: ['Credits', ' depleted.'], delayMs: 30, headers: creditHeaders(false) }],
  'credits-ok': [{ kind: 'text', chunks: ['Credits', ' restored.'], delayMs: 30, headers: creditHeaders(true) }],
  // S21 review.summary: `/refine [[hh:review]]` forks a background review
  // whose last user message carries the marker (agent/background_review.py);
  // a successful memory write inside the fork is what makes Hermes publish
  // "💾 Self-improvement review: Memory updated".
  review: [
    { kind: 'tools', calls: [{ name: 'memory', arguments: { action: 'add', target: 'memory', content: 'hh scripted fact: the probe colour is blue' } }] },
    { kind: 'text', chunks: ['Saved.'], delayMs: 30 },
  ],
  // S21 memory write in an ordinary turn (only tool.start/tool.complete).
  memwrite: [
    { kind: 'tools', calls: [{ name: 'memory', arguments: { action: 'add', target: 'memory', content: 'hh scripted fact: the pane wrote this' } }] },
    { kind: 'text', chunks: ['Memory', ' written.'], delayMs: 30 },
  ],
  // S21 subagent.start / subagent.complete: the child's first user message is
  // the goal (tools/delegate_tool_child_run.py), so its marker selects
  // `say-child` and the child answers "ANSWER child".
  // S26/S27 connection.request: Hermes' manage_connections tool opens a
  // connection operation (tools/connectors) whose card a client must draw.
  // Bundled catalog entries, so nothing leaves the machine: n8n-official asks
  // for a credential (the card's form), airtable is OAuth (the card's browser
  // leg). The turn waits on the card until it is answered or times out.
  'connect-form': [
    { kind: 'tools', calls: [{ name: 'manage_connections', arguments: { action: 'install', connectors: [{ name: 'n8n-official', mcp: true }] } }] },
    { kind: 'text', chunks: ['Connection', ' step done.'], delayMs: 30 },
  ],
  'connect-oauth': [
    { kind: 'tools', calls: [{ name: 'manage_connections', arguments: { action: 'install', connectors: [{ name: 'airtable', mcp: true }] } }] },
    { kind: 'text', chunks: ['Connection', ' step done.'], delayMs: 30 },
  ],
  delegate: [
    { kind: 'tools', calls: [{ name: 'delegate_task', arguments: { tasks: [{ goal: '[[hh:say-child]] Say hi from the child.', context: 'none' }] } }] },
    { kind: 'text', chunks: ['Dispatched.'], delayMs: 30 },
  ],
}

function creditHeaders(paidAccess: boolean): Record<string, string> {
  return {
    'x-nous-credits-version': '1',
    'x-nous-credits-remaining-micros': '0',
    'x-nous-credits-remaining-usd': '0.00',
    'x-nous-credits-subscription-micros': '0',
    'x-nous-credits-subscription-usd': '0.00',
    'x-nous-credits-rollover-micros': '0',
    'x-nous-credits-purchased-micros': '0',
    'x-nous-credits-purchased-usd': '0.00',
    'x-nous-tool-pool-micros': '0',
    'x-nous-tool-pool-gated-off': 'false',
    'x-nous-credits-denominator-kind': 'none',
    'x-nous-credits-paid-access': String(paidAccess),
    'x-nous-credits-as-of-ms': String(Date.now()),
    ...(paidAccess ? {} : { 'x-nous-credits-disabled-reason': 'out_of_credits' }),
  }
}

const MARKER = /\[\[hh:([a-z0-9-]+)\]\]/

/**
 * Parameterised markers for the lane recordings (research/record-lane.mts)
 * and the S17+ scenarios, so every phone/pane message gets a reply that
 * names it without editing the table above per run. (t2 did that by
 * mutating SCENARIOS from its driver; a stateless rule keeps two concurrent
 * processes — gateway and serve — on one script by construction.)
 *
 * - `say-<label>`: `ANSWER <label>` after 150 ms (a quick turn);
 * - `slow-<label>`: `ANSWER <label>` after 6 s (long enough to race: the
 *   pane submits while the phone turn is still in the model call, t2 E);
 * - `toolslow-<label>`: a terminal tool call (`sleep 2`), then the 6 s answer,
 *   so the durable tail passes through assistant(tool_calls) and tool rows
 *   while busy (plan U5: does the busy rule hold with tool-using turns?).
 *
 * WHY the answer carries no punctuation or hyphen beyond the label: Telegram
 * MarkdownV2 escaping turned `-` into `\-` in t2 run2 and broke a text match.
 */
function derivedSteps(name: string): Step[] | undefined {
  const match = /^(say|slow|toolslow)-([a-z0-9-]+)$/.exec(name)
  if (!match) return undefined
  const answer: Step = { kind: 'text', chunks: [`ANSWER ${match[2]}`], delayMs: match[1] === 'say' ? 150 : 6000 }
  if (match[1] !== 'toolslow') return [answer]
  return [{ kind: 'tools', calls: [{ name: 'terminal', arguments: { command: `sleep 2 && echo hh-${match[2]}` } }] }, answer]
}

function textOf(content: unknown): string {
  if (typeof content === 'string') return content
  if (Array.isArray(content)) {
    return content.map((part) => (part && typeof part === 'object' && 'text' in part ? String((part as { text: unknown }).text) : '')).join('')
  }
  return ''
}

/** Scenario + step for a chat request, or null when no marker is present. */
export function selectStep(messages: ChatMessage[]): { scenario: string; step: Step; index: number } | null {
  let lastUser = -1
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (messages[index].role === 'user') {
      lastUser = index
      break
    }
  }
  if (lastUser < 0) return null
  const match = MARKER.exec(textOf(messages[lastUser].content))
  if (!match) return null
  const steps = SCENARIOS[match[1]] ?? derivedSteps(match[1])
  if (!steps) return null
  const assistantTurns = messages.slice(lastUser + 1).filter((message) => message.role === 'assistant').length
  const index = Math.min(assistantTurns, steps.length - 1)
  return { scenario: match[1], step: steps[index], index }
}

function chunk(model: string, delta: Record<string, unknown>, finish: string | null, usage?: unknown): string {
  return `data: ${JSON.stringify({
    id: 'chatcmpl-scripted',
    object: 'chat.completion.chunk',
    created: 1,
    model,
    choices: [{ index: 0, delta, finish_reason: finish }],
    ...(usage ? { usage } : {}),
  })}\n\n`
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export type ScriptedModelServer = {
  url: string
  port: number
  /**
   * Every request body received, for assertions on what Hermes sent.
   * `startedAt`/`finishedAt` (epoch ms; finishedAt 0 until the response
   * ends) are the ground truth for "was a model turn running at time t" in
   * the lane recordings (plan H3), carried over from t2's scratch copy.
   */
  requests: Array<{ path: string; body: unknown; startedAt: number; finishedAt: number }>
  close(): Promise<void>
}

export async function startScriptedModel(port = 0, model = 'scripted-1'): Promise<ScriptedModelServer> {
  const requests: ScriptedModelServer['requests'] = []
  const server = http.createServer((req, res) => {
    let raw = ''
    req.on('data', (part) => {
      raw += part
    })
    req.on('end', () => {
      void handle(req.url ?? '/', raw, res)
    })
  })

  async function handle(path: string, raw: string, res: http.ServerResponse): Promise<void> {
    const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : null
    const record = { path, body, startedAt: Date.now(), finishedAt: 0 }
    requests.push(record)
    res.on('finish', () => {
      record.finishedAt = Date.now()
    })
    if (/\/v1\/models(\/|$)/.test(path)) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ object: 'list', data: [{ id: model, object: 'model', owned_by: 'scripted' }] }))
      return
    }
    if (!path.endsWith('/chat/completions') || !body) {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: { message: 'not found' } }))
      return
    }
    const messages = (body.messages as ChatMessage[]) ?? []
    const selected = selectStep(messages)
    const usage = { prompt_tokens: 12, completion_tokens: 6, total_tokens: 18 }
    // Auxiliary calls (session titles, compression) carry no marker. They are
    // answered plainly so Hermes' background work never stalls a scenario.
    const step: Step = selected?.step ?? { kind: 'text', chunks: ['Scripted reply.'] }
    if (step.kind === 'http-error') {
      res.writeHead(step.status, { 'content-type': 'application/json' })
      res.end(JSON.stringify(step.body))
      return
    }
    if (!body.stream) {
      const content = step.kind === 'text' ? step.chunks.join('') : ''
      // Some callers (the gateway, auxiliary clients) may ask for a
      // non-streaming completion. Keep the scripted latency so a race that
      // depends on "the model call is still running" holds either way (t2).
      if (selected && step.kind === 'text' && step.delayMs) await sleep(step.chunks.length * step.delayMs)
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({
        id: 'chatcmpl-scripted',
        object: 'chat.completion',
        created: 1,
        model,
        choices: [{ index: 0, message: { role: 'assistant', content: selected ? content : 'Scripted title' }, finish_reason: 'stop' }],
        usage,
      }))
      return
    }
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', ...(step.kind === 'text' ? step.headers : undefined) })
    if (step.kind === 'text') {
      for (const [index, text] of step.chunks.entries()) {
        if (res.destroyed) return
        res.write(chunk(model, index === 0 ? { role: 'assistant', content: text } : { content: text }, null))
        if (step.delayMs) await sleep(step.delayMs)
      }
      res.write(chunk(model, {}, 'stop', usage))
    } else {
      res.write(chunk(model, { role: 'assistant', content: null }, null))
      for (const [index, call] of step.calls.entries()) {
        const id = `call_${selected?.scenario ?? 'x'}_${selected?.index ?? 0}_${index}`
        res.write(chunk(model, { tool_calls: [{ index, id, type: 'function', function: { name: call.name, arguments: '' } }] }, null))
        res.write(chunk(model, { tool_calls: [{ index, function: { arguments: JSON.stringify(call.arguments) } }] }, null))
      }
      res.write(chunk(model, {}, 'tool_calls', usage))
    }
    res.end('data: [DONE]\n\n')
  }

  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve))
  const actual = (server.address() as AddressInfo).port
  return {
    url: `http://127.0.0.1:${actual}/v1`,
    port: actual,
    requests,
    close: () => new Promise((resolve) => {
      server.closeAllConnections()
      server.close(() => resolve())
    }),
  }
}
