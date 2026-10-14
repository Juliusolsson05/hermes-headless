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
  | { kind: 'text'; chunks: string[]; delayMs?: number }
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
}

const MARKER = /\[\[hh:([a-z0-9-]+)\]\]/

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
  const steps = SCENARIOS[match[1]]
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
  /** Every request body received, for assertions on what Hermes sent. */
  requests: Array<{ path: string; body: unknown }>
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
    requests.push({ path, body })
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
    res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' })
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
