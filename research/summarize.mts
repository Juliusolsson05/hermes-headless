// Print a recording as one line per entry, or a corpus census.
//
//   tsx research/summarize.mts <file.jsonl>          — timeline of one recording
//   tsx research/summarize.mts --census [corpus dir]  — counts per event type,
//       server request method, RPC method and payload key path, per scenario
//
// The census is what decomposition §7 "Stage 0 results" quotes: counts, not
// impressions (staged-decomposition: count the real data).

import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

type Entry = { t: number; dir: string | null; kind: string; frame: Record<string, unknown> }

function load(path: string): Entry[] {
  return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line) as Entry)
}

function describe(entry: Entry): string {
  const frame = entry.frame
  if (entry.kind === 'http') {
    return `${entry.dir} http ${String(frame.method ?? '')} ${String(frame.url ?? '').replace(/^https?:\/\/[^/]+/, '')} ${frame.status ?? ''}`
  }
  if (entry.kind === 'note') return `note ${String(frame.text)} ${frame.data ? JSON.stringify(frame.data).slice(0, 160) : ''}`
  if (entry.kind !== 'ws') return `${entry.kind} ${JSON.stringify(frame).slice(0, 120)}`
  const params = frame.params as Record<string, unknown> | undefined
  if (frame.method === 'event') {
    const payload = JSON.stringify(params?.payload ?? '').slice(0, 140)
    return `${entry.dir} event ${String(params?.type)} sid=${String(params?.session_id ?? '')} seq=${String(params?.seq ?? '')} ${payload}`
  }
  if (typeof frame.method === 'string') {
    return `${entry.dir} ${typeof frame.id === 'string' && frame.id.startsWith('srq') ? 'REQUEST' : 'call'} ${frame.method} id=${String(frame.id ?? '')} ${JSON.stringify(params ?? {}).slice(0, 160)}`
  }
  const body = 'error' in frame ? frame.error : frame.result
  return `${entry.dir} resp id=${String(frame.id)} ${'error' in frame ? 'ERROR ' : ''}${JSON.stringify(body).slice(0, 160)}`
}

function keyPaths(value: unknown, prefix: string, out: Set<string>): void {
  if (Array.isArray(value)) {
    for (const item of value) keyPaths(item, `${prefix}[]`, out)
    return
  }
  if (value && typeof value === 'object') {
    for (const [key, inner] of Object.entries(value)) keyPaths(inner, prefix ? `${prefix}.${key}` : key, out)
    return
  }
  out.add(prefix)
}

function census(root: string): void {
  const events = new Map<string, number>()
  const requests = new Map<string, number>()
  const calls = new Map<string, number>()
  const payloadKeys = new Map<string, Set<string>>()
  let files = 0
  let frames = 0
  for (const scenario of readdirSync(root)) {
    const dir = join(root, scenario)
    if (!statSync(dir).isDirectory()) continue
    for (const file of readdirSync(dir).filter((name) => name.endsWith('.jsonl'))) {
      files += 1
      for (const entry of load(join(dir, file))) {
        if (entry.kind !== 'ws') continue
        frames += 1
        const frame = entry.frame
        const params = frame.params as Record<string, unknown> | undefined
        if (frame.method === 'event') {
          const type = String(params?.type)
          events.set(type, (events.get(type) ?? 0) + 1)
          const keys = payloadKeys.get(type) ?? new Set<string>()
          keyPaths(params?.payload ?? {}, '', keys)
          payloadKeys.set(type, keys)
        } else if (typeof frame.method === 'string' && entry.dir === 'in') {
          requests.set(frame.method, (requests.get(frame.method) ?? 0) + 1)
          const keys = payloadKeys.get(`request:${frame.method}`) ?? new Set<string>()
          keyPaths(params ?? {}, '', keys)
          payloadKeys.set(`request:${frame.method}`, keys)
        } else if (typeof frame.method === 'string') {
          calls.set(frame.method, (calls.get(frame.method) ?? 0) + 1)
        }
      }
    }
  }
  const sorted = (map: Map<string, number>) => [...map.entries()].sort((a, b) => b[1] - a[1])
  console.log(`files=${files} ws-frames=${frames}`)
  console.log('\n## events (inbound)')
  for (const [name, count] of sorted(events)) console.log(`${count}\t${name}`)
  console.log('\n## server→client requests')
  for (const [name, count] of sorted(requests)) console.log(`${count}\t${name}`)
  console.log('\n## client calls')
  for (const [name, count] of sorted(calls)) console.log(`${count}\t${name}`)
  console.log('\n## payload key paths')
  for (const [name, keys] of [...payloadKeys.entries()].sort()) console.log(`${name}: ${[...keys].sort().join(', ')}`)
}

const args = process.argv.slice(2)
if (args[0] === '--census') {
  census(args[1] ?? fileURLToPath(new URL('./corpus/', import.meta.url)))
} else {
  for (const entry of load(args[0])) console.log(`${String(entry.t).padStart(6)} ${describe(entry)}`)
}
