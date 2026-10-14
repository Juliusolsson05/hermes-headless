// Load Stage 0 recordings for tests. The corpus lives in research/corpus and
// is the oracle (decomposition §12): tests read it, they never edit it.

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export type CorpusEntry = {
  t: number
  dir: 'out' | 'in' | null
  kind: 'ws' | 'http' | 'ws-open' | 'ws-close' | 'note'
  frame: Record<string, unknown>
}

// From the module's own path as a STRING: consumers' renderer tests run under
// happy-dom, which replaces the global URL class, and Node's fileURLToPath
// rejects a happy-dom URL object ("must be of scheme file").
export const corpusRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'research', 'corpus')

export function corpusScenarios(): string[] {
  if (!existsSync(corpusRoot)) return []
  return readdirSync(corpusRoot).sort()
}

/** Every recording file of a scenario, oldest first (file names are ISO run ids). */
export function recordingFiles(scenario: string): string[] {
  const dir = join(corpusRoot, scenario)
  if (!existsSync(dir)) return []
  return readdirSync(dir).filter((name) => name.endsWith('.jsonl')).sort().map((name) => join(dir, name))
}

export function loadRecording(path: string): CorpusEntry[] {
  return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line) as CorpusEntry)
}

/** The newest recording of a scenario. Throws when the scenario was never recorded. */
export function latestRecording(scenario: string): CorpusEntry[] {
  const files = recordingFiles(scenario)
  if (files.length === 0) throw new Error(`no recording for ${scenario} in ${corpusRoot}`)
  return loadRecording(files[files.length - 1])
}

/** Inbound WebSocket frames as raw objects, in arrival order. */
export function inboundFrames(entries: CorpusEntry[]): Array<Record<string, unknown>> {
  return entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in').map((entry) => entry.frame)
}
