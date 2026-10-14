// Load Stage 0 recordings for tests. The corpus lives in research/corpus and
// is the oracle (decomposition §12): tests read it, they never edit it.
//
// Layout: research/corpus/<Scenario>/<file>.jsonl. v0.21.7 files are named
// `<ISO run id>.jsonl` (recorded before the recorder knew about releases);
// later files are `<release>-<ISO run id>.jsonl` so several Hermes releases
// sit side by side in one scenario directory.
//
// WHY selection is by the provenance note and not the file name: before this
// change `latestRecording` took "the lexically newest file". With two releases
// in one directory that silently switches every test to whichever release
// happens to sort last ('v0.22.0-…' > '2026-…'), so a test written against
// v0.21.7 would start judging v0.22.0 frames without anyone deciding that. The
// first line of every recording is the recorder's provenance note
// (research/record-wire.mts) and is the source of truth for which Hermes
// produced it; the file name is only a convenience for humans.

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

export type CorpusEntry = {
  t: number
  dir: 'out' | 'in' | null
  kind: 'ws' | 'http' | 'ws-open' | 'ws-close' | 'note'
  frame: Record<string, unknown>
}

/**
 * The release every existing test was written against. Changing this default
 * re-points every `latestRecording(x)` call in this package AND in the app
 * (which imports `hermes-headless/testing/corpus`), so it is a deliberate,
 * reviewed decision, never a side effect of recording a new release.
 */
export const DEFAULT_CORPUS_RELEASE = 'v0.21.7'

// From the module's own path as a STRING: consumers' renderer tests run under
// happy-dom, which replaces the global URL class, and Node's fileURLToPath
// rejects a happy-dom URL object ("must be of scheme file").
export const corpusRoot = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'research', 'corpus')

/**
 * Scenario directories only. The corpus root also holds per-release
 * MANIFEST-<release>.json files; listing them as scenarios would make
 * `recordingFiles` readdir a file.
 */
export function corpusScenarios(): string[] {
  if (!existsSync(corpusRoot)) return []
  return readdirSync(corpusRoot).filter((name) => statSync(join(corpusRoot, name)).isDirectory()).sort()
}

/** Every recording file of a scenario, all releases, in file-name order. */
export function recordingFiles(scenario: string): string[] {
  const dir = join(corpusRoot, scenario)
  if (!existsSync(dir)) return []
  return readdirSync(dir).filter((name) => name.endsWith('.jsonl')).sort().map((name) => join(dir, name))
}

export function loadRecording(path: string): CorpusEntry[] {
  return readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line) as CorpusEntry)
}

export type RecordingProvenance = {
  release: string
  sourceCommit?: string
  /** Wall-clock start of the run, when the recorder wrote it (v0.22.0+ files). */
  recordedAt?: string
}

/**
 * Provenance from a recording's first line. Two shapes exist:
 * - current: `data.release` + `data.sourceCommit` (+ `recordedAt`), written
 *   from the runtime's SOURCE_COMMIT by research/record-wire.mts;
 * - original v0.21.7: only `data.hermesSource: 'v0.21.7 71d34bad'`.
 * Reads just the first line so sweeping a large corpus stays cheap.
 */
export function recordingProvenance(path: string): RecordingProvenance | undefined {
  const text = readFileSync(path, 'utf8')
  const end = text.indexOf('\n')
  const first = JSON.parse(end < 0 ? text : text.slice(0, end)) as CorpusEntry
  if (first.kind !== 'note') return undefined
  const data = (first.frame.data ?? {}) as Record<string, unknown>
  if (typeof data.release === 'string') {
    return {
      release: data.release,
      sourceCommit: typeof data.sourceCommit === 'string' ? data.sourceCommit : undefined,
      recordedAt: typeof data.recordedAt === 'string' ? data.recordedAt : undefined,
    }
  }
  if (typeof data.hermesSource === 'string') {
    const [release, commit] = data.hermesSource.split(/\s+/)
    return { release, sourceCommit: commit }
  }
  return undefined
}

/** Recording files of one scenario produced by one release, oldest first. */
export function recordingFilesFor(scenario: string, release: string): string[] {
  return recordingFiles(scenario)
    .map((path) => ({ path, provenance: recordingProvenance(path) }))
    .filter((entry) => entry.provenance?.release === release)
    // recordedAt when present, else the ISO run id inside the file name; both
    // sort chronologically as strings. The release prefix is stripped so it
    // never decides the order.
    .sort((a, b) => sortKey(a.path, a.provenance).localeCompare(sortKey(b.path, b.provenance)))
    .map((entry) => entry.path)
}

function sortKey(path: string, provenance: RecordingProvenance | undefined): string {
  if (provenance?.recordedAt) return provenance.recordedAt
  const name = path.split(/[\\/]/).pop() ?? path
  return name.replace(/^v[^-]+-/, '')
}

/**
 * The newest recording of a scenario for one release (default
 * DEFAULT_CORPUS_RELEASE). Throws when that release never recorded it.
 */
export function latestRecording(scenario: string, opts: { release?: string } = {}): CorpusEntry[] {
  const release = opts.release ?? DEFAULT_CORPUS_RELEASE
  const files = recordingFilesFor(scenario, release)
  if (files.length === 0) throw new Error(`no ${release} recording for ${scenario} in ${corpusRoot}`)
  return loadRecording(files[files.length - 1])
}

/**
 * The newest recording of a scenario for an explicitly named release. Unlike
 * `latestRecording` there is no default: a lane test written for v0.22.0
 * must say so, and a missing recording is an error that names the
 * recorder command that would produce it.
 */
export function recordingFor(scenario: string, release: string): CorpusEntry[] {
  const files = recordingFilesFor(scenario, release)
  if (files.length === 0) {
    const have = [...new Set(recordingFiles(scenario).map((path) => recordingProvenance(path)?.release ?? '?'))]
    throw new Error(
      `no ${release} recording for scenario ${scenario} (recorded releases: ${have.length ? have.join(', ') : 'none'}). `
      + `Record it with research/record-wire.mts --release ${release} ${scenario} (or research/record-lane.mts for T*).`,
    )
  }
  return loadRecording(files[files.length - 1])
}

/** Inbound WebSocket frames as raw objects, in arrival order. */
export function inboundFrames(entries: CorpusEntry[]): Array<Record<string, unknown>> {
  return entries.filter((entry) => entry.kind === 'ws' && entry.dir === 'in').map((entry) => entry.frame)
}
