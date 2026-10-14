// Writes research/corpus/MANIFEST-<release>.json: which scenarios a release
// has recordings for, from the recordings themselves (plan Stage 0
// "a MANIFEST.json per release {release, sourceCommit, contract, scenarios[]}").
//
// Usage: node --import tsx research/write-manifest.mts --release v0.22.0
//
// WHY next to the scenario directories and not as corpus/<release>/MANIFEST.json:
// the corpus layout stays research/corpus/<Scenario>/<release>-<run id>.jsonl
// (existing tests and the app import it), so a per-release directory does not
// exist. src/testing/corpus.ts lists directories only, so these files are
// never mistaken for scenarios.
//
// Every value is read from a recording: the release/commit from each file's
// provenance note, the desktop contract from the first `info.desktop_contract`
// Hermes sent. A scenario whose newest file ends in `scenario failed` is
// listed with `finished: false`, never silently dropped.

import { writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { corpusRoot, corpusScenarios, loadRecording, recordingFilesFor, recordingProvenance } from '../src/testing/corpus.js'
import { parseReleaseArgs } from './provenance.js'

const { release } = parseReleaseArgs(process.argv.slice(2))
const scenarios: Array<Record<string, unknown>> = []
const commits = new Set<string>()
let contract: number | undefined

for (const scenario of corpusScenarios()) {
  const files = recordingFilesFor(scenario, release)
  if (!files.length) continue
  const path = files[files.length - 1]
  const entries = loadRecording(path)
  const provenance = recordingProvenance(path)
  if (provenance?.sourceCommit) commits.add(provenance.sourceCommit)
  for (const entry of entries) {
    if (contract !== undefined) break
    const info = (entry.frame.result as { info?: { desktop_contract?: unknown } } | undefined)?.info
    if (entry.kind === 'ws' && entry.dir === 'in' && typeof info?.desktop_contract === 'number') contract = info.desktop_contract
  }
  scenarios.push({
    id: scenario,
    title: (entries[0]?.frame as { text?: string }).text,
    file: path.slice(corpusRoot.length + 1),
    entries: entries.length,
    wsFrames: entries.filter((entry) => entry.kind === 'ws').length,
    httpExchanges: entries.filter((entry) => entry.kind === 'http' && entry.dir === 'in').length,
    finished: entries.some((entry) => entry.kind === 'note' && entry.frame.text === 'scenario finished'),
  })
}

// The original v0.21.7 files carry an 8-character commit ('71d34bad'); newer
// files carry the full one. A short commit that prefixes a full one is the
// same commit, so only the full spelling is kept.
const full = [...commits].filter((commit) => ![...commits].some((other) => other !== commit && other.startsWith(commit)))
commits.clear()
for (const commit of full) commits.add(commit)
if (commits.size > 1) throw new Error(`${release} recordings disagree on the source commit: ${[...commits].join(', ')}`)
const manifest = {
  release,
  sourceCommit: [...commits][0] ?? null,
  // null when no recording of this release ever reached a session (v0.20.4:
  // S25 stops at the handshake; report r1 gives contract 6 from source).
  contract: contract ?? null,
  scenarios,
}
const out = join(corpusRoot, `MANIFEST-${release}.json`)
writeFileSync(out, `${JSON.stringify(manifest, null, 2)}\n`)
console.log(`${out}: ${scenarios.length} scenarios`)
