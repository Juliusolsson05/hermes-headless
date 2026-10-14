// Which Hermes produced a recording, read from the runtime the harness
// actually starts.
//
// WHY not a hard-coded string any more (r7 F3/F7): the first recorder wrote
// `hermesSource: 'v0.21.7 71d34bad'` into every file. r8 then recorded v0.22.0
// with a hand-edited copy of the recorder, and a run that forgot the edit
// would have labelled v0.22.0 frames as v0.21.7 — which tests then select by
// (src/testing/corpus.ts). The only fact that cannot drift from the process
// is `$HERMES_LIVE_RUNTIME/SOURCE_COMMIT`, written by
// scripts/setup-live-hermes.mjs from `git rev-parse HEAD` of the archived tree.
//
// WHY the release is still a flag: SOURCE_COMMIT holds a commit, not a tag
// (the setup script runs on shallow clones and on the vendored checkout,
// which carries no tags). So the caller names the release and this module
// refuses a pairing that contradicts a known tag → commit mapping. An
// unknown release is allowed (new tags arrive weekly) but is labelled with
// whatever commit the runtime really has.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'

import { runtimeRoot } from '../src/testing/live-hermes/harness.js'

/**
 * Tag → peeled commit, from `git ls-remote --tags` of NousResearch/hermes-agent
 * (2026-10-14). v0.20.4 has no `v0.20.4` tag upstream; it shipped as the date
 * tag `v2026.8.18` (pyproject `version = "0.20.4"`, report r1 table).
 * v0.21.7's commit is the `rc.15-v0.21.7` peel, which is what
 * vendor/hermes-agent-src is checked out at.
 */
export const KNOWN_RELEASES: Readonly<Record<string, string>> = {
  'v0.20.4': 'e624e9fde561e1add9388384012b295fde669ade',
  'v0.21.7': '71d34bad1045610252eda02ee79549436896e784',
  'v0.22.0': 'e06df9c2eae6901f4c88f464e0ae6e753c0428b8',
}

export type Provenance = { release: string; sourceCommit: string; runtime: 'HERMES_LIVE_RUNTIME' | 'default' }

export function readProvenance(release: string, root = runtimeRoot): Provenance {
  let commit: string
  try {
    commit = readFileSync(join(root, 'SOURCE_COMMIT'), 'utf8').trim().split(/\s+/)[0]
  } catch {
    throw new Error(`no SOURCE_COMMIT in the Hermes runtime (${root}); run scripts/setup-live-hermes.mjs first`)
  }
  const expected = KNOWN_RELEASES[release]
  if (expected && expected !== commit) {
    throw new Error(`--release ${release} expects commit ${expected}, but the runtime the harness starts is ${commit}. `
      + 'Point HERMES_LIVE_RUNTIME at the right runtime or pass the matching --release.')
  }
  return { release, sourceCommit: commit, runtime: process.env.HERMES_LIVE_RUNTIME ? 'HERMES_LIVE_RUNTIME' : 'default' }
}

/** `--release <tag>` from argv; returns the tag and the remaining (scenario) arguments. */
export function parseReleaseArgs(argv: string[], fallback = 'v0.21.7'): { release: string; rest: string[] } {
  const rest: string[] = []
  let release = fallback
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index]
    if (arg === '--release') {
      release = argv[index + 1] ?? ''
      index += 1
    } else if (arg.startsWith('--release=')) {
      release = arg.slice('--release='.length)
    } else {
      rest.push(arg)
    }
  }
  if (!/^v\d+\.\d+\.\d+$/.test(release)) throw new Error(`--release must look like v0.22.0, got ${JSON.stringify(release)}`)
  return { release, rest }
}

/** The file name for a new recording: `<release>-<ISO run id>.jsonl`. */
export function recordingFileName(release: string, runId: string): string {
  return `${release}-${runId}.jsonl`
}
