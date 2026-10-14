// Whole-file secret scan over the corpus (decomposition §7, memory q36: read
// the WHOLE fixture before committing). The tap redacts at write time; this is
// the independent second check that nothing credential-shaped survived.
//
// Flags any string value that looks like a token: ≥ 24 characters of
// base64url/JWT alphabet with high character variety, that is not a run of the
// tap's `x` mask, and is not on the allow-list of known public shapes (session
// ids, request ids, epochs). Exit code 1 on any finding.

import { readFileSync, readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = process.argv[2] ?? fileURLToPath(new URL('./corpus/', import.meta.url))
const TOKENISH = /^[A-Za-z0-9_\-.+/=]{24,}$/
const ALLOWED = [
  /^\d{8}_\d{6}_[0-9a-f]{6}$/, // stored session id
  /^srq-[0-9a-f]{12}$/, // server request id
  /^[0-9a-f]{32}$/, // replay epoch / install id (public, per-process)
  /^call_[a-z0-9_-]+$/, // scripted tool call ids
  /^x+$/, // the tap's mask
]

/**
 * Token-shaped: mixed upper case, lower case and digits with high variety.
 * Bearer tokens, tickets and PKCE codes are base64url and always mix all
 * three; skill names, snake_case keys and stored session ids never do.
 */
function entropyish(value: string): boolean {
  return /[A-Z]/.test(value) && /[a-z]/.test(value) && /[0-9]/.test(value) && new Set(value).size >= 14
}

/** Personal machine paths must never reach the corpus (tap.ts rewrites them). */
const PERSONAL = [
  /\/Users\/[^/]+\//, /\/private\/var\/folders\//, /\/home\/(?!dev\b)[^/]+\//,
  // The Telegram stand-in's bot token (fake, but a bot token in a fixture
  // must never look normal) and the stand-in's Bot API URLs, which carry it.
  /\d{6,}:[A-Za-z0-9_-]{6,}/, /\/bot\d+:/,
]

const findings: string[] = []
function visit(value: unknown, where: string): void {
  if (typeof value === 'string') {
    for (const pattern of PERSONAL) if (pattern.test(value)) findings.push(`${where}: personal path ${value.slice(0, 40)}`)
    for (const piece of value.split(/[\s"'=&?:,;()/\\]+/)) {
      if (TOKENISH.test(piece) && entropyish(piece) && !ALLOWED.some((pattern) => pattern.test(piece))) {
        findings.push(`${where}: ${piece.slice(0, 12)}… (${piece.length} chars)`)
      }
    }
  } else if (Array.isArray(value)) {
    value.forEach((item, index) => visit(item, `${where}[${index}]`))
  } else if (value && typeof value === 'object') {
    for (const [key, inner] of Object.entries(value)) visit(inner, `${where}.${key}`)
  }
}

let files = 0
for (const scenario of readdirSync(root)) {
  // MANIFEST-<release>.json files sit beside the scenario directories.
  if (!statSync(join(root, scenario)).isDirectory()) continue
  for (const file of readdirSync(join(root, scenario)).filter((name) => name.endsWith('.jsonl'))) {
    files += 1
    const lines = readFileSync(join(root, scenario, file), 'utf8').trim().split('\n')
    lines.forEach((line, index) => visit(JSON.parse(line), `${scenario}/${file}:${index + 1}`))
  }
}
if (findings.length) {
  console.error(`${findings.length} credential-shaped strings in ${files} files:`)
  for (const finding of findings.slice(0, 50)) console.error(`  ${finding}`)
  process.exitCode = 1
} else {
  console.log(`scan clean: ${files} files`)
}
