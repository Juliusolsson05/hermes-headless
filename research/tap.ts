// Wire tap for Stage 0 recordings (decomposition §7).
//
// WHY a dumb tap and not a recording mode inside the client: the corpus is the
// ORACLE every later stage is tested against. If the recorder interpreted
// frames through the same parser it is meant to validate, a parser bug would be
// baked into the fixtures and every test would agree with it. So this file only
// timestamps and redacts; the driver that produces traffic is equally naive
// (research/driver.ts), and nothing under src/ is imported here.
//
// WHY redaction happens at WRITE time, same-length: a fixture must never hold a
// live credential even transiently on disk (memory: q36, read the whole fixture
// before committing), and keeping the length means byte offsets and size-based
// behaviour (coalescing, 4 MiB rings) in the recording stay truthful.

import { appendFileSync, mkdirSync, realpathSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

/**
 * Machine paths are not credentials, but they are personal data in a public
 * repository (the developer's username, project layout, temp dirs). Hermes
 * echoes its cwd and the session home in `session.info`, tool args and
 * results, so every occurrence is rewritten to a neutral path. Longest first,
 * so the temp dir (under /private/var/…) is replaced before its parent.
 */
// The Hermes runtime root (where `hermes serve` runs from and reports as its
// cwd). Before this rewrite the v0.21.7 corpus carried the developer's
// worktree layout under /home/dev/… 225 times; a neutral root keeps
// recordings from different machines and releases diffable. Resolved the
// same way as src/testing/live-hermes/harness.ts (not imported: tap.ts must
// not depend on src/, see the header).
const RUNTIME_ROOT = resolve(process.env.HERMES_LIVE_RUNTIME ?? join(dirname(fileURLToPath(import.meta.url)), '..', '.cache', 'hermes-runtime'))

function realOrSelf(path: string): string {
  try {
    return realpathSync(path)
  } catch {
    return path
  }
}

const PATH_REWRITES: Array<[string, string]> = [
  [RUNTIME_ROOT, '/opt/hermes-runtime'],
  [realOrSelf(RUNTIME_ROOT), '/opt/hermes-runtime'],
  // macOS: tmpdir() is /var/folders/…, but processes report the realpath
  // /private/var/folders/…; both spellings appear in Hermes' payloads.
  [realpathSync(tmpdir()), '/tmp/hh'],
  [tmpdir(), '/tmp/hh'],
  [homedir(), '/home/dev'],
].filter(([from]) => from && from.length > 1).sort((a, b) => b[0].length - a[0].length) as Array<[string, string]>

function rewritePaths(value: string): string {
  let out = value
  for (const [from, to] of PATH_REWRITES) out = out.split(from).join(to)
  return out
}

export type TapDirection = 'out' | 'in'
export type TapKind = 'ws' | 'http' | 'ws-open' | 'ws-close' | 'note'

export type TapEntry = {
  /** Milliseconds since the tap opened. Relative so recordings diff cleanly. */
  t: number
  dir: TapDirection | null
  kind: TapKind
  /** Parsed JSON frame / HTTP exchange / note payload, already redacted. */
  frame: unknown
  /**
   * Which client socket a ws/ws-open/ws-close entry belongs to, when a
   * scenario opens several and names them (S22: client A vs client B). Absent
   * in single-client recordings and in every v0.21.7 file; two sockets on
   * one session otherwise produce identical, indistinguishable lines.
   */
  peer?: string
}

/**
 * Keys whose VALUES are credentials or user secrets. Matched case-insensitively
 * on the property name anywhere in a frame. The list is the union of every
 * credential-bearing field in hermes_cli/dashboard_auth (token/refresh/ticket,
 * PKCE code + verifier) and the answer fields of the `secret` / `sudo` /
 * `vault.*` server requests (`value`), which carry what a user typed.
 */
const SECRET_KEYS = new Set([
  'access_token',
  'refresh_token',
  'ticket',
  'code_verifier',
  'code_challenge',
  'password',
  'authorization',
  'cookie',
  'set-cookie',
  'x-hermes-session-token',
])

/**
 * Keys that are secret only in a specific shape. A blanket rule over-redacted
 * real contract data in the first S15 recording: `error_surface.code` (the
 * provider error class) was masked because the PKCE exchange also has a
 * `code`. So `code` is masked only beside `code_verifier` (the token request
 * body), and `value` only in a bare `{value}` object, which is the answer shape
 * of the sudo / secret / vault server requests (contracts ValueResult).
 */
function contextualSecret(key: string, siblings: Record<string, unknown>): boolean {
  if (key === 'code') return 'code_verifier' in siblings
  if (key === 'value') return Object.keys(siblings).length === 1
  return false
}

/** Query parameters that carry credentials in URLs (ws `?ticket=`, `?token=`, loopback `?code=`). */
const SECRET_QUERY = /([?&](?:ticket|token|code|code_challenge|state)=)([^&#\s"]+)/g

export function maskSameLength(value: string): string {
  // `x` keeps the character count; a recognisable marker would change it.
  return 'x'.repeat(value.length)
}

export function redact(value: unknown, keyHint?: string): unknown {
  if (typeof value === 'string') {
    if (keyHint && SECRET_KEYS.has(keyHint.toLowerCase())) return maskSameLength(value)
    return rewritePaths(value).replace(SECRET_QUERY, (_m, prefix: string, secret: string) => prefix + maskSameLength(secret))
  }
  if (Array.isArray(value)) return value.map((item) => redact(item, keyHint))
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const [key, inner] of Object.entries(value as Record<string, unknown>)) {
      // A secret key's whole subtree is a secret, including nested objects.
      if ((SECRET_KEYS.has(key.toLowerCase()) || contextualSecret(key, value as Record<string, unknown>)) && typeof inner !== 'object') {
        out[key] = typeof inner === 'string' ? maskSameLength(inner) : inner
      } else {
        out[key] = redact(inner, key)
      }
    }
    return out
  }
  return value
}

export class WireTap {
  private readonly started = Date.now()

  /** Epoch ms the tap opened; converts external timestamps (model requests) to tap time. */
  get startedAt(): number {
    return this.started
  }

  constructor(private readonly path: string) {
    mkdirSync(dirname(path), { recursive: true })
    writeFileSync(path, '')
  }

  write(dir: TapDirection | null, kind: TapKind, frame: unknown, peer?: string): void {
    const entry: TapEntry = { t: Date.now() - this.started, dir, kind, frame: redact(frame), ...(peer ? { peer } : {}) }
    appendFileSync(this.path, JSON.stringify(entry) + '\n')
  }

  note(text: string, data?: unknown): void {
    this.write(null, 'note', data === undefined ? { text } : { text, data })
  }
}
