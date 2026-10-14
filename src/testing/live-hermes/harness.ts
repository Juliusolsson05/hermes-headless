// Boots a REAL `hermes serve` for recordings and the live test tier.
//
// WHY gated even though it binds 127.0.0.1: every real user reaches Hermes on
// a VPS, where a non-loopback bind turns the auth gate on (hermes_cli/
// web_server.py `_should_require_auth`). A loopback bind with a non-loopback
// `dashboard.public_url` turns on the SAME gate without exposing a port to the
// LAN, so the recordings carry the real native-PKCE + ws-ticket path instead
// of the loopback session-token shortcut no user of this package will see.
//
// WHY an isolated HERMES_HOME per boot: Hermes writes state.db, sessions,
// memories and logs under its home. The user's own ~/.hermes must never be
// touched (Hermes' root AGENTS.md: "tests never write to ~/.hermes/"), and a
// fresh home makes every scenario start from the same empty conversation list.
//
// Runtime: `npm run live:setup` installs Hermes' locked dependencies into
// `.cache/hermes-runtime` (scripts/setup-live-hermes.mjs). Hermes refuses to
// be built as a wheel (it is a run-from-source project), so it runs as
// `python -m hermes_cli.main` with the archived source on PYTHONPATH.

import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

export const LIVE_USERNAME = 'agentcode'
// A fixed, obviously-test password: it guards a loopback process for the
// lifetime of one test run and is redacted from recordings anyway.
export const LIVE_PASSWORD = 'hh-live-password-0001'

const packageRoot = resolve(fileURLToPath(new URL('../../..', import.meta.url)))
export const runtimeRoot = resolve(process.env.HERMES_LIVE_RUNTIME ?? join(packageRoot, '.cache', 'hermes-runtime'))

export function liveRuntimeAvailable(): boolean {
  return existsSync(join(runtimeRoot, 'venv', 'bin', 'python')) && existsSync(join(runtimeRoot, 'src', 'hermes_cli'))
}

export type LiveHermesOptions = {
  modelUrl: string
  /** Access-token lifetime for the password provider (S14 needs a short one). */
  sessionTtlSeconds?: number
  /** Reap grace for clientless sessions (S9 shortens it; default is Hermes' 20 s). */
  orphanReapGraceSeconds?: number
  /** `approvals.timeout` seconds (tools/approval_context.py; Hermes default 300). */
  approvalTimeoutSeconds?: number
  /** Reuse a home across a restart (S11: same state.db, new process). */
  home?: string
  /**
   * Extra top-level config.yaml lines, appended verbatim (S17's
   * `quick_commands`). Must not repeat a key this file already writes
   * (model, dashboard, approvals): YAML keeps the last duplicate silently.
   */
  extraConfigYaml?: string[]
  /**
   * The `--host` to bind (default 127.0.0.1). S25 binds `localhost.`: v0.20.4
   * engages its auth gate only when the bind host is not one of the literal
   * strings {localhost, 127.0.0.1, ::1} (web_server.py should_require_auth;
   * `dashboard.public_url` does not exist there yet), and the VPS that
   * failed ran WITH the gate. `localhost.` (the fully-qualified spelling)
   * is not in that set but still resolves to loopback only, so the real
   * gated path runs without exposing a port beyond this machine.
   */
  bindHost?: string
}

export type LiveHermes = {
  baseUrl: string
  port: number
  home: string
  username: string
  password: string
  stop(): Promise<void>
  /** Kill and start again on the same home (and a new port). */
  restart(): Promise<LiveHermes>
}

function writeHome(home: string, options: LiveHermesOptions): void {
  mkdirSync(join(home, 'skills', 'hh-secret'), { recursive: true })
  writeFileSync(join(home, 'config.yaml'), [
    'model:',
    '  default: scripted-1',
    '  provider: custom',
    `  base_url: ${options.modelUrl}`,
    '  api_key: scripted',
    'dashboard:',
    // Any non-loopback public URL engages the gate (see the header comment).
    '  public_url: https://hermes.example',
    // `dashboard.ws_orphan_reap_grace_s` (tui_gateway/server.py
    // _ws_orphan_setting). After this many seconds without a client Hermes
    // INTERRUPTS a running turn and reaps the session, so S9 shortens it to
    // record that path quickly.
    ...(options.orphanReapGraceSeconds !== undefined
      ? [`  ws_orphan_reap_grace_s: ${options.orphanReapGraceSeconds}`]
      : []),
    ...(options.approvalTimeoutSeconds !== undefined
      ? ['approvals:', `  timeout: ${options.approvalTimeoutSeconds}`]
      : []),
    ...(options.extraConfigYaml ?? []),
    '',
  ].join('\n'))
  writeFileSync(join(home, '.env'), [
    `HERMES_DASHBOARD_BASIC_AUTH_USERNAME=${LIVE_USERNAME}`,
    `HERMES_DASHBOARD_BASIC_AUTH_PASSWORD=${LIVE_PASSWORD}`,
    // A fixed signing secret so a restart (S11) keeps sessions valid, exactly
    // as Hermes' docs recommend for VPS installs.
    'HERMES_DASHBOARD_BASIC_AUTH_SECRET=hh-live-signing-secret-000000000000000000000000',
    ...(options.sessionTtlSeconds !== undefined
      ? [`HERMES_DASHBOARD_BASIC_AUTH_TTL_SECONDS=${options.sessionTtlSeconds}`]
      : []),
    '',
  ].join('\n'))
  // A skill whose required variable is unset: viewing it makes Hermes ask the
  // client for a secret (tools/skills_tool_setup.py), which is the only
  // tool-driven path to a real `secret` server request.
  writeFileSync(join(home, 'skills', 'hh-secret', 'SKILL.md'), [
    '---',
    'name: hh-secret',
    'description: Test skill that needs a token so Hermes asks for a secret.',
    'required_environment_variables:',
    '  - name: HH_TEST_TOKEN',
    '    prompt: Test token for the hh-secret skill',
    '---',
    '',
    'This skill only exists to make Hermes ask for HH_TEST_TOKEN.',
    '',
  ].join('\n'))
}

export async function startLiveHermes(options: LiveHermesOptions): Promise<LiveHermes> {
  if (!liveRuntimeAvailable()) {
    throw new Error(`Hermes runtime not found at ${runtimeRoot}; run \`npm run live:setup\` first`)
  }
  const home = options.home ?? mkdtempSync(join(tmpdir(), 'hh-hermes-home-'))
  if (!options.home) writeHome(home, options)
  const child: ChildProcess = spawn(
    join(runtimeRoot, 'venv', 'bin', 'python'),
    ['-m', 'hermes_cli.main', 'serve', '--host', options.bindHost ?? '127.0.0.1', '--port', '0', '--skip-build'],
    {
      cwd: join(runtimeRoot, 'src'),
      env: {
        PATH: process.env.PATH ?? '/usr/bin:/bin',
        // HOME too, not only HERMES_HOME: Hermes anchors its profiles root on
        // HOME on purpose (root AGENTS.md: `_get_profiles_root()` is
        // HOME-anchored), so a real HOME would let a test touch ~/.hermes.
        // The env is otherwise EMPTY so no developer API key reaches the test
        // agent. HERMES_TEST_ISOLATION is deliberately NOT set: it turns
        // contract drift into exceptions and so changes production behaviour,
        // and recordings must show what users' backends actually do.
        HOME: home,
        HERMES_HOME: home,
        PYTHONPATH: join(runtimeRoot, 'src'),
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  )
  let stderr = ''
  child.stderr?.on('data', (part: Buffer) => {
    stderr = (stderr + part.toString()).slice(-8000)
  })
  const port = await new Promise<number>((resolvePort, reject) => {
    let stdout = ''
    const timer = setTimeout(() => reject(new Error(`hermes serve did not announce a port in 120 s\n${stderr}`)), 120_000)
    child.stdout?.on('data', (part: Buffer) => {
      stdout += part.toString()
      // The neutral sentinel Hermes Desktop's backend-ready.ts parses.
      const match = /HERMES_(?:BACKEND|DASHBOARD)_READY port=(\d+)/.exec(stdout)
      if (match) {
        clearTimeout(timer)
        resolvePort(Number(match[1]))
      }
    })
    child.once('exit', (code) => {
      clearTimeout(timer)
      reject(new Error(`hermes serve exited ${code} before ready\n${stderr}`))
    })
  })
  const stop = () => new Promise<void>((resolveStop) => {
    if (child.exitCode !== null) return resolveStop()
    child.once('exit', () => resolveStop())
    child.kill('SIGTERM')
    setTimeout(() => child.kill('SIGKILL'), 8000).unref()
  })
  const live: LiveHermes = {
    baseUrl: `http://${options.bindHost ?? '127.0.0.1'}:${port}`,
    port,
    home,
    username: LIVE_USERNAME,
    password: LIVE_PASSWORD,
    stop,
    restart: async () => {
      await stop()
      return startLiveHermes({ ...options, home })
    },
  }
  return live
}

/** Remove a home created by startLiveHermes (never a caller-provided one). */
export function removeLiveHome(home: string): void {
  if (home.startsWith(tmpdir())) rmSync(home, { recursive: true, force: true })
}
