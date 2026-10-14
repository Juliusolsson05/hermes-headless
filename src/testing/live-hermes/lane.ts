// Boots the Telegram LANE setup for recordings: upstream's Telegram Bot API
// stand-in, a real `hermes gateway run` whose real Telegram adapter talks to
// it, and a real `hermes serve` (the live harness) on the SAME home and
// state.db. Ported from the t2 research driver
// (temp/hermes-research/scratch-t2/experiment.mts), which proved this setup.
//
// WHY two Hermes processes on one home: on a real VPS the Telegram gateway
// and the dashboard/`serve` are separate processes sharing one HERMES_HOME.
// Everything lane mode has to get right — the global `sessions.changed`
// signal, the cross-process turn lease, rows another process appends, ended
// rows after `/new` — exists only BETWEEN two processes. A single `serve`
// cannot produce any of it.
//
// WHY the env is empty apart from these keys: like harness.ts, no developer
// API key or real Telegram token may reach either child. The only Telegram
// token is the stand-in's fixed fake ("123456:standin-token").
//
// The gateway env keys (all from t2, which found each one necessary):
// - HERMES_GATEWAY_EXTERNAL_SUPERVISOR=1: the gateway would otherwise try to
//   (re)install a launchd/systemd service for itself;
// - HERMES_TELEGRAM_DISABLE_FALLBACK_IPS=1: no fallback to Telegram's real
//   IP addresses — the stand-in is the only Telegram this process may reach;
// - HERMES_TELEGRAM_TEXT_BATCH_*=0: deliver each phone message as its own
//   turn immediately instead of batching bursts (the recordings time races);
// - HERMES_DISABLE_LAZY_INSTALLS=1: never pip-install at runtime.

import { spawn, type ChildProcess } from 'node:child_process'
import { appendFileSync, mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { fileURLToPath } from 'node:url'

import { LIVE_PASSWORD, LIVE_USERNAME, runtimeRoot, startLiveHermes, type LiveHermes } from './harness.js'

/** Fake Telegram user ids (the stand-in's private chat id == user id). */
export const LANE_USERS = { owner: 111, second: 112, third: 113 } as const

const controllerScript = fileURLToPath(new URL('./telegram_standin_ctl.py', import.meta.url))
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export type StandinSnapshot = {
  connected: boolean
  visible: Array<{ message_id: string; text: string; [key: string]: unknown }>
  sends: Array<{ method: string; params: Record<string, unknown>; [key: string]: unknown }>
}

export type LaneDbSnapshot = {
  sessions: Array<Record<string, unknown>>
  messages: Array<Record<string, unknown>>
}

export type LaneHermes = {
  serve: LiveHermes
  home: string
  python: string
  /** Send a phone message (optionally in a DM topic). */
  dm(text: string, userId?: number, threadId?: number): Promise<unknown>
  snapshot(userId?: number): Promise<StandinSnapshot>
  /** Tap the newest inline button whose label contains `text` (e.g. "Approve Once"). */
  press(text: string, userId?: number): Promise<{ pressed: string; callback_data: string }>
  db(): Promise<LaneDbSnapshot>
  /** Wait until the stand-in shows a bot message containing `needle` (MarkdownV2 unescaped). */
  waitPhoneText(needle: string, userId?: number, timeoutMs?: number): Promise<void>
  gatewayPid(): number | undefined
  /** Gateway stdout+stderr file (inside the home; removed with it). */
  gatewayLog: string
  stop(): Promise<void>
}

export type LaneOptions = {
  modelUrl: string
  /** Extra top-level config.yaml keys (merged shallowly). */
  config?: Record<string, unknown>
}

export async function startLaneHermes(options: LaneOptions): Promise<LaneHermes> {
  const python = join(runtimeRoot, 'venv', 'bin', 'python')
  const src = join(runtimeRoot, 'src')
  // A short home under the system tmpdir: t2 put its home under a long
  // scratch path and the gateway's loop-tick watchdog could not bind its
  // AF_UNIX socket ("path too long", 104-byte limit on macOS).
  const home = mkdtempSync(join(tmpdir(), 'hh-lane-'))
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? '/usr/bin:/bin',
    HOME: home,
    HERMES_HOME: home,
    PYTHONPATH: src,
    PYTHONUNBUFFERED: '1',
    HERMES_DISABLE_LAZY_INSTALLS: '1',
    HERMES_GATEWAY_EXTERNAL_SUPERVISOR: '1',
    HERMES_TELEGRAM_DISABLE_FALLBACK_IPS: '1',
    HERMES_TELEGRAM_TEXT_BATCH_DELAY_SECONDS: '0',
    HERMES_TELEGRAM_TEXT_BATCH_SPLIT_DELAY_SECONDS: '0',
  }
  const standinLog = join(home, 'standin.log')
  const gatewayLog = join(home, 'gateway.log')

  const standin = spawn(python, [controllerScript], { cwd: src, env, stdio: ['pipe', 'pipe', 'pipe'] })
  standin.stderr!.on('data', (part: Buffer) => appendFileSync(standinLog, part))
  const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>()
  let nextId = 1
  const ready = new Promise<{ api_base: string; file_base: string; token: string }>((resolve, reject) => {
    standin.once('exit', (code) => reject(new Error(`telegram stand-in exited ${code}`)))
    createInterface({ input: standin.stdout! }).on('line', (line) => {
      const message = JSON.parse(line) as { ready?: boolean; id?: number; result?: unknown; error?: string; api_base: string; file_base: string; token: string }
      if (message.ready) {
        resolve(message)
        return
      }
      const waiter = pending.get(message.id!)
      pending.delete(message.id!)
      if (message.error) waiter?.reject(new Error(message.error))
      else waiter?.resolve(message.result)
    })
  })
  const control = <T>(command: string, params: Record<string, unknown> = {}): Promise<T> => {
    const id = nextId++
    return new Promise<T>((resolve, reject) => {
      pending.set(id, { resolve: resolve as (value: unknown) => void, reject })
      standin.stdin!.write(`${JSON.stringify({ id, command, ...params })}\n`)
    })
  }
  const telegram = await ready

  // JSON is valid YAML; config.yaml as JSON keeps nested keys unambiguous.
  writeFileSync(join(home, 'config.yaml'), JSON.stringify({
    model: { default: 'scripted-1', provider: 'custom', base_url: options.modelUrl, api_key: 'scripted' },
    dashboard: { public_url: 'https://hermes.example' },
    updates: { check: false },
    platforms: { telegram: { enabled: true, extra: { base_url: telegram.api_base, base_file_url: telegram.file_base } } },
    approvals: { mode: 'manual' },
    ...options.config,
  }, null, 2))
  writeFileSync(join(home, '.env'), [
    `HERMES_DASHBOARD_BASIC_AUTH_USERNAME=${LIVE_USERNAME}`,
    `HERMES_DASHBOARD_BASIC_AUTH_PASSWORD=${LIVE_PASSWORD}`,
    'HERMES_DASHBOARD_BASIC_AUTH_SECRET=hh-live-signing-secret-000000000000000000000000',
    `TELEGRAM_BOT_TOKEN=${telegram.token}`,
    `TELEGRAM_ALLOWED_USERS=${Object.values(LANE_USERS).join(',')}`,
    'TELEGRAM_HOME_CHANNEL=999',
    // The gateway keys again, in the home's .env and not only in the spawn
    // env: `POST /api/gateway/restart` (S24) kills this gateway and runs the
    // replacement inside a child of SERVE, with serve's environment rebuilt
    // by host_gateway_child_env (hermes_cli/web_server_gateway.py). Hermes
    // loads $HERMES_HOME/.env over os.environ at startup (env_loader
    // load_hermes_dotenv, override=True), so this is the only way the
    // replacement still refuses Telegram's real fallback IPs (adapter.py
    // ~2969: without the flag it does DNS-over-HTTPS discovery of
    // api.telegram.org) and keeps the stand-in as its only Telegram.
    'HERMES_TELEGRAM_DISABLE_FALLBACK_IPS=1',
    'HERMES_TELEGRAM_TEXT_BATCH_DELAY_SECONDS=0',
    'HERMES_TELEGRAM_TEXT_BATCH_SPLIT_DELAY_SECONDS=0',
    'HERMES_GATEWAY_EXTERNAL_SUPERVISOR=1',
    'HERMES_DISABLE_LAZY_INSTALLS=1',
    '',
  ].join('\n'))

  let gateway: ChildProcess | undefined = spawn(python, ['-m', 'hermes_cli.main', 'gateway', 'run'], { cwd: home, env, stdio: ['ignore', 'pipe', 'pipe'] })
  gateway.stdout!.on('data', (part: Buffer) => appendFileSync(gatewayLog, part))
  gateway.stderr!.on('data', (part: Buffer) => appendFileSync(gatewayLog, part))
  const until = async <T>(fn: () => Promise<T | undefined | false>, label: string, ms = 60_000): Promise<T> => {
    const end = Date.now() + ms
    while (Date.now() < end) {
      const value = await fn()
      if (value) return value
      await sleep(200)
    }
    throw new Error(`timed out: ${label}`)
  }
  const stopChild = async (child: ChildProcess | undefined) => {
    if (!child || child.exitCode !== null) return
    child.kill('SIGTERM')
    await Promise.race([new Promise((resolve) => child.once('exit', resolve)), sleep(8000)])
    if (child.exitCode === null) child.kill('SIGKILL')
  }
  const stopStandin = async () => {
    standin.stdin!.end()
    await Promise.race([new Promise((resolve) => standin.once('exit', resolve)), sleep(3000)])
    if (standin.exitCode === null) standin.kill('SIGKILL')
  }

  let serve: LiveHermes
  try {
    await until(async () => {
      if (gateway!.exitCode !== null) throw new Error(`gateway exited ${gateway!.exitCode} before connecting`)
      return (await control<StandinSnapshot>('snapshot')).connected
    }, 'gateway long-poll connected to the stand-in', 90_000)
    serve = await startLiveHermes({ modelUrl: options.modelUrl, home })
  } catch (error) {
    // Never leave a gateway polling the stand-in (or the stand-in itself)
    // behind a failed boot: a stray `hermes gateway run` is also what the
    // next `gateway restart`'s orphan reaper would go hunting for.
    await stopChild(gateway)
    await stopStandin()
    throw error
  }

  return {
    serve,
    home,
    python,
    gatewayLog,
    dm: (text, userId = LANE_USERS.owner, threadId) => control('dm', { text, user_id: userId, ...(threadId === undefined ? {} : { thread_id: threadId }) }),
    snapshot: (userId = LANE_USERS.owner) => control<StandinSnapshot>('snapshot', { user_id: userId }),
    press: (text, userId = LANE_USERS.owner) => control('press', { text, user_id: userId }),
    db: () => control<LaneDbSnapshot>('db', { path: join(home, 'state.db') }),
    waitPhoneText: async (needle, userId = LANE_USERS.owner, timeoutMs = 60_000) => {
      await until(async () => {
        const snap = await control<StandinSnapshot>('snapshot', { user_id: userId })
        // MarkdownV2 escapes reserved characters (`-` → `\-`); t2 run2 timed
        // out matching the escaped text.
        return snap.visible.some((entry) => entry.text.replace(/\\(.)/g, '$1').includes(needle))
      }, `phone shows "${needle}"`, timeoutMs)
    },
    gatewayPid: () => gateway?.pid,
    stop: async () => {
      await serve.stop()
      await stopChild(gateway)
      gateway = undefined
      await stopStandin()
    },
  }
}
