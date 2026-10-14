// Where each slash command goes. Mirrors Hermes Desktop's routing table
// (apps/desktop/src/lib/desktop-slash-commands.ts at v0.22.0), which is
// upstream's own record of which commands CANNOT ride `slash.exec`.
//
// WHY not "everything through slash.exec" (what v1 did): `slash.exec` runs
// the command in a slash-worker process and returns what it printed inside
// a capture window. Three families break that contract:
//  - /btw and /bg print their answer from a thread AFTER the window closed,
//    so the pane saw "started" and then silence forever (#99065, #97635,
//    #57444; r4 F6). They have dedicated RPCs whose answer arrives later as
//    `btw.complete` / `background.complete`.
//  - /compress calls the model to summarise, which outlives the worker's
//    30 s budget on a large session, and the fallback then reports a bogus
//    "not a quick/plugin/skill command" (#44456).
//  - /title, /status, /save have direct session RPCs that answer the live
//    session instead of a worker's copy of it.
//
// Commands that need a picker or a Desktop surface (/model, /resume,
// /handoff, /profile, /branch, …) are refused by the CONSUMER before they
// reach here (Agent Code's composer catalog marks them unavailable with a
// sentence). This table only knows how to execute.

export type SlashRoute =
  /** `/new`, `/reset`, `/clear`: client-side conversation reset. */
  | { kind: 'new-conversation' }
  /** `/stop`: interrupt the running turn (session.interrupt). */
  | { kind: 'stop' }
  /** `/bg`, `/btw`: a detached side agent whose answer arrives as an event. */
  | { kind: 'side'; side: 'background' | 'btw' }
  | { kind: 'compress' }
  | { kind: 'title' }
  | { kind: 'status' }
  | { kind: 'save' }
  /** Everything else: `slash.exec`, falling back to `command.dispatch` on 4018. */
  | { kind: 'exec' }

const ROUTES: Readonly<Record<string, SlashRoute>> = {
  new: { kind: 'new-conversation' },
  reset: { kind: 'new-conversation' },
  // Desktop treats /clear as /new (desktop-slash-commands.ts: aliases of /new).
  clear: { kind: 'new-conversation' },
  stop: { kind: 'stop' },
  bg: { kind: 'side', side: 'background' },
  background: { kind: 'side', side: 'background' },
  btw: { kind: 'side', side: 'btw' },
  compress: { kind: 'compress' },
  compact: { kind: 'compress' },
  title: { kind: 'title' },
  status: { kind: 'status' },
  save: { kind: 'save' },
}

export type ParsedSlash = {
  /** Lower-cased command name without the slash. */
  name: string
  /** Everything after the first run of whitespace, trimmed. */
  arg: string
  /** The command as typed, without the leading slash. */
  body: string
}

export function parseSlash(input: string): ParsedSlash {
  const body = input.trim().replace(/^\//, '')
  const match = /^(\S*)\s*([\s\S]*)$/.exec(body)
  return { name: (match?.[1] ?? '').toLowerCase(), arg: (match?.[2] ?? '').trim(), body }
}

export function routeSlash(name: string): SlashRoute {
  return ROUTES[name.toLowerCase()] ?? { kind: 'exec' }
}
