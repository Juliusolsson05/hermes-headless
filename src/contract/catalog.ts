// What this package does with every event type and server request Hermes can
// send. The decomposition's "one owner per signal" table (§9), in code.
//
// WHY an explicit catalog instead of "handle what we know, drop the rest":
// silently dropped frames are how a provider grows a feature nobody notices
// (Hermes adds events weekly). An event type that is in neither list is
// reported as an `unknown` shape once per type, so a new upstream event is
// discovered from a real session instead of from a missing UI months later.

/** Event types the reconciler consumes. */
export const HANDLED_EVENTS = [
  'gateway.ready',
  'session.info',
  'session.title',
  'session.usage',
  'session.reclaimed',
  'message.start',
  'message.delta',
  'message.interim',
  'message.complete',
  'reasoning.delta',
  'reasoning.available',
  // Spinner rewrites and provider-wait notes (Hermes Desktop shows them only
  // as a status line, message-stream.ts:243-253). Consumed as activity text.
  'thinking.delta',
  'tool.generating',
  'tool.start',
  'tool.complete',
  'status.update',
  'error',
  'notice',
  'request.cancel',
  'approval.cancelled',
  // Moved out of IGNORED by agent-code#1497 / plan Stage B (r4 F9 judged
  // each one): dropping them hid real information from the pane.
  //  - out-of-band notices: model-switch refusals, provider warnings, "still
  //    starting the agent" (server.py:995). Silently invisible before.
  'notification.show',
  'notification.clear',
  //  - "Hermes updated its memory/skills" after a turn (server.py:1060).
  'review.summary',
  //  - delegate_task children: without these a long delegation was one
  //    spinner and nothing else. Start/complete only, as activity text.
  'subagent.start',
  'subagent.complete',
  //  - a safety signal on tool output (prompt-injection / secret findings).
  'tool.output_risk',
  //  - a connector the turn waits on; Agent Code cannot show the card, but
  //    it must say where to act, or the turn looks hung.
  'connection.request',
  //  - the ANSWERS to /bg and /btw. Lost entirely before (r4 F6).
  'background.complete',
  'btw.complete',
  //  - the cross-process change signal. HermesHeadless (not the reconciler)
  //    consumes it in Telegram-lane mode to refetch history (plan Stage D).
  'sessions.changed',
] as const

/**
 * Event types deliberately ignored in v1, with why. Each is a Desktop-only
 * surface (preview panes, pets, voice, onboarding), a change signal for lists
 * this package does not keep, or a feature listed as out of scope in the plan.
 */
export const IGNORED_EVENTS: Readonly<Record<string, string>> = {
  'skin.changed': 'Hermes theme; Agent Code has its own',
  'cron.changed': 'change signal',
  'platforms.changed': 'change signal',
  'projects.changed': 'change signal',
  'pairing.changed': 'change signal',
  'bot_relay.outbox.pending': 'bot-to-bot relay (Bot Mode), out of scope',
  'tip.show': 'Desktop onboarding bubble',
  'pane.reveal': 'Desktop pane control',
  'preview.open': 'Desktop preview pane',
  'preview.close': 'Desktop preview pane',
  'todo.updated': 'todo panel, out of scope v1',
  'reaction': 'affection reaction',
  'message.reaction': 'agent reaction to a message',
  'session.control.update': 'goal/loop/heartbeat controls',
  'session.resume_progress': 'deferred hydration progress (we never defer)',
  'preview.restart.complete': 'Desktop preview restart',
  'preview.restart.progress': 'Desktop preview restart',
  'agent.terminal.output': 'background process output pane',
  'terminal.close': 'background process pane',
  'browser.progress': 'browser control surface',
  'browser.controller.command': 'browser control surface',
  'browser.controller.cancel': 'browser control surface',
  'voice.interrupted': 'voice mode',
  'voice.status': 'voice mode',
  'voice.partial': 'voice mode',
  'voice.transcript': 'voice mode',
  'wake.detected': 'voice mode',
  'pet.changed': 'pets',
  'pet.generate.progress': 'pets',
  'pet.hatch.progress': 'pets',
  'onboarding.ended': 'first-run onboarding',
  'setup.ready': 'first-run onboarding',
  'free_tier.challenge': 'Nous free tier sign-in',
  'billing.step_up.verification': 'billing',
  'moa.reference': 'mixture-of-agents view, out of scope v1',
  'moa.aggregating': 'mixture-of-agents view',
  'moa.progress': 'mixture-of-agents view',
  'moa.phase': 'mixture-of-agents view',
  'subagent.spawn_requested': 'subagent roster; start/complete are handled as activity',
  'subagent.progress': 'subagent roster',
  'subagent.thinking': 'subagent roster',
  'subagent.tool': 'subagent roster',
  'connection.update': 'connector operation cards (the request itself becomes a notice)',
  'display.status': 'remote display',
  'display.lease': 'remote display',
  'display.install.log': 'remote display',
  'display.install.done': 'remote display',
}

/** Server→client requests a consumer can answer. */
export const ANSWERABLE_REQUESTS = ['approval', 'clarify', 'sudo', 'secret'] as const
export type AnswerableRequestMethod = (typeof ANSWERABLE_REQUESTS)[number]

export function isAnswerableRequest(method: string): method is AnswerableRequestMethod {
  return (ANSWERABLE_REQUESTS as readonly string[]).includes(method)
}

export function isHandledEvent(type: string): boolean {
  return (HANDLED_EVENTS as readonly string[]).includes(type)
}

export function isIgnoredEvent(type: string): boolean {
  return Object.prototype.hasOwnProperty.call(IGNORED_EVENTS, type)
}
