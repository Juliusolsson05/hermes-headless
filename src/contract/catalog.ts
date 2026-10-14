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
] as const

/**
 * Event types deliberately ignored in v1, with why. Each is a Desktop-only
 * surface (preview panes, pets, voice, onboarding), a change signal for lists
 * this package does not keep, or a feature listed as out of scope in the plan.
 */
export const IGNORED_EVENTS: Readonly<Record<string, string>> = {
  'skin.changed': 'Hermes theme; Agent Code has its own',
  'sessions.changed': 'change signal for a session list we do not mirror',
  'cron.changed': 'change signal',
  'platforms.changed': 'change signal',
  'projects.changed': 'change signal',
  'pairing.changed': 'change signal',
  'bot_relay.outbox.pending': 'bot-to-bot relay (Bot Mode), out of scope',
  'notification.show': 'Desktop toast; the reconciler surfaces turn errors itself',
  'notification.clear': 'Desktop toast',
  'tip.show': 'Desktop onboarding bubble',
  'pane.reveal': 'Desktop pane control',
  'preview.open': 'Desktop preview pane',
  'preview.close': 'Desktop preview pane',
  'todo.updated': 'todo panel, out of scope v1',
  'tool.output_risk': 'risk badge, out of scope v1',
  'reaction': 'affection reaction',
  'message.reaction': 'agent reaction to a message',
  'review.summary': 'background review',
  'session.control.update': 'goal/loop/heartbeat controls',
  'session.resume_progress': 'deferred hydration progress (we never defer)',
  'background.complete': '/background side agent',
  'btw.complete': '/btw side question',
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
  'subagent.spawn_requested': 'subagent roster, out of scope v1',
  'subagent.start': 'subagent roster',
  'subagent.progress': 'subagent roster',
  'subagent.thinking': 'subagent roster',
  'subagent.tool': 'subagent roster',
  'subagent.complete': 'subagent roster',
  'connection.request': 'connector operation cards, out of scope v1',
  'connection.update': 'connector operation cards',
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
