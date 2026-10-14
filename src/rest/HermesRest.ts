// The few dashboard REST routes this package needs beside the WebSocket.
//
// WHY REST at all (plan §3.3): the WS `session.list` carries no
// `session_key`, `chat_type` or `ended_at`, so it cannot tell which stored
// conversation IS the owner's Telegram DM thread. `GET /api/sessions`
// returns every column but the system prompt and model config
// (web_routers/sessions.py get_sessions), and it is what Hermes Desktop's
// own "Messaging" list reads. Restarting the messaging gateway likewise has
// no WS method (`/restart` is gateway-only), only `POST /api/gateway/restart`.
//
// Like the sign-in helpers this owns no credentials: the consumer passes an
// `accessToken()` capability (Agent Code's token broker, which single-flights
// refreshes). A refused bearer surfaces as HermesAuthError `access-expired`;
// the request is retried ONCE with `{ forceRefresh: true }` so a token that
// died between the broker's 60 s margin and the server's clock skew does not
// sign the user out.

import { authorizedJson, HermesAuthError, type FetchLike } from '../auth/index.js'

export type AccessTokenSource = (options?: { forceRefresh?: boolean }) => Promise<string>

export type HermesRestOptions = {
  baseUrl: string
  accessToken: AccessTokenSource
  fetchImpl?: FetchLike
}

/**
 * One row of `GET /api/sessions` (hermes_state sessions table, compact).
 * Only the fields this package reads are typed; the rest pass through.
 */
export type SessionListRow = {
  id: string
  source: string | null
  title?: string | null
  /** The gateway's routing key; for a Telegram DM `agent:main:telegram:dm:<chat_id>`. */
  session_key?: string | null
  chat_id?: string | null
  chat_type?: string | null
  thread_id?: string | null
  user_id?: string | null
  display_name?: string | null
  started_at?: number | null
  ended_at?: number | null
  end_reason?: string | null
  parent_session_id?: string | null
  last_active?: number | null
  message_count?: number | null
  [key: string]: unknown
}

export type SessionListResult = { sessions: SessionListRow[]; total?: number }

export type LatestDescendant = {
  requested_session_id: string
  session_id: string
  path: string[]
  changed: boolean
}

export type GatewayRestartResult = { ok: boolean; pid?: number; name?: string }

export class HermesRest {
  constructor(private readonly options: HermesRestOptions) {}

  async listSessions(query: { source?: string; order?: 'recent' | 'created'; limit?: number; profile?: string } = {}): Promise<SessionListResult> {
    const params = new URLSearchParams()
    if (query.source) params.set('source', query.source)
    if (query.order) params.set('order', query.order)
    if (query.limit !== undefined) params.set('limit', String(query.limit))
    if (query.profile) params.set('profile', query.profile)
    const qs = params.toString()
    const body = await this.request<SessionListResult>({ path: `/api/sessions${qs ? `?${qs}` : ''}`, what: 'Listing Hermes conversations' })
    return { sessions: Array.isArray(body?.sessions) ? body.sessions : [], total: body?.total }
  }

  /** The compression tip of a stored conversation (follows compression only, never `/new`). */
  async latestDescendant(sessionId: string, profile?: string): Promise<LatestDescendant> {
    const qs = profile ? `?profile=${encodeURIComponent(profile)}` : ''
    return this.request<LatestDescendant>({
      path: `/api/sessions/${encodeURIComponent(sessionId)}/latest-descendant${qs}`,
      what: 'Finding the current Hermes conversation',
    })
  }

  /**
   * `hermes gateway restart`, spawned by the dashboard (actions.py
   * restart_gateway). Returns as soon as the child is spawned; the restart
   * itself takes ~1 minute and is observed through `/api/status`
   * (`gateway_running`). Hermes coalesces repeats within 10 s
   * (web_server.py GATEWAY_RESTART_COOLDOWN_SECONDS).
   */
  async restartGateway(): Promise<GatewayRestartResult> {
    return this.request<GatewayRestartResult>({ path: '/api/gateway/restart', method: 'POST', body: {}, what: 'Restarting the Hermes gateway' })
  }

  private async request<T>(args: { path: string; method?: 'GET' | 'POST'; body?: unknown; what: string }): Promise<T> {
    const call = async (forceRefresh: boolean) => authorizedJson<T>({
      baseUrl: this.options.baseUrl,
      accessToken: await this.options.accessToken(forceRefresh ? { forceRefresh: true } : undefined),
      ...args,
    }, this.options.fetchImpl)
    try {
      return await call(false)
    } catch (error) {
      if (!(error instanceof HermesAuthError) || error.kind !== 'access-expired') throw error
      return call(true)
    }
  }
}
