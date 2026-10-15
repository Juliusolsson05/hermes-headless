# Opt-in deployment smoke against a real Hermes server (agent-code#1502)

Size: short plan. Known shape: every step already has a library call and a recorded wire.

## Outcome
`HERMES_DEPLOYMENT_TEST=1 npm run test:live` checks that a REAL, already-deployed Hermes server (the owner's VPS) accepts everything Agent Code does on connect. It never prompts the agent, never restarts anything, and never writes to the server. Without the flag it is skipped, so CI and `npm test` never touch the network.

## Inputs
- `HERMES_DEPLOYMENT_URL`: the dashboard base URL.
- `HERMES_DEPLOYMENT_TOKEN_FILE`: a JSON `HermesTokens` file written by `research/deployment-sign-in.mts` (native PKCE in the system browser). It is never the app's saved credentials, which are encrypted and owned by the app.

## Steps (one `it` each, in order)
1. `GET /api/status` (no auth): 200, `version` in `TESTED_HERMES_RELEASES`, `auth_required`.
2. `POST /api/auth/ws-ticket` with the bearer token.
3. WSS with the ticket in the subprotocol: the server answers with the gateway subprotocol alone, then `gateway.ready` and `client.capabilities`.
4. WS `session.list`.
5. REST `GET /api/sessions?source=telegram` with the bearer token. This answers the open question of whether the proxy route forwards `Authorization` for REST.

The output is counts and versions only. No titles, chat ids, tokens or message text are printed.

## Out of scope
`/api/gateway/restart` (not read-only). Observed separately: the dashboard's Restart Gateway worked on the owner's Hostinger image on 2026-10-14 (gateway back to Running in about 20 s).

## Verification boundary
Running it needs the owner's token file. The package's own checks (typecheck, contract, `npm test`) prove it is skipped by default and type-correct.
