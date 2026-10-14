# hermes-headless

Drive a remote [Hermes Agent](https://github.com/NousResearch/hermes-agent) programmatically. It connects to the `hermes serve` backend that Hermes' own desktop app talks to, and exposes one conversation as typed event channels and commands.

It is built for [Agent Code](https://github.com/Juliusolsson05/agent-code), where a Hermes chat replaces the Telegram chat inside an agent pane. It mirrors the layout of its sibling packages (`claude-code-headless`, `codex-headless`, `opencode-headless`, `grok-code-headless`, `pi-terminal-headless`).

**The consumer owns everything with a side effect:**

- dialing sockets;
- storing and refreshing credentials;
- opening the sign-in browser;
- deciding when to reconnect.

This package never spawns, stores or schedules anything.

## Shape

### `HermesHeadless`

One conversation. You give it:

- `dial()`, which returns an open WebSocket, minting a fresh single-use ticket each time;
- a `target`: `{mode:'create', source}` or `{mode:'resume', storedSessionId}`.

Commands:

- `start()` and `reconnect()`;
- `prompt(text)` and `interrupt()`;
- `newConversation()` (Telegram's `/new`);
- `answerRequest()` and `declineRequest()`;
- `completeSlash()` and `runSlash()`;
- `stop()`.

### Channels

The same three names every sibling uses:

- **`semantic`**: the live turn, emitted directly in Agent Code's fold vocabulary (`turn_started`, `block_started` with a numeric `blockIndex`, `text_delta` with `textSoFar`, `tool_input_finalized`, `tool_result`, `turn_stopped`, `api_error`, …).
- **`committed`**: durable rows keyed by `row:<row_id>` / `tool:<tool_call_id>`, plus `boundary` resets for resume, `/new` and compression rewrites.
- **`screen`**: activity and status text.

### Emitter events

- `session`: stored id, runtime id, title, info.
- `request` / `request-closed`: approval, clarify, sudo and secret.
- `connection`.
- `session-replaced`.
- `unknown`.

### Auth helpers (pure HTTP, `fetch` injected)

`probe`, `createPkce`, `buildAuthorizeUrl`, `readAuthorizationCode`, `exchangeCode`, `refreshTokens`, `mintWsTicket`, `buildWsUrl`, `normalizeBaseUrl`.

These implement Hermes' RFC 8252 native-app sign-in, so password and Nous OAuth backends work the same way.

### `RpcConnection`

The transport: one JSON-RPC connection over one socket generation. It handles:

- the handshake;
- the heartbeat;
- declining requests nobody can answer;
- exactly-once event dispatch across reconnects (seq watermarks plus a replay hold).

### `backendCompatibility(desktop_contract)`

Tested against contract 8 (Hermes v0.21.7). Below 7 is unsupported: those backends send approvals as notifications this package does not speak.

## What it guarantees, and the recording behind each guarantee

`research/corpus/` holds real `hermes serve` traffic, recorded by `research/record-wire.mts`. The backend is Hermes v0.21.7 with its auth gate on and a scripted OpenAI-compatible model (`src/testing/scripted-model`). Credentials are redacted at write time and personal paths rewritten; `research/scan-corpus.mts` re-checks every file.

- **Sign-in and tickets (S1).** The native PKCE flow works for a third-party app with no registration. Tickets are single use.
- **Exactly-once across a dropped socket (S8).** `session.events.since` returns exactly the gap. The tests replay it with the drop moved to every position, and with live frames racing the reply.
- **Open approvals survive a reconnect (S10).** They are answered through `request.answer`.
- **Backend restart (S11).** A new `replay_epoch` means runtime ids are void, so the conversation is resumed by its stored id.
- **Detached turns keep running (S9).** A detached turn that is still producing is not interrupted. Hermes reaps only idle sessions or stalled turns.
- **A prompt while busy is held (S16).** A plain `prompt.submit` to a busy session *redirects*, which kills the running turn. The package sends `queued: true` instead.
- **Answer text is not shown twice (S3–S5).** `reasoning.available` carried the final answer verbatim in every recording, so it only becomes a thinking block when it isn't the answer.
- **Tool rows have no `row_id` (S4).** The resume projection carries tool rows with only `tool_call_id`, and without their result.
- **Compression rewrites row ids (S12).** It is detected as a history rewrite instead of being appended as duplicates.

## Tests

- **`npm test`**: core + system tiers on the recorded corpus. No Python needed; this is what CI runs.
- **`npm run live:setup && npm run test:live`**: boots the pinned Hermes from source (`uv` + Python 3.14) and drives the public API end to end. This covers sign-in, streaming, tools, every request type, a mid-turn drop, a busy prompt, `/new`, resume, slash commands and a backend restart.
- **`npm run record -- S8`**: re-records one scenario.

## Credit

The transport mirrors behaviour of Hermes Desktop's `@hermes/shared` client (MIT, Nous Research): ticket per dial, capability announcement, replay hold and epoch checks. The wire types in `src/contract/` are trimmed from Hermes' generated contract at v0.21.7.

## License

MIT
