# Held prompts survive a reconnect

Size: short plan (root cause known from the server log). Tracks agent-code#1527.

## Evidence (verified, do not re-derive)
Hermes v0.22.0 server log, 2026-10-15 UTC, one Agent Code lane pane:
- 15:50:43 socket dropped (1006); runtime detached.
- 15:51:04 reconnect, `session.activate` on the old runtime → 4001.
- 15:51:07 `session.history` on the old runtime → 4001.
- 15:51:08 `prompt.submit` on the old runtime → 4001; the consumer showed "Could not send your held message".

4001 "session not found" comes from `tui_gateway/server.py` `_sess_nowait`: the runtime id is unknown, so the prompt was never accepted. The phone turn had run past `STALE_BUSY_TAIL_MS`, so the lane counted as free. A lane refresh (change signal) during the re-attach read history and drained the held prompt with the runtime id captured before `session.resume` replaced it.

## Change
1. **Attach barrier.** Every re-attach (`reconnect`, the `reclaimed` re-attach, the 4001 recovery) runs through one tracked promise. Session-scoped work that reads the runtime id (`submit`, `scheduleHistoryFetch`, `refreshLaneOnce`) waits for it first. A failed attach releases the barrier (the caller surfaces that failure).
2. **4001 recovery in `submit`.** On RpcError 4001, re-attach by stored id once and resubmit once. Safe: 4001 is answered before the prompt is accepted, so no duplicate turn. A second 4001 throws as today.

## Tests (recorded T5 lane traffic, v0.22.0; the 4001 frame is the server's `_err(rid, 4001, "session not found")`)
- Held prompt, socket drops, the phone turn goes stale, a change signal arrives while `session.resume` is pending → the server never sees a history/submit on the old runtime; exactly one `prompt.submit`, on the resumed runtime; no `hold-failed`. Fails before the fix.
- Connected, runtime reaped server-side: first submit answers 4001 → one `session.resume`, one retried submit on the new runtime, the outcome resolves.

## Verification
`npm test` and `npx tsc --noEmit` in the package; the agent-code bump runs its own typecheck and suite. Not verified here: a real network drop against the VPS (reported as such in the PR).

## Out of scope
Consumer UX (put text back, show the reason, client log): agent-code#1527's app PR.
