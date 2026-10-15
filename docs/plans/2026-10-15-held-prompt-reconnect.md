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
1. **Attach barrier.** Every re-attach (`reconnect`, the `reclaimed` re-attach, the 4001 recovery, and, after review, the lane-move resume) runs through one tracked promise. Session-scoped work that reads the runtime id (`submit`, `scheduleHistoryFetch`, `refreshLaneOnce`) waits for it first. A failed attach releases the barrier (the caller surfaces that failure). Overlapping attaches QUEUE behind the one in flight (review); they never replace it.
2. **4001 recovery in `submit`.** On RpcError 4001, re-attach by stored id once and resubmit once. Safe: 4001 is answered before the prompt is accepted, so no duplicate turn. A second 4001 throws as today.

## Tests (recorded T5 lane traffic, v0.22.0; the 4001 frame is the server's `_err(rid, 4001, "session not found")`)
- Held prompt, socket drops, the phone turn goes stale, a change signal arrives while `session.resume` is pending → the server never sees a history/submit on the old runtime; exactly one `prompt.submit`, on the resumed runtime; no `hold-failed`. Fails before the fix.
- Connected, runtime reaped server-side: first submit answers 4001 → one `session.resume`, one retried submit on the new runtime, the outcome resolves.

## Review round (agent-code#1529, one Claude and one Codex reviewer)
Accepted and fixed; each has a test that fails with the fix reverted unless noted.
- **Overlapping attaches** (both reviewers reproduced it): a second attach replaced the barrier, was refused by `acquire()`, and released the barrier while the first resume was pending. Fix: `trackAttach` queues. An uncontended attach still starts synchronously, because reconnect callers push the server's first frame in the same tick. Test: a `session.reclaimed` broadcast during a pending 4001 recovery, then `sessions.changed` → no history or submit on the old runtime.
- **Two 4001s → two resumes:** `recoverRuntime` waits for an attach in flight and re-attaches only if the runtime is still the stale one. Test: two prompts refused together → one recovery resume, both sent.
- **A failed recovery read as "may have been sent":** wrapped as `HermesPromptError('not-ready', …, 4001, <refused submit id>)`. Test: recovery resume answers 5000.
- **Lane retry into an ended conversation** (Codex, reproduced on T6): after recovery, lane mode runs `refreshLane()` (re-resolve the tip, follow a move under the barrier) before the one retry. Busy is not re-decided; `queued: true` covers a turn that started meanwhile. Test: the thread's descendant chain moves while the recovery resume is pending → the retry goes to the new tip.
- **Lane 4007 → `session.create`** (Claude): `reattach` never creates in lane mode (the gateway would not route the thread to it). Hermes also answers 4007 transiently ("retry resume"), so it is rethrown. Test: no `session.create`, no `session-replaced`.
- **"Send now" dropped the text on failure:** `releaseHeld` emits `hold-failed` with the text, then rethrows. Test: a 4090 refusal.
- **Lane move under the barrier:** applied. Defense in depth with no isolated test; the mutant survives the suite.

## Verification
`npm test` and `npx tsc --noEmit` in the package; the agent-code bump runs its own typecheck and suite. Not verified here: a real network drop against the VPS (reported as such in the PR).

## Out of scope
Consumer UX (put text back, show the reason, client log): agent-code#1527's app PR.
