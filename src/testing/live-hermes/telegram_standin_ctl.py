"""JSONL controller around upstream's UNCHANGED Telegram Bot API stand-in.

Ported from the t2 research driver (temp/hermes-research/scratch-t2/standin.py).

WHY upstream's stand-in and not a fake of our own: only the remote platform
(Telegram's servers) is simulated. The child `hermes gateway run` still runs
its real Telegram plugin, python-telegram-bot's HTTP stack, admission, the
session store, the agent loop and delivery, pointed at this server through the
adapter's supported `platforms.telegram.extra.base_url`. A home-grown fake would
encode our reading of the gateway; this one is what upstream tests itself with
(tests/fakes/platforms/telegram_standin.py, tests/e2e/core/platforms/_drv_telegram.py).

Run with the Hermes runtime's own python and `cwd`/PYTHONPATH = runtime/src so
`tests.fakes…` imports from the archived source of the release under test.

Protocol: one JSON object per line on stdin `{id, command, ...}`, one reply per
line on stdout `{id, result}` or `{id, error}`. The first stdout line is
`{ready, api_base, file_base, token}`. The token is the stand-in's fixed fake
("123456:standin-token"); it is never a real bot token.
"""
import dataclasses
import json
import sqlite3
import sys
import traceback

from tests.fakes.platforms.telegram_standin import TelegramStandin

standin = TelegramStandin()
standin.start()
print(json.dumps({"ready": True, "api_base": standin.api_base, "file_base": standin.file_base, "token": standin.token}), flush=True)


def _columns(db, table):
    return [row[1] for row in db.execute(f"pragma table_info({table})")]


def _select(db, table, wanted, order):
    # Select only columns this release has: the sessions schema grows between
    # releases (v0.21.7 vs v0.22.0) and the controller serves both.
    have = set(_columns(db, table))
    cols = [c for c in wanted if c in have]
    return [dict(x) for x in db.execute(f"select {', '.join(cols)} from {table} order by {order}")]


try:
    for line in sys.stdin:
        request = json.loads(line)
        try:
            command = request["command"]
            if command == "dm":
                user_id = int(request.get("user_id", 111))
                thread_id = request.get("thread_id")
                if thread_id is None:
                    result = standin.dm(user_id, request["text"])
                else:
                    # The public dm() has no topic parameter. A Bot API private
                    # chat message in a DM topic carries `message_thread_id`
                    # (and `is_topic_message`), so this builds the same update
                    # through the stand-in's own `_inbound` (which accepts any
                    # message fields) instead of patching the stand-in.
                    chat = {"id": user_id, "type": "private", "first_name": "Tester", "username": f"user{user_id}"}
                    result = standin._inbound(chat, user_id, text=request["text"],
                                              message_thread_id=int(thread_id), is_topic_message=True)
            elif command == "snapshot":
                chat_id = str(request.get("user_id", 111))
                result = {
                    "connected": bool(standin.calls_of("getUpdates")),
                    "visible": [dataclasses.asdict(x) for x in standin.visible(chat_id)],
                    # Only the outbound "send something visible" calls; the
                    # getUpdates long-poll loop would dominate the list.
                    "sends": [dataclasses.asdict(x) for x in standin.calls
                              if x.method not in ("getUpdates", "getMe", "getMyCommands", "setMyCommands",
                                                  "deleteWebhook", "getChat", "sendChatAction")],
                }
            elif command == "press":
                # A phone user tapping an inline button. Telegram `/new` (and
                # other destructive commands) answer with a "Confirm /new"
                # card whose buttons must be tapped before anything happens
                # (first T6 run: the row never ended because nobody tapped).
                user_id = int(request.get("user_id", 111))
                needle = request["text"]
                matches = [b for b in standin.buttons(str(user_id)) if needle in str(b.get("text", ""))]
                if not matches:
                    raise ValueError(f"no button containing {needle!r}: {[b.get('text') for b in standin.buttons(str(user_id))]}")
                button = matches[-1]
                standin.callback(user_id, user_id, int(button["message_id"]), button["callback_data"])
                result = {"pressed": button.get("text"), "callback_data": button.get("callback_data")}
            elif command == "db":
                db = sqlite3.connect("file:" + request["path"] + "?mode=ro", uri=True)
                db.row_factory = sqlite3.Row
                result = {
                    "sessions": _select(db, "sessions", [
                        "id", "source", "session_key", "chat_id", "chat_type", "thread_id", "title",
                        "started_at", "ended_at", "end_reason", "parent_session_id", "message_count",
                    ], "started_at"),
                    "messages": _select(db, "messages", ["id", "session_id", "role", "content", "timestamp"], "id"),
                }
                db.close()
            else:
                raise ValueError(command)
            print(json.dumps({"id": request["id"], "result": result}, default=str), flush=True)
        except Exception:
            print(json.dumps({"id": request["id"], "error": traceback.format_exc()}), flush=True)
finally:
    standin.stop()
