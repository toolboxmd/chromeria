"""THROWAWAY (overnight 2026-10-13, from chromeria#248): rebuild Cue requests for rocky's logged moments, as ContextmdServing.transcript does.

Reads ~/.t3/userdata/contextmd.sqlite and chromeria-v2.sqlite read-only (mode=ro) and writes one JSON line per
moment: the items Chromeria would have sent, the moment's call and output (approximated from the projected tool
item, the hook payload is not stored), and the live reply logged for it (window_chars, scores) for validation.

    python3 -I extract.py OUT.jsonl [--hours 24] [--pre-tool N] [--seed S] [--slice FROM_H TO_H]

--slice keeps every moment received between FROM_H and TO_H hours ago, in time order (no pre-tool sampling).
"""

import argparse
import json
import os
import random
import sqlite3
import time
from datetime import datetime

HOME = os.path.expanduser("~")
CM = f"file:{HOME}/.t3/userdata/contextmd.sqlite?mode=ro"
DB = f"file:{HOME}/.t3/userdata/chromeria-v2.sqlite?mode=ro"
USERS, REPLIES, SINCE = 6, 100, 100  # ContextmdServing.ts TRANSCRIPT_*
ITEM_CHARS, TOOL_CHARS = 12_000, 300  # Cue.ts
TOOLS = ("command_execution", "dynamic_tool", "file_change", "file_search", "web_search")


def clip(t, n):
    return t if len(t) <= n else f"{t[: n // 2]}[...]{t[len(t) - n // 2:]}"


def js(v):
    return v if isinstance(v, str) else "" if v is None else json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def items_of(rows):
    """Cue.ts transcriptItems."""
    out = []
    for _, typ, payload in rows:
        try:
            p = json.loads(payload)
        except ValueError:
            continue
        if typ == "user_message" and isinstance(p.get("text"), str):
            out.append(["user", clip(p["text"], ITEM_CHARS)])
        elif typ == "assistant_message" and isinstance(p.get("text"), str):
            if p["text"].strip():
                out.append(["asst", clip(p["text"], ITEM_CHARS)])
        elif typ in TOOLS:
            name = js(p.get("toolName")) or typ
            what = js(p.get("input")) or js(p.get("fileName")) or js(p.get("pattern")) or js(p.get("patterns"))
            out.append(["tool", clip(f"{name}: {what}", TOOL_CHARS)])
    return out


def ts(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def transcript(db, thread, cut):
    """ContextmdServing.transcript over the items with ordinal <= cut."""
    q = "SELECT ordinal, type, payload_json FROM orchestration_v2_projection_turn_items WHERE thread_id = ? "
    users = db.execute(q + "AND type = 'user_message' AND ordinal <= ? ORDER BY ordinal DESC LIMIT ?",
                       (thread, cut, USERS)).fetchall()
    if not users:
        return []
    first, current = users[-1][0], users[0][0]
    replies = db.execute(q + "AND type = 'assistant_message' AND ordinal > ? AND ordinal < ? "
                         "ORDER BY ordinal DESC LIMIT ?", (thread, first, current, REPLIES)).fetchall()
    since = db.execute(q + f"AND ordinal > ? AND ordinal <= ? AND type IN ('assistant_message', "
                       f"{', '.join(repr(t) for t in TOOLS)}) ORDER BY ordinal DESC LIMIT ?",
                       (thread, current, cut, SINCE)).fetchall()
    return items_of(sorted(users + replies + since))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--pre-tool", type=int, default=1200)
    ap.add_argument("--seed", type=int, default=248)
    ap.add_argument("--slice", type=float, nargs=2)
    a = ap.parse_args()
    cm, db = sqlite3.connect(CM, uri=True), sqlite3.connect(DB, uri=True)
    since = time.time() - a.hours * 3600
    rows = cm.execute("SELECT id, received_at, harness, kind, session, thread_id, tool_use_id, cue_status, cue_json "
                      "FROM moments WHERE received_at > ? AND thread_id IS NOT NULL AND kind IN "
                      "('user_message', 'pre_tool', 'tool_failure') ORDER BY id", (since,)).fetchall()
    if a.slice:
        lo, hi = time.time() - a.slice[0] * 3600, time.time() - a.slice[1] * 3600
        picked = [r for r in rows if lo <= r[1] < hi]
    else:
        pre = [r for r in rows if r[3] == "pre_tool" and r[6]]
        random.Random(a.seed).shuffle(pre)
        keep = {r[0] for r in pre[: a.pre_tool]}
        picked = [r for r in rows if r[3] != "pre_tool" or r[0] in keep]
    n = {"no_item": 0, "no_user": 0, "written": 0}
    with open(a.out, "w") as fh:
        for mid, at, harness, kind, session, thread, tool_use, status, cue_json in picked:
            call = output = None
            if kind == "user_message":
                cur = db.execute("SELECT ordinal, payload_json FROM orchestration_v2_projection_turn_items WHERE "
                                 "thread_id = ? AND type = 'user_message' ORDER BY ordinal DESC", (thread,)).fetchall()
                cut = next((o for o, p in cur if ts(json.loads(p)["startedAt"]) <= at + 2), None)
            else:
                if not tool_use:
                    continue
                it = db.execute("SELECT ordinal, type, payload_json FROM orchestration_v2_projection_turn_items "
                                "WHERE thread_id = ? AND (turn_item_id LIKE ? OR turn_item_id LIKE ?)",
                                (thread, f"%:{tool_use}", f"%3A{tool_use}")).fetchone()  # Grok ids are URL-encoded
                if it is None:
                    n["no_item"] += 1
                    continue
                cut = it[0] - 1  # the call itself is sent as `call`, not as an item
                p = json.loads(it[2])
                name = js(p.get("toolName")) or ("Bash" if it[1] == "command_execution" else it[1])
                call = f"{name}: {js(p.get('input'))}"[:2000]
                if kind == "tool_failure":
                    output = js(p.get("output"))[-2000:]
            if cut is None:
                n["no_user"] += 1
                continue
            items = transcript(db, thread, cut)
            if not any(k == "user" for k, _ in items):
                n["no_user"] += 1
                continue
            # The live reply: the moment's own for a score, the call's tool_result for a started pre-tool request.
            live = (status, cue_json, mid)
            if kind == "pre_tool":
                live = cm.execute("SELECT cue_status, cue_json, id FROM moments WHERE kind = 'tool_result' AND "
                                  "harness = ? AND session = ? AND tool_use_id = ? ORDER BY id LIMIT 1",
                                  (harness, session, tool_use)).fetchone() or (None, None, None)
            scores = {}
            if live[0] == "ok":
                scores = {e: s for e, s in cm.execute("SELECT entry, score FROM candidates WHERE moment_id = ? AND "
                                                      "matcher = 'intent'", (live[2],))}
            fh.write(json.dumps({"id": mid, "at": at, "harness": harness, "session": session, "kind": kind, "thread": thread,
                                 "items": items[-400:], "call": call, "output": output,
                                 "live_status": live[0], "live": json.loads(live[1]) if live[1] else None,
                                 "live_scores": scores}) + "\n")
            n["written"] += 1
    print(json.dumps({"moments": len(rows), "picked": len(picked), **n}))


if __name__ == "__main__":
    main()
