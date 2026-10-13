"""THROWAWAY (overnight 2026-10-13, contextmd#53): every live Cue score on rocky with the moment's current user message.

Reads contextmd.sqlite and chromeria-v2.sqlite read-only. One line per Cue-scored moment (cue_status ok): ids,
kind, harness, session, run, thread, time, the intent scores and outcomes, and the current user message (the last
user item of the window Cue scored) for classification. PRIVATE output: transcript text.

    python3 -I misfire_extract.py OUT.jsonl
"""

import json
import sqlite3
import sys

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from extract import CM, DB, transcript, ts  # noqa: E402


def main():
    out = sys.argv[1]
    cm, db = sqlite3.connect(CM, uri=True), sqlite3.connect(DB, uri=True)
    rows = cm.execute("SELECT id, received_at, harness, kind, session, thread_id, run_id, tool_use_id, holdout "
                      "FROM moments WHERE cue_status = 'ok' ORDER BY id").fetchall()
    n = {"written": 0, "no_thread": 0, "no_cut": 0}
    with open(out, "w") as fh:
        for mid, at, harness, kind, session, thread, run, tool_use, holdout in rows:
            if not thread:
                n["no_thread"] += 1
                continue
            if kind == "user_message":
                cur = db.execute("SELECT ordinal, payload_json FROM orchestration_v2_projection_turn_items WHERE "
                                 "thread_id = ? AND type = 'user_message' ORDER BY ordinal DESC", (thread,)).fetchall()
                cut = next((o for o, p in cur if ts(json.loads(p)["startedAt"]) <= at + 2), None)
            else:
                it = db.execute("SELECT ordinal FROM orchestration_v2_projection_turn_items WHERE thread_id = ? AND "
                                "(turn_item_id LIKE ? OR turn_item_id LIKE ?)",
                                (thread, f"%:{tool_use}", f"%3A{tool_use}")).fetchone() if tool_use else None
                cut = it[0] - 1 if it else None
            if cut is None:
                n["no_cut"] += 1
                continue
            items = transcript(db, thread, cut)
            users = [t for k, t in items if k == "user"]
            lu = db.execute("SELECT max(ordinal) FROM orchestration_v2_projection_turn_items WHERE thread_id = ? AND "
                            "type = 'user_message' AND ordinal <= ?", (thread, cut)).fetchone()[0]
            notes = [json.loads(p) for (p,) in db.execute(
                "SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE thread_id = ? AND "
                "type = 'notification' AND ordinal > ? AND ordinal <= ? ORDER BY ordinal", (thread, lu or -1, cut))]
            notice = [n.get("summary", "")[:120] for n in notes
                      if (n.get("source") or {}).get("kind") in ("delegated_task", "background_command", "monitor")]
            cands = cm.execute("SELECT entry, score, outcome FROM candidates WHERE moment_id = ? AND matcher = "
                               "'intent'", (mid,)).fetchall()
            fh.write(json.dumps({"id": mid, "at": at, "harness": harness, "kind": kind, "session": session,
                                 "run": run, "thread": thread, "holdout": holdout, "cut": cut,
                                 "user": users[-1] if users else None, "notices": notice, "n_users": len(users),
                                 "scores": {e: s for e, s, _ in cands}, "outcomes": {e: o for e, _, o in cands}}) + "\n")
            n["written"] += 1
    print(json.dumps({"moments": len(rows), **n}))


main()
