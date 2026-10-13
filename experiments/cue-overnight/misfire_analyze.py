"""THROWAWAY (overnight 2026-10-13, contextmd#53): replay serving policies over rocky's logged Cue scores.

Reference: the live full-window scores (threshold 0.5625) and today's serving rule (an entry is served once per
run; later fires in the same run are deduped). A "serve" below is a fire that today's rule would serve (first in
its run), counted for holdout sessions too (their would-serves). Policies change only which fires serve.

    python3 -I misfire_analyze.py MOMENTS.jsonl [LABELS.json]

LABELS.json (optional): {"<session>|<sha1 of the current user message, 16 hex>": "yes" | "no"}: whether
Reflection applies to that user message, used to split each policy's removed serves into right and wrong ones.
"""

import collections
import hashlib
import json
import sys

T = 0.5625
REF = "agentsmd-reflection"


def key(r):
    return f"{r['session']}|{hashlib.sha1((r['user'] or '').encode()).hexdigest()[:16]}"


def serves(R, policy, t_ref=T):
    """(moment id, entry) pairs served under a policy, in time order."""
    out, seen_run, seen_msg = [], set(), set()
    for r in sorted(R, key=lambda r: r["at"]):
        for e, s in sorted(r["scores"].items()):
            t = t_ref if e == REF else T
            if s is None or s < t:
                continue
            if policy.get("skip_notice") and r["notices"]:
                continue
            if policy.get("ref_user_only") and e == REF and r["kind"] != "user_message":
                continue
            if (r["run"], e) in seen_run:
                continue
            m = (r["session"], e, r["user"] or "")
            if policy.get("once_per_msg") and m in seen_msg:
                continue
            seen_run.add((r["run"], e))
            seen_msg.add(m)
            out.append((r["id"], e))
    return out


POLICIES = {
    "today (once per run)": {},
    "A skip notice-started turns": {"skip_notice": True},
    "B once per session, entry and user message": {"once_per_msg": True},
    "C Reflection only at user messages": {"ref_user_only": True},
    "A+B": {"skip_notice": True, "once_per_msg": True},
    "Reflection threshold 1.0": {"t_ref": 1.0},
    "Reflection threshold 1.5": {"t_ref": 1.5},
    "Reflection threshold 2.0": {"t_ref": 2.0},
    "Reflection threshold 3.0": {"t_ref": 3.0},
    "B + Reflection threshold 1.0": {"once_per_msg": True, "t_ref": 1.0},
    "B + Reflection threshold 1.5": {"once_per_msg": True, "t_ref": 1.5},
    "A+B + Reflection threshold 1.5": {"skip_notice": True, "once_per_msg": True, "t_ref": 1.5},
}


def main():
    R = [json.loads(line) for line in open(sys.argv[1])]
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    labels = json.load(open(args[1])) if len(args) > 1 else {}
    by = {r["id"]: r for r in R}
    sessions = len({r["session"] for r in R})
    print(f"moments {len(R)} ({dict(collections.Counter(r['kind'] for r in R))}), sessions {sessions}, "
          f"notice-turn moments {sum(1 for r in R if r['notices'])}")
    fires = [(r["id"], e) for r in R for e, s in r["scores"].items() if s is not None and s >= T]
    print(f"fires (score >= {T}) {len(fires)}: {dict(collections.Counter(e for _, e in fires))}")
    base = serves(R, {})
    base_ref = [x for x in base if x[1] == REF]

    # Describe today's Reflection serves.
    first = {}
    rep_msg = 0
    for mid, e in base_ref:
        r = by[mid]
        if key(r) in first:
            rep_msg += 1
        else:
            first[key(r)] = mid
    print(f"\ntoday's Reflection serves {len(base_ref)} (of {len([f for f in fires if f[1] == REF])} fires) in "
          f"{len({by[m]['session'] for m, _ in base_ref})} sessions; distinct user messages {len(first)}; "
          f"repeat serves on an already-served user message {rep_msg}; on notice turns "
          f"{sum(1 for m, _ in base_ref if by[m]['notices'])}; by kind "
          f"{dict(collections.Counter(by[m]['kind'] for m, _ in base_ref))}")
    if labels:
        lab = [labels.get(key(by[m])) for m, _ in base_ref]
        print(f"labelled: Reflection applies (yes) {lab.count('yes')}, does not (no) {lab.count('no')}, "
              f"unlabelled {lab.count(None)}")

    def classify(served):
        """Each Reflection serve: right (first serve on a message where it applies), repeat (that message was
        already served), wrong (it does not apply), unclear (a tool failure under a message where it does not
        apply: the failure itself may be the agent's mistake)."""
        c, seen = collections.Counter(), set()
        for m, e in sorted((x for x in served if x[1] == REF), key=lambda x: by[x[0]]["at"]):
            k = key(by[m])
            lab = labels.get(k)
            if k in seen:
                c["repeat"] += 1
            elif lab == "yes":
                c["right"] += 1
            elif by[m]["kind"] == "tool_failure":
                c["unclear"] += 1
            else:
                c["wrong" if lab == "no" else "unlabelled"] += 1
            seen.add(k)
        return c

    print("\n| Policy | All serves | Reflection serves | Right | Repeat | Wrong | Unclear (tool failure) | "
          "Right lost vs today | Other entries' serves removed |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    bset = set(base)
    c0 = classify(base)
    for name, p in POLICIES.items():
        s = serves(R, p, p.get("t_ref", T))
        ref = [x for x in s if x[1] == REF]
        other_removed = [x for x in bset - set(s) if x[1] != REF]
        c = classify(s)
        print(f"| {name} | {len(s)} | {len(ref)} | {c['right']} | {c['repeat']} | {c['wrong']} | {c['unclear']} | "
              f"{c0['right'] - c['right']} | {len(other_removed)} |")

    # Distinct user messages behind Reflection serves, for labelling.
    if "--list" in sys.argv:
        for k, mid in first.items():
            n = sum(1 for m, _ in base_ref if key(by[m]) == k)
            print(json.dumps({"key": k, "serves": n, "kind": by[mid]["kind"], "notices": by[mid]["notices"][:1]}))


main()
