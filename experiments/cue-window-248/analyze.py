"""THROWAWAY (chromeria#248): window variant table from replay results, against the full (today's) window.

    python3 -I analyze.py REQUESTS.jsonl RESULTS.jsonl [--threshold 0.5625]

Latency: (a) this copy's measured scoring ms, beside live Cue on a shared GPU, and (b) each window's token count
mapped through live Cue's own logged service time by token count (rocky's serving log, read-only).
Agreement: fires (score >= threshold) of the variant against the full window's fires on the same moments.
"""

import argparse
import bisect
import collections
import json
import os
import sqlite3

CM = f"file:{os.path.expanduser('~')}/.t3/userdata/contextmd.sqlite?mode=ro"
ORDER = ["full", "items2x25", "b6000", "b4000", "b3000", "b2000", "b1500", "b1000", "b500"]


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


RUN_START = 1791913020  # 2026-10-13T17:37Z: the replay shared the GPU after this, slowing live Cue


def live_curve():
    """Live service ms by window tokens, before the replay: a nearest-neighbour median over 50 calls."""
    rows = sorted((j["window_tokens"], j["ms"]) for j in (json.loads(c) for (c,) in sqlite3.connect(CM, uri=True)
                  .execute("SELECT cue_json FROM moments WHERE cue_status = 'ok' AND cue_json IS NOT NULL AND received_at < ?",
                  (RUN_START,))))
    toks = [t for t, _ in rows]

    def at(t):
        i = bisect.bisect_left(toks, t)
        near = rows[max(0, i - 25): i + 25]
        return pct([m for _, m in near], 0.5)
    return at, len(rows), toks[0], toks[-1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("requests")
    ap.add_argument("results")
    ap.add_argument("--threshold", type=float, default=0.5625)
    a = ap.parse_args()
    th = a.threshold
    req = {r["id"]: r for r in map(json.loads, open(a.requests))}
    res = collections.defaultdict(dict)
    for o in map(json.loads, open(a.results)):
        res[o["id"]][o["variant"]] = o
    ids = [i for i, v in res.items() if all(k in v for k in ORDER)]
    at, n_live, lo, hi = live_curve()
    fires = lambda o: {e for e, s in o["scores"].items() if s >= th}
    print(f"moments with every variant: {len(ids)} "
          f"({collections.Counter(req[i]['kind'] for i in ids)}); live curve from {n_live} calls, {lo}-{hi} tokens\n")

    # Validation: the full replay against what live Cue logged for the same moment.
    val = [i for i in ids if req[i]["live_status"] == "ok" and req[i]["live_scores"]]
    if val:
        tok_eq = sum(abs(res[i]["full"]["window_tokens"] - req[i]["live"]["window_tokens"]) <= 5 for i in val)
        lf = sum(len({e for e, s in req[i]["live_scores"].items() if s >= th}) for i in val)
        same = sum({e for e, s in req[i]["live_scores"].items() if s >= th} == fires(res[i]["full"]) for i in val)
        diffs = [abs(res[i]["full"]["scores"][e] - s) for i in val for e, s in req[i]["live_scores"].items()
                 if e in res[i]["full"]["scores"]]
        print(f"validation vs live log: {len(val)} moments, window tokens within 5: {tok_eq}, same fire set: {same}, "
              f"live fires {lf}, score |diff| median {pct(diffs, .5):.3f} p90 {pct(diffs, .9):.3f}\n")

    ref = {i: fires(res[i]["full"]) for i in ids}
    nref = sum(len(f) for f in ref.values())
    print("| Window | Tokens p50 / p90 | Copy ms p50 / p90 | Live-curve ms p50 / p90 | Full-window fires kept | "
          "Extra fires | Fire agreement | Moments with same fires |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    per_entry = {}
    for v in ORDER:
        toks = [res[i][v]["window_tokens"] for i in ids]
        ms = [res[i][v]["ms"] for i in ids]
        pred = [at(t) for t in toks]
        kept = sum(len(ref[i] & fires(res[i][v])) for i in ids)
        extra = sum(len(fires(res[i][v]) - ref[i]) for i in ids)
        same = sum(ref[i] == fires(res[i][v]) for i in ids)
        agree = kept / (nref + extra) if nref + extra else float("nan")
        print(f"| {v} | {pct(toks, .5)} / {pct(toks, .9)} | {pct(ms, .5):.0f} / {pct(ms, .9):.0f} | "
              f"{pct(pred, .5):.0f} / {pct(pred, .9):.0f} | {kept}/{nref} ({kept / nref:.1%}) | {extra} | "
              f"{agree:.1%} | {same}/{len(ids)} ({same / len(ids):.1%}) |")
        pe = collections.Counter()
        for i in ids:
            f = fires(res[i][v])
            for e in ref[i] - f:
                pe[(e, "lost")] += 1
            for e in f - ref[i]:
                pe[(e, "extra")] += 1
        per_entry[v] = pe

    print("\nPer entry (full-window fires; per variant lost / extra):\n")
    ents = sorted({e for i in ids for e in res[i]["full"]["scores"]})
    print("| Entry | Full fires | " + " | ".join(ORDER[1:]) + " |")
    print("| --- | --- | " + " | ".join("---" for _ in ORDER[1:]) + " |")
    for e in ents:
        nf = sum(e in ref[i] for i in ids)
        cells = [f"-{per_entry[v][(e, 'lost')]} / +{per_entry[v][(e, 'extra')]}" for v in ORDER[1:]]
        print(f"| {e.removeprefix('agentsmd-')} | {nf} | " + " | ".join(cells) + " |")

    print("\nBy moment kind (fire agreement / live-curve p90 ms):\n")
    print("| Kind | n | full fires | " + " | ".join(ORDER[1:]) + " |")
    print("| --- | --- | --- | " + " | ".join("---" for _ in ORDER[1:]) + " |")
    for kind in ("user_message", "pre_tool", "tool_failure"):
        ks = [i for i in ids if req[i]["kind"] == kind]
        if not ks:
            continue
        nr = sum(len(ref[i]) for i in ks)
        cells = []
        for v in ORDER[1:]:
            kept = sum(len(ref[i] & fires(res[i][v])) for i in ks)
            extra = sum(len(fires(res[i][v]) - ref[i]) for i in ks)
            p90 = pct([at(res[i][v]["window_tokens"]) for i in ks], .9)
            cells.append(f"{kept / (nr + extra):.0%} / {p90:.0f}" if nr + extra else f"n/a / {p90:.0f}")
        print(f"| {kind} | {len(ks)} | {nr} | " + " | ".join(cells) + " |")


main()
