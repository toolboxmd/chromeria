"""THROWAWAY (overnight 2026-10-13, experiment 2): agreement of each scorer with the deployed 4B, and latency.

    python3 -I analyze_student.py DATA.json REF_DIR NAME=DIR [NAME=DIR ...]

REF_DIR: the deployed 4B (W8A8 compiled) on raw10k; its fires at 0.5625 are the reference. For every other scorer:
  t_match  its threshold chosen on held-out val to agree best with the 4B's val fires (agreement below);
  t_own    its threshold chosen on held-out val against the teacher vote (max min(recall, precision)), as eval.py;
agreement = both fire / (4B fires + scorer-only fires), on dev (272 windows x 9 entries, out of sample for both
thresholds) with a 95% bootstrap interval over windows (2,000 resamples); AUC of the scorer's score against the
4B's fires; val objective = min(recall, precision) against the teacher vote at t_own; ms = dev window median, p90.
"""

import json
import random
import sys

T4B = 0.5625


def pairs(S, name):
    return {(w, e): s for w, d in S.get(name, {}).items() for e, s in d.items()}


def agree(a, b, keys):
    both = sum(1 for k in keys if a[k] and b[k])
    na = sum(1 for k in keys if a[k])
    nb = sum(1 for k in keys if b[k])
    den = na + nb - both
    return both / den if den else 1.0, both, na, nb


def auc(scores, labels):
    pos = [s for s, y in zip(scores, labels) if y]
    neg = [s for s, y in zip(scores, labels) if not y]
    if not pos or not neg:
        return float("nan")
    allv = sorted([(s, 1) for s in pos] + [(s, 0) for s in neg])
    rank, i, rsum = 0, 0, 0.0
    while i < len(allv):
        j = i
        while j < len(allv) and allv[j][0] == allv[i][0]:
            j += 1
        r = (i + j + 1) / 2
        rsum += r * sum(1 for k in range(i, j) if allv[k][1])
        i = j
    return (rsum - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))


def best_t(sc, target, keys):
    """Threshold maximizing agreement of (score >= t) with target over keys."""
    cands = sorted({round(sc[k], 4) for k in keys})
    best = (-1, None)
    for t in cands[::max(1, len(cands) // 400)] + [max(cands) + 1]:
        fires = {k: sc[k] >= t for k in keys}
        a = agree(fires, target, keys)[0]
        if a > best[0]:
            best = (a, t)
    return best[1]


def own_t(sc, vote, keys):
    cands = sorted({round(sc[k], 4) for k in keys})
    best = (-1, 0, None)
    P = sum(1 for k in keys if vote[k])
    for t in cands[::max(1, len(cands) // 400)]:
        tp = sum(1 for k in keys if sc[k] >= t and vote[k])
        fp = sum(1 for k in keys if sc[k] >= t and not vote[k])
        r, p = tp / P if P else 0, tp / (tp + fp) if tp + fp else 0
        if (min(r, p), r + p) > best[:2]:
            best = (min(r, p), r + p, t)
    return best[2], best[0]


def main():
    D = json.load(open(sys.argv[1]))
    ref = json.load(open(f"{sys.argv[2]}/scores.json"))
    refm = json.load(open(f"{sys.argv[2]}/meta.json"))
    vote = {(w, e): v >= 0.5 for w, d in D["teacher"].items() for e, v in d.items()}
    R = {s: {k: v >= T4B for k, v in pairs(ref, s).items()} for s in ("dev", "val")}
    devw = sorted({w for w, _ in R["dev"]})
    rng = random.Random(20)
    boots = [[rng.choice(devw) for _ in devw] for _ in range(2000)]
    by_w = {}
    for k in R["dev"]:
        by_w.setdefault(k[0], []).append(k)

    def row(name, d, meta):
        S = {s: pairs(d, s) for s in ("dev", "val")}
        vk = sorted(set(S["val"]) & set(R["val"]))
        dk = sorted(set(S["dev"]) & set(R["dev"]))
        tm = best_t(S["val"], R["val"], vk)
        to, vobj = own_t(S["val"], vote, vk)
        out = {"name": name}
        for lab, t in (("match", tm), ("own", to)):
            f = {k: S["dev"][k] >= t for k in dk}
            a, both, na, nb = agree(f, R["dev"], dk)
            bs = []
            for b in boots:
                keys = [k for w in b for k in by_w[w]]
                bb = sum(1 for k in keys if f[k] and R["dev"][k])
                den = sum(1 for k in keys if f[k]) + sum(1 for k in keys if R["dev"][k]) - bb
                bs.append(bb / den if den else 1)
            bs.sort()
            out[lab] = (t, a, bs[50], bs[1949], both, na, nb)
        out["auc_dev"] = auc([S["dev"][k] for k in dk], [R["dev"][k] for k in dk])
        out["auc_val"] = auc([S["val"][k] for k in vk], [R["val"][k] for k in vk])
        out["val_obj"] = vobj
        ms = sorted(meta["ms"]["dev"])
        out["ms"] = (ms[len(ms) // 2], ms[int(0.9 * len(ms))])
        return out

    _, ref_obj = own_t({k: v for k, v in pairs(ref, "val").items()}, vote, sorted(R["val"]))
    ms = sorted(refm["ms"]["dev"])
    print(f"4B reference: dev fires {sum(R['dev'].values())} of {len(R['dev'])} pairs, val fires "
          f"{sum(R['val'].values())} of {len(R['val'])}; its val objective at its own val-chosen threshold "
          f"{ref_obj:.3f}; dev ms {ms[len(ms) // 2]} / {ms[int(0.9 * len(ms))]}\n")
    print("| Scorer | Dev ms p50 / p90 | AUC vs 4B fires dev / val | Agreement with 4B on dev, t_match (95% CI) "
          "[both / scorer / 4B fires] | Agreement, t_own (95% CI) | Val objective vs teacher (t_own) |")
    print("| --- | --- | --- | --- | --- | --- |")
    for arg in sys.argv[3:]:
        name, d = arg.split("=", 1)
        r = row(name, json.load(open(f"{d}/scores.json")), json.load(open(f"{d}/meta.json")))
        m, o = r["match"], r["own"]
        print(f"| {name} | {r['ms'][0]:.0f} / {r['ms'][1]:.0f} | {r['auc_dev']:.3f} / {r['auc_val']:.3f} | "
              f"{m[1]:.1%} ({m[2]:.0%}-{m[3]:.0%}) [{m[4]} / {m[5]} / {m[6]}] | {o[1]:.1%} ({o[2]:.0%}-{o[3]:.0%}) | "
              f"{r['val_obj']:.3f} |")


main()
