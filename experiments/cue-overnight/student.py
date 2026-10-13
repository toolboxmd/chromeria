"""THROWAWAY (overnight 2026-10-13, contextmd#20 / #4 experiment 2): score dev and held-out val windows with one
Cue scorer, through the serving code (the deployed app's qwen.Scorer), for agreement with the 4B and latency.

Sets: dev = the 272 dev windows x the 9 detector entries; val = every teacher window x its held-out entries of the
validation part only. Held-out test entries are never scored (asserted).

    python -I student.py APP DATA.json OUT_DIR --model qwen3-0.6b|qwen3-4b [--adapter DIR] [--quant none|w8a8]
                         [--compile] [--sets dev,val]
"""

import argparse
import json
import os
import sys
import time

MODELS = {"qwen3-0.6b": ("Qwen/Qwen3-Reranker-0.6B", "e61197ed45024b0ed8a2d74b80b4d909f1255473"),
          "qwen3-4b": ("Qwen/Qwen3-Reranker-4B", "22e683669bc0f0bd69640a1354a6d0aebcfeede5")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("app")
    ap.add_argument("data")
    ap.add_argument("out")
    ap.add_argument("--model", required=True)
    ap.add_argument("--adapter")
    ap.add_argument("--quant", default="none")
    ap.add_argument("--compile", action="store_true")
    ap.add_argument("--sets", default="dev,val")
    a = ap.parse_args()
    import torch
    sys.path.insert(0, a.app)
    from contextmd.cue import qwen, service
    qwen.MODELS.update(MODELS)
    D = json.load(open(a.data))
    t0 = time.time()
    s = qwen.Scorer(a.model, D["instruction"], a.adapter, a.quant, a.compile)
    for text in service.warmup_windows():
        s.score(text, service.WARMUP_DOCS)
    for k in list(D["dev"]["ids"])[:20]:  # warm with real shapes too
        s.score(D["windows"][k], [D["entries"][e]["doc"] for e in D["dev"]["entries"]])
    load_s = time.time() - t0
    val = {e for e, x in D["entries"].items() if x["split"] == "heldout" and D["heldout_part"].get(e) == "val"}
    test = {e for e, x in D["entries"].items() if x["split"] == "heldout" and D["heldout_part"].get(e) == "test"}
    jobs = {}
    for name in a.sets.split(","):
        if name == "dev":
            jobs[name] = {w: list(D["dev"]["entries"]) for w in D["dev"]["ids"]}
        elif name == "val":
            jobs[name] = {w: sorted(e for e in lab if e in val) for w, lab in D["teacher"].items()}
            jobs[name] = {w: v for w, v in jobs[name].items() if v}
        for w, es in jobs[name].items():
            assert not set(es) & test, "a held-out test entry would be scored"
    os.makedirs(a.out, exist_ok=True)
    res, ms = {}, {}
    for name, J in jobs.items():
        res[name], ms[name] = {}, []
        for w, es in J.items():
            torch.cuda.synchronize()
            t = time.perf_counter()
            sc, info = s.score(D["windows"][w], [D["entries"][e]["doc"] for e in es])
            ms[name].append(round((time.perf_counter() - t) * 1000, 1))
            res[name][w] = dict(zip(es, sc))
    json.dump(res, open(f"{a.out}/scores.json", "w"))
    json.dump({"model": a.model, "adapter": a.adapter, "quant": a.quant, "compile": a.compile, "data": a.data,
               "load_warm_s": round(load_s, 1), "ms": ms,
               "peak_gib": round(torch.cuda.max_memory_allocated() / 2**30, 2)}, open(f"{a.out}/meta.json", "w"))
    v = sorted(ms.get("dev", [0]))
    print(json.dumps({"out": a.out, "dev_ms_p50": v[len(v) // 2], "dev_ms_p90": v[int(0.9 * len(v))],
                      "load_warm_s": round(load_s, 1)}))


main()
