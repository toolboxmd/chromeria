"""THROWAWAY (overnight 2026-10-13, chromeria#248 experiment 1): can Cue reuse the shared part of a request?

Three scorers over the same rebuilt requests, in time order, with a private copy of the live variant
(q4b-cloud-step64-w8a8c: same base, adapter, W8A8, torch.compile, threshold):

  base    today's qwen.Scorer.score, unchanged (imported from the deployed app copy)
  static  the system prompt and instruction (identical in every request) are run once; their KV cache is reused
  thread  as static, plus the longest token prefix shared with an earlier request (same thread, same turn: the
          trained window part is identical for every tool moment of a turn) is reused from an LRU of KV caches

Entries come after the window and attend to it, so they cannot be reused across windows; they are recomputed in
every scorer. One output line per (moment, scorer): ms, token counts, reused tokens and scores. No window text.

    python -I prefix.py APP_COPY ENTRIES_DIR REQUESTS.jsonl OUT.jsonl [--lru 16] [--limit N]
    python -I prefix.py APP_COPY ENTRIES_DIR --bench OUT.jsonl      (forward time by token count)
"""

import argparse
import json
import random
import sys
import time
from collections import OrderedDict
from pathlib import Path


def entries(d):
    out = []
    for p in sorted(Path(d).glob("*.md")):
        if p.name == "README.md":
            continue
        front = p.read_text().split("---")[1]
        f = dict(line.split(": ", 1) for line in front.strip().splitlines() if ": " in line)
        out.append((f["id"], f"{f['name']}: {f['applies']}"))
    return out


def load(app, spec):
    sys.path.insert(0, str(app))
    from contextmd.cue import qwen
    return qwen.Scorer(spec["model"], spec["instruction"], spec.get("adapter"), spec.get("quant", "none"),
                       bool(spec.get("compile")))


class Cached:
    """Scores like qwen.Scorer.score, reusing KV caches of token prefixes it has already run."""

    def __init__(self, s, lru, reuse_windows):
        import torch
        from transformers import DynamicCache
        self.s, self.torch, self.DC = s, torch, DynamicCache
        self.lru, self.reuse_windows = lru, reuse_windows
        self.base = s.pre + s.head
        self.static = self._run(self.base, None, 0)  # KV of the shared head, never mutated
        self.caches = OrderedDict()  # key -> (tokens, cache)

    def _fwd(self, ids, pos, mask, cache):
        return self.s.causal.model(input_ids=ids, position_ids=pos, attention_mask=mask, past_key_values=cache,
                                   use_cache=True).last_hidden_state

    def _run(self, toks, cache, L):
        """Extend ``cache`` (holding L tokens) with plain causal ``toks``; returns the cache."""
        torch = self.torch
        cache = cache if cache is not None else self.DC(config=self.s.causal.config)
        T = len(toks)
        ids = torch.tensor(toks, device="cuda").unsqueeze(0)
        pos = torch.arange(L, L + T, device="cuda").unsqueeze(0)
        q = torch.arange(T, device="cuda")
        allow = torch.cat([torch.ones(T, L, dtype=torch.bool, device="cuda"), q.unsqueeze(0) <= q.unsqueeze(1)], 1)
        dt = torch.bfloat16
        mask = torch.zeros(1, 1, T, L + T, dtype=dt, device="cuda").masked_fill_(~allow, torch.finfo(dt).min)
        self._fwd(ids, pos, mask, cache)
        return cache

    def _clone(self, cache, n):
        c = self.DC(config=self.s.causal.config)
        for li, layer in enumerate(cache.layers):
            c.update(layer.keys[:, :, :n].clone(), layer.values[:, :, :n].clone(), li)
        return c

    def score(self, window, docs, key=None):
        torch, s = self.torch, self.s
        pre, cut = s.prefix(window)
        best, bL = None, len(self.base)
        if self.reuse_windows:
            for k, (toks, c) in self.caches.items():
                n = 0
                m = min(len(toks), len(pre))
                while n < m and toks[n] == pre[n]:
                    n += 1
                if n > bL:
                    best, bL = k, n
        L = bL
        cache = self._clone(self.caches[best][1] if best is not None else self.static, L)
        rest = pre[L:]
        if self.reuse_windows and rest:  # extend to the full window prefix, then keep a copy for later moments
            cache = self._run(rest, cache, L)
            L, rest = len(pre), []
            k = key or object()
            self.caches[k] = (pre, self._clone(cache, L))
            self.caches.move_to_end(k)
            while len(self.caches) > self.lru:
                self.caches.popitem(last=False)
        sufs = [s.doc(d) for d in docs]
        new = rest + [t for x in sufs for t in x]
        T = len(new)
        P = len(pre)
        ids = torch.tensor(new, device="cuda").unsqueeze(0)
        pos = torch.cat([torch.arange(L, P)] + [torch.arange(P, P + len(x)) for x in sufs]).to("cuda").unsqueeze(0)
        blk = torch.cat([torch.full((P - L,), -1)] + [torch.full((len(x),), k) for k, x in enumerate(sufs)]).to("cuda")
        q = torch.arange(T, device="cuda")
        inner = (q.unsqueeze(0) <= q.unsqueeze(1)) & ((blk.unsqueeze(0) == -1) | (blk.unsqueeze(0) == blk.unsqueeze(1)))
        allow = torch.cat([torch.ones(T, L, dtype=torch.bool, device="cuda"), inner], 1)
        dt = torch.bfloat16
        mask = torch.zeros(1, 1, T, L + T, dtype=dt, device="cuda").masked_fill_(~allow, torch.finfo(dt).min)
        last = torch.tensor([(P - L) + sum(len(x) for x in sufs[:k + 1]) - 1 for k in range(len(sufs))], device="cuda")
        h = self._fwd(ids, pos, mask, cache)
        w = s.causal.lm_head.weight[[s.yes, s.no]]
        lg = torch.nn.functional.linear(h[0, last], w).float()
        out = (lg[:, 0] - lg[:, 1]).tolist()
        torch.cuda.synchronize()
        return out, {"window_tokens": P - len(self.base), "window_cut": cut, "reused": bL, "computed": T,
                     "total": P + sum(len(x) for x in sufs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("app", type=Path)
    ap.add_argument("entries")
    ap.add_argument("requests", nargs="?")
    ap.add_argument("out")
    ap.add_argument("--config", default=str(Path.home() / ".config/contextmd-cue/service.json"))
    ap.add_argument("--variant", default="q4b-cloud-step64-w8a8c")
    ap.add_argument("--lru", type=int, default=16)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--bench", action="store_true")
    ap.add_argument("--scorers", default="base,static,thread")
    a = ap.parse_args()
    import torch
    spec = json.loads(Path(a.config).read_text())["variants"][a.variant]
    sys.path.insert(0, str(a.app))
    from contextmd.cue import service
    ents = entries(a.entries)
    docs = [d for _, d in ents]
    t0 = time.time()
    s = load(a.app, spec)
    for text in service.warmup_windows():
        s.score(text, service.WARMUP_DOCS)
    scorers = {"base": lambda w, d, k: s.score(w, d)}
    if "static" in a.scorers:
        st = Cached(s, 0, False)
        scorers["static"] = lambda w, d, k: st.score(w, d)
    if "thread" in a.scorers:
        th = Cached(s, a.lru, True)
        scorers["thread"] = th.score
    scorers = {k: v for k, v in scorers.items() if k in a.scorers.split(",")}
    for name, f in scorers.items():  # warm the cached paths' shapes too
        for text in service.warmup_windows():
            f(text, service.WARMUP_DOCS, None)
    if "thread" in scorers:
        th.caches.clear()
    print(json.dumps({"loaded_warm_s": round(time.time() - t0, 1), "entries": [e for e, _ in ents],
                      "doc_tokens": [len(s.doc(d)) for d in docs], "head_tokens": len(s.pre + s.head)}), flush=True)

    if a.bench:  # forward time of the packed request by window length, today's scorer only
        word = "The agent reads the request and plans the next step. "
        with open(a.out, "w") as fh:
            for rep in range(5):
                for n in (0, 1, 4, 16, 32, 64, 128, 200):
                    w = (word * n).strip() or "ok"
                    for name, f in scorers.items():
                        t = time.monotonic()
                        _, info = f(w, docs, None)
                        ms = (time.monotonic() - t) * 1000
                        fh.write(json.dumps({"rep": rep, "n": n, "scorer": name, "ms": round(ms, 1), **info}) + "\n")
                    if "thread" in scorers:
                        th.caches.clear()
        return

    reqs = [json.loads(line) for line in open(a.requests)][: a.limit]
    reqs.sort(key=lambda r: r["at"])
    from contextmd.cue import window as W
    rng = random.Random(1)
    with open(a.out, "w") as fh:
        for n, r in enumerate(reqs):
            try:
                text = W.window(r["items"], r["kind"], r["call"], r["output"])
            except ValueError:
                continue
            order = list(scorers)
            rng.shuffle(order)
            for name in order:
                t = time.monotonic()
                scores, info = scorers[name](text, docs, (r["thread"],))
                ms = (time.monotonic() - t) * 1000
                fh.write(json.dumps({"id": r["id"], "kind": r["kind"], "scorer": name, "ms": round(ms, 1), **info,
                                     "scores": {e: round(x, 4) for (e, _), x in zip(ents, scores)}}) + "\n")
            fh.flush()
            if n % 100 == 0:
                print(json.dumps({"done": n, "of": len(reqs), "t": round(time.time() - t0),
                                  "mem_gb": round(torch.cuda.max_memory_allocated() / 1e9, 1)}), flush=True)


main()
