"""THROWAWAY (chromeria#248): score rebuilt Cue requests at shorter windows with a private copy of the live variant.

Runs on bestia beside the live Cue service, never touching it: own process, own inductor cache copy, VRAM capped,
model loaded on the CPU and quantized one layer at a time on the GPU (no full bf16 copy on the GPU). The window
code is the deployed app's ``window.py`` (imported from a copy), with its budgets changed per variant. The current
message keeps today's treatment in every variant (cut only past 2,500 characters when over budget), and a tool
moment's call and failure output keep theirs.

    python -I replay.py APP_COPY ENTRIES_DIR REQUESTS.jsonl OUT.jsonl [--duty 0.5] [--limit N]

OUT has one line per (moment, variant): ms, window_chars, window_tokens and scores. No window text is written.
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path

# variant -> (BUDGET, WORK_CHARS, items limit or None); "full" is today's window.
VARIANTS = {
    "full": (10000, 2500, None),
    "b6000": (6000, 1500, None),
    "b4000": (4000, 1000, None),
    "b3000": (3000, 750, None),
    "b2000": (2000, 500, None),
    "b1500": (1500, 400, None),
    "b1000": (1000, 300, None),
    "b500": (500, 200, None),
    "items2x25": (10000, 2500, (2, 25)),  # the Issue's example: 2 user messages, 25 items, today's budget
}


def limit_items(items, users, n):
    """Chromeria-side cut: the last `users` user items, at most n replies between them and n items since."""
    us = [i for i, (k, _) in enumerate(items) if k == "user"][-users:]
    first, cur = us[0], us[-1]
    between = [i for i in range(first + 1, cur) if items[i][0] == "asst"][-n:]
    since = list(range(cur + 1, len(items)))[-n:]
    return [items[i] for i in sorted(us + between + since)]


def entries(d):
    out = []
    for p in sorted(Path(d).glob("*.md")):
        if p.name == "README.md":
            continue
        front = p.read_text().split("---")[1]
        f = dict(line.split(": ", 1) for line in front.strip().splitlines() if ": " in line)
        out.append((f["id"], f"{f['name']}: {f['applies']}"))
    return out


def scorer(app, spec):
    """qwen.Scorer of the live variant, built without a full bf16 model on the GPU."""
    import torch
    sys.path.insert(0, str(app))
    from contextmd.cue import qwen
    from peft import PeftModel
    from torchao.quantization import Int8DynamicActivationInt8WeightConfig, quantize_
    from transformers import AutoModelForCausalLM, AutoTokenizer

    torch.cuda.set_per_process_memory_fraction(0.25)  # about 6 GB of 24
    mid, rev = qwen.MODELS[spec["model"]]
    s = qwen.Scorer.__new__(qwen.Scorer)
    s.tok = AutoTokenizer.from_pretrained(mid, revision=rev)
    m = AutoModelForCausalLM.from_pretrained(mid, revision=rev, dtype=torch.bfloat16, attn_implementation="sdpa")
    m.config.use_cache = False
    m = PeftModel.from_pretrained(m, spec["adapter"]).merge_and_unload()
    for layer in m.model.layers:
        layer.to("cuda")
        quantize_(layer, Int8DynamicActivationInt8WeightConfig(), filter_fn=lambda mod, fqn: isinstance(mod, torch.nn.Linear))
    m.to("cuda")
    m.eval()
    m.model = torch.compile(m.model, dynamic=True)
    s.causal = m
    s.enc = lambda t: s.tok.encode(t, add_special_tokens=False)
    # The prompt strings are qwen.Scorer.__init__'s; the validation against logged live scores checks them.
    s.pre = s.enc("<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and "
                  "the Instruct provided. Note that the answer can only be \"yes\" or \"no\".<|im_end|>\n"
                  "<|im_start|>user\n")
    s.head = s.enc(f"<Instruct>: {spec['instruction']}\n<Query>: ")
    s.suf = s.enc("<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n")
    s.yes, s.no = s.tok.convert_tokens_to_ids("yes"), s.tok.convert_tokens_to_ids("no")
    s._docs = {}
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("app", type=Path)
    ap.add_argument("entries")
    ap.add_argument("requests")
    ap.add_argument("out")
    ap.add_argument("--config", default=str(Path.home() / ".config/contextmd-cue/service.json"))
    ap.add_argument("--duty", type=float, default=0.5, help="GPU share: sleep (1/duty - 1) x each scoring")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--pause-file", default=str(Path.home() / "tmp/cue-window-248/PAUSE"))
    a = ap.parse_args()
    cfg = json.loads(Path(a.config).read_text())
    spec = cfg["variants"][cfg["active"]]
    sys.path.insert(0, str(a.app))
    from contextmd.cue import service, window as W

    ents = entries(a.entries)
    docs = [d for _, d in ents]
    t0 = time.time()
    s = scorer(a.app, spec)
    for text in service.warmup_windows():
        s.score(text, service.WARMUP_DOCS)
    print(json.dumps({"loaded_warm_s": round(time.time() - t0, 1), "entries": [e for e, _ in ents]}), flush=True)

    reqs = [json.loads(line) for line in open(a.requests)][: a.limit]
    done = set()
    if Path(a.out).exists():
        done = {(r["id"], r["variant"]) for r in map(json.loads, open(a.out))}
    rng = random.Random(248)
    with open(a.out, "a") as fh:
        for n, r in enumerate(reqs):
            order = list(VARIANTS)
            rng.shuffle(order)
            for v in order:
                if (r["id"], v) in done:
                    continue
                while Path(a.pause_file).exists():
                    time.sleep(5)
                budget, work, lim = VARIANTS[v]
                W.BUDGET, W.WORK_CHARS = budget, work
                items = limit_items(r["items"], *lim) if lim else r["items"]
                text = W.window(items, r["kind"], r["call"], r["output"])
                t = time.monotonic()
                scores, info = s.score(text, docs)
                ms = (time.monotonic() - t) * 1000
                fh.write(json.dumps({"id": r["id"], "variant": v, "ms": round(ms, 1), "wall": round(time.time(), 2),
                                     "window_chars": len(text), **info,
                                     "scores": {e: round(x, 4) for (e, _), x in zip(ents, scores)}}) + "\n")
                fh.flush()
                time.sleep(ms / 1000 * (1 / a.duty - 1))
            if n % 50 == 0:
                print(json.dumps({"done": n, "of": len(reqs), "t": round(time.time() - t0)}), flush=True)


main()
