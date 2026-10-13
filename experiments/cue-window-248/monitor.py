"""THROWAWAY (chromeria#248): live Cue latency from rocky's serving log (read-only) and /health, per bucket.

    python3 -I monitor.py [--minutes 120] [--bucket 10]

One line per bucket: ok calls, live service ms p50/p90 (cue_json.ms), round trip p90, timeouts, and a /health time.
"""

import argparse
import json
import os
import sqlite3
import time
import urllib.request

CM = f"file:{os.path.expanduser('~')}/.t3/userdata/contextmd.sqlite?mode=ro"


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1) if xs else None


ap = argparse.ArgumentParser()
ap.add_argument("--minutes", type=float, default=120)
ap.add_argument("--bucket", type=float, default=10)
a = ap.parse_args()
t = time.monotonic()
try:
    urllib.request.urlopen("http://100.116.0.2:8710/health", timeout=2).read()
    health = round((time.monotonic() - t) * 1000, 1)
except OSError as e:
    health = f"error {e}"
db = sqlite3.connect(CM, uri=True)
now = time.time()
rows = db.execute("SELECT received_at, cue_status, cue_ms, cue_json FROM moments WHERE received_at > ? AND "
                  "cue_status IN ('ok', 'timeout')", (now - a.minutes * 60,)).fetchall()
buckets = {}
for at, status, ms, cj in rows:
    b = buckets.setdefault(int((now - at) // (a.bucket * 60)), {"svc": [], "rt": [], "timeout": 0})
    if status == "timeout":
        b["timeout"] += 1
    else:
        b["svc"].append(json.loads(cj)["ms"])
        b["rt"].append(ms)
for k in sorted(buckets, reverse=True):
    b = buckets[k]
    print(json.dumps({"ago_min": f"{k * a.bucket:g}-{(k + 1) * a.bucket:g}", "ok": len(b["svc"]),
                      "svc_p50": pct(b["svc"], .5), "svc_p90": pct(b["svc"], .9), "rt_p90": pct(b["rt"], .9),
                      "timeout": b["timeout"]}))
allsvc = [json.loads(cj)["ms"] for _, s, _, cj in rows if s == "ok"]
print(json.dumps({"window_min": a.minutes, "ok": len(allsvc), "svc_p50": pct(allsvc, .5), "svc_p90": pct(allsvc, .9),
                  "timeout": sum(1 for r in rows if r[1] == "timeout"), "health_ms": health}))
