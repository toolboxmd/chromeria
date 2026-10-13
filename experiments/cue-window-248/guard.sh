#!/bin/bash
# THROWAWAY (chromeria#248): pause the bestia replay when live Cue degrades past its pre-run range.
M=~/dev/toolboxmd/chromeria-worktrees/cue-window-248/experiments/cue-window-248/monitor.py
while ssh -o ConnectTimeout=10 bestia 'pgrep -f "replay.py app entries" >/dev/null'; do
  L=$(python3 -I $M --minutes 10 --bucket 10 | tail -1)
  P90=$(echo "$L" | python3 -c 'import sys,json; print(json.load(sys.stdin)["svc_p90"] or 0)')
  TO=$(echo "$L" | python3 -c 'import sys,json; print(json.load(sys.stdin)["timeout"])')
  if python3 -c "import sys; sys.exit(0 if $P90 > 1000 or $TO > 35 else 1)"; then
    ssh bestia 'touch ~/tmp/cue-window-248/PAUSE'; A=pause
  else
    ssh bestia 'rm -f ~/tmp/cue-window-248/PAUSE'; A=run
  fi
  echo "$(date -u +%H:%MZ) $A $L $(ssh bestia 'wc -l < ~/tmp/cue-window-248/results.jsonl')" >> ~/tmp/cue-window-248/guard.log
  sleep 600
done
echo "$(date -u +%H:%MZ) replay ended" >> ~/tmp/cue-window-248/guard.log
