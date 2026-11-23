#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "Usage: $0 [--remote <ssh-host>:<checkout>] <server-port> <base-dir> <mobile-origin> <agent-device-command> <target-args...>" >&2
  exit 2
}

remote_host=""
remote_checkout=""
if [[ "${1:-}" == "--remote" ]]; then
  [[ $# -ge 2 && "$2" == ?*:?* ]] || usage
  remote_host="${2%%:*}"
  remote_checkout="${2#*:}"
  shift 2
fi

[[ $# -ge 5 ]] || usage

server_port="$1"
base_dir="$2"
mobile_origin="$3"
agent_device_command="$4"
shift 4

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

mint_command=(
  node apps/server/src/bin.ts auth pairing create
  --base-dir "$base_dir"
  --base-url "$mobile_origin"
  --ttl 15m
  --label "agent-mobile"
)

mint() {
  if [[ -n "$remote_host" ]]; then
    ssh -- "$remote_host" \
      "cd $(printf '%q' "$remote_checkout") && T3CODE_PORT=$(printf '%q' "$server_port") $(printf '%q ' "${mint_command[@]}")"
  else
    T3CODE_PORT="$server_port" "${mint_command[@]}"
  fi
}

if ! pairing_output="$(mint 2>&1)"; then
  echo "Could not mint a mobile pairing credential." >&2
  exit 1
fi

pairing_url="$(printf '%s\n' "$pairing_output" | sed -n 's/^Pair URL: //p' | tail -n 1)"
if [[ -z "$pairing_url" ]]; then
  echo "Could not parse the mobile pairing URL." >&2
  exit 1
fi

deep_link="$(PAIRING_URL="$pairing_url" node - <<'NODE'
const query = new URLSearchParams({
  pairingUrl: process.env.PAIRING_URL,
  autoConnect: "1",
});
process.stdout.write(`t3code-dev://connections/new?${query}`);
NODE
)"

if ! "$agent_device_command" open com.t3tools.t3code.dev "$deep_link" "$@" \
  >/dev/null 2>&1; then
  echo "AgentDevice could not open the pairing route. Check the Device panel and retry with a fresh credential." >&2
  exit 1
fi

echo "Opened the existing Add Environment route with a fresh pairing credential."
