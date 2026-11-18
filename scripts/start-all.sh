#!/usr/bin/env bash
# Start the full stack: portal (8080), collector (8090), worker+ledger (8091),
# and the headless virtual browser control API (8095).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p runtime/logs

start() {
  local name="$1"; shift
  if [ -f "runtime/logs/$name.pid" ] && kill -0 "$(cat "runtime/logs/$name.pid")" 2>/dev/null; then
    echo "$name already running (pid $(cat "runtime/logs/$name.pid"))"
    return
  fi
  "$@" >"runtime/logs/$name.log" 2>&1 &
  echo $! >"runtime/logs/$name.pid"
  echo "started $name pid $!"
}

AUTO_FLAG=""
if [ "${1:-}" = "--auto" ]; then AUTO_FLAG="--auto"; fi

start portal    python3 -I run_portal.py --data-dir runtime/portal
start collector python3 -I run_collector.py --data-dir runtime/collector
start worker    python3 -I run_worker.py --data-dir runtime/worker
sleep 1
start vbrowser  node vbrowser/index.js $AUTO_FLAG \
  --download-dir "$PWD/runtime/vbrowser-downloads"

echo
echo "portal UI:    http://127.0.0.1:8080/ui"
echo "collector UI: http://127.0.0.1:8090/ui"
echo "ledger API:   http://127.0.0.1:8091/ledger"
echo "vbrowser:     http://127.0.0.1:8095/status"
