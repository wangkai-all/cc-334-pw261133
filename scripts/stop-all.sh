#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for name in portal collector worker vbrowser; do
  pf="runtime/logs/$name.pid"
  if [ -f "$pf" ]; then
    pid="$(cat "$pf")"
    kill "$pid" 2>/dev/null && echo "stopped $name ($pid)" || echo "$name not running"
    rm -f "$pf"
  fi
done
