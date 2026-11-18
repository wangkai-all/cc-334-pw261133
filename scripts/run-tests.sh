#!/usr/bin/env bash
# Unit tests (no running stack needed) + cross-language trailer round-trip.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -I tests/test_trailer.py
python3 -I - <<'PY'
import subprocess, base64, json, sys
sys.path.insert(0, '.')
from portal.report import strip_trailer
fixtures = json.loads(subprocess.check_output(['node', 'tests/js_trailer_fixture.mjs']))
bodies = [b'task_id,report\nabc,sales\n# detail,x\n', b'a,b\n1,2\n']
for b64, body in zip(fixtures[:2], bodies):
    assert strip_trailer(base64.b64decode(b64)) == body
print('cross-language trailer round-trip OK')
PY
echo
echo "E2E scenarios require a running stack (scripts/start-all.sh):"
echo "  python3 -I tests/e2e_scenarios.py"
