"""Deterministic fictional report generation.

The canonical report body is the exact bytes whose SHA-256 is stored on the task.
The browser appends a manifest trailer after download; the worker strips the
trailer before hashing, so the portal never sees the trailer.
"""
from __future__ import annotations

import csv
import io

from common.py.httpkit import canonical_json

REPORT_KINDS = ("sales", "inventory", "traffic")
REGIONS = ("north", "south", "east", "west", "all")

MANIFEST_TRAILER_PREFIX = b"# __MANIFEST__="


def validate_filter(f: dict) -> dict:
    if not isinstance(f, dict):
        raise ValueError("filter must be an object")
    kind = str(f.get("report", "sales"))
    region = str(f.get("region", "all"))
    period = str(f.get("period", "2026-11"))
    if kind not in REPORT_KINDS:
        raise ValueError(f"unknown report kind: {kind}")
    if region not in REGIONS:
        raise ValueError(f"unknown region: {region}")
    return {"report": kind, "region": region, "period": period,
            "detail": str(f.get("detail", ""))[:80]}


def declared_filename(f: dict) -> str:
    # Deliberately non-unique: every task of the same kind shares the name.
    return f"{f['report']}-report.csv"


def render_body(task_id: str, f: dict, row_count: int = 20) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["task_id", "report", "region", "period", "seq", "item", "amount", "qty"])
    for i in range(1, row_count + 1):
        # Pseudo-deterministic numbers derived from filter + index.
        seed = abs(hash((canonical_json(f), i))) % 10000
        w.writerow([
            task_id, f["report"], f["region"], f["period"], i,
            f"ITEM-{seed % 97:03d}", (seed * 13) % 9000 + 100, (seed * 7) % 90 + 1,
        ])
    if f.get("detail"):
        w.writerow(["# detail", f["detail"]])
    return buf.getvalue().encode("utf-8")


def tamper(body: bytes) -> bytes:
    """Fault injection: alter content without updating the recorded hash."""
    lines = body.split(b"\n")
    lines.insert(1, b'tampered,injected,row,after,header,EVIL,9999,99')
    return b"\n".join(lines)


def strip_trailer(data: bytes) -> bytes:
    """Remove the browser-appended manifest trailer, reproducing canonical body.

    Layout is ``<body>\\n# __MANIFEST__=<b64>`` where ``<body>`` itself ends in
    ``\\n`` and the trailer line adds no extra newline before it. We cut at the
    start of the marker line, keeping the body's own terminating newline.
    """
    marker = b"\n" + MANIFEST_TRAILER_PREFIX
    idx = data.rfind(marker)
    if idx == -1:
        return data
    return data[:idx + 1]
