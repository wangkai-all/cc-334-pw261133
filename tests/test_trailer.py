"""Trailer strip contract (Python side).

The authoritative round-trip is cross-language: tests/js_trailer_fixture.mjs
builds files exactly as the browser does, and e2e_scenarios / the check below
verify portal.report.strip_trailer reproduces the canonical body. This file
covers the Python-only invariants.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from portal.report import strip_trailer, MANIFEST_TRAILER_PREFIX


def main():
    marker = MANIFEST_TRAILER_PREFIX
    # Normal report bodies end in '\n'; trailer starts a new line, no extra newline.
    body = b"task_id,report\nabc,sales\n"
    on_disk = body + marker + b"eyJ4IjoxfQ"
    assert strip_trailer(on_disk) == body

    # A body without a trailer passes through untouched.
    assert strip_trailer(body) == body

    print("trailer strip contract OK")


if __name__ == "__main__":
    main()
