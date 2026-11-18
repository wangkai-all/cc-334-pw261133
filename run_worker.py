#!/usr/bin/env python3
"""Repo-root launcher for the worker (see run_portal.py)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from worker.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
