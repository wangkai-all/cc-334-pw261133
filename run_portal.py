#!/usr/bin/env python3
"""Repo-root launcher so the app works under `python3 -I` (isolated mode).

Under -I neither PYTHONPATH nor cwd is importable; the script's own directory
still is, so we insert the repo root explicitly before importing the package.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from portal.app import main  # noqa: E402

if __name__ == "__main__":
    main()
