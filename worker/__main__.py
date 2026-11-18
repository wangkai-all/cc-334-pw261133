"""Worker process entry point: ingestion loop + read-only ledger API (8091)."""
from __future__ import annotations

import argparse
import threading
import time

from worker.api import create_app


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--portal", default="http://127.0.0.1:8080")
    p.add_argument("--collector", default="http://127.0.0.1:8090")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8091)
    p.add_argument("--data-dir", default="/workspace/runtime/worker")
    args = p.parse_args()

    app, worker = create_app(args.data_dir, args.portal, args.collector)
    port = app.start(args.host, args.port)
    t = threading.Thread(target=worker.run_forever, daemon=True, name="ingest-loop")
    t.start()
    print(f"worker: ingest loop running; ledger API on http://{args.host}:{port}/ledger")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        worker.stop_flag.set()
        app.stop()


if __name__ == "__main__":
    main()
