"""Read-only HTTP view over the worker ledger (ingestion status & destinations)."""
from __future__ import annotations

import json
import os

from common.py.httpkit import App
from worker.ingest import WorkerLedger, IngestWorker, HTTPClient


def create_app(data_dir: str, portal_url: str, collector_url: str) -> tuple[App, IngestWorker]:
    ledger = WorkerLedger(data_dir)
    worker = IngestWorker(HTTPClient(portal_url), HTTPClient(collector_url), data_dir)
    app = App("worker-api")

    @app.route("GET", "/ledger")
    def get_ledger(req):
        return {"ingests": ledger.list_all(), "worker_log": worker.log[-100:]}

    @app.route("GET", "/ledger/{download_id}")
    def get_one(req, download_id):
        row = ledger.get(download_id)
        if row is None:
            from common.py.httpkit import HTTPError
            raise HTTPError(404, "unknown download_id")
        return row

    @app.route("GET", "/health")
    def health(req):
        counts: dict[str, int] = {}
        for r in ledger.list_all():
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        return {"ok": True, "counts": counts}

    return app, worker
