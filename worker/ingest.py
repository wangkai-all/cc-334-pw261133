"""Ingestion worker.

Pulls leased envelopes from the collector, runs the three mandatory checks
(task / filter / content — never filename or recency), then commits the result
transactionally to its own ledger and ACKs. A lost ACK is harmless: lease
expiry redelivers and the ledger unique constraint makes reprocessing a no-op.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager

from common.py.httpkit import canonical_json, now_iso
from portal.report import strip_trailer

MAX_ATTEMPTS = 5
VERIFY_RETRYABLE = {"portal_unreachable"}


class HTTPClient:
    def __init__(self, base: str, timeout: float = 10.0):
        self.base = base.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, body=None, headers=None):
        url = self.base + path
        data = None
        hdrs = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode()
            hdrs["Content-Type"] = "application/json"
        hdrs.update(headers or {})
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                return exc.code, json.loads(raw)
            except json.JSONDecodeError:
                return exc.code, {"error": raw.decode("utf-8", "replace")}


class WorkerLedger:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.library = os.path.join(data_dir, "library")
        self.quarantine = os.path.join(data_dir, "quarantine")
        os.makedirs(self.library, exist_ok=True)
        os.makedirs(self.quarantine, exist_ok=True)
        self._lock = threading.RLock()
        with self.conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingests(
                    download_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    filter_json TEXT,
                    manifest_sha256 TEXT,
                    observed_sha256 TEXT,
                    portal_sha256 TEXT,
                    library_path TEXT,
                    quarantine_path TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ingest_task ON ingests(task_id);
                """
            )

    @contextmanager
    def conn(self):
        with self._lock:
            c = sqlite3.connect(os.path.join(self.data_dir, "worker.db"), timeout=30)
            c.row_factory = sqlite3.Row
            try:
                c.execute("PRAGMA journal_mode=WAL")
                yield c
                c.commit()
            finally:
                c.close()

    def get(self, download_id: str) -> dict | None:
        with self.conn() as c:
            r = c.execute("SELECT * FROM ingests WHERE download_id=?",
                          (download_id,)).fetchone()
        return dict(r) if r else None

    def list_all(self) -> list[dict]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM ingests ORDER BY updated_at DESC").fetchall()
        return [dict(r) for r in rows]

    def record_attempt(self, download_id: str, task_id: str, manifest: dict):
        now = now_iso()
        with self.conn() as c:
            c.execute(
                "INSERT INTO ingests(download_id,task_id,status,filter_json,"
                "manifest_sha256,attempts,created_at,updated_at) "
                "VALUES(?,?,?,?,?,1,?,?) ON CONFLICT(download_id) DO UPDATE SET "
                "attempts=attempts+1,updated_at=excluded.updated_at",
                (download_id, task_id, "processing", canonical_json(manifest.get("filter")),
                 manifest.get("sha256"), now, now))

    def commit(self, download_id: str, status: str, **fields):
        now = now_iso()
        cols = ["status", "updated_at"] + list(fields)
        vals = [status, now] + list(fields.values())
        sets = ", ".join(f"{c}=?" for c in cols)
        with self.conn() as c:
            c.execute(f"UPDATE ingests SET {sets} WHERE download_id=?", vals + [download_id])


class IngestWorker:
    def __init__(self, portal: HTTPClient, collector: HTTPClient, data_dir: str):
        self.portal = portal
        self.collector = collector
        self.ledger = WorkerLedger(data_dir)
        self.stop_flag = threading.Event()
        self.log: list[str] = []

    def note(self, msg: str):
        line = f"{now_iso()} {msg}"
        self.log.append(line)
        del self.log[:-300]

    def run_forever(self):
        self.note("worker started")
        while not self.stop_flag.is_set():
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                self.note(f"tick error: {exc!r}")
                time.sleep(1)

    def tick(self):
        status, resp = self.collector.request("POST", "/api/queue/lease", {})
        env = (resp or {}).get("envelope")
        if not env:
            time.sleep(0.5)
            return
        self.process(env)

    def process(self, env: dict) -> dict:
        did = env["download_id"]
        existing = self.ledger.get(did)
        if existing and existing["status"] == "ingested":
            # Redelivery after a lost ACK: content-addressing makes this idempotent.
            self.note(f"{did}: redelivery of ingested envelope -> idempotent ACK")
            self._ack(did)
            return existing
        if existing and existing["status"] == "quarantined":
            self.note(f"{did}: redelivery of quarantined envelope -> ACK to stop retries")
            self._ack(did)
            return existing

        manifest = env["manifest"]
        self.ledger.record_attempt(did, env["task_id"], manifest)
        result = self._check_and_store(env, manifest)
        self._ack(did, result)
        return result

    def _check_and_store(self, env: dict, manifest: dict) -> dict:
        did = env["download_id"]
        src = env.get("incoming_path")
        if not src or not os.path.isfile(src):
            return self._fail(env, "file_missing", "incoming file absent at processing time",
                              dead=True)

        raw = open(src, "rb").read()
        # Gate 0 (channel integrity): the bytes received must equal the bytes
        # shipped, before any semantic interpretation.
        transport_hash = _sha_bytes(raw)
        if manifest.get("transport_sha256") and \
                manifest["transport_sha256"] != transport_hash:
            return self._fail(
                env, "transport_hash_mismatch",
                f"observed={transport_hash[:12]} "
                f"declared={manifest['transport_sha256'][:12]}", dead=True)

        body = strip_trailer(raw)  # trailer is browser metadata, not report content
        observed = _sha_bytes(body)
        # ---- Check 1+2: task existence/state and authoritative filter ------------
        qs = urllib.parse.urlencode({
            "token": manifest["token"],
            "filter_json": canonical_json(manifest["filter"]),
        })
        try:
            code, verdict = self.portal.request(
                "GET", f"/api/tasks/{manifest['task_id']}/verify?{qs}")
        except Exception as exc:  # network-level failure -> retryable NACK
            return self._fail(env, "portal_unreachable", repr(exc), dead=False)
        if code != 200 or not verdict.get("ok"):
            reason = verdict.get("reason", f"verify_http_{code}")
            # cancelled/superseded/unknown/filter mismatch are permanent decisions
            permanent = reason not in VERIFY_RETRYABLE
            return self._fail(env, reason, json.dumps(verdict, ensure_ascii=False),
                              dead=permanent)

        # ---- Check 3: content hash three-way agreement ---------------------------
        portal_hash = verdict["content_sha256"]
        manifest_hash = manifest["sha256"]
        if not (observed == manifest_hash == portal_hash):
            return self._fail(
                env, "content_hash_mismatch",
                f"observed={observed[:12]} manifest={manifest_hash[:12]} "
                f"portal={portal_hash[:12]}", dead=True)

        # ---- Commit: content-addressed library path, never overwrites ------------
        safe_name = os.path.basename(manifest.get("declared_filename", "report.csv"))
        lib_dir = os.path.join(self.ledger.library, manifest["task_id"])
        os.makedirs(lib_dir, exist_ok=True)
        lib_path = os.path.join(lib_dir, f"{observed[:16]}-{safe_name}")
        if not os.path.exists(lib_path):
            with open(lib_path, "wb") as f:
                f.write(body)
                f.flush()
                os.fsync(f.fileno())
        self.ledger.commit(
            did, "ingested", reason=None,
            task_id=manifest["task_id"],
            filter_json=canonical_json(manifest["filter"]),
            manifest_sha256=manifest_hash, observed_sha256=observed,
            portal_sha256=portal_hash, library_path=lib_path, quarantine_path=None)
        self.note(f"{did}: INGESTED task={manifest['task_id']} -> {lib_path}")
        return self.ledger.get(did)

    def _fail(self, env: dict, reason: str, detail: str, dead: bool) -> dict:
        did = env["download_id"]
        qpath = None
        if dead:
            # Permanent rejection: preserve evidence in quarantine.
            qdir = os.path.join(self.ledger.quarantine, reason)
            os.makedirs(qdir, exist_ok=True)
            src = env.get("incoming_path")
            if src and os.path.isfile(src):
                qpath = os.path.join(qdir, f"{did}.bin")
                shutil.copy2(src, qpath)
        self.ledger.commit(
            did, "quarantined" if dead else "retry_pending",
            reason=f"{reason}: {detail}", quarantine_path=qpath)
        self.note(f"{did}: {'QUARANTINE' if dead else 'RETRY'} {reason} ({detail})")
        code, resp = self.collector.request(
            "POST", f"/api/queue/nack/{did}",
            {"error": f"{reason}: {detail}", "dead": dead})
        return self.ledger.get(did)

    def _ack(self, did: str, result: dict | None = None):
        code, resp = self.collector.request("POST", f"/api/queue/ack/{did}", {})
        if code == 200:
            self.note(f"{did}: acked")
        elif code == 503 and "ack loss" in json.dumps(resp, ensure_ascii=False):
            # Injected failure: our ledger commit stands; lease expiry redelivers
            # and process() takes the idempotent path above.
            self.note(f"{did}: ACK LOST (injected); ledger already committed, will redeliver")
        else:
            self.note(f"{did}: ack failed http={code} {resp}")


def _sha_bytes(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--portal", default="http://127.0.0.1:8080")
    p.add_argument("--collector", default="http://127.0.0.1:8090")
    p.add_argument("--data-dir", default="/workspace/runtime/worker")
    args = p.parse_args()
    w = IngestWorker(HTTPClient(args.portal), HTTPClient(args.collector), args.data_dir)
    w.run_forever()


if __name__ == "__main__":
    main()
