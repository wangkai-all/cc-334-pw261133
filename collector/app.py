"""Browser download collector.

* Browser events land at /api/events (the notification channel) — never paired
  by filename, only recorded against download_id.
* File + manifest land at /api/envelopes (the file channel), spooled first and
  atomically renamed; either channel may arrive first.
* A durable lease queue feeds the ingestion worker; ACK/NACK are idempotent,
  lease expiry makes a lost receipt self-healing.
* A reconcile sweep recovers orphaned .part uploads and self-describing files
  left behind by a browser that exited before POSTing.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from common.py.httpkit import App, HTTPError, canonical_json, now_iso, sha256_file

DEFAULT_CONTROL = {
    "drop_ack": 0,          # swallow worker ACKs (success, no receipt)
    "slow_spool_ms": 0,     # artificial delay while writing spool
    "lease_seconds": 30,
}

LEASE_TIMEOUT_FALLBACK = 30
ORPHAN_PART_AFTER_S = 20
AWAIT_FILE_GRACE_S = 60


class CollectorDB:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.spool = os.path.join(data_dir, "spool")
        self.incoming = os.path.join(data_dir, "incoming")
        for d in (self.spool, self.incoming):
            os.makedirs(d, exist_ok=True)
        self._lock = threading.RLock()
        os.makedirs(data_dir, exist_ok=True)
        with self.conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    download_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    UNIQUE(download_id, kind, payload_json)
                );
                CREATE TABLE IF NOT EXISTS envelopes(
                    download_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    spool_path TEXT,
                    incoming_path TEXT,
                    manifest_json TEXT NOT NULL,
                    size_bytes INTEGER,
                    sha256 TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    leased_at REAL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_env_status ON envelopes(status);
                CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT NOT NULL);
                """
            )
        for k, v in DEFAULT_CONTROL.items():
            with self.conn() as c:
                c.execute("INSERT OR IGNORE INTO kv(k,v) VALUES(?,?)", (k, str(v)))

    @contextmanager
    def conn(self):
        with self._lock:
            c = sqlite3.connect(os.path.join(self.data_dir, "collector.db"), timeout=30)
            c.row_factory = sqlite3.Row
            try:
                c.execute("PRAGMA journal_mode=WAL")
                yield c
                c.commit()
            finally:
                c.close()

    def control(self) -> dict:
        with self.conn() as c:
            rows = c.execute("SELECT k,v FROM kv").fetchall()
        out = dict(DEFAULT_CONTROL)
        out.update({r["k"]: r["v"] for r in rows})
        return {k: int(v) for k, v in out.items()}

    def set_control(self, patch: dict) -> dict:
        cur = self.control()
        for k in DEFAULT_CONTROL:
            if k in patch:
                cur[k] = max(0, int(patch[k]))
        with self.conn() as c:
            c.executemany("UPDATE kv SET v=? WHERE k=?",
                          [(str(v), k) for k, v in cur.items()])
        return cur

    def record_event(self, download_id: str, kind: str, payload: dict) -> dict:
        with self.conn() as c:
            c.execute(
                "INSERT OR IGNORE INTO events(download_id,kind,payload_json,received_at) "
                "VALUES(?,?,?,?)",
                (download_id, kind, canonical_json(payload), now_iso()))
            if kind == "interrupted":
                c.execute(
                    "INSERT INTO envelopes(download_id,task_id,status,manifest_json,"
                    "created_at,updated_at,last_error) VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(download_id) DO UPDATE SET last_error=excluded.last_error,"
                    "updated_at=excluded.updated_at",
                    (download_id, payload.get("task_id", "unknown"), "orphan_partial",
                     canonical_json(payload), now_iso(), now_iso(),
                     f"browser interrupted: {payload.get('reason','?')}"))
        return self.events_of(download_id)

    def events_of(self, download_id: str) -> list[dict]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM events WHERE download_id=? ORDER BY id", (download_id,)).fetchall()
        return [{"kind": r["kind"], "payload": json.loads(r["payload_json"]),
                 "received_at": r["received_at"]} for r in rows]

    def get_envelope(self, download_id: str) -> dict | None:
        with self.conn() as c:
            row = c.execute("SELECT * FROM envelopes WHERE download_id=?",
                            (download_id,)).fetchone()
        return self._row(row) if row else None

    def list_envelopes(self, limit: int = 300) -> list[dict]:
        with self.conn() as c:
            rows = c.execute(
                "SELECT * FROM envelopes ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        return [self._row(r) for r in rows]

    def _row(self, r: sqlite3.Row) -> dict:
        d = dict(r)
        d["manifest"] = json.loads(d.pop("manifest_json"))
        return d

    def upsert_awaiting(self, manifest: dict) -> dict:
        did = manifest["download_id"]
        now = now_iso()
        with self.conn() as c:
            c.execute(
                "INSERT INTO envelopes(download_id,task_id,status,manifest_json,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(download_id) DO UPDATE SET manifest_json=excluded.manifest_json,"
                "updated_at=excluded.updated_at",
                (did, manifest["task_id"], "awaiting_file",
                 canonical_json(manifest), now, now))
        return self.get_envelope(did)

    def finalize_envelope(self, manifest: dict, incoming_path: str, size: int, digest: str) -> dict:
        did = manifest["download_id"]
        now = now_iso()
        with self.conn() as c:
            c.execute(
                "INSERT INTO envelopes(download_id,task_id,status,incoming_path,"
                "manifest_json,size_bytes,sha256,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(download_id) DO UPDATE SET status=excluded.status,"
                "incoming_path=excluded.incoming_path,manifest_json=excluded.manifest_json,"
                "size_bytes=excluded.size_bytes,sha256=excluded.sha256,"
                "updated_at=excluded.updated_at,last_error=NULL",
                (did, manifest["task_id"], "ready", incoming_path,
                 canonical_json(manifest), size, digest, now, now))
        return self.get_envelope(did)

    def mark_orphan_partial(self, download_id: str, task_id: str, spool_path: str, reason: str):
        now = now_iso()
        with self.conn() as c:
            c.execute(
                "INSERT INTO envelopes(download_id,task_id,status,spool_path,manifest_json,"
                "last_error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(download_id) DO UPDATE SET status=excluded.status,"
                "spool_path=excluded.spool_path,last_error=excluded.last_error,"
                "updated_at=excluded.updated_at",
                (download_id, task_id, "orphan_partial", spool_path,
                 "{}", reason, now, now))

    def lease(self) -> dict | None:
        lease_s = self.control().get("lease_seconds", LEASE_TIMEOUT_FALLBACK)
        cutoff = time.time() - lease_s
        with self.conn() as c:
            row = c.execute(
                "SELECT * FROM envelopes WHERE status='ready' OR "
                "(status='leased' AND leased_at < ?) ORDER BY created_at LIMIT 1",
                (cutoff,)).fetchone()
            if row is None:
                return None
            c.execute(
                "UPDATE envelopes SET status='leased', leased_at=?, attempts=attempts+1, "
                "updated_at=? WHERE download_id=?",
                (time.time(), now_iso(), row["download_id"]))
        return self.get_envelope(row["download_id"])

    def ack(self, download_id: str) -> bool:
        with self.conn() as c:
            cur = c.execute("SELECT status FROM envelopes WHERE download_id=?",
                            (download_id,)).fetchone()
            if cur is None:
                return False
            c.execute("UPDATE envelopes SET status='acked', leased_at=NULL, "
                      "updated_at=?, last_error=NULL WHERE download_id=?",
                      (now_iso(), download_id))
        return True

    def nack(self, download_id: str, error: str, dead: bool = False):
        with self.conn() as c:
            row = c.execute("SELECT attempts FROM envelopes WHERE download_id=?",
                            (download_id,)).fetchone()
            attempts = row["attempts"] if row else 0
            status = "dead_letter" if dead or attempts >= 5 else "ready"
            c.execute("UPDATE envelopes SET status=?, leased_at=NULL, last_error=?, "
                      "updated_at=? WHERE download_id=?",
                      (status, error, now_iso(), download_id))

    def reconcile(self) -> dict:
        """Sweep spool/incoming/DB for every crash/ordering pathology."""
        found = {"orphan_parts": [], "orphan_files_recovered": [],
                 "stuck_awaiting": [], "expired_leases": 0}
        now = now_iso()
        # 1) spool .part files whose POST never finished (browser exit / broken pipe)
        for name in os.listdir(self.spool):
            if not name.endswith(".part"):
                continue
            did = name[:-5]
            path = os.path.join(self.spool, name)
            age = time.time() - os.path.getmtime(path)
            if age < ORPHAN_PART_AFTER_S:
                continue
            env = self.get_envelope(did)
            if env and env["status"] in ("ready", "acked", "leased"):
                continue
            self.mark_orphan_partial(did, (env or {}).get("task_id", "unknown"),
                                     path, f"spool upload interrupted ({int(age)}s)")
            found["orphan_parts"].append(did)
        # 2) self-describing complete files in incoming with no DB row
        known = {e["download_id"] for e in self.list_envelopes(10000)}
        for name in os.listdir(self.incoming):
            did = name[:-4] if name.endswith(".bin") else name
            if did in known:
                continue
            path = os.path.join(self.incoming, name)
            manifest = _read_trailer_manifest(path)
            if manifest is None:
                continue
            digest = sha256_file(path)
            self.finalize_envelope(manifest, path, os.path.getsize(path), digest)
            found["orphan_files_recovered"].append(did)
        # 3) events that never got a file within the grace window
        with self.conn() as c:
            for r in c.execute(
                    "SELECT * FROM envelopes WHERE status='awaiting_file'").fetchall():
                age = time.time() - _iso_epoch(r["updated_at"])
                if age > AWAIT_FILE_GRACE_S:
                    c.execute("UPDATE envelopes SET status='dead_letter', last_error=?, "
                              "updated_at=? WHERE download_id=?",
                              (f"file never arrived after {int(age)}s", now, r["download_id"]))
                    found["stuck_awaiting"].append(r["download_id"])
            expired = c.execute(
                "SELECT COUNT(*) n FROM envelopes WHERE status='leased' AND leased_at < ?",
                (time.time() - self.control().get("lease_seconds", LEASE_TIMEOUT_FALLBACK),)
            ).fetchone()["n"]
            found["expired_leases"] = expired
        return found


def _iso_epoch(s: str) -> float:
    import datetime
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return time.time()


def _read_trailer_manifest(path: str) -> dict | None:
    """Recover manifest from the browser-appended self-describing trailer."""
    from portal.report import MANIFEST_TRAILER_PREFIX
    import base64
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 8192))
            tail = f.read()
        idx = tail.rfind(b"\n" + MANIFEST_TRAILER_PREFIX)
        if idx == -1:
            return None
        line = tail[idx + 1:].strip()
        raw = line[len(MANIFEST_TRAILER_PREFIX):]
        return json.loads(base64.urlsafe_b64decode(raw))
    except Exception:
        return None


def create_app(data_dir: str) -> App:
    db = CollectorDB(data_dir)
    app = App("collector")
    app.db = db  # type: ignore[attr-defined]

    @app.route("POST", "/api/events")
    def events(req):
        payload = req.json()
        did = payload.get("download_id")
        kind = payload.get("kind")
        if not did or kind not in ("created", "completed", "interrupted"):
            raise HTTPError(400, "need download_id and kind in created|completed|interrupted")
        if kind == "completed":
            db.upsert_awaiting(payload["manifest"])
        return {"download_id": did, "events": db.record_event(did, kind, payload)}

    @app.route("GET", "/api/events/{download_id}")
    def get_events(req, download_id):
        return {"download_id": download_id, "events": db.events_of(download_id)}

    @app.route("POST", "/api/envelopes")
    def envelopes(req):
        form = req.form()
        manifest_raw = form.field("manifest")
        try:
            manifest = json.loads(manifest_raw)
        except json.JSONDecodeError as exc:
            raise HTTPError(400, f"bad manifest: {exc}")
        for key in ("download_id", "task_id", "token", "filter", "sha256"):
            if key not in manifest:
                raise HTTPError(400, f"manifest missing {key}")
        filename, _ctype, content = form.file("file")
        did = manifest["download_id"]
        ctrl = db.control()
        spool_path = os.path.join(db.spool, f"{did}.part")
        final_path = os.path.join(db.incoming, f"{did}.bin")
        # 1) spool first, fsync, then atomic rename — notification-before-write safe.
        existing = db.get_envelope(did)
        if existing and existing["status"] == "acked":
            return {"status": "duplicate_ignored", "envelope": existing}
        with open(spool_path, "wb") as f:
            if ctrl["slow_spool_ms"]:
                step = max(1, len(content) // 4)
                for i in range(0, len(content), step):
                    f.write(content[i:i + step])
                    f.flush()
                    os.fsync(f.fileno())
                    time.sleep(ctrl["slow_spool_ms"] / 1000.0)
            else:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
        os.replace(spool_path, final_path)
        size = os.path.getsize(final_path)
        transport_digest = sha256_file(final_path)
        # Channel-integrity gate: bytes must arrive exactly as shipped. The
        # semantic content gate (manifest.sha256 vs portal) runs in the worker.
        declared_transport = manifest.get("transport_sha256")
        if declared_transport and declared_transport != transport_digest:
            db.mark_orphan_partial(
                did, manifest["task_id"], final_path,
                f"transport hash mismatch: observed={transport_digest[:12]} "
                f"declared={declared_transport[:12]}")
            raise HTTPError(422, "transport integrity check failed; envelope quarantined")
        env = db.finalize_envelope(manifest, final_path, size, transport_digest)
        return {"status": "accepted", "envelope": env,
                "observed_transport_sha256": transport_digest,
                "manifest_content_sha256": manifest["sha256"]}

    @app.route("GET", "/api/envelopes")
    def list_envelopes(req):
        return {"envelopes": db.list_envelopes(), "control": db.control()}

    @app.route("GET", "/api/envelopes/{download_id}")
    def get_envelope(req, download_id):
        env = db.get_envelope(download_id)
        if env is None:
            raise HTTPError(404, "unknown download_id")
        env["events"] = db.events_of(download_id)
        return env

    @app.route("POST", "/api/queue/lease")
    def lease(req):
        env = db.lease()
        if env is None:
            return {"envelope": None}
        return {"envelope": env}

    @app.route("POST", "/api/queue/ack/{download_id}")
    def ack(req, download_id):
        # Decide the injected failure BEFORE mutating state, otherwise the row
        # would be marked acked while the worker still sees a 503 — breaking
        # lease-based redelivery.
        if db.control()["drop_ack"]:
            raise HTTPError(503, "injected ack loss")
        ok = db.ack(download_id)
        if not ok:
            raise HTTPError(404, "unknown download_id")
        return {"status": "acked", "download_id": download_id}

    @app.route("POST", "/api/queue/nack/{download_id}")
    def nack(req, download_id):
        body = req.json() or {}
        db.nack(download_id, body.get("error", "unknown"), dead=bool(body.get("dead")))
        return {"status": "nacked", "download_id": download_id}

    @app.route("POST", "/api/reconcile")
    def reconcile(req):
        return db.reconcile()

    @app.route("GET", "/api/control")
    def get_control(req):
        return db.control()

    @app.route("PUT", "/api/control")
    def put_control(req):
        return db.set_control(req.json())

    @app.route("GET", "/ui")
    def ui(req):
        app.serve_static(req, "/ui", os.path.join(os.path.dirname(__file__), "static"))

    @app.route("GET", "/ui/{rest}")
    def ui_rest(req, rest):
        app.serve_static(req, "/ui", os.path.join(os.path.dirname(__file__), "static"))

    def reconcile_loop():
        while True:
            time.sleep(5)
            try:
                db.reconcile()
            except Exception as exc:  # noqa: BLE001
                app.error_log.append(f"reconcile: {exc!r}")

    threading.Thread(target=reconcile_loop, daemon=True, name="collector-reconcile").start()
    return app


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--data-dir", default="/workspace/runtime/collector")
    args = p.parse_args()
    app = create_app(args.data_dir)
    port = app.start(args.host, args.port)
    print(f"collector listening on http://{args.host}:{port}/ui")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
