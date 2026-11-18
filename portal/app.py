"""Fictional report portal.

Responsibilities:
  * create/cancel/supersede report generation tasks (persisted SQLite);
  * serve real file downloads bound to task id + HMAC token (never filename);
  * push generation notifications over SSE with controllable delay/dup/drop;
  * answer authoritative task/filter verify calls from the ingestion worker;
  * expose fault-injection switches from the UI.
"""
from __future__ import annotations

import hmac
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

from common.py.httpkit import (
    App, HTTPError, canonical_json, new_id, now_iso, sha256_bytes, serve_file_download,
)
from portal import report

DEFAULT_CONTROL = {
    "sse_delay_ms": 0,          # delay every ready event by N ms
    "sse_duplicate": 0,         # send each ready event N extra times
    "sse_drop_ready": 0,        # 1 = never emit task.ready (file still downloadable)
    "download_chunk_delay_ms": 0,  # slow attachment streaming
    "tamper_download": 0,       # 1 = swap bytes without updating recorded hash
    "download_hold": 0,         # 1 = block each download after headers until released
}


class PortalDB:
    def __init__(self, path: str, secret: bytes):
        self.path = path
        self.secret = secret
        self._lock = threading.RLock()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with self.conn() as c:
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks(
                    task_id TEXT PRIMARY KEY,
                    token TEXT NOT NULL,
                    filter_json TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    body BLOB NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    status TEXT NOT NULL,
                    superseded_by TEXT,
                    cancel_reason TEXT,
                    created_at TEXT NOT NULL,
                    ready_at TEXT,
                    cancelled_at TEXT
                );
                CREATE TABLE IF NOT EXISTS outbox(
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    due_at REAL NOT NULL,
                    delivered INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT NOT NULL);
                """
            )
        for k, v in DEFAULT_CONTROL.items():
            self.kv_setdefault(k, str(v))

    @contextmanager
    def conn(self):
        with self._lock:
            c = sqlite3.connect(self.path, timeout=30)
            c.row_factory = sqlite3.Row
            try:
                c.execute("PRAGMA journal_mode=WAL")
                c.execute("PRAGMA foreign_keys=ON")
                yield c
                c.commit()
            finally:
                c.close()

    def kv_setdefault(self, k: str, v: str) -> None:
        with self.conn() as c:
            c.execute("INSERT OR IGNORE INTO kv(k,v) VALUES(?,?)", (k, v))

    def get_control(self) -> dict:
        with self.conn() as c:
            rows = c.execute("SELECT k,v FROM kv").fetchall()
        out = dict(DEFAULT_CONTROL)
        out.update({r["k"]: r["v"] for r in rows})
        return {k: int(v) for k, v in out.items()}

    def set_control(self, patch: dict) -> dict:
        cur = self.get_control()
        for k in DEFAULT_CONTROL:
            if k in patch:
                cur[k] = max(0, int(patch[k]))
        with self.conn() as c:
            c.executemany("UPDATE kv SET v=? WHERE k=?",
                          [(str(v), k) for k, v in cur.items()])
        return cur

    def sign(self, task_id: str, filt: dict, issued_at: str) -> str:
        msg = f"{task_id}|{canonical_json(filt)}|{issued_at}".encode()
        import hashlib
        return hmac.new(self.secret, msg, hashlib.sha256).hexdigest()

    def create_task(self, filt: dict) -> dict:
        task_id = new_id()
        issued = now_iso()
        token = self.sign(task_id, filt, issued)
        body = report.render_body(task_id, filt)
        digest = sha256_bytes(body)
        fname = report.declared_filename(filt)
        with self.conn() as c:
            c.execute(
                """INSERT INTO tasks(task_id,token,filter_json,filename,body,
                   content_sha256,status,created_at)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (task_id, token, canonical_json(filt), fname, body, digest,
                 "created", issued))
            self._enqueue(c, "task.created", {"task_id": task_id, "filter": filt,
                                              "filename": fname, "at": issued})
            self._enqueue(c, "task.ready", {"task_id": task_id, "filter": filt,
                                            "filename": fname, "at": now_iso()})
            c.execute("UPDATE tasks SET status='ready', ready_at=? WHERE task_id=?",
                      (now_iso(), task_id))
        return self.get_task(task_id)

    def _enqueue(self, c, event: str, payload: dict, due_at: float | None = None):
        c.execute("INSERT INTO outbox(event,payload_json,due_at) VALUES(?,?,?)",
                  (event, json.dumps(payload, ensure_ascii=False),
                   due_at if due_at is not None else time.time()))

    def cancel_task(self, task_id: str, reason: str, supersede: str | None = None) -> dict:
        with self.conn() as c:
            row = c.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise HTTPError(404, "unknown task")
            if row["status"] in ("cancelled", "superseded"):
                return dict(row)
            new_status = "superseded" if supersede else "cancelled"
            c.execute(
                "UPDATE tasks SET status=?, superseded_by=?, cancel_reason=?, cancelled_at=? WHERE task_id=?",
                (new_status, supersede, reason, now_iso(), task_id))
            self._enqueue(c, "task.cancelled",
                          {"task_id": task_id, "reason": reason,
                           "superseded_by": supersede, "at": now_iso()})
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict | None:
        with self.conn() as c:
            row = c.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def list_tasks(self, limit: int = 200) -> list[dict]:
        with self.conn() as c:
            rows = c.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?",
                             (limit,)).fetchall()
        return [self._row_to_task(r) for r in rows]

    def _row_to_task(self, r: sqlite3.Row) -> dict:
        return {
            "task_id": r["task_id"],
            "token": r["token"],
            "filter": json.loads(r["filter_json"]),
            "filename": r["filename"],
            "content_sha256": r["content_sha256"],
            "status": r["status"],
            "superseded_by": r["superseded_by"],
            "cancel_reason": r["cancel_reason"],
            "created_at": r["created_at"],
            "ready_at": r["ready_at"],
            "cancelled_at": r["cancelled_at"],
            "size_bytes": len(r["body"]),
        }

    def body_for_download(self, task_id: str) -> tuple[bytes, dict]:
        ctrl = self.get_control()
        with self.conn() as c:
            row = c.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise HTTPError(404, "unknown task")
        body = bytes(row["body"])
        if ctrl["tamper_download"]:
            body = report.tamper(body)  # recorded hash stays stale -> content mismatch
        return body, self._row_to_task(row)

    def verify(self, task_id: str, token: str, claimed_filter: dict | None) -> dict:
        task = self.get_task(task_id)
        if task is None:
            return {"ok": False, "reason": "task_unknown", "task_id": task_id}
        if not hmac.compare_digest(token, task["token"]):
            return {"ok": False, "reason": "token_invalid", "task_id": task_id}
        if task["status"] != "ready":
            return {"ok": False, "reason": f"task_{task['status']}",
                    "task_id": task_id, "status": task["status"],
                    "superseded_by": task["superseded_by"]}
        if claimed_filter is not None and \
                canonical_json(claimed_filter) != canonical_json(task["filter"]):
            return {"ok": False, "reason": "filter_mismatch", "task_id": task_id,
                    "authoritative_filter": task["filter"]}
        return {"ok": True, "task_id": task_id, "status": "ready",
                "filter": task["filter"], "content_sha256": task["content_sha256"],
                "filename": task["filename"]}


def create_app(data_dir: str) -> App:
    os.makedirs(data_dir, exist_ok=True)
    secret_path = os.path.join(data_dir, "hmac.key")
    if os.path.exists(secret_path):
        with open(secret_path, "rb") as f:
            secret = f.read()
    else:
        secret = os.urandom(32)
        with open(secret_path, "wb") as f:
            f.write(secret)
    db = PortalDB(os.path.join(data_dir, "portal.db"), secret)
    app = App("portal")
    app.db = db  # type: ignore[attr-defined]
    # Gate for the download_hold fault: a download blocks on this event after
    # headers are sent, letting tests cancel a task while bytes are in flight.
    download_gate = threading.Event()
    download_gate.set()
    app.download_gate = download_gate  # type: ignore[attr-defined]
    held_counter = {"waiting": 0}
    app.held_counter = held_counter  # type: ignore[attr-defined]

    @app.route("POST", "/api/tasks")
    def create_task(req):
        payload = req.json()
        try:
            filt = report.validate_filter(payload.get("filter", {}))
        except ValueError as exc:
            raise HTTPError(400, str(exc))
        task = db.create_task(filt)
        app.hub.broadcast("task.created", task)
        return req.handler._send_json(201, task)

    @app.route("POST", "/api/tasks/{task_id}/cancel")
    def cancel_task(req, task_id):
        reason = (req.json() or {}).get("reason", "user_cancelled")
        task = db.cancel_task(task_id, reason)
        app.hub.broadcast("task.cancelled", task)
        return task

    @app.route("POST", "/api/tasks/{task_id}/regenerate")
    def regenerate(req, task_id):
        """Cancel the old task (mark superseded) and immediately create a new one."""
        old = db.get_task(task_id)
        if old is None:
            raise HTTPError(404, "unknown task")
        new = db.create_task(old["filter"])
        db.cancel_task(task_id, "superseded_by_new_task", supersede=new["task_id"])
        app.hub.broadcast("task.cancelled", db.get_task(task_id))
        app.hub.broadcast("task.created", new)
        return {"cancelled": db.get_task(task_id), "new_task": new}

    @app.route("GET", "/api/tasks")
    def list_tasks(req):
        return {"tasks": db.list_tasks(), "control": db.get_control()}

    @app.route("GET", "/api/tasks/{task_id}")
    def get_task(req, task_id):
        task = db.get_task(task_id)
        if task is None:
            raise HTTPError(404, "unknown task")
        return task

    @app.route("GET", "/api/tasks/{task_id}/verify")
    def verify_task(req, task_id):
        token = req.query.get("token", [""])[0]
        claimed = None
        if "filter_json" in req.query:
            try:
                claimed = json.loads(req.query["filter_json"][0])
            except json.JSONDecodeError:
                raise HTTPError(400, "bad filter_json")
        return db.verify(task_id, token, claimed)

    @app.route("GET", "/api/downloads/{task_id}")
    def download(req, task_id):
        token = req.query.get("token", [""])[0]
        task = db.get_task(task_id)
        if task is None:
            raise HTTPError(404, "unknown task")
        if not hmac.compare_digest(token, task["token"]):
            raise HTTPError(403, "invalid token")
        # Fault gate: when armed, an established download blocks here while the
        # test cancels/supersedes the task, producing a genuine late in-flight
        # file. Such a download was authorized before cancellation, so once
        # released it still streams its bytes; the worker's verify gate rejects
        # it downstream instead of the portal cutting the transfer.
        was_held = False
        if db.get_control()["download_hold"]:
            was_held = True
            app.held_counter["waiting"] += 1
            app.download_gate.wait(timeout=30)
            app.held_counter["waiting"] = max(0, app.held_counter["waiting"] - 1)
        if not was_held and task["status"] not in ("ready",):
            # Cancelled/superseded tasks refuse fresh downloads; late files from
            # already-started downloads are handled downstream (verify rejects).
            raise HTTPError(409, f"task not downloadable: {task['status']}")
        # Re-read after the gate: the task may have been cancelled while held.
        body, full = db.body_for_download(task_id)
        ctrl = db.get_control()
        headers = {
            "X-Task-Id": task_id,
            "X-Task-Token": token,
            "X-Content-Sha256": full["content_sha256"],
            "X-Task-Status": full["status"],
        }
        serve_file_download(req.handler, body, full["filename"], headers,
                            chunk_delay=ctrl["download_chunk_delay_ms"] / 1000.0)

    @app.route("POST", "/api/hold/arm")
    def arm_hold(req):
        app.download_gate.clear()
        app.held_counter["waiting"] = 0
        return {"armed": True}

    @app.route("GET", "/api/hold/status")
    def hold_status(req):
        return {"armed": not app.download_gate.is_set(),
                "waiting": app.held_counter["waiting"]}

    @app.route("POST", "/api/hold/release")
    def release_hold(req):
        app.download_gate.set()
        return {"released": True}

    @app.route("GET", "/api/control")
    def get_control(req):
        return db.get_control()

    @app.route("PUT", "/api/control")
    def put_control(req):
        return db.set_control(req.json())

    @app.route("GET", "/api/events/stream")
    def sse(req):
        app.sse_response(req)

    @app.route("GET", "/ui")
    def ui(req):
        app.serve_static(req, "/ui", os.path.join(os.path.dirname(__file__), "static"))

    @app.route("GET", "/ui/{rest}")
    def ui_rest(req, rest):
        app.serve_static(req, "/ui", os.path.join(os.path.dirname(__file__), "static"))

    def pump_outbox():
        """Move due outbox rows to SSE, honoring delay/duplicate/drop controls.

        Never sleeps while holding the SQLite lock: request threads need the
        same (process-wide) lock and would otherwise time out into empty replies.
        """
        while True:
            try:
                ctrl = db.get_control()
                # Phase 1: claim a due row under the lock, then release it.
                with db.conn() as c:
                    row = c.execute(
                        "SELECT * FROM outbox WHERE delivered=0 AND due_at<=? "
                        "ORDER BY seq LIMIT 1", (time.time(),)).fetchone()
                    if row is None:
                        due = c.execute(
                            "SELECT MIN(due_at) d FROM outbox WHERE delivered=0").fetchone()
                        wait = max(0.05, (due["d"] - time.time())) if due and due["d"] else 0.05
                        drop_ready = False
                        delay = 0.0
                    else:
                        drop_ready = row["event"] == "task.ready" and ctrl["sse_drop_ready"]
                        if drop_ready:
                            c.execute("UPDATE outbox SET delivered=1 WHERE seq=?", (row["seq"],))
                        delay = ctrl["sse_delay_ms"] / 1000.0 if row["event"] == "task.ready" else 0
                # Phase 2: all slow work happens outside the DB lock.
                if row is None:
                    time.sleep(min(wait, 1.0))
                    continue
                if drop_ready:
                    continue
                payload = json.loads(row["payload_json"])
                if delay:
                    time.sleep(delay)
                app.hub.broadcast(row["event"], payload)
                if row["event"] == "task.ready":
                    for _ in range(ctrl["sse_duplicate"]):
                        app.hub.broadcast("task.ready", payload)
                with db.conn() as c:
                    c.execute("UPDATE outbox SET delivered=1 WHERE seq=?", (row["seq"],))
            except Exception as exc:  # noqa: BLE001
                app.error_log.append(f"outbox pump: {exc!r}")
                time.sleep(0.2)

    t = threading.Thread(target=pump_outbox, daemon=True, name="portal-outbox")
    t.start()
    return app


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--data-dir", default="/workspace/runtime/portal")
    args = p.parse_args()
    app = create_app(args.data_dir)
    port = app.start(args.host, args.port)
    print(f"portal listening on http://{args.host}:{port}/ui")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
