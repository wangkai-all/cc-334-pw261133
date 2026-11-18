"""Stdlib-only HTTP toolkit: routing, JSON, SSE, multipart parsing, hashing."""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


def now_iso() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_id() -> str:
    return str(uuid.uuid4())


def canonical_json(obj) -> str:
    """Stable JSON string used for filter comparison and signing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str, bufsize: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


class SSEHub:
    """Process-local fan-out for Server-Sent Events clients."""

    def __init__(self):
        self._cond = threading.Condition()
        self._clients: list[queue.Queue | None] = []
        self._seq = 0

    def connect(self):
        q: "queue.Queue" = queue.Queue(maxsize=1000)
        with self._cond:
            self._clients.append(q)
        return q

    def disconnect(self, q) -> None:
        with self._cond:
            if q in self._clients:
                self._clients.remove(q)

    def broadcast(self, event: str, data: dict) -> None:
        payload = (event, data)
        with self._cond:
            self._seq += 1
            dead = []
            for q in self._clients:
                try:
                    q.put_nowait(payload)
                except Exception:
                    dead.append(q)
            for q in dead:
                self._clients.remove(q)


import queue  # noqa: E402  (used by SSEHub annotations)


class HTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Request:
    def __init__(self, handler: "Handler", method: str, path: str, query: dict, headers):
        self.handler = handler
        self.method = method
        self.path = path
        self.query = query
        self.headers = headers
        self._body: bytes | None = None

    def body(self) -> bytes:
        if self._body is None:
            length = int(self.headers.get("Content-Length", "0") or "0")
            self._body = self.handler.rfile.read(length) if length else b""
        return self._body

    def json(self):
        raw = self.body()
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise HTTPError(400, f"invalid json: {exc}")

    def form(self) -> "MultipartForm":
        ctype = self.headers.get("Content-Type", "")
        if not ctype.startswith("multipart/form-data"):
            raise HTTPError(400, "expected multipart/form-data")
        marker = "boundary="
        if marker not in ctype:
            raise HTTPError(400, "missing boundary")
        boundary = ctype.split(marker, 1)[1].strip().strip('"')
        return parse_multipart(self.body(), boundary)


class MultipartForm:
    def __init__(self, fields: dict, files: dict):
        self.fields = fields  # name -> str
        self.files = files    # name -> (filename, content_type, bytes)

    def field(self, name: str, required: bool = True) -> str:
        if name not in self.fields:
            if required:
                raise HTTPError(400, f"missing form field: {name}")
            return ""
        return self.fields[name]

    def file(self, name: str):
        if name not in self.files:
            raise HTTPError(400, f"missing form file: {name}")
        return self.files[name]


def parse_multipart(body: bytes, boundary: str) -> MultipartForm:
    """Binary-safe multipart/form-data parser (RFC 7578 subset)."""
    delim = b"--" + boundary.encode("ascii")
    fields: dict[str, str] = {}
    files: dict[str, tuple] = {}
    # Split on boundary; first segment is preamble, last is closing "--"
    parts = body.split(delim)
    for raw in parts[1:]:
        if raw[:2] == b"--":
            break  # closing delimiter
        if raw[:2] == b"\r\n":
            raw = raw[2:]
        if raw.endswith(b"\r\n"):
            raw = raw[:-2]
        header_blob, _, content = raw.partition(b"\r\n\r\n")
        if not _:
            continue
        headers = {}
        for line in header_blob.split(b"\r\n"):
            if b":" in line:
                k, v = line.split(b":", 1)
                headers[k.decode("latin1").strip().lower()] = v.decode("latin1").strip()
        disp = headers.get("content-disposition", "")
        name = None
        filename = None
        for piece in disp.split(";"):
            piece = piece.strip()
            if piece.startswith("name="):
                name = piece[5:].strip('"')
            elif piece.startswith("filename="):
                filename = piece[9:].strip('"')
        if name is None:
            continue
        if filename is not None:
            files[name] = (filename, headers.get("content-type", "application/octet-stream"), content)
        else:
            fields[name] = content.decode("utf-8")
    return MultipartForm(fields, files)


class Handler(BaseHTTPRequestHandler):
    # Bound per-server via App.start(); ThreadingHTTPServer instantiates this
    # class per connection, so state must live on the class, not the instance.
    app: "App" = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):  # capture instead of printing to stderr
        line = f"{self.command} {self.path} -> " + (fmt % args)
        app = getattr(self, "app", None)
        if app is not None:
            app.access_log.append(line)
            if len(app.access_log) > 500:
                del app.access_log[:200]

    def _dispatch(self):
        parsed = urlparse(self.path)
        req = Request(self, self.command, parsed.path, parse_qs(parsed.query), self.headers)
        try:
            handler, kwargs = self.app.match(self.command, parsed.path)
            if handler is None:
                self._send_json(404, {"error": "not found", "path": parsed.path})
                return
            result = handler(req, **kwargs)
            # Handlers either write the response themselves (SSE/static/file
            # downloads, returning None) or return a JSON-serializable object.
            if result is not None:
                self._send_json(200, result)
        except HTTPError as exc:
            self._send_json(exc.status, {"error": exc.message})
        except Exception as exc:  # noqa: BLE001
            import traceback
            tb = traceback.format_exc()
            if self.app is not None:
                self.app.error_log.append(f"{parsed.path}: {exc!r}\n{tb}")
            try:
                self._send_json(500, {"error": "internal error", "detail": repr(exc)})
            except Exception:
                pass  # connection may already be gone; nothing more to do

    def do_GET(self):
        self._dispatch()

    def do_POST(self):
        self._dispatch()

    def do_PUT(self):
        self._dispatch()

    def do_DELETE(self):
        self._dispatch()

    def _send_json(self, status: int, obj) -> None:
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()  # small replies otherwise sit in the socket buffer

    def send_bytes(self, status: int, data: bytes, content_type: str, extra_headers: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()


class App:
    def __init__(self, name: str):
        self.name = name
        self.routes: list[tuple[str, str, object]] = []  # method, pattern, fn
        self.hub = SSEHub()
        self.access_log: list[str] = []
        self.error_log: list[str] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def route(self, method: str, pattern: str):
        def deco(fn):
            self.routes.append((method.upper(), pattern, fn))
            return fn
        return deco

    def match(self, method: str, path: str):
        for m, pattern, fn in self.routes:
            if m != method:
                continue
            kwargs = self._match_pattern(pattern, path)
            if kwargs is not None:
                return fn, kwargs
        return None, {}

    @staticmethod
    def _match_pattern(pattern: str, path: str):
        p_parts = [p for p in pattern.strip("/").split("/") if p]
        a_parts = [p for p in path.strip("/").split("/") if p]
        if len(p_parts) != len(a_parts):
            return None
        kwargs = {}
        for pp, ap in zip(p_parts, a_parts):
            if pp.startswith("{") and pp.endswith("}"):
                kwargs[pp[1:-1]] = ap
            elif pp != ap:
                return None
        return kwargs

    def serve_static(self, req: Request, prefix: str, static_dir: str):
        import os
        rel = req.path[len(prefix):].lstrip("/") or "index.html"
        target = os.path.normpath(os.path.join(static_dir, rel))
        if not target.startswith(os.path.abspath(static_dir)):
            raise HTTPError(403, "forbidden")
        if not os.path.isfile(target):
            raise HTTPError(404, "not found")
        ctype = {
            ".html": "text/html; charset=utf-8",
            ".js": "text/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".json": "application/json",
            ".svg": "image/svg+xml",
        }.get(os.path.splitext(target)[1], "application/octet-stream")
        with open(target, "rb") as f:
            req.handler.send_bytes(200, f.read(), ctype)

    def sse_response(self, req: Request):
        h = req.handler
        h.send_response(200)
        h.send_header("Content-Type", "text/event-stream")
        h.send_header("Cache-Control", "no-cache")
        h.send_header("Connection", "keep-alive")
        h.end_headers()
        q = self.hub.connect()
        try:
            h.wfile.write(b": connected\n\n")
            h.wfile.flush()
            while True:
                try:
                    event, data = q.get(timeout=15)
                except queue.Empty:
                    h.wfile.write(b": keepalive\n\n")
                    h.wfile.flush()
                    continue
                chunk = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n".encode()
                h.wfile.write(chunk)
                h.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            self.hub.disconnect(q)

    def start(self, host: str, port: int) -> int:
        # One process runs one App per port; class-level binding is safe and is
        # what the per-connection handler instances actually read.
        Handler.app = self
        ThreadingHTTPServer.allow_reuse_address = True
        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self._server.server_address[1]

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server.server_close()
            self._server = None


def serve_file_download(handler: Handler, data: bytes, filename: str, extra_headers: dict | None = None,
                        chunk_delay: float = 0.0):
    """Send a file as attachment; optionally stream slowly to simulate slow writes."""
    handler.send_response(200)
    handler.send_header("Content-Type", "text/csv; charset=utf-8")
    handler.send_header("Content-Disposition", f'attachment; filename="{filename}"')
    handler.send_header("Content-Length", str(len(data)))
    for k, v in (extra_headers or {}).items():
        handler.send_header(k, v)
    handler.end_headers()
    if chunk_delay <= 0:
        handler.wfile.write(data)
        handler.wfile.flush()
        return
    view = memoryview(data)
    step = max(1, len(data) // 8)
    for i in range(0, len(data), step):
        handler.wfile.write(view[i:i + step])
        handler.wfile.flush()
        time.sleep(chunk_delay)
