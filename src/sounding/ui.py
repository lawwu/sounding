"""Loopback-only UI with origin/Host validation and per-server API tokens."""

from __future__ import annotations

import errno
import json
import logging
import secrets
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from urllib.parse import parse_qs, urlsplit

from . import catalog
from .errors import SoundingError, integer, public_error

MAX_BODY = 400_000  # at most 100k Unicode SQL characters plus JSON overhead
PORT_TRIES = 10


class Server(ThreadingHTTPServer):
    # The stdlib default backlog is 5. macOS resets connections past it instead of
    # retrying, so a burst of 8 parallel requests failed there but not on Linux.
    request_queue_size = 64

    def __init__(self, address, handler=None):
        self.api_token = secrets.token_urlsafe(32)
        super().__init__(address, handler or Handler)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(15)

    def _send(self, body: bytes, ctype: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status: int = 200) -> None:
        self._send(json.dumps(obj).encode(), "application/json", status)

    def _authorize(self) -> bool:
        hosts = self.headers.get_all("Host", [])
        allowed = {f"127.0.0.1:{self.server.server_port}", f"localhost:{self.server.server_port}"}
        if len(hosts) != 1 or hosts[0].lower() not in allowed:
            self._json({"error": "Invalid Host."}, 403)
            return False
        origin = self.headers.get("Origin")
        if ((origin is not None and origin != "http://" + hosts[0].lower())
                or self.headers.get("Sec-Fetch-Site") not in (None, "same-origin", "none")):
            self._json({"error": "Cross-origin requests are not allowed."}, 403)
            return False
        u = urlsplit(self.path)
        if u.scheme or u.netloc or not self.path.startswith("/"):
            self._json({"error": "Invalid request target."}, 400)
            return False
        if u.path.startswith("/api/"):
            token = self.headers.get("X-Sounding-Token", "")
            if not token.isascii() or not secrets.compare_digest(token, self.server.api_token):
                self._json({"error": "Invalid API token. Reload the Sounding page."}, 403)
                return False
        if self.command == "POST" and origin is None:
            self._json({"error": "POST requests require a same-origin Origin header."}, 403)
            return False
        return True

    def _error(self, error):
        message, status = public_error(error)
        if status >= 500:
            logging.exception("Sounding request failed")
        self._json({"error": message}, status)

    def do_GET(self) -> None:
        if not self._authorize():
            return
        u = urlsplit(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path == "/":
                page = files("sounding").joinpath("ui.html").read_text()
                return self._send(page.replace("__SOUNDING_TOKEN__", self.server.api_token).encode(), "text/html; charset=utf-8")
            if u.path == "/ui.js":
                return self._send(files("sounding").joinpath("ui.js").read_bytes(), "text/javascript; charset=utf-8")
            if u.path == "/api/stats":
                with catalog.connect() as con:
                    result = catalog.stats(con)
                return self._json(result)
            if u.path == "/api/search":
                try:
                    offset = int(q.get("offset", "0"))
                except ValueError as error:
                    raise SoundingError("offset must be an integer.") from error
                with catalog.connect() as con:
                    result = catalog.search(con, q.get("q", ""), limit=catalog.SEARCH_LIMIT, offset=offset)
                return self._json(result)
            if u.path == "/api/table":
                with catalog.connect() as con:
                    result = catalog.describe(con, q.get("fqn", ""))
                return self._json(result)
            self._json({"error": "Not found."}, 404)
        except Exception as error:
            self._error(error)

    def do_POST(self) -> None:
        if not self._authorize():
            return
        if self.headers.get_content_type() != "application/json":
            return self._json({"error": "Content-Type must be application/json."}, 415)
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get("Transfer-Encoding") or len(lengths) != 1:
            return self._json({"error": "A single Content-Length is required."}, 400)
        try:
            length = int(lengths[0])
        except ValueError:
            return self._json({"error": "Invalid Content-Length."}, 400)
        if not 0 < length <= MAX_BODY:
            return self._json({"error": "Request body is empty or too large."}, 413)
        try:
            try:
                body = json.loads(self.rfile.read(length))
            except (ValueError, UnicodeError) as error:
                raise SoundingError("Request body must be valid JSON.") from error
            if not isinstance(body, dict):
                raise SoundingError("Request body must be a JSON object.")
            from . import bq
            path = urlsplit(self.path).path
            if path == "/api/estimate":
                return self._json(bq.dry_run(body.get("sql", "")))
            if path == "/api/preview":
                return self._json(bq.preview(body.get("fqn", ""), 10))
            if path == "/api/column-costs":
                fqn = body.get("fqn", "")
                receipt = bq.column_costs(fqn)["receipt"]
                with catalog.connect() as con:
                    result = catalog.describe(con, fqn)
                return self._json(result | {"receipt": receipt})
            if path == "/api/cost-report":
                return self._json(bq.cost_report(body.get("hours", 24)))
            self._json({"error": "Not found."}, 404)
        except Exception as error:
            self._error(error)

    def do_OPTIONS(self):
        self._json({"error": "Cross-origin API access is not supported."}, 405)

    def log_message(self, *args) -> None:
        pass


def bind(port: int) -> Server:
    integer(port, "port", 0, 65535)
    ports = [0] if port == 0 else range(port, min(port + PORT_TRIES, 65536))
    for p in ports:
        try:
            return Server(("127.0.0.1", p))
        except OSError as error:
            if error.errno != errno.EADDRINUSE:
                raise
            print(f"port {p} is in use", file=sys.stderr)
    raise SoundingError("No available port in the requested range; pass --port.")


def serve(port: int = 8765, open_browser: bool = False) -> None:
    with bind(port) as srv:
        url = f"http://127.0.0.1:{srv.server_port}/"
        print(f"sounding ui on {url}  (catalog: {catalog.CATALOG})", flush=True)
        if open_browser:
            webbrowser.open(url)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
