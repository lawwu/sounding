"""A small local web UI over the catalog: `sounding ui`.

Standard library only. Search and table pages read the catalog and never call BigQuery.
The three action endpoints make free BigQuery calls only (dry runs, tabledata.list).

Each request opens and closes its own catalog connection, so `sounding harvest` in
another terminal can take DuckDB's write lock between requests.
"""

from __future__ import annotations

import errno
import json
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from urllib.parse import parse_qs, urlparse

from . import catalog

SEARCH_LIMIT = 200  # results per search; 366 GA date shards would otherwise flood the list


class Handler(BaseHTTPRequestHandler):
    def _send(self, body: bytes, ctype: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status: int = 200) -> None:
        self._send(json.dumps(obj, default=str).encode(), "application/json", status)

    def do_GET(self) -> None:
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path == "/":
                return self._send(files("sounding").joinpath("ui.html").read_bytes(), "text/html; charset=utf-8")
            if u.path == "/api/stats":
                with catalog.connect() as con:
                    return self._json(catalog.stats(con))
            if u.path == "/api/search":
                with catalog.connect() as con:
                    return self._json(catalog.search(con, q.get("q", ""), limit=SEARCH_LIMIT))
            if u.path == "/api/table":
                with catalog.connect() as con:
                    return self._json(catalog.describe(con, q["fqn"]))
            if u.path == "/api/preview":
                from . import bq
                return self._json(bq.preview(q["fqn"], 10))
            self._json({"error": "not found"}, 404)
        except Exception as e:  # surface BigQuery/permission errors in the page
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)

    def do_POST(self) -> None:
        u = urlparse(self.path)
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            from . import bq
            if u.path == "/api/estimate":
                return self._json(bq.dry_run(body["sql"]))
            if u.path == "/api/column-costs":
                with catalog.connect() as con:
                    bq.column_costs(body["fqn"], con)
                    return self._json(catalog.describe(con, body["fqn"]))
            self._json({"error": "not found"}, 404)
        except Exception as e:
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)

    def log_message(self, *args) -> None:  # keep the terminal quiet
        pass


PORT_TRIES = 10  # if the requested port is busy, try the next few before giving up


def bind(port: int) -> ThreadingHTTPServer:
    """Bind on localhost only: the page can trigger (free) BigQuery calls with your
    credentials. A busy port (often another `sounding ui`) moves to the next one."""
    for p in range(port, port + PORT_TRIES):
        try:
            return ThreadingHTTPServer(("127.0.0.1", p), Handler)
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise
            print(f"port {p} is in use, trying {p + 1}", file=sys.stderr)
    raise SystemExit(f"ports {port}-{port + PORT_TRIES - 1} are all in use; pass --port")


def serve(port: int = 8765, open_browser: bool = False) -> None:
    srv = bind(port)
    url = f"http://127.0.0.1:{srv.server_port}/"
    print(f"sounding ui on {url}  (catalog: {catalog.CATALOG})", flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
