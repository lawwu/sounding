"""The UI's catalog endpoints, against a temp catalog. No BigQuery calls."""

import datetime as dt
import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from sounding import catalog
from sounding.ui import Handler


@pytest.fixture
def server(tmp_path, monkeypatch):
    monkeypatch.setattr(catalog, "CATALOG", tmp_path / "c.duckdb")
    with catalog.connect() as con:
        now = dt.datetime(2026, 11, 4)
        con.execute("INSERT INTO snapshots VALUES (1, ?, 'p.d')", [now])
        con.execute("INSERT INTO tables VALUES (1, 'p.d', 'orders', 'p.d.orders', 'TABLE', 5, 100, ?, ?, NULL, NULL, NULL, NULL)", [now, now])
        con.execute("INSERT INTO columns VALUES (1, 'p.d.orders', 'hits', 'RECORD', 'REPEATED', NULL, NULL)")
        con.execute("INSERT INTO columns VALUES (1, 'p.d.orders', 'hits.page.pagePath', 'STRING', 'NULLABLE', NULL, NULL)")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def get(url):
    with urllib.request.urlopen(url) as r:
        return r.headers["Content-Type"], r.read()


def test_page_and_catalog_endpoints(server):
    ctype, body = get(server + "/")
    assert ctype.startswith("text/html") and b"<title>Sounding</title>" in body

    stats = json.loads(get(server + "/api/stats")[1])
    assert stats["datasets"] == 1 and stats["tables"] == 1 and stats["bytes"] == 100

    hits = json.loads(get(server + "/api/search?q=pagepath")[1])
    assert [h["table"] for h in hits] == ["p.d.orders"]
    assert hits[0]["matched_columns"] == ["hits.page.pagePath"]

    t = json.loads(get(server + "/api/table?fqn=p.d.orders")[1])
    assert [c["path"] for c in t["columns"]] == ["hits", "hits.page.pagePath"]


def test_busy_port_falls_forward():
    from sounding.ui import bind
    first = bind(0)  # any free port
    try:
        second = bind(first.server_port)
        assert second.server_port != first.server_port
        second.server_close()
    finally:
        first.server_close()
