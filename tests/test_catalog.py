"""Offline tests: no BigQuery credentials needed."""

import datetime as dt
from types import SimpleNamespace as NS

from sounding import catalog
from sounding.bq import flatten


def field(name, type_="STRING", fields=(), mode="NULLABLE"):
    return NS(name=name, field_type=type_, fields=list(fields), mode=mode, description=None)


def test_flatten_descends_into_records():
    schema = [field("id", "INT64"),
              field("hits", "RECORD", [field("page", "RECORD", [field("pagePath")])], mode="REPEATED")]
    assert [p for p, _ in flatten(schema)] == ["id", "hits", "hits.page", "hits.page.pagePath"]


def test_search_and_describe_use_latest_snapshot(tmp_path):
    con = catalog.connect(tmp_path / "c.duckdb")
    now = dt.datetime(2026, 11, 4)
    for sid, rows in [(1, 10), (2, 20)]:
        con.execute("INSERT INTO snapshots VALUES (?, ?, 'p.d')", [sid, now])
        con.execute("INSERT INTO tables VALUES (?, 'p.d', 't', 'p.d.t', 'TABLE', ?, ?, ?, ?, NULL, NULL, 'orders', NULL)",
                    [sid, rows, 2**40, now, now])
        con.execute("INSERT INTO columns VALUES (?, 'p.d.t', 'sale_price', 'FLOAT64', 'NULLABLE', NULL, NULL)", [sid])

    hits = catalog.search(con, "sale_price")
    assert len(hits) == 1 and hits[0]["rows"] == 20
    assert hits[0]["full_scan_usd"] == catalog.USD_PER_TIB
    assert catalog.search(con, "nope") == []

    d = catalog.describe(con, "p.d.t")
    assert d["snapshot_id"] == 2 and [c["path"] for c in d["columns"]] == ["sale_price"]
    assert "error" in catalog.describe(con, "p.d.missing")
