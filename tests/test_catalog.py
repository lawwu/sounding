import datetime as dt
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from sounding import catalog
from sounding.errors import CatalogBusy, CatalogVersionError, MissingTable, SoundingError
from conftest import column_row, table_row


def test_latest_snapshot_and_deleted_tables(seed):
    seed([table_row(num_rows=10), table_row("p.d.deleted")], [column_row("sale_price")])
    seed([table_row(num_rows=20)], [column_row("new_column")])
    seed([table_row("p.other.t")], [], "p.other")
    with catalog.connect() as con:
        assert catalog.describe(con, "p.d.t")["num_rows"] == 20
        assert catalog.search(con, "sale_price")["items"] == []
        assert catalog.describe(con, "p.d.t")["columns"][0]["path"] == "new_column"
        with pytest.raises(MissingTable):
            catalog.describe(con, "p.d.deleted")
        assert catalog.stats(con)["tables"] == 2


def test_empty_snapshot_stays_current_and_counts_dataset(seed):
    seed()
    seed([], [])
    with catalog.connect() as con:
        assert catalog.search(con)["items"] == []
        stats = catalog.stats(con)
        assert stats["datasets"] == 1 and stats["tables"] == 0
        assert stats["harvested_at"].endswith("+00:00")


def test_arrow_names_null_fields_empty_schema_and_utc(seed):
    source = table_row(description=None, num_rows=None, num_bytes=None)
    reordered = dict(reversed(list(source.items())))
    seed([reordered], [])
    with catalog.connect() as con:
        con.execute("SET TimeZone = 'America/Los_Angeles'")
        t = catalog.describe(con, "p.d.t")
        assert t["created"] == "2026-01-01T00:00:00+00:00"
        assert t["modified"] == "2026-01-02T00:00:00+00:00"
        assert t["dataset"] == "p.d" and t["type"] == "TABLE"
        assert t["columns"] == [] and t["description"] is None
        assert t["full_scan_usd"] is None


def test_failed_insert_rolls_back_entire_snapshot(seed):
    seed()
    with pytest.raises(duckdb.ConstraintException):
        seed([table_row(num_rows=999)], [column_row(), column_row()])
    with catalog.connect() as con:
        assert con.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1
        assert catalog.describe(con, "p.d.t")["num_rows"] == 10
    assert seed() == 2


def test_concurrent_writers_allocate_unique_ids(seed):
    with ThreadPoolExecutor(4) as pool:
        ids = list(pool.map(lambda i: seed([table_row(f"p.d{i}.t")], [], f"p.d{i}"), range(4)))
    assert sorted(ids) == [1, 2, 3, 4]
    seed([table_row("p.d0.new")], [], "p.d0")
    with catalog.connect() as con:
        tables = [r["table"] for r in catalog.search(con)["items"]]
        assert len(tables) == 4 and "p.d0.t" not in tables and "p.d0.new" in tables


def test_literal_substring_search(seed):
    seed([table_row(description="50% off")], [column_row("sale_price"), column_row("saleXprice")])
    with catalog.connect() as con:
        assert catalog.search(con, "sale_price")["items"][0]["matched_columns"] == ["sale_price"]
        assert catalog.search(con, "%")["items"][0]["matched_columns"] == []
        assert catalog.search(con, "\\")["items"] == []
        assert catalog.search(con, "' OR 1=1 --")["items"] == []


def test_search_pagination_and_bounded_matches(seed):
    seed([table_row(f"p.d.shard_{i:03}") for i in range(205)],
         [column_row(f"hits.child_{j:03}", f"p.d.shard_{i:03}") for i in range(205) for j in range(100)])
    with catalog.connect() as con:
        pages = [catalog.search(con, "hits", limit=100, offset=i) for i in (0, 100, 200)]
    assert [len(p["items"]) for p in pages] == [100, 100, 5]
    assert [p["next_offset"] for p in pages] == [100, 200, None]
    names = [r["table"] for p in pages for r in p["items"]]
    assert len(set(names)) == 205
    assert all(len(r["matched_columns"]) == 20 and r["matched_column_count"] == 100 for p in pages for r in p["items"])
    assert len(json.dumps(pages[0])) < 100_000


@pytest.mark.parametrize("limit,offset", [(0, 0), (101, 0), (1, -1), (True, 0)])
def test_search_bounds(limit, offset):
    with catalog.connect() as con, pytest.raises(SoundingError):
        catalog.search(con, limit=limit, offset=offset)


def test_prices_preserve_unknowns_and_include_table_minimums(seed):
    assert catalog.usd(None) is None
    assert catalog.billing_range(0) is None
    assert catalog.billing_range(1) == {"min": catalog.usd(10 * 2**20), "max": catalog.usd(10 * 2**20)}
    assert catalog.billing_range(1000 * 1024, 1000)["min"] == catalog.usd(1000 * 10 * 2**20)
    cost = catalog.billing_range(100 * 2**20, 2)
    assert cost == {"min": catalog.usd(100 * 2**20), "max": catalog.usd(110 * 2**20)}
    seed([table_row(type="VIEW", num_bytes=None)], [])
    with catalog.connect() as con:
        assert catalog.describe(con, "p.d.t")["full_scan_usd"] is None
        assert catalog.search(con)["items"][0]["full_scan_usd"] is None


def test_legacy_catalog_is_preserved(tmp_path):
    path = tmp_path / "legacy.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE TABLE snapshots(snapshot_id INT, taken_at TIMESTAMP, dataset VARCHAR)")
        con.execute("INSERT INTO snapshots VALUES (1, '2026-01-01', 'p.d')")
    before = path.read_bytes()
    with pytest.raises(CatalogVersionError, match="new file"):
        with catalog.connect(path):
            pass
    assert path.read_bytes() == before


def test_other_process_waits_for_connection_and_then_succeeds(seed):
    seed()
    env = dict(os.environ, SOUNDING_CATALOG=str(catalog.CATALOG))
    with catalog.connect(write=True):
        child = subprocess.Popen([sys.executable, "-c",
            "from sounding import catalog; print('ready',flush=True);\nwith catalog.connect() as c: print(catalog.stats(c)['tables'])"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert child.stdout.readline().strip() == "ready"
            with pytest.raises(subprocess.TimeoutExpired):
                child.communicate(timeout=0.2)
        except BaseException:
            child.kill()
            child.communicate()
            raise
    out, err = child.communicate(timeout=10)
    assert child.returncode == 0, err
    assert out.strip() == "1"


def test_lock_timeout_has_safe_error(seed, monkeypatch):
    seed()
    monkeypatch.setattr(catalog, "LOCK_TIMEOUT", 0.01)
    with catalog.connect(), pytest.raises(CatalogBusy, match="Catalog is busy"):
        with catalog.connect():
            pass
