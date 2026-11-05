import datetime as dt
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from google.cloud import bigquery

from sounding import bq, catalog
from sounding.errors import MissingTable, SoundingError
from conftest import column_row, table_row


def field(name, kind="STRING", fields=(), mode="NULLABLE"):
    return NS(name=name, field_type=kind, fields=fields, mode=mode, description=None)


def remote_table(name="t", schema=None):
    return NS(project="p", dataset_id="d", table_id=name, table_type="TABLE", num_rows=10,
              num_bytes=100, created=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
              modified=dt.datetime(2026, 1, 2, tzinfo=dt.UTC), time_partitioning=None,
              range_partitioning=None, clustering_fields=None, description=None, view_query=None,
              schema=[field("id")] if schema is None else schema)


def fake_client(monkeypatch, tables=None):
    tables = [remote_table()] if tables is None else tables
    client = Mock(project="p")
    client.list_tables.return_value = [NS(reference=t) for t in tables]
    client.get_table.side_effect = lambda ref: ref
    monkeypatch.setattr(bq, "client", lambda: client)
    return client


def query_job(size=123, statement="SELECT", accuracy="PRECISE", refs=None, **stats):
    refs = [dict(projectId="p", datasetId="d", tableId="t")] if refs is None else refs
    stats.update(statementType=statement, totalBytesProcessedAccuracy=accuracy, referencedTables=refs)
    if size is not None:
        stats["totalBytesProcessed"] = str(size)
    return bigquery.QueryJob.from_api_repr({"jobReference": {"projectId": "p", "jobId": "dry"},
        "configuration": {"dryRun": True, "query": {"query": "SELECT 1"}},
        "statistics": {"query": stats}}, Mock(project="p"))


def test_nested_empty_schemas_and_canonical_dataset(monkeypatch):
    schema = [field("hits", "RECORD", [field("page", "RECORD", [field("path")])], "REPEATED")]
    c = fake_client(monkeypatch, [remote_table(schema=schema), remote_table("empty", [])])
    bq.harvest(["d"])
    c.list_tables.assert_called_once_with("p.d")
    with catalog.connect() as con:
        cols = catalog.describe(con, "p.d.t")["columns"]
        assert [v["path"] for v in cols] == ["hits", "hits.page", "hits.page.path"]
        assert cols[0]["mode"] == "REPEATED"
        assert catalog.describe(con, "p.d.empty")["columns"] == []
    c.list_tables.return_value = []
    bq.harvest(["p.d"])
    with catalog.connect() as con:
        assert catalog.search(con)["items"] == []
        assert catalog.stats(con)["datasets"] == 1
    c.query.assert_not_called()


def test_failed_fetch_never_publishes_partial_snapshot(seed, monkeypatch):
    seed()
    c = fake_client(monkeypatch, [remote_table("a"), remote_table("b")])
    c.get_table.side_effect = [remote_table("a"), RuntimeError("network failure")]
    with pytest.raises(RuntimeError):
        bq.harvest(["d"])
    with catalog.connect() as con:
        assert catalog.describe(con, "p.d.t")["snapshot_id"] == 1
        assert con.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1


def test_harvest_does_not_hold_catalog_during_network_io(seed, monkeypatch):
    seed()
    c = fake_client(monkeypatch)
    entered, release = threading.Event(), threading.Event()
    def fetch(ref):
        entered.set()
        assert release.wait(5)
        return ref
    c.get_table.side_effect = fetch
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(bq.harvest, ["d"])
        assert entered.wait(5)
        try:
            with catalog.connect() as con:
                assert catalog.stats(con)["tables"] == 1
        finally:
            release.set()
        future.result(timeout=5)


@pytest.mark.parametrize("sql", ["SELECT * FROM `p.d.t`", "EXECUTE IMMEDIATE 'SELECT 1'", "IF TRUE THEN SELECT 1; END IF", "DROP TABLE `p.d.t`", "SELECT 'x`; DROP TABLE t; --'"])
def test_every_query_is_serialized_as_a_dry_run(sql, monkeypatch):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job()
    c.query.return_value.result = Mock(side_effect=AssertionError("must never execute or fetch results"))
    bq.dry_run(sql)
    assert c.query.call_args.args == (sql,)
    assert c.query.call_args.kwargs["job_config"].to_api_repr() == {"query": {"useQueryCache": False}, "dryRun": True}
    c.query.return_value.result.assert_not_called()
    c.list_rows.assert_not_called()


@pytest.mark.parametrize("size,statement,accuracy", [(None,"SELECT","PRECISE"), (0,"SELECT","PRECISE"), (10,"SCRIPT","PRECISE"), (10,"CALL","PRECISE"), (10,"SELECT","LOWER_BOUND"), (10,"SELECT","UNKNOWN")])
def test_unknown_estimates_never_become_zero_quotes(monkeypatch, size, statement, accuracy):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(size, statement, accuracy)
    out = bq.dry_run("SELECT 1")
    assert out["estimate_status"] == "unknown" and out["bytes"] is None
    assert out["usd_on_demand"] is None and out["usd_on_demand_range"] is None
    assert not out["referenced_tables_complete"]


def test_known_estimate_includes_minimum_and_multiple_table_range(monkeypatch):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(100)
    out = bq.dry_run("SELECT id FROM `p.d.t`")
    assert out["usd_on_demand"] == catalog.usd(10 * 2**20)
    refs = [dict(projectId="p", datasetId="d", tableId=f"t{i}") for i in range(2)]
    c.query.return_value = query_job(100 * 2**20, refs=refs)
    out = bq.dry_run("SELECT 1")
    assert out["usd_on_demand"] is None
    assert out["usd_on_demand_range"] == catalog.billing_range(100 * 2**20, 2)
    c.query.return_value = query_job(100, referencedRoutines=[{"routineId":"r"}])
    assert bq.dry_run("SELECT 1")["estimate_status"] == "unknown"
    c.query.return_value = query_job(100, accuracy="UPPER_BOUND")
    out = bq.dry_run("SELECT 1")
    assert out["estimate_status"] == "upper_bound"
    assert out["usd_on_demand"] is None and out["usd_on_demand_range"]["min"] is None


def test_information_schema_and_bad_inputs_do_not_call_bigquery():
    for sql in ["SELECT * FROM p.d.INFORMATION_SCHEMA.TABLES", "", "x" * (bq.MAX_SQL_LENGTH + 1)]:
        with pytest.raises(SoundingError):
            bq.dry_run(sql)


def test_column_costs_qualify_and_escape_identifiers(seed, monkeypatch):
    seed(columns=[column_row("t"), column_row("back`tick\\name")])
    calls = []
    def dry(sql):
        calls.append(sql)
        # A reader can open while a cost calculation is in progress.
        with catalog.connect() as con:
            assert catalog.stats(con)["tables"] == 1
        return {"bytes": 42}
    monkeypatch.setattr(bq, "dry_run", dry)
    assert bq.column_costs("p.d.t") == [("t",42),("back`tick\\name",42)]
    assert calls == ["SELECT sounding_source.`t` FROM `p.d.t` AS sounding_source",
                     "SELECT sounding_source.`back\\`tick\\\\name` FROM `p.d.t` AS sounding_source"]
    with catalog.connect() as con:
        assert all(c["scan_bytes"] == 42 for c in catalog.describe(con,"p.d.t")["columns"])


def test_column_costs_empty_deleted_and_changed_snapshot(seed, monkeypatch):
    seed(columns=[])
    assert bq.column_costs("p.d.t") == []
    seed([], [])
    with pytest.raises(MissingTable):
        bq.column_costs("p.d.t")
    seed()
    def dry(sql):
        seed()
        return {"bytes": 99}
    monkeypatch.setattr(bq, "dry_run", dry)
    with pytest.raises(SoundingError, match="changed"):
        bq.column_costs("p.d.t")
    with catalog.connect() as con:
        assert con.execute("SELECT count(*) FROM columns WHERE scan_bytes IS NOT NULL").fetchone()[0] == 0


def test_column_costs_failure_does_not_partially_cache(seed, monkeypatch):
    seed(columns=[column_row("a"),column_row("b")])
    monkeypatch.setattr(bq,"dry_run",Mock(side_effect=[{"bytes":1},RuntimeError("failure")]))
    with pytest.raises(RuntimeError): bq.column_costs("p.d.t")
    with catalog.connect() as con:
        assert con.execute("SELECT count(*) FROM columns WHERE scan_bytes IS NOT NULL").fetchone()[0] == 0


@pytest.mark.parametrize("n",[-1,0,51,True])
def test_preview_bounds(n):
    with pytest.raises(SoundingError): bq.preview("p.d.t",n)


def test_preview_never_falls_back_to_query(monkeypatch):
    c=fake_client(monkeypatch)
    c.get_table.side_effect=None
    c.get_table.return_value=remote_table()
    c.list_rows.return_value=[{"nested":{"when":dt.datetime(2026,1,1,tzinfo=dt.UTC),"bytes":b'\xff'}}]
    assert bq.preview("p.d.t",5)==[{"nested":{"when":"2026-01-01T00:00:00+00:00","bytes":"/w=="}}]
    c.list_rows.assert_called_once_with(c.get_table.return_value,max_results=5)
    for kind in ["VIEW","EXTERNAL","MATERIALIZED_VIEW"]:
        c.get_table.return_value.table_type=kind
        with pytest.raises(SoundingError): bq.preview("p.d.t")
    c.query.assert_not_called()
