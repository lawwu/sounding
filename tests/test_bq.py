import datetime as dt
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

from sounding import bq, catalog
from sounding.errors import MissingTable, SoundingError
from conftest import column_row, table_row


def field(name, kind="STRING", fields=(), mode="NULLABLE", **extra):
    f = {"name": name, "type": kind, "mode": mode} | extra
    if fields:
        f["fields"] = list(fields)
    return f


def remote_table(name="t", schema=None, **resource):
    """A tables.get response, built the way google-cloud-bigquery builds one."""
    return bigquery.Table.from_api_repr({
        "tableReference": {"projectId": "p", "datasetId": "d", "tableId": name}, "type": "TABLE",
        "numRows": "10", "numBytes": "100", "creationTime": "1767225600000", "lastModifiedTime": "1767312000000",
        "schema": {"fields": [field("id")] if schema is None else schema}} | resource)


def remote_routine(name="f", **resource):
    return bigquery.Routine.from_api_repr({
        "routineReference": {"projectId": "p", "datasetId": "d", "routineId": name},
        "routineType": "SCALAR_FUNCTION", "language": "SQL", "definitionBody": "x + 1",
        "arguments": [{"name": "x", "dataType": {"typeKind": "INT64"}}],
        "returnType": {"typeKind": "INT64"}} | resource)


def fake_client(monkeypatch, tables=None, routines=(), dataset=None):
    tables = [remote_table()] if tables is None else tables
    client = Mock(project="p")
    client.get_dataset.return_value = bigquery.Dataset.from_api_repr(
        {"datasetReference": {"projectId": "p", "datasetId": "d"}, "location": "US"} | (dataset or {}))
    client.list_tables.return_value = [NS(reference=t) for t in tables]
    client.get_table.side_effect = lambda ref: ref
    client.list_routines.return_value = [NS(reference=r) for r in routines]
    client.get_routine.side_effect = lambda ref: ref
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


def test_bad_inputs_do_not_call_bigquery():
    for sql in ["", "x" * (bq.MAX_SQL_LENGTH + 1)]:
        with pytest.raises(SoundingError):
            bq.dry_run(sql)


def test_information_schema_is_only_dry_run(monkeypatch):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(10 * 2**20)
    sql = "SELECT * FROM p.d.INFORMATION_SCHEMA.TABLES"
    assert bq.dry_run(sql)["estimate_status"] == "estimated"
    assert c.query.call_args.kwargs["job_config"].dry_run is True


def test_zero_byte_select_explains_why_cost_is_unknown(monkeypatch):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(0)
    out = bq.dry_run("SELECT * FROM `p.d.t` WHERE FALSE")
    assert out["estimate_status"] == "unknown" and out["reported_bytes"] == 0
    assert any("reported 0 bytes" in w and "external" in w for w in out["warnings"])


def test_missing_accuracy_field_is_reported_not_silent(monkeypatch, caplog):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(100, accuracy=None)
    out = bq.dry_run("SELECT id FROM `p.d.t`")
    assert out["estimate_status"] == "unknown"
    assert any("estimate-accuracy" in w for w in out["warnings"])
    assert "totalBytesProcessedAccuracy" in caplog.text


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
        c.get_table.return_value=remote_table(type=kind)
        with pytest.raises(SoundingError): bq.preview("p.d.t")
    c.query.assert_not_called()


def test_table_resource_fields_are_captured(monkeypatch):
    schema = [field("id", "INT64"), field("email", policyTags={"names": ["projects/p/locations/us/taxonomies/1/policyTags/2"]})]
    full = remote_table("orders", schema, location="US", requirePartitionFilter=True, numPartitions="32",
        timePartitioning={"type": "DAY", "field": "day", "expirationMs": "86400000"},
        clustering={"fields": ["id"]}, labels={"team": "data"}, expirationTime="1798761600000",
        numActiveLogicalBytes="70", numLongTermLogicalBytes="30", numTotalPhysicalBytes="12",
        encryptionConfiguration={"kmsKeyName": "projects/p/keys/k"},
        snapshotDefinition={"baseTableReference": {"projectId": "p", "datasetId": "d", "tableId": "base"}, "snapshotTime": "2026-01-01T00:00:00Z"},
        tableConstraints={"primaryKey": {"columns": ["id"]}, "foreignKeys": [{"name": "fk_user",
            "referencedTable": {"projectId": "p", "datasetId": "d", "tableId": "users"},
            "columnReferences": [{"referencingColumn": "user_id", "referencedColumn": "id"}]}]})
    bare = remote_table("plain")
    fake_client(monkeypatch, [full, bare])
    bq.harvest(["d"])
    with catalog.connect() as con:
        t = catalog.describe(con, "p.d.orders")
        plain = catalog.describe(con, "p.d.plain")
        resource = json.loads(con.execute("SELECT resource FROM tables WHERE fqn = 'p.d.orders'").fetchone()[0])
    assert t["partitioning"] == "DAY:day" and t["require_partition_filter"] is True
    assert t["partition_expiration_ms"] == 86_400_000 and t["num_partitions"] == 32
    assert t["primary_key"] == ["id"]
    assert t["foreign_keys"] == [{"name": "fk_user", "references": "p.d.users", "columns": [["user_id", "id"]]}]
    assert t["storage"] == {"active_logical": 70, "long_term_logical": 30, "total_physical": 12}
    assert t["labels"] == {"team": "data"} and t["location"] == "US"
    assert t["expires"] == "2027-01-01T00:00:00+00:00" and t["kms_key"] == "projects/p/keys/k"
    assert (t["base_kind"], t["base_table"]) == ("SNAPSHOT", "p.d.base")
    assert [c["policy_tags"] for c in t["columns"]] == [None, ["projects/p/locations/us/taxonomies/1/policyTags/2"]]
    assert any("must filter on the partition column" in n for n in t["notes"])
    assert any("policy tags" in n and "email" in n for n in t["notes"])
    assert any("not enforced" in n for n in t["notes"])
    assert "resource" not in t and "schema" not in resource and resource["numPartitions"] == "32"
    # Absent fields are unknown (None), never zero or False.
    assert plain["storage"] is None and plain["num_partitions"] is None
    assert plain["require_partition_filter"] is None and plain["primary_key"] is None and plain["notes"] == []


def test_partitioned_table_without_flag_does_not_require_filter(monkeypatch):
    fake_client(monkeypatch, [remote_table(timePartitioning={"type": "DAY"})])
    bq.harvest(["d"])
    with catalog.connect() as con:
        t = catalog.describe(con, "p.d.t")
    assert t["partitioning"] == "DAY:_PARTITIONTIME" and t["require_partition_filter"] is False


def test_views_materialized_views_and_external_tables(monkeypatch):
    fake_client(monkeypatch, [
        remote_table("v", type="VIEW", view={"query": "SELECT 1"}),
        remote_table("mv", type="MATERIALIZED_VIEW", materializedView={"query": "SELECT 2", "enableRefresh": True},
                     materializedViewStatus={"refreshWatermark": "2026-01-01T00:00:00Z"}),
        remote_table("ext", type="EXTERNAL", externalDataConfiguration={"sourceFormat": "PARQUET", "sourceUris": ["gs://b/*"]})])
    bq.harvest(["d"])
    with catalog.connect() as con:
        v, mv, ext = (catalog.describe(con, f"p.d.{n}") for n in ("v", "mv", "ext"))
    assert v["view_sql"] == "SELECT 1" and mv["view_sql"] == "SELECT 2"
    assert mv["materialized_view"] == {"enableRefresh": True, "status": {"refreshWatermark": "2026-01-01T00:00:00Z"}}
    assert ext["external"]["sourceFormat"] == "PARQUET"
    assert any("External table" in n for n in ext["notes"])


def test_dataset_metadata_never_stores_access_entries(monkeypatch):
    fake_client(monkeypatch, dataset={"description": "Orders", "labels": {"env": "prod"},
        "defaultTableExpirationMs": "3600000", "storageBillingModel": "PHYSICAL", "maxTimeTravelHours": "168",
        "access": [{"role": "READER", "userByEmail": "someone@example.com"}]})
    bq.harvest(["d"])
    with catalog.connect() as con:
        d = catalog.describe_dataset(con, "p.d")
    assert d["location"] == "US" and d["description"] == "Orders" and d["labels"] == {"env": "prod"}
    assert d["default_table_expiration_ms"] == 3_600_000 and d["storage_billing_model"] == "PHYSICAL"
    assert d["max_time_travel_hours"] == 168 and d["tables"] == 1 and d["routines_status"] == "harvested"
    assert b"someone@example.com" not in catalog.CATALOG.read_bytes()


def test_routines_are_harvested_and_searchable(monkeypatch):
    big = "SELECT " + "x, " * 600
    fake_client(monkeypatch, routines=[
        remote_routine("add_one", description="Adds one"),
        remote_routine("tags", arguments=[{"name": "s", "dataType": {"typeKind": "STRING"}},
                                         {"name": "v", "argumentKind": "ANY_TYPE"}],
                       returnType={"typeKind": "ARRAY", "arrayElementType": {"typeKind": "STRUCT", "structType":
                                   {"fields": [{"name": "k", "type": {"typeKind": "STRING"}}]}}}),
        remote_routine("rows", routineType="TABLE_VALUED_FUNCTION", returnType=None, definitionBody=big,
                       returnTableType={"columns": [{"name": "n", "type": {"typeKind": "INT64"}}]})])
    bq.harvest(["d"])
    with catalog.connect() as con:
        d = catalog.describe_dataset(con, "p.d")
        found = catalog.search_routines(con, "adds")
        page = catalog.search_routines(con, limit=2)
        rows = catalog.describe_routine(con, "p.d.rows")
        listed = catalog.search_routines(con, "rows")["items"][0]
        assert catalog.stats(con)["routines"] == 3
        with pytest.raises(MissingTable):
            catalog.describe_routine(con, "p.d.missing")
    by_name = {r["routine"]: r for r in d["routines"]}
    assert by_name["p.d.add_one"]["signature"] == "(x INT64)" and by_name["p.d.add_one"]["returns"] == "INT64"
    assert by_name["p.d.tags"]["signature"] == "(s STRING, v ANY TYPE)"
    assert by_name["p.d.tags"]["returns"] == "ARRAY<STRUCT<k STRING>>"
    assert by_name["p.d.rows"]["returns"] == "TABLE<n INT64>"
    assert all("definition" not in r for r in d["routines"])
    assert [r["routine"] for r in found["items"]] == ["p.d.add_one"]
    assert len(page["items"]) == 2 and page["next_offset"] == 2
    assert rows["definition"] == big and len(listed["definition"]) == 1000


def test_denied_routines_keep_tables(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_routines.side_effect = Forbidden("no routines.list")
    bq.harvest(["d"])
    with catalog.connect() as con:
        assert catalog.describe(con, "p.d.t")["fqn"] == "p.d.t"
        d = catalog.describe_dataset(con, "p.d")
    assert d["routines_status"] == "denied" and d["routines"] == []
