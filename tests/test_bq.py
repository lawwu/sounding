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
    client.list_models.return_value = []
    client._call_api.return_value = {}
    client.list_rows.return_value = []
    client._http.get.return_value = Mock(status_code=200, json=lambda: {})
    monkeypatch.setattr(bq, "client", lambda: client)
    return client


def query_job(size=123, statement="SELECT", accuracy="PRECISE", refs=None, job_ref=None, dry=True, **stats):
    """A dry-run response as BigQuery returns it: no job ID, dryRun echoed, 0 bytes billed."""
    refs = [dict(projectId="p", datasetId="d", tableId="t")] if refs is None else refs
    stats = dict(statementType=statement, totalBytesProcessedAccuracy=accuracy, referencedTables=refs,
                 totalBytesBilled="0") | stats
    if size is not None:
        stats["totalBytesProcessed"] = str(size)
    config = {"query": {"query": "SELECT 1"}} | ({"dryRun": True} if dry else {})
    return bigquery.QueryJob.from_api_repr({"jobReference": {"projectId": "p"} | (job_ref or {}),
        "configuration": config, "statistics": {"query": stats}}, Mock(project="p"))


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


def test_dry_run_receipt_copies_bigquery_evidence(monkeypatch):
    c = fake_client(monkeypatch)
    c.query.return_value = query_job(100)
    receipt = bq.dry_run("SELECT id FROM `p.d.t`")["receipt"]
    assert receipt == {"call": "jobs.insert (dry run)", "dry_run_confirmed": True, "billed_bytes": 0,
                       "job_id": None, "job_project": "p", "free": True, "source": receipt["source"]}


@pytest.mark.parametrize("response", [dict(totalBytesBilled="10485760"), dict(job_ref={"jobId": "real"}),
                                      dict(dry=False), dict(totalBytesBilled=None)])
def test_unconfirmed_dry_run_receipt_is_flagged(monkeypatch, response):
    c = fake_client(monkeypatch)
    stats = {"totalBytesBilled": response.pop("totalBytesBilled")} if "totalBytesBilled" in response else {}
    job = query_job(100, **response, **stats)
    if stats.get("totalBytesBilled") is None and "totalBytesBilled" in stats:
        del job._properties["statistics"]["query"]["totalBytesBilled"]
    c.query.return_value = job
    out = bq.dry_run("SELECT id FROM `p.d.t`")
    assert out["receipt"]["free"] is False
    assert any("did not confirm a free dry run" in w for w in out["warnings"])


def test_combined_receipt_is_free_only_if_every_dry_run_was():
    free = {"dry_run_confirmed": True, "billed_bytes": 0, "job_id": None, "free": True}
    assert bq.combine_receipts([free, free]) | {"source": None} == {
        "call": "jobs.insert (dry run)", "calls": 2, "dry_run_confirmed": True, "billed_bytes": 0,
        "jobs_created": 0, "free": True, "source": None}
    worse = bq.combine_receipts([free, {"dry_run_confirmed": True, "billed_bytes": None, "job_id": "j", "free": False}])
    assert worse["free"] is False and worse["billed_bytes"] is None and worse["jobs_created"] == 1
    assert bq.combine_receipts([{}])["free"] is False  # a missing receipt is never assumed free


def job_listing(job_id, kind="query", billed="0", **query):
    resource = {"jobReference": {"projectId": "p", "jobId": job_id}, "state": "DONE",
                "configuration": {kind: {"query": "SELECT 1"} if kind == "query" else {}},
                "statistics": {"creationTime": "1793938978896", "totalSlotMs": "10",
                               kind: ({} if billed is None else {"totalBytesBilled": billed}) | query}}
    return bigquery.job.UnknownJob.from_api_repr(resource, Mock(project="p")) if kind != "query" else \
        bigquery.QueryJob.from_api_repr(resource, Mock(project="p"))


def test_cost_report_with_no_jobs_proves_nothing_was_billed(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_jobs.return_value = []
    r = bq.cost_report(24)
    assert (r["project"], r["job_count"], r["billed_bytes"], r["usd_on_demand"], r["jobs"]) == ("p", 0, 0, 0.0, [])
    kwargs = c.list_jobs.call_args.kwargs
    assert kwargs["max_results"] == bq.COST_REPORT_MAX_JOBS + 1 and "all_users" not in kwargs
    assert dt.datetime.fromisoformat(r["until"]) - kwargs["min_creation_time"] == dt.timedelta(hours=24)
    c.query.assert_not_called()


def test_cost_report_sums_bigquery_billed_bytes(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_jobs.return_value = [job_listing("a", billed=str(10 * 2**20), statementType="SELECT"),
                                job_listing("b", billed="0", cacheHit=True)]
    r = bq.cost_report(1)
    assert r["job_count"] == 2 and r["billed_bytes"] == 10 * 2**20
    assert r["usd_on_demand"] == catalog.usd(10 * 2**20)
    assert r["jobs"][0]["created"].endswith("+00:00") and r["jobs"][1]["cache_hit"] is True
    c.list_jobs.return_value = [job_listing("a", billed=None)]
    r = bq.cost_report(1)
    assert r["billed_bytes"] is None and r["usd_on_demand"] is None
    assert any("total is unknown" in w for w in r["warnings"])


def test_cost_report_bounds_and_truncation(monkeypatch):
    for hours in [0, bq.COST_REPORT_MAX_HOURS + 1, True]:
        with pytest.raises(SoundingError):
            bq.cost_report(hours)
    c = fake_client(monkeypatch)
    monkeypatch.setattr(bq, "COST_REPORT_MAX_JOBS", 2)
    c.list_jobs.return_value = [job_listing(str(i)) for i in range(3)]
    r = bq.cost_report(1)
    assert r["truncated"] and r["job_count"] == 2 and any("Use a shorter window" in w for w in r["warnings"])


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
    result = bq.column_costs("p.d.t")
    assert result["columns"] == [("t",42),("back`tick\\name",42)]
    assert result["receipt"]["calls"] == 2 and result["receipt"]["free"] is False  # the fake returns no receipts
    assert calls == ["SELECT sounding_source.`t` FROM `p.d.t` AS sounding_source",
                     "SELECT sounding_source.`back\\`tick\\\\name` FROM `p.d.t` AS sounding_source"]
    with catalog.connect() as con:
        assert all(c["scan_bytes"] == 42 for c in catalog.describe(con,"p.d.t")["columns"])


def test_column_costs_empty_deleted_and_changed_snapshot(seed, monkeypatch):
    seed(columns=[])
    assert bq.column_costs("p.d.t")["columns"] == []
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
    out=bq.preview("p.d.t",5)
    assert out["rows"]==[{"nested":{"when":"2026-01-01T00:00:00+00:00","bytes":"/w=="}}]
    assert out["receipt"]["call"]=="tables.get, tabledata.list" and out["receipt"]["jobs_created"]==0
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
    assert "resource" not in t and resource["schema"]["fields"] == schema and resource["numPartitions"] == "32"
    # Absent fields are unknown (None), never zero or False.
    assert plain["storage"] is None and plain["num_partitions"] is None
    assert plain["require_partition_filter"] is None and plain["primary_key"] is None and plain["notes"] == []


def test_partitioned_table_without_flag_does_not_require_filter(monkeypatch):
    fake_client(monkeypatch, [remote_table(timePartitioning={"type": "DAY"})])
    bq.harvest(["d"])
    with catalog.connect() as con:
        t = catalog.describe(con, "p.d.t")
    assert t["partitioning"] == "DAY:_PARTITIONTIME" and t["require_partition_filter"] is False
    assert t["partition"]["ingestion_time"] is True
    assert not any(c["partition_column"] for c in t["columns"])


def test_column_roles_and_range_bounds_come_from_saved_metadata(monkeypatch):
    fake_client(monkeypatch, [remote_table(schema=[field('id','INT64', defaultValueExpression='42'),field('email', maxLength='100',collation='und:ci')],
        rangePartitioning={'field':'id','range':{'start':'0','end':'100','interval':'10'}},
        clustering={'fields':['email','id']}, tableConstraints={'primaryKey':{'columns':['id']}})])
    bq.harvest(['d'])
    with catalog.connect() as con:
        table = catalog.describe(con, 'p.d.t')
    assert table['partition'] == {'kind':'RANGE','field':'id','ingestion_time':False,
                                  'range':{'start':'0','end':'100','interval':'10'}}
    id_column, email = table['columns']
    assert id_column['partition_column'] and id_column['primary_key']
    assert id_column['clustering_order'] == 2 and email['clustering_order'] == 1
    assert not email['partition_column'] and not email['primary_key']
    assert id_column['metadata'] == {'defaultValueExpression':'42'}
    assert email['metadata'] == {'maxLength':'100','collation':'und:ci'}


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


def test_harvest_returns_a_receipt_of_free_calls(monkeypatch):
    fake_client(monkeypatch, [remote_table("a"), remote_table("b")], [remote_routine()])
    receipt = bq.harvest(["p.d"])
    assert receipt["jobs_created"] == 0 and receipt["free"] is True
    assert receipt["get_calls"] == {"datasets.get": 1, "tables.get": 2, "routines.get": 1, "models.get": 0}


def test_column_cost_dry_runs_run_concurrently_in_column_order(seed, monkeypatch):
    seed(columns=[column_row(c) for c in "abcd"])
    both_running = threading.Barrier(2, timeout=5)  # a serial loop would time out here
    def dry(sql):
        if "`a`" in sql or "`b`" in sql:
            both_running.wait()
        return {"bytes": len(sql)}
    monkeypatch.setattr(bq, "dry_run", dry)
    progress = Recorder()
    result = bq.column_costs("p.d.t", progress)
    assert [c for c, _ in result["columns"]] == list("abcd")
    assert progress.total == progress.done == 4


class Recorder:
    def __init__(self):
        self.total = self.done = 0
        self.lines, self.lock = [], threading.Lock()
    def add(self, n=1):
        with self.lock: self.total += n
    def advance(self, n=1):
        with self.lock: self.done += n
    def log(self, line):
        with self.lock: self.lines.append(line)
    def close(self):
        pass


def test_datasets_are_harvested_concurrently_with_progress(monkeypatch):
    c = fake_client(monkeypatch, [remote_table("a"), remote_table("b")], [remote_routine()])
    both_running = threading.Barrier(2, timeout=5)
    dataset = c.get_dataset.return_value
    c.get_dataset.side_effect = lambda ref: both_running.wait() and dataset or dataset
    progress = Recorder()
    receipt = bq.harvest(["p.one", "p.two"], progress)
    assert receipt["get_calls"] == {"datasets.get": 2, "tables.get": 4, "routines.get": 2, "models.get": 0}
    assert progress.total == progress.done == 2 + 4 + 2 + 4 + 2  # datasets, tables, routines, policies, regional cache
    assert sorted(line.split(":")[0] for line in progress.lines) == ["p.one", "p.two"]
    with catalog.connect() as con:
        assert catalog.stats(con)["datasets"] == 2


def test_one_failed_dataset_does_not_stop_the_others(monkeypatch):
    c = fake_client(monkeypatch)
    dataset = c.get_dataset.return_value
    def get_dataset(ref):
        if ref.dataset_id == "bad":
            raise RuntimeError("network failure")
        return dataset
    c.get_dataset.side_effect = get_dataset
    progress = Recorder()
    with pytest.raises(RuntimeError, match="network failure"):
        bq.harvest(["p.bad", "p.good"], progress)
    with catalog.connect() as con:
        assert [r[0] for r in con.execute("SELECT dataset FROM snapshots").fetchall()] == ["p.good"]
    assert any(line.startswith("p.bad: failed (RuntimeError)") for line in progress.lines)
    assert not any("network failure" in line for line in progress.lines)  # error details stay out of logs


def test_bad_dataset_name_fails_before_any_network_call(monkeypatch):
    c = fake_client(monkeypatch)
    with pytest.raises(SoundingError):
        bq.harvest(["p.ok", ""])
    c.get_dataset.assert_not_called()


@pytest.mark.parametrize("partitioning,where", [
    ("DAY:event_date", "WHERE sounding_source.`event_date` IS NOT NULL"),
    ("DAY:_PARTITIONTIME", "WHERE _PARTITIONTIME IS NOT NULL")])
def test_column_costs_satisfy_a_required_partition_filter(seed, monkeypatch, partitioning, where):
    seed([table_row(partitioning=partitioning, require_partition_filter=True)],
         [column_row("event_date"), column_row("title")])
    calls = []
    def dry(sql):
        calls.append(sql)
        return {"bytes": 30 if "`event_date` FROM" in sql else 100}  # every run also reads event_date
    monkeypatch.setattr(bq, "dry_run", dry)
    result = bq.column_costs("p.d.t")
    assert all(sql.endswith(where) for sql in calls)
    if partitioning.endswith("event_date"):  # the filter column's bytes are subtracted once
        assert result["columns"] == [("event_date", 30), ("title", 70)]
    else:  # pseudo-columns cost nothing
        assert result["columns"] == [("event_date", 30), ("title", 100)]
    assert "requires a partition filter" in result["warnings"][0]


def test_unknown_partition_column_bytes_make_other_costs_unknown(seed, monkeypatch):
    seed([table_row(partitioning="DAY:event_date", require_partition_filter=True)],
         [column_row("event_date"), column_row("title")])
    monkeypatch.setattr(bq, "dry_run", lambda sql: {"bytes": None if "`event_date` FROM" in sql else 100})
    assert bq.column_costs("p.d.t")["columns"] == [("event_date", None), ("title", None)]


def test_table_deleted_between_list_and_get_is_skipped(monkeypatch):
    from google.api_core.exceptions import NotFound
    gone = remote_table("gone")
    c = fake_client(monkeypatch, [remote_table("kept"), gone])
    c.get_table.side_effect = lambda ref: (_ for _ in ()).throw(NotFound("deleted")) if ref is gone else ref
    receipt = bq.harvest(["p.d"])
    assert receipt["get_calls"]["tables.get"] == 1
    with catalog.connect() as con:
        assert [r["table"] for r in catalog.search(con)["items"]] == ["p.d.kept"]
