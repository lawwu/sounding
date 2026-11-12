import datetime as dt
import json
from unittest.mock import Mock

import duckdb
import pytest
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from sounding import bq, catalog, workload
from sounding.errors import SoundingError
from test_bq import query_job


def family(**overrides):
    return dict(query_key="hash:abc", query_hash="abc", statement_type="SELECT", executions=12,
        first_seen=dt.datetime(2026, 1, 1, tzinfo=dt.UTC), last_seen=dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
        known_billed_bytes=2**40, missing_billed_jobs=0,
        known_processed_bytes=2**40, missing_processed_jobs=0,
        known_slot_ms=3_600_000, missing_slot_jobs=0, reservation_jobs=0,
        avg_duration_ms=1000.0, max_duration_ms=2000,
        resource_warning_jobs=0, slot_contention_jobs=0, insufficient_shuffle_jobs=0,
        high_cardinality_join_jobs=0, partition_skew_jobs=0,
        sample_jobs=[{"job_id": "costly", "creation_time": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                      "duration_ms": 2000}], total_candidates=1) | overrides


def monitor_client(monkeypatch, *, rows=None, size=10 * 1024**2, accuracy="PRECISE"):
    c = Mock(project="billing-project")
    preview = query_job(size=size, accuracy=accuracy)
    paid = bigquery.QueryJob.from_api_repr({
        "jobReference": {"projectId": "billing-project", "jobId": "scan", "location": "EU"},
        "configuration": {"query": {"query": "SELECT metadata"}}, "status": {"state": "DONE"},
        "statistics": {"query": {"totalBytesProcessed": str(size), "totalBytesBilled": str(size),
                                  "totalSlotMs": "2000"}}}, c)
    paid.result = Mock(return_value=[family()] if rows is None else rows)
    c.query.side_effect = [preview, paid]
    monkeypatch.setattr(bq, "client", lambda: c)
    return c, preview, paid


def test_default_preview_is_free_and_does_not_save(monkeypatch):
    c, preview, paid = monitor_client(monkeypatch)
    preview.result = Mock(side_effect=AssertionError("must not retrieve results for a dry run"))
    result = bq.query_opportunities("EU", project="workloads", scope="project")
    assert result["status"] == "preview" and result["receipt"]["free"]
    assert result["project"] == "workloads" and result["job_project"] == "billing-project"
    assert result["within_byte_cap"] is True
    assert c.query.call_count == 1
    args = c.query.call_args.kwargs
    assert args["location"] == "EU" and args["job_retry"] is None
    assert args["job_config"].dry_run is True
    assert args["job_config"].use_legacy_sql is False
    assert args["job_config"].maximum_bytes_billed == workload.DEFAULT_MAX_BYTES
    preview.result.assert_not_called()
    paid.result.assert_not_called()
    assert not catalog.CATALOG.exists()


def test_paid_scan_is_capped_and_saved_with_receipts(monkeypatch):
    c, _, _ = monitor_client(monkeypatch, rows=[family(total_candidates=25)])
    result = bq.query_opportunities("EU", project="workloads", scope="project", rank_by="slot_ms",
                                    limit=1, allow_billed=True)
    assert result["status"] == "complete" and result["truncated"]
    assert result["total_candidates"] == 25
    assert result["receipt"]["free"] is False and result["preview_receipt"]["free"]
    assert result["receipt"]["billed_bytes"] == 10 * 1024**2
    assert result["receipt"]["slot_ms"] == 2000
    assert result["opportunities"][0]["usd_on_demand_equivalent"] == 6.25
    assert result["opportunities"][0]["findings"][0]["signal"] == "repeated_work"
    assert result["opportunities"][0]["sample_jobs"][0]["creation_time"].endswith("+00:00")
    dry, paid = c.query.call_args_list
    assert dry.args == paid.args
    assert dry.kwargs["job_config"].dry_run is True
    config = paid.kwargs["job_config"]
    assert config.dry_run is False and config.use_query_cache is False
    assert config.maximum_bytes_billed == workload.DEFAULT_MAX_BYTES
    assert config.labels == {"sounding_action": "query_opportunities"}
    assert int(config.job_timeout_ms) == 60000
    assert paid.kwargs["job_retry"] is None and paid.kwargs["location"] == "EU"
    assert dry.kwargs["job_config"].query_parameters == config.query_parameters
    monkeypatch.setattr(bq, "client", lambda: pytest.fail("local report must not call BigQuery"))
    with catalog.connect() as con:
        assert catalog.workload_report(con) == result
        assert catalog.workload_report(con, result["report_id"]) == result


@pytest.mark.parametrize("kwargs", [
    {"location": "US`; DELETE FROM x"}, {"location": "region-us"}, {"project": "p`"},
    {"hours": 0}, {"hours": 4321}, {"hours": True}, {"limit": 101},
    {"scope": "organization"}, {"rank_by": "bytes DESC;"},
    {"max_bytes_billed": 1}, {"max_bytes_billed": workload.MAX_BYTES + 1},
    {"min_billed_bytes": -1}, {"min_slot_ms": -1}, {"allow_billed": "true"},
])
def test_bad_inputs_fail_before_bigquery(kwargs):
    with pytest.raises(SoundingError):
        bq.query_opportunities(**({"location": "US"} | kwargs))


@pytest.mark.parametrize("kind", ["budget", "receipt"])
def test_execution_blocked_without_safe_preview(monkeypatch, kind):
    c, preview, _ = monitor_client(monkeypatch, size=workload.MAX_BYTES if kind == "budget" else 10**7)
    if kind == "receipt":
        preview._properties["statistics"]["query"]["totalBytesBilled"] = "1"
    result = bq.query_opportunities("EU", allow_billed=True)
    assert result["status"] == "blocked"
    assert c.query.call_count == 1
    assert "opportunities" not in result


@pytest.mark.parametrize("size,accuracy", [(0, "PRECISE"), (1000, None), (1000, "LOWER_BOUND")])
def test_unknown_preview_remains_unknown_and_execution_keeps_cap(monkeypatch, size, accuracy):
    c, _, _ = monitor_client(monkeypatch, size=size, accuracy=accuracy)
    result = bq.query_opportunities("EU", allow_billed=True)
    assert result["estimated_bytes"] is None and result["estimate_status"] == "unknown"
    assert result["within_byte_cap"] is None
    assert c.query.call_args.kwargs["job_config"].maximum_bytes_billed == workload.DEFAULT_MAX_BYTES


def test_failed_execution_keeps_job_and_unknown_billing_without_retry(monkeypatch):
    c, _, paid = monitor_client(monkeypatch)
    paid.result.side_effect = BadRequest("private SQL and credentials")
    paid._properties["statistics"]["query"] = {}
    result = bq.query_opportunities("EU", allow_billed=True)
    assert result["status"] == "failed" and result["total_candidates"] is None
    assert result["receipt"]["job_id"] == "scan" and result["receipt"]["billed_bytes"] is None
    assert "private" not in result["error"]
    assert c.query.call_count == 2


def test_save_failure_does_not_discard_paid_result(monkeypatch):
    monitor_client(monkeypatch)
    monkeypatch.setattr(catalog, "save_workload_report", Mock(side_effect=OSError("full")))
    result = bq.query_opportunities("EU", allow_billed=True)
    assert result["status"] == "complete" and result["report_id"] is None
    assert result["receipt"]["job_id"] == "scan" and result["opportunities"]
    assert any("could not be saved" in warning for warning in result["warnings"])


def test_empty_scope_is_not_a_zero_usage_claim(monkeypatch):
    monitor_client(monkeypatch, rows=[])
    result = bq.query_opportunities("EU", allow_billed=True)
    assert result["total_candidates"] == 0 and not result["truncated"]
    assert any("does not establish zero" in warning for warning in result["warnings"])


def test_unknown_metrics_capacity_and_insight_evidence():
    result = workload.opportunity(family(missing_billed_jobs=1, missing_processed_jobs=1,
        reservation_jobs=4, slot_contention_jobs=2, high_cardinality_join_jobs=1))
    assert result["billed_bytes"] is None and result["processed_bytes"] is None
    assert result["known_billed_bytes"] == 2**40 and result["slot_ms"] == 3_600_000
    assert result["usd_on_demand_equivalent"] is None
    signals = {f["signal"]: f for f in result["findings"]}
    assert signals["slot_contention"]["jobs"] == 2
    assert "does not prove" in signals["slot_contention"]["next_step"]
    assert signals["high_cardinality_join"]["jobs"] == 1
    assert len(result["warnings"]) == 2
    assert workload.opportunity(family(statement_type="CREATE_MODEL"))["usd_on_demand_equivalent"] is None


@pytest.mark.parametrize("scope", ["user", "project"])
@pytest.mark.parametrize("rank", ["billed_bytes", "slot_ms"])
def test_sql_scope_deduplication_pruning_and_group_before_limit(scope, rank):
    sql = workload.query_sql("my-project", "EU", scope, rank)
    assert f"`my-project`.`region-eu`.INFORMATION_SCHEMA.JOBS_BY_{scope.upper()}" in sql
    assert "creation_time >= @since AND creation_time < @until" in sql
    assert "statement_type != 'SCRIPT' OR statement_type IS NULL" in sql
    assert "state = 'DONE' AND error_result IS NULL" in sql
    assert "cache_hit = FALSE" in sql and "sounding_action" in sql
    assert "CONCAT('job:', job_id)" in sql
    assert sql.index("GROUP BY query_key") < sql.index("LIMIT @limit")
    assert f"ORDER BY known_{rank} DESC NULLS LAST" in sql
    assert "SELECT *" not in sql and "user_email" not in sql
    assert "parent_job_id IS NULL" not in sql  # child statements still count


def test_v3_upgrade_preserves_metadata_and_report_reads(tmp_path):
    path = tmp_path / "v3.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(catalog.SCHEMA_V2)
        con.execute(catalog.MIGRATIONS[2])
        con.execute("INSERT INTO snapshots VALUES (1, '2026-01-01', 'p.d')")
    with catalog.connect(path) as con:
        assert con.execute("SELECT count(*) FROM snapshots").fetchone()[0] == 1
        assert con.execute("SELECT version FROM catalog_meta").fetchone()[0] == 4
        with pytest.raises(SoundingError, match="No saved"):
            catalog.workload_report(con)


def test_saved_reports_are_distinct_and_exact_lookup_is_local():
    first = catalog.save_workload_report({"project": "first", "status": "complete"})
    second = catalog.save_workload_report({"project": "second", "status": "complete"})
    with catalog.connect() as con:
        assert catalog.workload_report(con) == second
        assert catalog.workload_report(con, first["report_id"]) == first
        with pytest.raises(SoundingError, match="No saved"):
            catalog.workload_report(con, "' OR 1=1 --")


def test_failed_report_schema_upgrade_rolls_back(tmp_path, monkeypatch):
    path = tmp_path / "v3.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(catalog.SCHEMA_V2)
        con.execute(catalog.MIGRATIONS[2])
    monkeypatch.setitem(catalog.MIGRATIONS, 3, catalog.MIGRATIONS[3] + "SELECT no_such_function();")
    with pytest.raises(duckdb.Error):
        with catalog.connect(path):
            pass
    with duckdb.connect(str(path), read_only=True) as con:
        assert con.execute("SELECT version FROM catalog_meta").fetchone()[0] == 3
        assert con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='workload_reports'").fetchone()[0] == 0


def test_job_inspection_uses_only_free_get_and_sql_is_opt_in(monkeypatch):
    c = Mock(project="p")
    c.get_job.return_value = bigquery.QueryJob.from_api_repr({
        "jobReference": {"projectId": "p", "jobId": "costly", "location": "US"},
        "configuration": {"query": {"query": "SELECT 'private'"}, "labels": {"dbt_model": "orders"}},
        "statistics": {"query": {"totalSlotMs": "42", "referencedTables": [
            {"projectId": "p", "datasetId": "d", "tableId": "orders"}],
            "performanceInsights": {"stagePerformanceStandaloneInsights": [{"stageId": "1", "slotContention": True}]},
            "queryPlan": [{"id": "1", "recordsRead": "2", "recordsWritten": "100",
                           "shuffleOutputBytesSpilled": "1000"}]}}, "status": {"state": "DONE"}}, c)
    monkeypatch.setattr(bq, "client", lambda: c)
    result = bq.inspect_query_job("costly", "US")
    assert result["receipt"]["free"] and result["sql"] is None
    assert result["billed_bytes"] is None and result["slot_ms"] == 42
    assert result["referenced_tables"] == ["p.d.orders"]
    assert result["labels"] == {"dbt_model": "orders"}
    assert {f["signal"] for f in result["findings"]} == {"slot_contention", "shuffle_spill", "row_expansion"}
    assert "private" not in json.dumps(result)
    assert bq.inspect_query_job("costly", "US", include_sql=True)["sql"] == "SELECT 'private'"
    c.get_job.assert_called_with("costly", project="p", location="US")
    c.query.assert_not_called()
