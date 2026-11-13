import datetime as dt
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import Forbidden
from google.cloud import bigquery

from sounding import bq, catalog, recommendations, refresh
from test_bq import fake_client, remote_table, Recorder


def model(name="m"):
    return bigquery.Model.from_api_repr({"modelReference": {"projectId": "p", "datasetId": "d", "modelId": name},
                                       "modelType": "LINEAR_REGRESSION", "description": "Revenue model",
                                       "featureColumns": [{"name": "amount", "type": {"typeKind": "FLOAT64"}}],
                                       "labelColumns": [{"name": "outcome", "type": {"typeKind": "FLOAT64"}}]})


def test_models_and_policy_pagination_saved_with_dataset(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_models.return_value = [model()]
    c.get_model.return_value = model()
    c._call_api.side_effect = [{"rowAccessPolicies": [{"rowAccessPolicyReference": {"policyId": "region"},
        "filterPredicate": "region = 'US'", "grantees": ["user:private@example.com"]}], "nextPageToken": "more"},
        {"rowAccessPolicies": [{"rowAccessPolicyReference": {"policyId": "tenant"}}]}]
    receipt = bq.harvest(["p.d"])
    with catalog.connect() as con:
        d, t, m = catalog.describe_dataset(con, "p.d"), catalog.describe(con, "p.d.t"), catalog.describe_model(con, "p.d.m")
        assert catalog.browse(con)["projects"][0]["models"] == 1
    assert d["models_status"] == "harvested" and len(d["models"]) == 1
    assert m["feature_columns"] == [{"name": "amount", "type": "FLOAT64"}]
    assert t["row_access_policies_status"] == "harvested" and len(t["row_access_policies"]) == 2
    assert "grantees" not in t["row_access_policies"][0]
    assert c._call_api.call_args.kwargs["query_params"]["pageToken"] == "more"
    assert all(call.kwargs["method"] == "GET" for call in c._call_api.call_args_list)
    assert receipt["coverage"]["p.d"]["status"] == "complete"
    assert t["preview_status"] == "not_requested" and t["preview_rows"] is None
    c.list_rows.assert_not_called()
    c.query.assert_not_called()


def test_optional_denials_keep_metadata_and_report_partial(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_models.side_effect = Forbidden("sensitive backend message")
    c._call_api.side_effect = Forbidden("private policy")
    result = refresh.Job(["p.d"]).run()
    assert result["status"] == "partial" and result["progress"]["datasets_done"] == 1
    with catalog.connect() as con:
        d, t = catalog.describe_dataset(con, "p.d"), catalog.describe(con, "p.d.t")
    assert d["models_status"] == "denied" and d["models"] == []
    assert t["row_access_policies_status"] == "denied" and t["row_access_policies"] is None
    assert "sensitive backend" not in json.dumps(result)
    assert "Previous snapshot retained" not in result["datasets"][0]["message"]


def test_model_get_denial_preserves_inventory(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_models.return_value = [model()]
    c.get_model.side_effect = Forbidden("denied")
    bq.harvest(["p.d"])
    with catalog.connect() as con:
        d = catalog.describe_dataset(con, "p.d")
        assert d["models_status"] == "partial"
        assert d["models"][0]["metadata_status"] == "denied"


def test_samples_are_opt_in_and_metadata_is_available_before_download_finishes(monkeypatch):
    c = fake_client(monkeypatch)
    entered, release = threading.Event(), threading.Event()
    def rows(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return [{"id": 1}]
    c.list_rows.side_effect = rows
    progress = Recorder()
    with ThreadPoolExecutor(1) as pool:
        task = pool.submit(bq.harvest, ["p.d"], progress, include_preview_rows=True)
        try:
            assert entered.wait(5)
            with catalog.connect() as con:
                assert catalog.describe(con, "p.d.t")["preview_status"] == "pending"
        finally:
            release.set()
        task.result()
    with catalog.connect() as con:
        t = catalog.describe(con, "p.d.t")
    assert t["preview_rows"] == [{"id": 1}] and t["preview_sampled_at"]
    assert progress.total == progress.done
    assert c.list_rows.call_args.kwargs["max_results"] == 10
    c.query.assert_not_called()


def test_sample_byte_cap_and_denial_are_explicit(monkeypatch):
    c = fake_client(monkeypatch)
    c.list_rows.return_value = [{"id": 1}, {"text": "é" * bq.SAMPLE_BYTES}]
    result = bq.table_sample(c, remote_table())
    assert result["preview_rows"] == [{"id": 1}] and "shortened" in result["preview_message"]
    assert len(json.dumps(result["preview_rows"]).encode()) <= bq.SAMPLE_BYTES
    c.list_rows.side_effect = Forbidden("no data access")
    bq.harvest(["p.d"], include_preview_rows=True)
    with catalog.connect() as con:
        t = catalog.describe(con, "p.d.t")
    assert t["preview_status"] == "denied" and t["preview_rows"] is None
    c.list_rows.reset_mock()
    assert bq.table_sample(c, remote_table(type="VIEW"))["preview_status"] == "not_applicable"
    c.list_rows.assert_not_called()


def test_older_sample_cannot_overwrite_a_new_snapshot(seed):
    first = seed()
    second = seed()
    assert second != first
    assert not catalog.save_sample("p.d.t", first, {"preview_rows": [{"old": True}], "preview_status": "harvested",
                                                   "preview_message": None, "preview_sampled_at": dt.datetime.now(dt.UTC)})
    with catalog.connect() as con:
        assert catalog.describe(con, "p.d.t")["preview_rows"] is None


def response(body, status=200):
    return Mock(status_code=status, json=lambda: body)


def test_recommender_pagination_deduplication_scoping_and_local_reads(monkeypatch):
    c = fake_client(monkeypatch)
    item = {"name": "r1", "description": "Cluster by city", "stateInfo": {"state": "ACTIVE"},
            "content": {"overview": {"clusterColumns": ["city"], "projectId": "p", "datasetId": "d", "tableId": "t"},
                        "operationGroups": [{"operations": [{"resource": "//bigquery.googleapis.com/projects/p/datasets/d/tables/t"}]}]}}
    c._http.get.side_effect = [response({"recommendations": [item], "nextPageToken": "next"}),
                               response({"recommendations": [item | {"name": "r2"}]}), response({"insights": []})]
    bq.harvest(["p.d"])
    recommendations.refresh(c, "p", "US")
    assert c._http.get.call_count == 3
    assert c._http.get.call_args_list[1].kwargs["params"]["pageToken"] == "next"
    with catalog.connect() as con:
        r = catalog.describe(con, "p.d.t")["recommendations"]
        assert len(next(s for s in r["scopes"] if s["kind"] == "recommendations")["items"]) == 2
        assert not any(s["items"] for s in catalog.recommendations(con, "p", table="p.d.other")["scopes"])
        assert catalog.recommendations(con, "other")["scopes"] == []
    assert c._http.get.call_count == 3  # browse does not contact Google
    assert all(call.args[0].startswith("https://recommender.googleapis.com/v1/projects/p/locations/us/") for call in c._http.get.call_args_list)


def test_failed_recommender_refresh_preserves_last_good_data(monkeypatch):
    c = fake_client(monkeypatch)
    c._http.get.side_effect = [response({"recommendations": [{"name": "saved"}]}), response({"insights": []})]
    with catalog.connect():
        pass
    recommendations.refresh(c, "p", "US")
    with catalog.connect(write=True) as con:
        con.execute("UPDATE recommendation_cache SET checked_at = checked_at - INTERVAL 2 DAY")
    c._http.get.side_effect = [response({"error": {"details": [{"reason": "SERVICE_DISABLED"}]}}, 403), response({}, 429)]
    scopes = recommendations.refresh(c, "p", "US")
    rec = next(s for s in scopes if s["kind"] == "recommendations")
    assert rec["status"] == "disabled" and rec["stale"] and rec["items"][0]["name"] == "saved"
    assert next(s for s in scopes if s["kind"] == "insights")["status"] == "quota"
    recommendations.refresh(c, "p", "US")
    assert c._http.get.call_count == 4


def test_refresh_remembers_empty_project(monkeypatch):
    monkeypatch.setattr(bq, "project_datasets", lambda p: {"datasets": [], "unreachable": [], "receipt": {}})
    assert refresh.Job(projects=["empty"]).run()["status"] == "complete"
    with catalog.connect() as con:
        project = catalog.browse(con)["projects"][0]
    assert project["project"] == "empty" and project["datasets"] == []
