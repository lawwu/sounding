import pytest

from sounding import catalog, sources
from sounding.errors import SoundingError
from conftest import column_row, table_row

P = "proj"


class Response:
    def __init__(self, status, body):
        self.status_code, self.body = status, body

    def json(self):
        return self.body


class FakeSession:
    """Answers GETs from a {url: [page, ...]} map; a page is a body or (status, body)."""

    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        pages = self.routes.get(url)
        if pages is None:
            return Response(404, {"error": {"status": "NOT_FOUND"}})
        index = 0
        if token := (params or {}).get("pageToken"):
            index = int(token)
        page = pages[index]
        status, body = page if isinstance(page, tuple) else (200, page)
        if status == 200 and index + 1 < len(pages):
            body = dict(body, nextPageToken=str(index + 1))
        return Response(status, body)


def disabled(api):
    return [(403, {"error": {"status": "PERMISSION_DENIED", "message": f"{api} has not been used in project 123"}})]


def scheduled(name, **overrides):
    return {"name": f"projects/123/locations/us/transferConfigs/{name}", "displayName": f"nightly {name}",
            "dataSourceId": "scheduled_query", "schedule": "every 24 hours", "state": "SUCCEEDED",
            "updateTime": "2026-10-01T08:00:00Z", "ownerInfo": {"email": "owner@example.com"},
            "userId": "42", **overrides}


def target(name, dataset="mart"):
    return {"database": P, "schema": dataset, "name": name}


def test_transfer_record_targets_and_dropped_owner():
    with_dest = sources.transfer_record(scheduled("a", destinationDatasetId="mart",
        params={"query": "SELECT 1", "destination_table_name_template": "daily"}), P)
    assert with_dest["target"] == "proj.mart.daily" and with_dest["target_from"] == "destination setting"
    assert with_dest["kind"] == "scheduled_query" and with_dest["sql"] == "SELECT 1"
    assert "ownerInfo" not in with_dest["resource"] and "userId" not in with_dest["resource"]
    assert "owner@example.com" not in repr(with_dest)
    templated = sources.transfer_record(scheduled("b", destinationDatasetId="mart",
        params={"query": "SELECT 1", "destination_table_name_template": "daily_{run_date}"}), P)
    assert templated["target_from"].startswith("destination template")
    ddl = sources.transfer_record(scheduled("c", params={
        "query": "-- refresh\nCREATE OR REPLACE TABLE `mart.orders` AS SELECT * FROM raw.orders"}), P)
    assert ddl["target"] == "proj.mart.orders" and ddl["target_from"] == "parsed from the SQL"
    merge = sources.transfer_record(scheduled("d", params={"query": "MERGE other.mart.t USING s ON TRUE"}), P)
    assert merge["target"] == "other.mart.t"
    ads = sources.transfer_record({"name": "projects/1/locations/eu/transferConfigs/x",
                                   "dataSourceId": "google_ads", "destinationDatasetId": "ads"}, P)
    assert ads["kind"] == "transfer:google_ads" and ads["sql"] is None and ads["target"] is None
    assert ads["location"] == "eu"


def test_dataform_record_kinds():
    relation = sources.dataform_record({"canonicalTarget": target("orders"), "filePath": "definitions/orders.sqlx",
        "relation": {"relationType": "INCREMENTAL_TABLE", "selectQuery": "SELECT * FROM `proj.raw.orders`",
                     "dependencyTargets": [target("orders", "raw")],
                     "relationDescriptor": {"description": "One row per order",
                                            "columns": [{"path": ["id"], "description": "order id"},
                                                        {"path": ["ship", "zip"], "description": "zip"}]}}},
        "projects/p/locations/us-central1/repositories/r", "us-central1")
    assert relation["kind"] == "dataform_incremental_table" and relation["target"] == "proj.mart.orders"
    assert relation["dependencies"] == ["proj.raw.orders"] and relation["description"] == "One row per order"
    assert relation["columns"] == [{"path": "id", "description": "order id"}, {"path": "ship.zip", "description": "zip"}]
    assert "selectQuery" not in relation["resource"]
    ops = sources.dataform_record({"target": target("log"), "operations": {"queries": ["DELETE x", "INSERT y"]}}, "r", "l")
    assert ops["kind"] == "dataform_operations" and ops["sql"] == "DELETE x;\nINSERT y"
    check = sources.dataform_record({"target": target("orders_assertion", "checks"),
        "assertion": {"selectQuery": "SELECT ...", "dependencyTargets": [target("orders")]}}, "r", "l")
    assert check["kind"] == "dataform_assertion" and check["dependencies"] == ["proj.mart.orders"]
    assert sources.dataform_record({"target": target("raw"), "declaration": {}}, "r", "l") is None


def builder_routes(eu=None):
    repo = "projects/proj/locations/us-central1/repositories/analytics"
    compiled = f"{repo}/compilationResults/c2"
    return {
        f"{sources.TRANSFER_API}/projects/proj/locations/us/transferConfigs": [
            {"transferConfigs": [scheduled("a", destinationDatasetId="mart",
                params={"query": "SELECT * FROM `proj.mart.orders`", "destination_table_name_template": "daily"})]}],
        f"{sources.TRANSFER_API}/projects/proj/locations/eu/transferConfigs": eu or disabled("BigQuery Data Transfer API"),
        f"{sources.DATAFORM_API}/projects/proj/locations/-/repositories": [{"repositories": [{"name": repo}]}],
        f"{sources.DATAFORM_API}/{repo}/compilationResults": [{"compilationResults": [
            {"name": f"{repo}/compilationResults/c1", "createTime": "2026-10-01T00:00:00Z"},
            {"name": compiled, "createTime": "2026-10-02T00:00:00Z"}]}],
        f"{sources.DATAFORM_API}/{compiled}:query": [  # two pages
            {"compilationResultActions": [{"canonicalTarget": target("orders"), "filePath": "definitions/orders.sqlx",
                "relation": {"relationType": "TABLE", "selectQuery": "SELECT * FROM raw.orders",
                             "dependencyTargets": [target("orders", "raw")]}}]},
            {"compilationResultActions": [{"target": target("orders", "raw"), "declaration": {}}]}],
    }


def test_builders_harvest_lineage_and_partial_failure(monkeypatch, seed):
    fake = FakeSession(builder_routes())
    monkeypatch.setattr(sources, "session", lambda: fake)
    result = sources.builders(P)
    assert result["counts"] == {"transfers us": 1, "dataform": 1}
    assert "not enabled" in result["errors"]["transfers eu"] and result["receipt"]["jobs_created"] == 0
    assert any(url.endswith("/compilationResults/c2:query") for url, _ in fake.calls)  # newest compilation
    seed([table_row("proj.mart.orders")], [column_row("id", "proj.mart.orders")], "proj.mart")
    with catalog.connect() as con:
        t = catalog.describe(con, "proj.mart.orders")
        assert [b["kind"] for b in t["built_by"]] == ["dataform_table"]
        assert t["built_by"][0]["sql"] == "SELECT * FROM raw.orders"
        assert t["built_by"][0]["dependencies"] == ["proj.raw.orders"]
        assert [b["kind"] for b in t["read_by"]] == []  # the scheduled query reads it via SQL only
        assert catalog.search_builders(con, "raw.orders")["items"][0]["target"] == "proj.mart.orders"
        assert len(catalog.search_builders(con)["items"]) == 2
        raw = catalog.lineage(con, "proj.raw.orders")
        assert raw["built_by"] == [] and [b["target"] for b in raw["read_by"]] == ["proj.mart.orders"]
        assert "sql" not in raw["read_by"][0]


def test_rerun_replaces_only_its_own_scope(monkeypatch):
    monkeypatch.setattr(sources, "session", lambda: FakeSession(builder_routes()))
    sources.builders(P)
    routes = builder_routes(eu=[{"transferConfigs": []}])
    routes[f"{sources.TRANSFER_API}/projects/proj/locations/us/transferConfigs"] = [{"transferConfigs": []}]
    monkeypatch.setattr(sources, "session", lambda: FakeSession(routes))
    assert sources.builders(P)["counts"] == {"transfers us": 0, "transfers eu": 0, "dataform": 1}
    with catalog.connect() as con:
        assert [b["kind"] for b in catalog.search_builders(con)["items"]] == ["dataform_table"]


def test_builders_fail_when_every_source_fails(monkeypatch):
    routes = {url: disabled("x") for url in builder_routes()}
    monkeypatch.setattr(sources, "session", lambda: FakeSession(routes))
    with pytest.raises(SoundingError, match="No builder source"):
        sources.builders(P)


def asset(name, kind, **overrides):
    return {"name": f"//{kind.split('/')[0]}/projects/proj/{name}", "assetType": kind, "project": "projects/123",
            "displayName": name, "location": "us", "createTime": "2026-01-01T00:00:00Z", **overrides}


def test_discover_pages_stores_and_searches(monkeypatch):
    url = f"{sources.ASSET_API}/projects/proj:searchAllResources"
    fake = FakeSession({url: [
        {"results": [asset("raw-landing", "storage.googleapis.com/Bucket", labels={"team": "data"}),
                     asset("events", "pubsub.googleapis.com/Topic", description="clickstream")]},
        {"results": [asset("orders-db", "spanner.googleapis.com/Database",
                           additionalAttributes={"owner": "someone@example.com"})]}]})
    monkeypatch.setattr(sources, "session", lambda: fake)
    result = sources.discover("projects/proj")
    assert result["assets"] == 3 and result["by_type"]["storage.googleapis.com/Bucket"] == 1
    sent = fake.calls[0][1]
    assert sent["assetTypes"] == list(sources.DATA_ASSET_TYPES) and "additionalAttributes" not in sent["readMask"]
    with catalog.connect() as con:
        assert [a["display_name"] for a in catalog.search_assets(con, asset_type="bucket")["items"]] == ["raw-landing"]
        hit = catalog.search_assets(con, "clickstream")["items"][0]
        assert hit["asset_type"] == "pubsub.googleapis.com/Topic"
        everything = catalog.search_assets(con)["items"]
        assert len(everything) == 3 and "someone@example.com" not in repr(everything)
        assert everything[0]["labels"] in ({}, {"team": "data"}) and everything[0]["updated"] is None


@pytest.mark.parametrize("scope", ["proj", "projects/", "billingAccounts/1", "projects/a b"])
def test_discover_rejects_bad_scopes(scope):
    with pytest.raises(SoundingError, match="Scope must be"):
        sources.discover(scope)


def test_disabled_asset_api_is_a_clear_error(monkeypatch):
    url = f"{sources.ASSET_API}/projects/proj:searchAllResources"
    monkeypatch.setattr(sources, "session", lambda: FakeSession({url: disabled("Cloud Asset API")}))
    with pytest.raises(sources.ApiUnavailable, match="Cloud Asset API is not enabled"):
        sources.discover("projects/proj")


def test_newest_compilation_reads_one_page_only():
    repo = "projects/p/locations/l/repositories/r"
    page = lambda start: {"compilationResults": [
        {"name": f"{repo}/compilationResults/c{i}", "createTime": f"2026-10-{i % 28 + 1:02}T00:00:00Z"}
        for i in range(start, start + 50)]}
    fake = FakeSession({f"{sources.DATAFORM_API}/{repo}/compilationResults": [page(0), page(50)]})
    assert sources._newest_compilation(fake, repo)["name"].endswith("/c27")
    assert len(fake.calls) == 1


def test_dataform_target_uses_compiled_name_with_overrides():
    record = sources.dataform_record({"target": target("orders", "mart_prod"),
                                      "canonicalTarget": target("orders", "mart"),
                                      "relation": {"selectQuery": "SELECT 1"}}, "r", "l")
    assert record["target"] == "proj.mart_prod.orders"


def test_written_table_ignores_comments_strings_and_temp_tables():
    w = sources.written_table
    assert w("-- replaces INSERT INTO old.report\nCREATE OR REPLACE TABLE mart.report AS SELECT 1") == "mart.report"
    assert w("CREATE TABLE staging.tmp AS SELECT 1; INSERT INTO mart.final SELECT * FROM staging.tmp") == "mart.final"
    assert w("CREATE OR REPLACE TABLE `proj`.`ds`.`t` AS SELECT 1") == "proj.ds.t"
    assert w("CREATE TEMP TABLE x AS SELECT 1; MERGE `p.d.t` USING x ON TRUE") == "p.d.t"
    assert w("SELECT 'INSERT INTO a.b' AS s /* MERGE c.d */") is None
    assert w("INSERT tbl VALUES (1)") is None


def test_overlapping_scopes_keep_each_resource_once(monkeypatch):
    monkeypatch.setattr(sources, "session", lambda: FakeSession(builder_routes()))
    sources.builders(P)
    regional = builder_routes()
    regional[f"{sources.DATAFORM_API}/projects/proj/locations/us-central1/repositories"] = \
        regional.pop(f"{sources.DATAFORM_API}/projects/proj/locations/-/repositories")
    monkeypatch.setattr(sources, "session", lambda: FakeSession(regional))
    sources.builders(P, dataform_location="us-central1")
    url = lambda scope: f"{sources.ASSET_API}/{scope}:searchAllResources"
    bucket = asset("raw-landing", "storage.googleapis.com/Bucket")
    for scope in ("organizations/1", "projects/proj"):
        monkeypatch.setattr(sources, "session", lambda: FakeSession({url(scope): [{"results": [bucket]}]}))
        sources.discover(scope)
    with catalog.connect() as con:
        assert len(catalog.lineage(con, "proj.mart.orders")["built_by"]) == 1
        assert len(catalog.search_assets(con)["items"]) == 1


def test_discover_keeps_what_it_found_past_the_cap(monkeypatch):
    monkeypatch.setattr(sources, "MAX_ASSETS", 2)
    url = f"{sources.ASSET_API}/projects/proj:searchAllResources"
    pages = [{"results": [asset(f"b{i}", "storage.googleapis.com/Bucket")]} for i in range(3)]
    monkeypatch.setattr(sources, "session", lambda: FakeSession({url: pages}))
    result = sources.discover("projects/proj")
    assert result["assets"] == 2 and result["truncated"] and "Stopped at 2" in result["warnings"][0]
    with catalog.connect() as con:
        assert len(catalog.search_assets(con)["items"]) == 2


def test_prototype_tables_leave_the_schema_version_alone(monkeypatch):
    monkeypatch.setattr(sources, "session", lambda: FakeSession(builder_routes()))
    with catalog.connect() as con:  # a fresh catalog has no source tables, and reads still work
        assert catalog.lineage(con, "p.d.t") == {"built_by": [], "read_by": []}
        assert catalog.search_builders(con)["items"] == [] == catalog.search_assets(con)["items"]
    sources.builders(P)
    with catalog.connect() as con:
        assert con.execute("SELECT version FROM catalog_meta").fetchone()[0] == catalog.SCHEMA_VERSION
