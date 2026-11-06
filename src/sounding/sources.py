"""Sources beyond BigQuery's own metadata (prototype). Free admin APIs only, over REST.

  builders(project, locations): what builds each table.
    - BigQuery Data Transfer API, transferConfigs.list: scheduled queries (their SQL and
      destination) and other transfers (their data source).
    - Dataform API: repositories.list, the newest compilation result, and its actions
      (each table's SELECT, dependencies, description and column descriptions).
  discover(scope): Cloud Asset Inventory searchAllResources, the data-holding resources
    across a project, folder or organization.

None of these calls is a BigQuery job, and none reads table data. Owner emails
(transfer configs' ownerInfo, userId) are dropped before anything is stored, the same
rule as dataset access lists.
"""

from __future__ import annotations

import datetime as dt
import re

import google.auth
from google.auth.transport.requests import AuthorizedSession

from . import catalog
from .errors import SoundingError, text
from .progress import Quiet

TRANSFER_API = "https://bigquerydatatransfer.googleapis.com/v1"
DATAFORM_API = "https://dataform.googleapis.com/v1"
ASSET_API = "https://cloudasset.googleapis.com/v1"
DEFAULT_TRANSFER_LOCATIONS = ("us", "eu")  # transfer configs live in the dataset's location
DATAFORM_ALL_LOCATIONS = "-"              # Dataform accepts locations/- for every region
PAGE_SIZE = 500
MAX_ASSETS = 20_000
MAX_DATAFORM_ACTIONS = 20_000
TRANSFER_DROP = {"ownerInfo", "userId", "emailPreferences"}
# Resource types that hold or move data. Cloud Asset rejects unknown types, so this list
# stays conservative; `sounding discover --type` overrides it.
DATA_ASSET_TYPES = (
    "bigquery.googleapis.com/Dataset", "bigquery.googleapis.com/Table",
    "storage.googleapis.com/Bucket", "spanner.googleapis.com/Database",
    "sqladmin.googleapis.com/Instance", "alloydb.googleapis.com/Cluster",
    "bigtableadmin.googleapis.com/Instance", "pubsub.googleapis.com/Topic",
    "firestore.googleapis.com/Database", "aiplatform.googleapis.com/Dataset",
    "aiplatform.googleapis.com/Model", "dataform.googleapis.com/Repository",
    "composer.googleapis.com/Environment", "looker.googleapis.com/Instance",
)
API_NAMES = {TRANSFER_API: "BigQuery Data Transfer API", DATAFORM_API: "Dataform API", ASSET_API: "Cloud Asset API"}
# DDL and DML that name the table a scheduled query writes, when it has no destination setting.
WRITE_TARGET = re.compile(
    r"\b(?:CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?|INSERT\s+(?:INTO\s+)?|MERGE\s+(?:INTO\s+)?)\s*`?([\w-]+(?:\.[\w-]+){1,2})`?",
    re.IGNORECASE)


def session() -> AuthorizedSession:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)


class ApiUnavailable(SoundingError):
    status = 403


def _get(s, url: str, params: dict | None = None) -> dict:
    r = s.get(url, params=params, timeout=60)
    if r.status_code == 200:
        return r.json()
    api = next((name for base, name in API_NAMES.items() if url.startswith(base)), "The API")
    try:
        reason = " " + r.json()["error"]["status"]
    except Exception:
        reason = ""
    if r.status_code == 403:
        raise ApiUnavailable(f"{api} is not enabled in this project, or you lack permission (HTTP 403{reason}).")
    raise SoundingError(f"{api} returned HTTP {r.status_code}{reason}.")


def _pages(s, url: str, key: str, params: dict | None = None, limit: int = MAX_ASSETS):
    params = dict(params or {})
    count = 0
    while True:
        body = _get(s, url, params)
        for item in body.get(key, []):
            count += 1
            if count > limit:
                raise SoundingError(f"More than {limit} results; narrow the scope.")
            yield item
        token = body.get("nextPageToken")
        if not token:
            return
        params["pageToken"] = token


def _time(value: str | None) -> dt.datetime | None:
    return None if not value else dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def _qualify(name: str, project: str, dataset: str | None = None) -> str:
    parts = name.split(".")
    if len(parts) == 3:
        return name
    if len(parts) == 2:
        return f"{project}.{name}"
    return f"{project}.{dataset}.{name}" if dataset else name


def transfer_record(r: dict, project: str) -> dict:
    """One transfer config. Scheduled queries carry SQL; other transfers name their data source."""
    params = r.get("params") or {}
    source = r.get("dataSourceId")
    sql = params.get("query") if source == "scheduled_query" else None
    dataset, template = r.get("destinationDatasetId"), params.get("destination_table_name_template")
    target, target_from = None, None
    if dataset and template and "{" not in template:
        target, target_from = f"{project}.{dataset}.{template}", "destination setting"
    elif dataset and template:
        target, target_from = f"{project}.{dataset}.{template}", "destination template (one table per run)"
    elif sql and (m := WRITE_TARGET.search(sql)):
        target, target_from = _qualify(m.group(1), project, dataset), "parsed from the SQL"
    return {"kind": "scheduled_query" if source == "scheduled_query" else f"transfer:{source}",
            "name": r["name"], "display_name": r.get("displayName"), "target": target,
            "target_from": target_from, "sql": sql, "dependencies": None, "schedule": r.get("schedule"),
            "state": r.get("state"), "description": None, "columns": None, "location": r["name"].split("/")[3],
            "updated": _time(r.get("updateTime")),
            "resource": {k: v for k, v in r.items() if k not in TRANSFER_DROP}}


def _target(t: dict | None) -> str | None:
    return f"{t['database']}.{t['schema']}.{t['name']}" if t and t.get("database") else None


def _columns(descriptor: dict | None) -> list[dict] | None:
    cols = [{"path": ".".join(c.get("path", [])), "description": c.get("description")}
            for c in (descriptor or {}).get("columns", [])]
    return cols or None


def dataform_record(action: dict, repository: str, location: str) -> dict | None:
    """One compiled Dataform action: a table, view, incremental table, operation or assertion."""
    target = _target(action.get("canonicalTarget") or action.get("target"))
    for kind in ("relation", "operations", "assertion"):
        if body := action.get(kind):
            break
    else:
        return None  # declarations only describe sources; nothing is built
    if kind == "relation":
        label, sql = (body.get("relationType") or "TABLE").lower(), body.get("selectQuery")
    elif kind == "operations":
        label, sql = "operations", ";\n".join(body.get("queries", []))
    else:
        label, sql = "assertion", body.get("selectQuery")
    descriptor = body.get("relationDescriptor") or {}
    return {"kind": f"dataform_{label}", "name": f"{repository}/{action.get('filePath', '')}#{target}",
            "display_name": action.get("filePath"), "target": target, "target_from": "Dataform compilation",
            "sql": sql, "dependencies": [d for d in map(_target, body.get("dependencyTargets", [])) if d],
            "schedule": None, "state": "disabled" if body.get("disabled") else None,
            "description": descriptor.get("description"), "columns": _columns(descriptor),
            "location": location, "updated": None,
            "resource": {k: v for k, v in body.items() if k not in ("selectQuery", "queries")}}


def _newest_compilation(s, repository: str) -> dict | None:
    results = list(_pages(s, f"{DATAFORM_API}/{repository}/compilationResults", "compilationResults",
                          {"pageSize": 50, "orderBy": "create_time desc"}, limit=50))
    return max(results, key=lambda r: r.get("createTime", ""), default=None)


def builders(project: str, transfer_locations=DEFAULT_TRANSFER_LOCATIONS,
             dataform_location: str = DATAFORM_ALL_LOCATIONS, progress: Quiet | None = None) -> dict:
    """Harvest scheduled queries, transfers and Dataform actions for one project.

    Each (source, location) is saved as its own run, so a disabled API or a denied
    location leaves the others intact. Returns counts, per-source errors, and a receipt.
    """
    text(project, "project")
    progress = progress or Quiet()
    with catalog.connect():
        pass
    s = session()
    counts, errors = {}, {}
    progress.add(len(transfer_locations) + 1)
    for loc in transfer_locations:
        scope = f"{project}/{loc}"
        try:
            rows = [transfer_record(r, project) for r in _pages(
                s, f"{TRANSFER_API}/projects/{project}/locations/{loc}/transferConfigs", "transferConfigs",
                {"pageSize": 1000})]
            catalog.save_source_run("transfers", scope, rows)
            counts[f"transfers {loc}"] = len(rows)
            progress.log(f"transfers {scope}: {len(rows)} configs")
        except SoundingError as error:
            errors[f"transfers {loc}"] = str(error)
            progress.log(f"transfers {scope}: skipped ({error})")
        progress.advance()
    try:
        rows = []
        repos = list(_pages(s, f"{DATAFORM_API}/projects/{project}/locations/{dataform_location}/repositories",
                            "repositories"))
        progress.add(len(repos))
        for repo in repos:
            location = repo["name"].split("/")[3]
            if compiled := _newest_compilation(s, repo["name"]):
                for action in _pages(s, f"{DATAFORM_API}/{compiled['name']}:query", "compilationResultActions",
                                     {"pageSize": 1000}, limit=MAX_DATAFORM_ACTIONS):
                    if record := dataform_record(action, repo["name"], location):
                        rows.append(record)
            progress.advance()
        catalog.save_source_run("dataform", f"{project}/{dataform_location}", rows)
        counts["dataform"] = len(rows)
        progress.log(f"dataform {project}: {len(repos)} repositories, {len(rows)} actions")
    except SoundingError as error:
        errors["dataform"] = str(error)
        progress.log(f"dataform {project}: skipped ({error})")
    progress.advance()
    progress.close()
    if not counts:
        raise SoundingError("No builder source could be read: " + "; ".join(f"{k}: {v}" for k, v in errors.items()))
    return {"counts": counts, "errors": errors, "receipt": admin_receipt(
        "bigquerydatatransfer transferConfigs.list", "dataform repositories.list",
        "dataform compilationResults.list", "dataform compilationResults.query")}


def asset_record(r: dict) -> dict:
    return {"name": r["name"], "asset_type": r.get("assetType"), "project": r.get("project"),
            "display_name": r.get("displayName"), "description": r.get("description"),
            "location": r.get("location"), "labels": r.get("labels") or {},
            "created": _time(r.get("createTime")), "updated": _time(r.get("updateTime")), "state": r.get("state")}


def discover(scope: str, asset_types=DATA_ASSET_TYPES) -> dict:
    """Cloud Asset Inventory: every data-holding resource under a project, folder or organization."""
    text(scope, "scope")
    if not re.fullmatch(r"(projects|folders|organizations)/[\w.:-]+", scope):
        raise SoundingError("Scope must be projects/ID, folders/NUMBER or organizations/NUMBER.")
    with catalog.connect():
        pass
    # Only the fields below are requested; additionalAttributes and IAM policies are never read.
    fields = "name,assetType,project,displayName,description,location,labels,createTime,updateTime,state"
    rows = [asset_record(r) for r in _pages(
        session(), f"{ASSET_API}/{scope}:searchAllResources", "results",
        {"assetTypes": list(asset_types), "pageSize": PAGE_SIZE, "readMask": fields})]
    catalog.save_source_run("assets", scope, rows)
    by_type = {}
    for r in rows:
        by_type[r["asset_type"]] = by_type.get(r["asset_type"], 0) + 1
    return {"scope": scope, "assets": len(rows), "by_type": dict(sorted(by_type.items())),
            "receipt": admin_receipt("cloudasset searchAllResources")}


def admin_receipt(*calls: str) -> dict:
    return {"call": ", ".join(calls), "jobs_created": 0, "free": True,
            "source": "Not a job: admin API list and search calls are not billed and read no table data. "
                      "`sounding cost-report` shows any job a project ran."}
